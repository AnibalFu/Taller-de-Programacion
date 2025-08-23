use crate::call_llm::{LlmClient, RealLlm};
use crate::constantes::*;
use crate::llm_error::LLMError;
use crate::redis_connect::RespConn;
use common::thread_pool::ThreadPool;
use json::json::ExpresionJson;
use json::json_parser::parser::obtener_json_raw;
use logger::logger::Logger;
use redis_client::protocol::protocol_resp::{
    resp_api_command_write, resp_client_command_read, resp_client_command_write,
};
use redis_client::tipos_datos::traits::TipoDatoRedis;
use std::io::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{JoinHandle, spawn};
use std::time::Duration;

/// Tipo concreto del microservicio en producción:
/// - Usa `TcpStream` como conexión RESP (hacia Redis).
/// - Usa `RealLlm` como cliente LLM (por ejemplo, Gemini).
pub type RealService = MicroserviceLLM<TcpStream, RealLlm>;

impl RealService {
    /// Crea una nueva instancia del microservicio LLM productivo.
    ///
    /// # Parámetros
    /// - `worker_amount`: cantidad de workers para el pool de hilos.
    /// - `channel_request`: nombre del canal de Redis donde se reciben las peticiones comunes.
    /// - `channel_global_request`: canal usado para pedir contenido completo de documentos.
    /// - `redis_addr`: dirección IP:puerto del servidor Redis.
    /// - `logger_path`: ruta al archivo de log.
    /// - `api_key`: clave de la API del proveedor LLM (p.ej., OpenAI).
    ///
    /// # Retorna
    /// Un `Result` que contiene un `RealService` en caso de éxito, o un error si falla
    /// la conexión a Redis.
    pub fn new(
        worker_amount: usize,
        channel_request: String,
        channel_global_request: String,
        redis_addr: SocketAddr,
        logger_path: &str,
        api_key: String,
    ) -> Result<RealService, Error> {
        Ok(MicroserviceLLM::new_internal(
            channel_request,
            channel_global_request,
            TcpStream::connect(redis_addr)?, //  C = TcpStream
            RealLlm::new(api_key),           //  L = RealLlm
            ThreadPool::new(worker_amount),
            Logger::new(logger_path),
        ))
    }
}

/// Estructura genérica del microservicio LLM.
///
/// Este microservicio:
/// - Se conecta a un backend Redis mediante una conexión RESP (`TcpStream` o mockeado).
/// - Suscribe a un canal de entrada para recibir prompts o solicitudes globales.
/// - Ejecuta tareas de inferencia con un cliente LLM provisto (p.ej., OpenAI, Gemini, etc).
/// - Publica las respuestas en Redis mediante un canal de salida.
///
/// # Parámetros de tipo
/// - `C`: tipo de conexión a Redis (implementa `RespConn`). Ej: `TcpStream`, `MemConn`.
/// - `L`: cliente que puede responder prompts (implementa `LlmClient`).
pub struct MicroserviceLLM<C, L>
where
    C: RespConn,
    L: LlmClient,
{
    /// Conexión al backend Redis.
    conn: C,

    /// Cliente de LLM que maneja la generación de respuestas.
    llm: L,

    /// Canal principal para recibir solicitudes de inferencia.
    channel_request: String,

    /// Canal para recibir y manejar solicitudes globales (requieren documento completo).
    channel_global_request: String,

    /// Pool de hilos para ejecutar inferencias en paralelo.
    thread_pool: Arc<ThreadPool>,

    /// Logger utilizado por todo el microservicio.
    logger: Logger,
}

impl<C, L> MicroserviceLLM<C, L>
where
    C: RespConn,
    L: LlmClient,
{
    /// Constructor interno del microservicio.
    ///
    /// Crea una instancia del microservicio LLM con todos sus componentes configurados:
    /// conexión a Redis, cliente de inferencia, logger y thread pool.
    ///
    /// Este constructor es genérico y utilizado tanto en producción (`TcpStream`, `RealLlm`)
    /// como en tests (`MemConn`, `StubLlm`).
    ///
    /// # Parámetros
    /// - `channel_request`: nombre del canal Redis donde llegan las solicitudes.
    /// - `channel_global_request`: canal Redis para solicitar documentos enteros.
    /// - `conn`: conexión RESP hacia Redis.
    /// - `llm`: cliente que sabe ejecutar prompts.
    /// - `pool`: pool de hilos para ejecución concurrente.
    /// - `logger`: logger para registro de eventos y errores.
    ///
    /// # Retorna
    /// Una instancia completamente inicializada de `MicroserviceLLM`.
    fn new_internal(
        channel_request: String,
        channel_global_request: String,
        conn: C,
        llm: L,
        pool: ThreadPool,
        logger: Logger,
    ) -> Self {
        Self {
            conn,
            llm,
            channel_request,
            channel_global_request,
            thread_pool: Arc::new(pool),
            logger,
        }
    }

    /// Punto de entrada principal del microservicio LLM.
    ///
    /// - Establece conexión con Redis (`TcpStream`).
    /// - Realiza `AUTH` y `SUBSCRIBE` al canal de peticiones entrantes.
    /// - Crea tres canales internos para comunicación entre hilos:
    ///     - `tx_out_writer`: canal para publicar mensajes al `writer`.
    ///     - `tx_out_global`: canal para redirigir peticiones `type_request = global`.
    ///     - `tx_out_text`: canal para redirigir respuestas que contienen texto de documentos.
    ///
    /// Luego lanza tres hilos:
    ///
    /// 1. [`request_reader`]:
    ///     - Lee frames RESP desde Redis.
    ///     - Detecta el tipo de petición (local/global/respuesta de doc).
    ///     - Encola tareas de LLM o reenvía datos por los canales correspondientes.
    ///
    /// 2. [`global_handler`]:
    ///     - Procesa las peticiones `type_request = global`.
    ///     - Solicita el texto del documento (por `PUBLISH`).
    ///     - Espera la respuesta (`rx_in_text`), arma el prompt completo y lo encola.
    ///
    /// 3. [`response_writer`]:
    ///     - Toma las respuestas finales generadas por el LLM desde `rx_in_writer`.
    ///     - Las publica por Redis en el canal correspondiente.
    ///
    /// # Errores
    /// - Devuelve `LLMError::IOError` si falla la clonación de la conexión o el `set_up_microserv()`.
    pub fn run(self: Arc<Self>) -> Result<(), LLMError> {
        let reader = self.conn.try_clone().map_err(LLMError::IOError)?; // TcpStream lo tiene; MemConn implementa lo mismo
        let mut writer = self.conn.try_clone().map_err(LLMError::IOError)?;
        self.set_up_microserv(&mut writer)?;

        let (tx_out_writer, rx_in_writer) = channel();
        let (tx_out_global, rx_in_global) = channel();
        let (tx_out_text, rx_in_text) = channel();

        let join_handler1 =
            self.request_reader(reader, tx_out_writer.clone(), tx_out_global, tx_out_text);
        let join_handler2 = self.global_handler(tx_out_writer, rx_in_global, rx_in_text);
        let join_handler3 = self.response_writer(writer, rx_in_writer);

        let _ = join_handler1.join();
        let _ = join_handler2.join();
        let _ = join_handler3.join();
        Ok(())
    }

    /// Envía comandos RESP para autenticar (`AUTH`) y suscribirse al canal de escucha (`SUBSCRIBE <channel>`).
    fn set_up_microserv(&self, writer: &mut C) -> Result<(), LLMError> {
        // Auth redis
        resp_client_command_write(AUTH_CMD.to_string(), writer)
            .map_err(LLMError::SetUpMicroserv)?;

        self.logger.info(
            &format!("Successfully auth Microservice [{AUTH_CMD:?}]"),
            "AUTH",
        );

        // Subscribes
        let cmd = format!("SUBSCRIBE {}", self.channel_request);
        resp_client_command_write(cmd.clone(), writer).map_err(LLMError::SetUpMicroserv)?;

        self.logger.info(
            &format!("Successfully subscribe request channel [{cmd:?}]"),
            "REQUEST",
        );
        Ok(())
    }

    /// Hilo que:
    /// - Lee frames RESP del socket de Redis.
    /// - Intenta parsear cada mensaje como JSON válido.
    /// - Determina el tipo de mensaje y actúa según corresponda:
    ///     - LOCAL → lo ejecuta con el LLM.
    ///     - GLOBAL → lo envía al hilo global_handler.
    ///     - TEXTO → lo reenvía al canal de respuesta esperada.
    /// - Loguea errores si el socket se cierra o si hay problemas de parsing.
    ///
    /// # Parámetros
    /// - `reader`: stream RESP desde Redis.
    /// - `tx_out_writer`: canal de publicación (Redis).
    /// - `tx_out_global`: canal hacia el hilo global_handler.
    /// - `tx_out_text`: canal hacia las tareas que esperan contenido de documento.
    fn request_reader(
        &self,
        mut reader: C,
        tx_out_writer: Sender<Vec<String>>,
        tx_out_global: Sender<ExpresionJson>,
        tx_out_text: Sender<ExpresionJson>,
    ) -> JoinHandle<()> {
        let logger = self.logger.clone();
        let pool = self.thread_pool.clone();
        let llm = self.llm.clone();

        spawn(move || {
            loop {
                match resp_client_command_read(&mut reader) {
                    Ok(resp) => {
                        let mensaje_raw = resp.convertir_resp_a_string();
                        let json = match extract_message(mensaje_raw.clone()) {
                            Ok(j) => j,
                            Err(_) => {
                                logger.warn("Mensaje inválido recibido", "READER");
                                continue;
                            }
                        };

                        logger.info(&format!("Nueva petición: [{mensaje_raw:?}]"), "READER");
                        let tipo = match json.get_value(TYPE_REQUEST_KEY) {
                            Ok(t) => t,
                            Err(_) => {
                                logger.warn("Falta campo tipo de mensaje", "READER");
                                continue;
                            }
                        };

                        match tipo.trim_matches('"') {
                            LOCAL_REQUEST => {
                                println!("[DEBUG] Local [REQUEST]");
                                let writer = tx_out_writer.clone();
                                let llm = llm.clone();
                                let logger = logger.clone();

                                let _ = pool.execute(move || {
                                    if let Err(e) = handler_request(&json, writer, llm) {
                                        logger.error(&format!("{e}"), "LLM_HANDLER");
                                    }
                                });
                            }

                            GLOBAL_REQUEST => {
                                println!("[DEBUG] Global REQUEST]");
                                if tx_out_global.send(json).is_err() {
                                    logger.warn("No se pudo enviar petición global", "READER");
                                }
                            }

                            DOC_TEXT_RESPONSE => {
                                println!("[DEBUG] Text complete REQUEST]");
                                if tx_out_text.send(json).is_err() {
                                    logger.warn("No se pudo enviar texto de documento", "READER");
                                }
                            }

                            _ => {
                                logger.warn("Tipo de mensaje desconocido", "READER");
                            }
                        }
                    }

                    Err(e) => {
                        let e_msg = e.convertir_resp_a_string();
                        logger.error(&e_msg, "READER");
                        break;
                    }
                }
            }

            logger.info("Finaliza hilo de lectura", "READER");
        })
    }

    /// Hilo que:
    /// - Escucha respuestas listas desde el `Receiver`.
    /// - Escribe respuestas RESP por el socket hacia Redis (ej: `PUBLISH canal {...}`).
    /// - Termina cuando se cierra el canal.
    fn response_writer(&self, mut writer: C, writer_rx: Receiver<Vec<String>>) -> JoinHandle<()> {
        let logger = self.logger.clone();
        spawn(move || {
            for response in writer_rx {
                println!("[DEBUG] DEVUELVO AL USUARIO: {response:?}");

                let e = resp_api_command_write(response, &mut writer);
                if let Err(e) = e {
                    logger.error(
                        &format!("Error write response [{:?}]", e.convertir_resp_a_string()),
                        "WRITER",
                    );
                }
            }
            logger.info("Finish the writer thread", "WRITE");
        })
    }

    /// Hilo dedicado al procesamiento de peticiones `type_request = true`.
    ///
    /// - Escucha en `rx_in_global` nuevas peticiones globales de la UI.
    /// - Publica una solicitud del documento al canal de control (`channel_global_request`).
    /// - Espera el texto del documento en `rx_in_text`.
    /// - Una vez recibido, concatena el prompt con el contenido del documento.
    /// - La petición completa se envía al `ThreadPool` para ejecución por el LLM.
    ///
    /// # Parámetros
    /// - `tx_out_writer`: canal para publicar en Redis.
    /// - `rx_in_global`: canal donde llegan las peticiones globales.
    /// - `rx_in_text`: canal donde llegan las respuestas con el contenido del documento.
    fn global_handler(
        &self,
        tx_out_writer: Sender<Vec<String>>,
        rx_in_global: Receiver<ExpresionJson>,
        rx_in_text: Receiver<ExpresionJson>,
    ) -> JoinHandle<()> {
        let request_ch_global = self.channel_global_request.clone();
        let pool = self.thread_pool.clone();
        let llm = self.llm.clone();

        spawn(move || {
            for json in rx_in_global.iter() {
                // Extraer campos requeridos de la petición global
                let (request_id, doc_id, prompt, request_ch) = match extract_global_fields(&json) {
                    Ok(fields) => fields,
                    Err(_) => continue,
                };
                // Armar y publicar mensaje para solicitar el contenido del documento
                if send_doc_request(
                    &tx_out_writer,
                    &request_ch_global,
                    &request_id,
                    &doc_id,
                    &request_ch,
                )
                .is_err()
                {
                    continue;
                }

                let writer_sender_clone = tx_out_writer.clone();
                let llm_clone = llm.clone();
                // Esperar la respuesta con el texto del documento
                match rx_in_text.recv_timeout(Duration::from_secs(TIMEOUT_DOC_TEXT_SEC)) {
                    Ok(json_text) => {
                        if let Ok(texto) = json_text.get_value(DOC_TEXT_KEY) {
                            println!("[DEBUG] TEXTO DEL DOC A COMPLETAR:-> \"{texto}\"");

                            let prompt_concat = format!("{}\n{}", prompt, texto.trim_matches('"'),);

                            let json_s = format!(
                                "{{\"{PROMPT_KEY}\":\"{prompt_concat}\", \"{CHANNEL_RESPONSE_KEY}\":\"{request_ch}\"}}"
                            );

                            if let Ok(json_completo) = obtener_json_raw(json_s) {
                                let _ = pool.execute(move || {
                                    let _ = handler_request(
                                        &json_completo,
                                        writer_sender_clone,
                                        llm_clone,
                                    );
                                });
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        })
    }
}

/// Maneja una solicitud individual recibida por el micro‑servicio.
///
/// Pasos:
/// 1. **Extraer** `prompt` y `channel_response` del mensaje RESP.
/// 2. **Invocar** al LLM (Gemini) con la API‑key.
/// 3. **Formatear** un comando `PUBLISH <canal> "<respuesta>"`.
/// 4. **Enviar** el comando por el `Sender` al hilo escritor.
///
/// # Parámetros
/// * `msj`: Frame RESP convertido a `String`.
/// * `writer_tx`: Canal hacia el hilo que publica en Redis.
/// * `api_key`: Clave de API del proveedor LLM.
///
/// # Errores
/// Un error de red o de cuota devuelve `ERROR_RESPONSE`.
fn handler_request<L: LlmClient>(
    json: &ExpresionJson,
    writer_tx: Sender<Vec<String>>,
    call_llm: L,
) -> Result<(), LLMError> {
    let prompt = json
        .get_value(PROMPT_KEY)
        .map_err(|_| LLMError::Malformed)?;
    let channel_response = json
        .get_value(CHANNEL_RESPONSE_KEY)
        .map_err(|_| LLMError::Malformed)?;

    println!("[DEBUG]: [{prompt:?}] | [{channel_response}]");

    let prompt_parsed = prompt.trim_matches('"');

    let channel_response_parsed = channel_response.trim_matches('"').to_string();

    let answer = call_llm
        .answer(prompt_parsed.to_string())
        .unwrap_or_else(|e| format!("{{\"{STATUS_KEY}\": \"err\", \"{DOC_TEXT_KEY}\":{e}}}"));

    let response = vec!["PUBLISH".to_string(), channel_response_parsed, answer];

    let _ = writer_tx.send(response);
    Ok(())
}

/// Extrae **prompt** y **response_channel** de un frame RESP convertido a texto.
///
/// Asume el formato:
/// ```text
/// 1) message\r\n
/// 2) llm:request\r\n
/// 3) "{"prompt":"...","response_channel":"..."}"
/// ```
///
/// # Retorna
/// `(prompt, response_channel)` como tupla.
///
/// # Errores
/// * `LLMError::Malformed` – si falta alguna parte.
fn extract_message(msj: String) -> Result<ExpresionJson, LLMError> {
    let part = msj.split("\r\n").nth(2).ok_or(LLMError::Malformed)?;
    let body = part
        .trim_start_matches(REQUEST_MESSAGE_PREFIX)
        .trim()
        .trim_matches('"')
        .to_string();
    let json = obtener_json_raw(body.clone()).map_err(|_| LLMError::Malformed)?;
    Ok(json)
}

/// Extrae los campos necesarios de una petición global.
/// Devuelve `ExpresionJson::new_invalid_json_err` si falta alguno.
fn extract_global_fields(
    json: &ExpresionJson,
) -> Result<(String, String, String, String), ExpresionJson> {
    Ok((
        json.get_value(REQUEST_ID_KEY)?
            .trim_matches('"')
            .to_string(),
        json.get_value(DOC_ID_KEY)?.trim_matches('"').to_string(),
        json.get_value(PROMPT_KEY)?.trim_matches('"').to_string(),
        json.get_value(CHANNEL_RESPONSE_KEY)?
            .trim_matches('"')
            .to_string(),
    ))
}

/// Publica una solicitud de documento al canal del microservicio de control.
/// Retorna `Result` por si hay un error al enviar.
fn send_doc_request(
    tx_out: &Sender<Vec<String>>,
    request_ch_global: &str,
    request_id: &str,
    doc_id: &str,
    response_ch: &str,
) -> Result<(), ()> {
    let peticion = format!(
        "{{\"{REQUEST_ID_KEY}\":\"{request_id}\", \"{DOC_ID_KEY}\":\"{doc_id}\", \"{CHANNEL_RESPONSE_KEY}\":\"{response_ch}\", \"{TYPE_REQUEST_KEY}\":\"{GET_DOC_TEXT}\"}}"
    );

    tx_out
        .send(vec![
            "PUBLISH".to_string(),
            request_ch_global.to_string(),
            peticion,
        ])
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::to_raw_string;
    use redis_client::protocol::dataencryption::{decrypt_resp, encrypt_resp};
    use std::io::{Cursor, Read, Write};
    use std::sync::Mutex;
    use std::sync::mpsc::channel;

    /// Stub Cliente http que devuelve Ok
    #[derive(Clone)]
    struct StubOk;
    impl LlmClient for StubOk {
        fn answer(&self, prompt: String) -> Result<String, LLMError> {
            Ok(format!("ECHO:{prompt}"))
        }
    }

    /// Stub Cliente http que devuelve Err
    #[derive(Clone)]
    struct StubErr;
    impl LlmClient for StubErr {
        fn answer(&self, _prompt: String) -> Result<String, LLMError> {
            Err(LLMError::Api("boom".into()))
        }
    }

    /// Microservicio fake
    impl MicroserviceLLM<MemConn, StubErr> {
        pub(crate) fn new_fake_err(
            conn: MemConn,
            llm: StubErr,
            channel_request: &str,
            channel_global_request: &str,
        ) -> Self {
            Self {
                conn,
                llm,
                channel_request: channel_request.to_string(),
                channel_global_request: channel_global_request.to_string(),
                thread_pool: Arc::new(ThreadPool::new(1)),
                logger: Logger::null(),
            }
        }
    }

    /// Stub LLM
    #[derive(Default, Clone, Debug)]
    pub struct StubLlm {
        response: String,
    }

    impl LlmClient for StubLlm {
        fn answer(&self, _prompt: String) -> Result<String, LLMError> {
            Ok(self.response.clone())
        }
    }

    /// Mock para simular conexion redis
    #[derive(Default, Clone, Debug)]
    pub struct MemConn {
        reader: Cursor<Vec<u8>>,
        writer: Arc<Mutex<Vec<u8>>>,
    }

    impl MemConn {
        /* Crea un stream vacío (lectura = EOF, escritura vacía) */

        /*
        pub fn empty() -> Self {
            Self {
                reader: Cursor::new(Vec::new()),
                writer: Arc::new(Mutex::new(Vec::new())),
            }
        }
         */

        /* Crea un stream precargado con `data` para leer */
        pub fn with_input<T: Into<Vec<u8>>>(data: T) -> Self {
            Self {
                reader: Cursor::new(data.into()),
                writer: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /* Devuelve todo lo escrito hasta ahora como String UTF‑8 */
        /*
        pub fn written(&self) -> String {
            let leido = self.writer.lock().unwrap().clone();
            String::from_utf8(leido).unwrap()
        }

         */

        /// Devuelve una copia *sin descifrar* de todo lo que se escribió.
        pub fn written_bytes(&self) -> Vec<u8> {
            self.writer.lock().unwrap().clone()
        }

        /* Devuelve un (lector, escritor) que comparten el buffer de salida */
        pub fn pair<T: Into<Vec<u8>>>(data: T) -> (Self, Self) {
            let base = Self::with_input(data);
            (
                Self {
                    reader: base.reader.clone(),
                    writer: Arc::clone(&base.writer),
                },
                base,
            )
        }
    }
    impl Read for MemConn {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.reader.read(buf)
        }
    }

    impl Write for MemConn {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.writer.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl RespConn for MemConn {
        fn try_clone(&self) -> Result<MemConn, Error> {
            Ok(Self {
                reader: self.reader.clone(),
                writer: Arc::clone(&self.writer),
            })
        }
    }

    /// Formato de llegada por parte del exterior
    fn make_resp(prompt: &str, ch: &str, global: bool) -> String {
        format!(
            concat!(
                "1) \"message\"\r\n",
                "2) \"llm:request\"\r\n",
                "3) \"{{\"prompt\":\"{}\",",
                "\"response_channel\":\"{}\",",
                "\"type_request\":{}}}\"\r\n",
            ),
            prompt, ch, global
        )
    }

    fn make_redis_write_cmd(comandos: Vec<String>) -> Vec<u8> {
        let mut comando_resp = format!("*{}\r\n", comandos.len());
        for token in comandos {
            comando_resp.push_str(&format!("${}\r\n{}\r\n", token.len(), token));
        }
        encrypt_resp(&comando_resp).unwrap()
    }

    /// Microservicio fake
    impl MicroserviceLLM<MemConn, StubLlm> {
        pub(crate) fn new_fake(
            conn: MemConn,
            llm: StubLlm,
            channel_request: &str,
            channel_global_request: &str,
        ) -> Self {
            Self {
                conn,
                llm,
                channel_request: channel_request.to_string(),
                channel_global_request: channel_global_request.to_string(),
                thread_pool: Arc::new(ThreadPool::new(1)),
                logger: Logger::null(),
            }
        }
    }

    fn read_all_resp_commands(mut slice: &[u8]) -> Vec<String> {
        let mut cmds = Vec::new();
        while let Ok(msg) = decrypt_resp(&mut slice) {
            cmds.push(msg);
        }
        cmds
    }

    #[test]
    fn test01_extract_message_ok() {
        let frame = make_resp("¿Cuál es la capital de Francia?", "canal:resp", false);
        let json: ExpresionJson = extract_message(frame).expect("debe parsear bien");

        assert_eq!(
            json.get_value(PROMPT_KEY).unwrap(),
            "\"¿Cuál es la capital de Francia?\""
        );
        assert_eq!(
            json.get_value(CHANNEL_RESPONSE_KEY).unwrap(),
            "\"canal:resp\""
        );
        assert_eq!(json.get_value("type_request").unwrap(), "false");
    }

    #[test]
    fn test02_extract_message_falta_cuerpo() {
        let frame = concat!("1) \"message\"\r\n", "2) \"llm:request\"\r\n",).to_string();

        let err = extract_message(frame).unwrap_err();
        assert!(matches!(err, LLMError::Malformed));
    }

    #[test]
    fn test03_extract_message_json_malformado() {
        let frame = concat!(
            "1) \"message\"\r\n",
            "2) \"llm:request\"\r\n",
            // llaves sin cerrar
            "3) \"{\\\"prompt\\\":\\\"hola\\\"\"\r\n",
        )
        .to_string();

        let err = extract_message(frame).unwrap_err();
        assert!(matches!(err, LLMError::Malformed));
    }

    #[test]
    fn test04_extract_message_canal_llm_request() {
        let frame = concat!(
            "1) \"message\"\r\n",
            "2) \"otro:canal\"\r\n",
            "3) \"{\"prompt\":\"hola\",\"response_channel\":\"x\",\"global\":false}\"\r\n",
        )
        .to_string();

        let json = extract_message(frame).unwrap();
        assert_eq!(json.get_value("prompt").unwrap(), "\"hola\"");
    }

    #[test]
    fn test05_handler_request_publica_respuesta_ok() {
        let frame = make_resp("Hola", "canal:res", false);
        let json: ExpresionJson = extract_message(frame).expect("debe parsear bien");
        let (tx, rx) = channel();

        handler_request(&json, tx, StubOk).expect("sin error");

        let cmd = rx.recv().unwrap();
        assert_eq!(cmd[0], "PUBLISH");
        assert_eq!(cmd[1], "canal:res");
        assert_eq!(cmd[2], to_raw_string("ECHO:Hola"));
    }

    #[test]
    fn test06_handler_request_publica_error_si_llm_falla() {
        let frame = make_resp("fail", "canal:err", false);
        let json: ExpresionJson = extract_message(frame).expect("debe parsear bien");

        let (tx, rx) = channel();

        // la función sigue devolviendo Ok(), pero el texto llevará el error
        handler_request(&json, tx, StubErr).expect("should not fail");

        let cmd = rx.recv().unwrap();
        assert_eq!(cmd[0], "PUBLISH");
        assert_eq!(cmd[1], "canal:err");
        assert!(
            cmd[2].contains("boom"),
            "la respuesta debe contener el texto del error: {:?}",
            cmd[2]
        );
    }

    #[test]
    fn test07_microservicio_procesa_mensaje_resp_y_responde() {
        let comandos: Vec<String> = vec![
            "PUBLISH".into(),
            "llm:request".into(),
            r#"{"prompt":"Hola","response_channel":"c1","type_request":"local"}"#.into(),
        ];

        let cmd_redis_fmt = make_redis_write_cmd(comandos);

        let input = Cursor::new(cmd_redis_fmt);
        let output = Arc::new(Mutex::new(vec![]));

        let conn = MemConn {
            reader: input,
            writer: output.clone(),
        };

        let llm = StubLlm {
            response: "respuesta-fake".to_string(),
        };

        let ms = MicroserviceLLM::new_fake(conn, llm, "llm:request", "");

        let (tx_out, rx_out) = channel();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();
        let (reader, _) = (ms.conn.try_clone(), ms.conn.try_clone());

        let _ = Arc::new(ms)
            .request_reader(reader.unwrap(), tx_out, tx1, tx2)
            .join();

        let msg = rx_out.recv().unwrap();
        assert_eq!(msg[0], "PUBLISH");
        assert_eq!(msg[1], "c1");
        assert_eq!(msg[2], to_raw_string("respuesta-fake"));
    }

    #[test]
    fn test08_microservicio_procesa_mensaje_json_error() {
        // Json formaro invalido (le falta '}' de cierre)
        let comandos: Vec<String> = vec![
            "PUBLISH".into(),
            "llm:request".into(),
            r#"{"prompt":"Hola","response_channel":"c1","type_request":"local""#.into(),
        ];
        let cmd_redis_fmt = make_redis_write_cmd(comandos);

        let input = Cursor::new(cmd_redis_fmt);
        let output = Arc::new(Mutex::new(vec![]));

        let conn = MemConn {
            reader: input,
            writer: output.clone(),
        };

        let llm = StubLlm {
            response: "respuesta-fake".to_string(),
        };

        let ms = MicroserviceLLM::new_fake(conn, llm, "llm:request", "");

        let (tx_out, rx_out) = channel();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();
        let (reader, _) = (ms.conn.try_clone(), ms.conn.try_clone());

        let _ = Arc::new(ms)
            .request_reader(reader.unwrap(), tx_out, tx1, tx2)
            .join();

        // No tiene nada para recibir
        let err = rx_out.recv();
        assert!(err.is_err());
    }

    #[test]
    fn test09_microservicio_procesa_mensaje_json_falta_campo_error() {
        // Json formaro invalido (le falta el campo de 'type_request')
        let comandos: Vec<String> = vec![
            "PUBLISH".into(),
            "llm:request".into(),
            r#"{"prompt":"Hola","response_channel":"c1"}"#.into(),
        ];
        let cmd_redis_fmt = make_redis_write_cmd(comandos);

        let input = Cursor::new(cmd_redis_fmt);
        let output = Arc::new(Mutex::new(vec![]));

        let conn = MemConn {
            reader: input,
            writer: output.clone(),
        };

        let llm = StubLlm {
            response: "respuesta-fake".to_string(),
        };

        let ms = MicroserviceLLM::new_fake(conn, llm, "llm:request", "");

        let (tx_out, rx_out) = channel();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();
        let (reader, _) = (ms.conn.try_clone(), ms.conn.try_clone());

        let _ = Arc::new(ms)
            .request_reader(reader.unwrap(), tx_out, tx1, tx2)
            .join();

        // No tiene nada para recibir
        let err = rx_out.recv();
        assert!(err.is_err());
    }

    #[test]
    fn test10_microservicio_procesa_mensaje_json_falta_campo_error() {
        // Json formaro invalido (le falta el campo de 'prompt')
        let comandos: Vec<String> = vec![
            "PUBLISH".into(),
            "llm:request".into(),
            r#"{"response_channel":"c1", "type_request":"local"}"#.into(),
        ];
        let cmd_redis_fmt = make_redis_write_cmd(comandos);

        let input = Cursor::new(cmd_redis_fmt);
        let output = Arc::new(Mutex::new(vec![]));

        let conn = MemConn {
            reader: input,
            writer: output.clone(),
        };

        let llm = StubLlm {
            response: "respuesta-fake".to_string(),
        };

        let ms = MicroserviceLLM::new_fake(conn, llm, "llm:request", "");

        let (tx_out, rx_out) = channel();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();
        let (reader, _) = (ms.conn.try_clone(), ms.conn.try_clone());

        let _ = Arc::new(ms)
            .request_reader(reader.unwrap(), tx_out, tx1, tx2)
            .join();

        // No tiene nada para recibir
        let err = rx_out.recv();
        assert!(err.is_err());
    }

    #[test]
    fn test11_microservicio_procesa_mensaje_json_falta_campo_error() {
        // Json formaro invalido (no existe el 'type_request')
        let comandos: Vec<String> = vec![
            "PUBLISH".into(),
            "llm:request".into(),
            r#"{"prompt":"Hola","response_channel":"c1","type_request":"no existe este tipo"}"#
                .into(),
        ];
        let cmd_redis_fmt = make_redis_write_cmd(comandos);

        let input = Cursor::new(cmd_redis_fmt);
        let output = Arc::new(Mutex::new(vec![]));

        let conn = MemConn {
            reader: input,
            writer: output.clone(),
        };

        let llm = StubLlm {
            response: "respuesta-fake".to_string(),
        };

        let ms = MicroserviceLLM::new_fake(conn, llm, "llm:request", "");

        let (tx_out, rx_out) = channel();
        let (tx1, _rx1) = channel();
        let (tx2, _rx2) = channel();
        let (reader, _) = (ms.conn.try_clone(), ms.conn.try_clone());

        let _ = Arc::new(ms)
            .request_reader(reader.unwrap(), tx_out, tx1, tx2)
            .join();

        // No tiene nada para recibir
        let err = rx_out.recv();
        assert!(err.is_err());
    }

    #[test]
    fn test12_microservicio_response_writer_escribe_comando_resp() {
        let (reader_side, writer_side) = MemConn::pair(Vec::<u8>::new()); // lectura vacía, escritura capturable

        let llm = StubLlm {
            response: "OK".to_string(),
        };

        let ms = Arc::new(MicroserviceLLM::new_fake(
            reader_side,
            llm,
            "llm:request",
            "",
        ));

        let (tx, rx) = channel();
        let handle = ms.clone().response_writer(writer_side, rx);

        tx.send(vec![
            "PUBLISH".into(),
            "canal".into(),
            to_raw_string("hola"),
        ])
        .unwrap();
        drop(tx);

        handle.join().unwrap();

        let encrypted = ms.conn.written_bytes();
        assert!(!encrypted.is_empty(), "se escribió algo");

        let mut cursor = Cursor::new(encrypted);
        let plaintext = decrypt_resp(&mut cursor).expect("debe descifrar");

        assert!(plaintext.contains("PUBLISH"));
        assert!(plaintext.contains("canal"));
        assert!(plaintext.contains("hola"));
    }

    #[test]
    fn test13_microservicio_response_writer_escribe_comando_resp() {
        let (reader_side, writer_side) = MemConn::pair(Vec::<u8>::new()); // lectura vacía, escritura capturable

        let llm = StubLlm {
            response: "OK".to_string(),
        };

        let ms = Arc::new(MicroserviceLLM::new_fake(
            reader_side,
            llm,
            "llm:request",
            "",
        ));

        let (tx, rx) = channel();
        let handle = ms.clone().response_writer(writer_side, rx);

        tx.send(vec![
            "PUBLISH".into(),
            "canal".into(),
            to_raw_string("hola"),
        ])
        .unwrap();
        drop(tx);

        handle.join().unwrap();

        let encrypted = ms.conn.written_bytes();
        assert!(!encrypted.is_empty(), "se escribió algo");

        let mut cursor = Cursor::new(encrypted);
        let plaintext = decrypt_resp(&mut cursor).expect("debe descifrar");

        assert!(plaintext.contains("PUBLISH"));
        assert!(plaintext.contains("canal"));
        assert!(plaintext.contains("hola"));
    }

    #[test]
    fn test_14_global_request_flow_success() {
        let (conn_writer, _conn_reader) = MemConn::pair(Vec::new());

        let llm = StubLlm {
            response: r#"{"prompt":"Completa este texto:\nEste es el contenido."}"#.into(),
        };

        let micro = MicroserviceLLM::new_fake(conn_writer, llm, "llm:request", "doc:texto");

        let (tx_out_writer, rx_out_writer) = channel();
        let (tx_out_global, rx_out_global) = channel();
        let (tx_out_text, rx_out_text) = channel();

        // Lanzar el hilo global_handler
        let _jh = micro.global_handler(tx_out_writer.clone(), rx_out_global, rx_out_text);

        // Simular que llega una petición global
        let json = obtener_json_raw(
            r#"{"prompt":"Completa este texto:","requestId":"abc123","response_channel":"res:ch","type_request":"global","docId": "doc1"}"#.to_string()
        ).unwrap();

        tx_out_global.send(json).unwrap();
        // Capturar el mensaje de solicitud de texto del documento
        let expected_request = rx_out_writer.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            expected_request,
            vec![
                "PUBLISH".to_string(),
                "doc:texto".to_string(),
                "{\"requestId\":\"abc123\", \"docId\":\"doc1\", \"response_channel\":\"res:ch\", \"type_request\":\"get\"}".to_string()
            ]
        );

        // Simular llegada del contenido del documento
        let doc_json = obtener_json_raw(r#"{"text":"Este es el contenido."}"#.to_string()).unwrap();
        tx_out_text.send(doc_json).unwrap();

        // Capturar el mensaje final generado por el LLM
        let final_msg = rx_out_writer.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            final_msg,
            vec![
                "PUBLISH".to_string(),
                "res:ch".to_string(),
                r#"{"prompt":"Completa este texto:\nEste es el contenido."}"#.to_string()
            ]
        );
    }

    #[test]
    fn test_15_global_request_invalid_json_missing_fields() {
        let (_writer, _reader) = MemConn::pair(Vec::new());
        let llm = StubLlm {
            response: "irrelevante".into(),
        };

        let micro = MicroserviceLLM::new_fake(_writer, llm, "llm:request", "doc:texto");

        let (tx_out_writer, rx_out_writer) = channel();
        let (tx_out_global, rx_out_global) = channel();
        let (_tx_out_text, _rx_out_text) = channel();

        let _jh = micro.global_handler(tx_out_writer, rx_out_global, _rx_out_text);

        let bad_json = obtener_json_raw(r#"{"prompt":"incompleto"}"#.to_string()).unwrap();

        tx_out_global.send(bad_json).unwrap();

        // El canal de salida no debe recibir nada
        assert!(
            rx_out_writer
                .recv_timeout(Duration::from_millis(200))
                .is_err()
        );
    }

    #[test]
    fn test_16_global_request_timeout_no_doc_text() {
        let (_writer, _reader) = MemConn::pair(Vec::new());
        let llm = StubLlm {
            response: "no importa".into(),
        };

        let micro = MicroserviceLLM::new_fake(_writer, llm, "llm:request", "doc:texto");

        let (tx_out_writer, rx_out_writer) = channel();
        let (tx_out_global, rx_out_global) = channel();
        let (_tx_out_text, rx_out_text) = channel(); // Nunca se envía texto

        let _jh = micro.global_handler(tx_out_writer, rx_out_global, rx_out_text);

        let json = obtener_json_raw(
            r#"{"prompt":"Completa este texto:","requestId":"abc123","response_channel":"res:ch","type_request":"global","docId": "doc1"}"#.to_string()
        ).unwrap();

        tx_out_global.send(json).unwrap();

        // Debe haber un primer mensaje solicitando el texto
        let request = rx_out_writer
            .recv_timeout(Duration::from_millis(100))
            .unwrap();
        assert_eq!(request[0], "PUBLISH");

        // Luego no debe haber publicación porque no llega el texto
        assert!(
            rx_out_writer
                .recv_timeout(Duration::from_millis(200))
                .is_err()
        );
    }

    #[test]
    fn test_17_global_request_llm_error() {
        let (_writer, _reader) = MemConn::pair(Vec::new());

        let micro = MicroserviceLLM::new_fake_err(_writer, StubErr, "llm:request", "doc:texto");

        let (tx_out_writer, rx_out_writer) = channel();
        let (tx_out_global, rx_out_global) = channel();
        let (tx_out_text, rx_out_text) = channel();

        let _jh = micro.global_handler(tx_out_writer, rx_out_global, rx_out_text);

        let json = obtener_json_raw(
            r#"{"prompt":"Completa este texto:","requestId":"abc123","response_channel":"res:ch","type_request":"global","docId": "doc1"}"#.to_string()
        ).unwrap();

        tx_out_global.send(json).unwrap();

        // Debe enviar la solicitud del documento
        let solicit = rx_out_writer
            .recv_timeout(Duration::from_millis(100))
            .unwrap();
        assert_eq!(solicit[0], "PUBLISH");

        // Simulamos que llega el texto
        let doc_json = obtener_json_raw(r#"{"doc_text":"mi contenido"}"#.to_string()).unwrap();
        tx_out_text.send(doc_json).unwrap();

        // No debería publicarse la respuesta final porque el LLM devuelve error
        assert!(
            rx_out_writer
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );
    }
    #[test]
    fn test_18_request_reader_global_and_doc_text() {
        use std::sync::mpsc::channel;
        use std::thread;
        use std::time::Duration;

        // Mensaje tipo_request = global
        let mensaje_global = r#"{"prompt":"Completa este texto:","requestId":"abc123","response_channel":"res:ch","type_request":"global","docId":"doc1"}"#;

        // Mensaje tipo_request = doc_text
        let mensaje_doc_text = r#"{"text":"Este es el contenido.","type_request":"doc_text"}"#;

        // Armar RESP tipo mensaje Redis: *3\r\n$7\r\nmessage\r\n$... para cada mensaje
        let payload_global = format!(
            "*3\r\n$7\r\nmessage\r\n${}\r\nllm:request\r\n${}\r\n{}\r\n",
            "llm:request".len(),
            mensaje_global.len(),
            mensaje_global
        );
        let payload_global = encrypt_resp(&payload_global).unwrap();

        let payload_doc = format!(
            "*3\r\n$7\r\nmessage\r\n${}\r\nllm:request\r\n${}\r\n{}\r\n",
            "llm:request".len(),
            mensaje_doc_text.len(),
            mensaje_doc_text
        );
        let payload_doc = encrypt_resp(&payload_doc).unwrap();

        // Simulamos lector con ambos mensajes como si vinieran de Redis
        let (writer_conn, reader_conn) = MemConn::pair([payload_global, payload_doc].concat());

        let llm = StubLlm {
            response: "irrelevante".to_string(),
        };

        let micro = MicroserviceLLM::new_fake(writer_conn, llm, "llm:request", "doc:texto");

        let (tx_writer, _rx_writer) = channel();
        let (tx_global, rx_global) = channel();
        let (tx_text, rx_text) = channel();

        let jh = micro.request_reader(reader_conn, tx_writer, tx_global.clone(), tx_text.clone());

        // Esperar que el thread procese
        thread::sleep(Duration::from_millis(200));

        let recibido_global = rx_global.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            recibido_global.get_value("requestId").unwrap(),
            "\"abc123\""
        );
        assert_eq!(
            recibido_global.get_value("type_request").unwrap(),
            "\"global\""
        );

        let recibido_doc = rx_text.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            recibido_doc.get_value("type_request").unwrap(),
            "\"doc_text\""
        );
        assert_eq!(
            recibido_doc.get_value("text").unwrap(),
            "\"Este es el contenido.\""
        );

        jh.join().unwrap();
    }

    #[test]
    fn test_set_up_microserv_auth_and_subscribe() {
        // Crear un writer MemConn vacío
        let mut writer = MemConn::with_input(vec![]);

        // Crear microservicio con canal ficticio
        let micro = MicroserviceLLM::new_fake(
            writer.try_clone().unwrap(),
            StubLlm {
                response: String::new(),
            },
            "llm:request",
            "doc:texto",
        );

        // Ejecutar la función bajo prueba
        let result = micro.set_up_microserv(&mut writer);

        // Debe completarse sin errores
        assert!(result.is_ok(), "set_up_microserv debe retornar Ok");

        // Usar decrypt_resp para convertir a String
        let cmds = read_all_resp_commands(&writer.written_bytes()[..]);
        println!("decrypted: {:?}", &cmds);

        // Verificamos que haya dos comandos RESP escritos
        assert!(
            cmds[0].contains("*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$7\r\ndefault\r\n"),
            "Debe contener comando AUTH"
        );

        assert!(
            cmds[1].contains("*2\r\n$9\r\nSUBSCRIBE\r\n$11\r\nllm:request\r\n"),
            "Debe contener comando SUBSCRIBE con el canal"
        );
    }
}
