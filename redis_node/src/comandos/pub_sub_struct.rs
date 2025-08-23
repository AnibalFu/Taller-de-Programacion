//! Este módulo contiene al struct de tipo "message broker",
//! utilizado par manejar conexiones de tipo pub-sub
//!
//! Se encarga de:
//! - Procesar comandos de clientes y nodos relacionados al sistema Pub/Sub.
//! - Gestionar los canales y suscriptores activos.
//! - Ejecutar comandos como SUBSCRIBE y UNSUBSCRIBE mediante un canal dedicado.

use crate::client_struct::client::Client;
use crate::cluster::node_message::{InnerMensajeNode, TipoMensajeNode};
use crate::comandos::const_cmd::{CMD_PUNSUBSCRIBE, CMD_SUNSUBSCRIBE, CMD_UNSUBSCRIBE};
use crate::comandos::utils::send_msj;
use crate::internal_protocol::moved_shard_pubsub::MovedShardPubSub;
use crate::log_msj::log_mensajes::log_cli_subscribe_channels;
use crate::utils::utils_functions::obtener_fn_pub_sub;
use logger::logger::Logger;
use redis_client::tipos_datos::traits::DatoRedis;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, RwLock};
use std::thread::spawn;

/// Alias que representa el nombre de un canal Pub/Sub.
pub type Canal = String;

/// Mapa de canales, donde cada canal tiene un diccionario de clientes suscritos identificados
/// por su ID, asociado a su respectivo `Sender<String>` para enviar mensajes.
pub type Channels = HashMap<Canal, HashMap<String, Sender<String>>>;

/// Indica el origen del comando: si proviene de un cliente externo o de un nodo del sistema.
#[derive(Clone, Debug)]
pub enum BrokerCommandFlag {
    Client,
    Node,
}

/// Resultado de los get cmd del struct BrokerCommand
type PubSubParseResult = (
    BrokerCommandFlag,
    Vec<String>,
    Option<Arc<RwLock<Client>>>,
    Option<Sender<TipoMensajeNode>>,
);

/// Comando que será procesado por el `PubSubBroker`.
/// Contiene el origen (cliente/nodo), los tokens del comando y una referencia al cliente.
#[derive(Debug, Clone)]
pub struct BrokerCommand {
    flag_type: BrokerCommandFlag,
    tokens: Vec<String>,
    client: Option<Arc<RwLock<Client>>>,
    shard_sender: Option<Sender<TipoMensajeNode>>,
}

impl BrokerCommand {
    /// Crea un nuevo comando proveniente de un cliente.
    pub fn new_client_cmd(
        tokens: Vec<String>,
        client: Arc<RwLock<Client>>,
        shard_sender: Option<Sender<TipoMensajeNode>>,
    ) -> Self {
        BrokerCommand {
            flag_type: BrokerCommandFlag::Client,
            tokens,
            client: Some(client),
            shard_sender,
        }
    }

    /// Crea un nuevo comando proveniente de un nodo interno.
    ///
    /// Pueden ser solo de PUBLISH, de otros nodos; o UNSUBSCRIBE, de self node cuando un cli
    /// se desconecta.
    pub fn new_node_cmd(tokens: Vec<String>, client: Option<Arc<RwLock<Client>>>) -> Self {
        BrokerCommand {
            flag_type: BrokerCommandFlag::Node,
            tokens,
            client,
            shard_sender: None,
        }
    }

    /// Devuelve una tupla con el tipo de comando, tokens y referencia al cliente.
    pub fn get_cmd(&self) -> PubSubParseResult {
        (
            self.flag_type.clone(),
            self.tokens.clone(),
            self.client.clone(),
            self.shard_sender.clone(),
        )
    }
}

/// Estructura principal del broker de publicación/suscripción.
/// Recibe comandos mediante un canal y los ejecuta en un hilo dedicado.
#[derive(Debug, Clone)]
pub struct PubSubBroker {
    logger: Logger,
    cmd_sender: Sender<BrokerCommand>,
}

impl PubSubBroker {
    /// Crea un nuevo `PubSubBroker` con un hilo asociado que escucha comandos entrantes.
    pub fn new(logger: Logger, slot_range: Range<u16>) -> Self {
        let (tx, rx) = channel::<BrokerCommand>();

        let mut core = PubSubCore::new(logger.clone(), slot_range);
        spawn(move || core.run(rx));

        PubSubBroker {
            logger,
            cmd_sender: tx,
        }
    }

    pub fn noop() -> Self {
        let (_sender, _receiver) = channel::<BrokerCommand>();
        Self {
            logger: Logger::null(),
            cmd_sender: _sender,
        }
    }

    /// Envía un comando al hilo del broker para su ejecución.
    pub fn send_cmd(&self, cmd: BrokerCommand) {
        if self.cmd_sender.send(cmd).is_err() {
            self.logger
                .error("Error al mandar comandos al broker Pub/Sub", "PubSubBroker");
        };
    }

    /// Sea un comandos determina si es o no un comandos de PUBSUB
    pub fn es_comando_pub_sub(cmd: &str) -> bool {
        obtener_fn_pub_sub(cmd).is_some()
    }

    /// Envia un comando especial para desuscribir a un cliente de todos los canales activos
    /// al momento de desconectarse del servidor.
    pub fn borrar_canales_cliente(&self, client: Arc<RwLock<Client>>) {
        let cmd = BrokerCommand::new_node_cmd(
            [CMD_UNSUBSCRIBE.to_string()].to_vec(),
            Option::from(client.clone()),
        );
        self.send_cmd(cmd);
        let cmd = BrokerCommand::new_node_cmd(
            [CMD_PUNSUBSCRIBE.to_string()].to_vec(),
            Option::from(client.clone()),
        );
        self.send_cmd(cmd);
        let cmd = BrokerCommand::new_node_cmd(
            [CMD_SUNSUBSCRIBE.to_string()].to_vec(),
            Option::from(client),
        );
        self.send_cmd(cmd);
    }
}

/// Parametros necesarios para el sistema de pubsub
pub(crate) struct PubSubCore {
    /// Todos los canales no‑shard
    pub(crate) channels: Channels,
    /// Patrones
    pub(crate) pchannels: Channels,
    /// Shard‑channels
    pub(crate) schannels: Channels,
    /// Rango de slots que maneja este nodo
    pub(crate) slot_range: Range<u16>,
    /// Cualquier otra cosa que hoy estés pasando suelta
    pub(crate) logger: Logger,
}

impl PubSubCore {
    /// Crea una nueva instancia de PubSubCore con los mapas vacíos,
    /// el rango de slots que maneja el nodo y el logger asociado.
    pub(crate) fn new(logger: Logger, slot_range: Range<u16>) -> Self {
        Self {
            channels: HashMap::new(),
            pchannels: HashMap::new(),
            schannels: HashMap::new(),
            slot_range,
            logger,
        }
    }

    /// Maneja un comando recibido desde el canal del broker.
    /// Distingue entre comandos de cliente y de nodo interno.
    fn handle_command(&mut self, cmd: BrokerCommand) {
        let (flag, tokens, client, shard_sender) = cmd.get_cmd();
        match flag {
            BrokerCommandFlag::Client => {
                self.procesar_pubsub_cli_node(client, tokens, &self.logger.clone(), shard_sender)
            }
            BrokerCommandFlag::Node => self.procesar_pubsub_inner_node(client, tokens),
        }
    }

    /// Bucle principal que recibe comandos desde el canal del broker
    /// y los procesa uno por uno.
    fn run(&mut self, rx: Receiver<BrokerCommand>) {
        for cmd in rx {
            self.handle_command(cmd);
        }
    }

    /// Procesa un comando PUBSUB proveniente de un cliente externo.
    /// Ejecuta el handler y envía la respuesta o error al cliente.
    /// Si la respuesta contiene un MOVED y hay `shard_sender`,
    /// redirige el mensaje a otro nodo.
    fn procesar_pubsub_cli_node(
        &mut self,
        client_opt: Option<Arc<RwLock<Client>>>,
        tokens: Vec<String>,
        logger: &Logger,
        shard_sender: Option<Sender<TipoMensajeNode>>,
    ) {
        let client = match client_opt {
            Some(c) => c,
            None => return,
        };

        match self.procesar_pub_sub_handler(&tokens, &client, logger) {
            Ok(resp) => {
                if let DatoRedis::Arrays(array) = resp.clone() {
                    if array.contains_dato(&DatoRedis::new_moved_error(0)) {
                        if let Some(sender) = shard_sender {
                            let moved = MovedShardPubSub::new(resp, client);
                            let _ = sender.send(TipoMensajeNode::InnerNode(
                                InnerMensajeNode::MovedShard(moved),
                            ));
                            return;
                        }
                    }
                }
                send_msj(client, resp, logger)
            }

            Err(err) => send_msj(client, err, logger),
        }
    }

    /// Procesa un comando PUBSUB proveniente de otro nodo del clúster.
    /// Si incluye un cliente, ejecuta `unsubscribe`; si no, es un `publish`.
    fn procesar_pubsub_inner_node(
        &mut self,
        client: Option<Arc<RwLock<Client>>>,
        tokens: Vec<String>,
    ) {
        if let Some(cli) = client {
            let _ = self.unsubscribe(&tokens, cli);
        } else {
            let _ = self.publish_internal(&tokens);
        }
    }

    /// Procesa un comando Pub/Sub proveniente de un cliente.
    /// Si el comando es válido, se ejecuta la lógica correspondiente (e.g., SUBSCRIBE, PUBLISH).
    fn procesar_pub_sub_handler(
        &mut self,
        tokens: &[String],
        client: &Arc<RwLock<Client>>,
        logger: &Logger,
    ) -> Result<DatoRedis, DatoRedis> {
        let comando = tokens[0].to_uppercase();
        if let Some(handler) = obtener_fn_pub_sub(&comando) {
            let res = handler(self, tokens, Arc::clone(client))?;
            log_cli_subscribe_channels(logger, client);
            return Ok(res);
        }
        Ok(DatoRedis::new_null())
    }
}

#[cfg(test)]
mod tests {
    use redis_client::protocol::dataencryption::decrypt_resp;

    use super::*;
    use crate::node_role::NodeRole;
    use std::io::{Cursor, Read};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

    fn dummy_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (response_tcp, _) = listener.accept().unwrap();

        (client, response_tcp)
    }

    fn setup_cliente(nombre: &str, logger: Logger) -> (Arc<RwLock<Client>>, TcpStream) {
        // Crear el cliente con el nombre proporcionado
        let (client_stream, response_tcp) = dummy_tcp_pair();
        let cliente = Client::new(
            nombre.to_string(),
            client_stream,
            logger,
            Arc::new(RwLock::new(NodeRole::Master)),
        );
        let cliente = Arc::new(RwLock::new(cliente));
        (cliente, response_tcp)
    }

    #[test]
    fn test01_pubsub_broker_subscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        // Suscribirse
        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["SUBSCRIBE".to_string(), "canal1".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        thread::sleep(Duration::from_millis(50)); // esperar al hilo
        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        let mut stream = Cursor::new(&buffer[..res]);

        let decrypt_message = decrypt_resp(&mut stream).unwrap();

        assert_eq!(
            decrypt_message,
            "*3\r\n$9\r\nsubscribe\r\n$6\r\ncanal1\r\n:1\r\n"
        );
        assert!(cliente.read().unwrap().get_channels().contains("canal1"));
    }

    #[test]
    fn test02_pubsub_broker_multiple_subscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        // Suscribirse
        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "SUBSCRIBE".to_string(),
                "canal1".to_string(),
                "canal2".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        thread::sleep(Duration::from_millis(50)); // esperar al hilo
        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");

        let mut stream = Cursor::new(&buffer[..res]);

        let decrypt_message = decrypt_resp(&mut stream).unwrap();

        assert_eq!(
            decrypt_message,
            "*6\r\n$9\r\nsubscribe\r\n$6\r\ncanal1\r\n:1\r\n$9\r\nsubscribe\r\n$6\r\ncanal2\r\n:2\r\n"
        );
        assert!(cliente.read().unwrap().get_channels().contains("canal1"));
        assert!(cliente.read().unwrap().get_channels().contains("canal2"));
    }

    #[test]
    fn test03_pubsub_broker_unsubscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "SUBSCRIBE".to_string(),
                "canal1".to_string(),
                "canal2".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        let unsubscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["UNSUBSCRIBE".to_string(), "canal1".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(unsubscribe_cmd);

        thread::sleep(Duration::from_millis(50)); // esperar al hilo

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");

        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        // procesa todos los mensajes q vienen del buffer
        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert_eq!(
            all_messages,
            "*6\r\n$9\r\nsubscribe\r\n$6\r\ncanal1\r\n:1\r\n$9\r\nsubscribe\r\n$6\r\ncanal2\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$6\r\ncanal1\r\n:1\r\n"
        );
        assert!(!cliente.read().unwrap().get_channels().contains("canal1"));
        assert!(cliente.read().unwrap().get_channels().contains("canal2"));
    }

    #[test]
    fn test04_pubsub_broker_multiple_unsubscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "SUBSCRIBE".to_string(),
                "canal1".to_string(),
                "canal2".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        let unsubscribe_cmd =
            BrokerCommand::new_client_cmd(vec!["UNSUBSCRIBE".to_string()], cliente.clone(), None);
        broker.send_cmd(unsubscribe_cmd);

        thread::sleep(Duration::from_millis(50)); // esperar al hilo

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");

        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert!(all_messages.contains("$11\r\nunsubscribe\r\n$6\r\ncanal2"));
        assert!(all_messages.contains("$11\r\nunsubscribe\r\n$6\r\ncanal1"));
        assert!(all_messages.ends_with(":0\r\n"));

        assert!(!cliente.read().unwrap().get_channels().contains("canal1"));
        assert!(!cliente.read().unwrap().get_channels().contains("canal2"));
    }

    #[test]
    fn test05_pubsub_broker_publish() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        // Suscribirse
        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "PUBLISH".to_string(),
                "canal1".to_string(),
                "Hola Mundo!".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        thread::sleep(Duration::from_millis(100)); // esperar al hilo
        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        //let mensaje = String::from_utf8_lossy(&buffer[..res]);
        let mut stream = Cursor::new(&buffer[..res]);

        let decrypt_message = decrypt_resp(&mut stream).unwrap();
        assert_eq!(decrypt_message, ":0\r\n");
    }

    #[test]
    fn test06_pubsub_broker_pubsub_channels() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["SUBSCRIBE".to_string(), "canal1".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);
        thread::sleep(Duration::from_millis(50));

        let channels_cmd = BrokerCommand::new_client_cmd(
            vec!["PUBSUB".to_string(), "CHANNELS".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(channels_cmd);
        thread::sleep(Duration::from_millis(50));

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");

        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert!(all_messages.contains("*1\r\n$6\r\ncanal1\r\n"));
    }

    #[test]
    fn test07_pubsub_broker_pubsub_numsub() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["SUBSCRIBE".to_string(), "canal1".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);
        thread::sleep(Duration::from_millis(50));

        let numsub_cmd = BrokerCommand::new_client_cmd(
            vec![
                "PUBSUB".to_string(),
                "NUMSUB".to_string(),
                "canal1".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(numsub_cmd);
        thread::sleep(Duration::from_millis(50));

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert!(all_messages.contains("*2\r\n$6\r\ncanal1\r\n:1\r\n"));
    }

    #[test]
    fn test08_pubsub_broker_ssubscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..16000);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["SSUBSCRIBE".to_string(), "HOLA".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);
        thread::sleep(Duration::from_millis(50));

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert_eq!(
            all_messages,
            "*3\r\n$10\r\nssubscribe\r\n$4\r\nHOLA\r\n:1\r\n"
        );
    }

    #[test]
    fn test09_pubsub_broker_ssubscribe_no_slot() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..0);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["SSUBSCRIBE".to_string(), "HOLA".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);
        thread::sleep(Duration::from_millis(50));

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert_eq!(all_messages, "*2\r\n$4\r\nHOLA\r\n!2695\r\n");
    }

    #[test]
    fn test10_pubsub_broker_sunsubscribe() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..16000);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "SUBSCRIBE".to_string(),
                "canal1".to_string(),
                "canal2".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        let unsubscribe_cmd = BrokerCommand::new_client_cmd(
            vec!["UNSUBSCRIBE".to_string(), "canal1".to_string()],
            cliente.clone(),
            None,
        );
        broker.send_cmd(unsubscribe_cmd);

        thread::sleep(Duration::from_millis(50)); // esperar al hilo

        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");

        let mut stream = Cursor::new(&buffer[..res]);
        let mut all_messages = String::new();

        // procesa todos los mensajes q vienen del buffer
        while (stream.position() as usize) < res {
            let msg = decrypt_resp(&mut stream).unwrap();
            all_messages.push_str(&msg);
        }

        assert_eq!(
            all_messages,
            "*6\r\n$9\r\nsubscribe\r\n$6\r\ncanal1\r\n:1\r\n$9\r\nsubscribe\r\n$6\r\ncanal2\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$6\r\ncanal1\r\n:1\r\n"
        );
        assert!(!cliente.read().unwrap().get_channels().contains("canal1"));
        assert!(cliente.read().unwrap().get_channels().contains("canal2"));
    }

    #[test]
    fn test11_pubsub_broker_spublish() {
        let logger = Logger::null();
        let broker = PubSubBroker::new(logger.clone(), 0..16000);

        let (cliente, mut response_tcp) = setup_cliente("cli1", logger);

        // Suscribirse
        let subscribe_cmd = BrokerCommand::new_client_cmd(
            vec![
                "PUBLISH".to_string(),
                "canal1".to_string(),
                "Hola Mundo!".to_string(),
            ],
            cliente.clone(),
            None,
        );
        broker.send_cmd(subscribe_cmd);

        thread::sleep(Duration::from_millis(100)); // esperar al hilo
        let mut buffer = [0u8; 1024];
        let res = response_tcp
            .read(&mut buffer)
            .expect("Fallo al leer del stream");
        //let mensaje = String::from_utf8_lossy(&buffer[..res]);
        let mut stream = Cursor::new(&buffer[..res]);

        let decrypt_message = decrypt_resp(&mut stream).unwrap();
        assert_eq!(decrypt_message, ":0\r\n");
    }
}
