//! Este modulo contiene a la estructura cliente
use crate::comandos::pub_sub_struct::Canal;
use crate::log_msj::log_mensajes::{log_writer_error, log_writer_response_send};
use crate::node_role::NodeRole;
use crate::utils::utils_functions::clonar_stream;
use logger::logger::Logger;
use redis_client::protocol::protocol_resp::resp_server_command_write;
use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
use std::collections::HashSet;
use std::net::TcpStream;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock, mpsc};
use std::thread::spawn;
// pub type Clients = HashMap<String, Arc<RwLock<Client>>>;

#[derive(Debug)]
pub struct Client {
    id: String,
    stream: TcpStream,
    channels: HashSet<Canal>,
    pchannels: HashSet<Canal>,
    schannels: HashSet<Canal>,
    sender: Sender<String>,
    type_of_node_connections: Arc<RwLock<NodeRole>>,
    handshake: bool,
}

impl Client {
    // Este new deberia devolver un error si no se puede clonar el stream
    /// Crea un nuevo cliente
    ///
    /// # Parametros
    /// * `id`: id del cliente
    /// * `stream`: stream asociado al cliente
    /// * `logger`: logger donde enviar los mensajes necesarios
    ///
    /// # Retorna
    /// - Un struct cliente
    pub fn new(
        id: String,
        stream: TcpStream,
        logger: Logger,
        node_role: Arc<RwLock<NodeRole>>,
    ) -> Client {
        let (tx_sender, rx_receiver) = mpsc::channel();

        match intentar_clonar_stream(&stream, &logger) {
            Ok(stream_writer) => {
                spawn_hilo_escritor(rx_receiver, logger.clone(), stream_writer);
            }
            Err(_) => {
                log_writer_error(&logger, "No se pudo clonar el stream del cliente");
                // Si falla el clonado, devolvemos un cliente sin el hilo escritor
            }
        }

        Client {
            id,
            stream,
            channels: HashSet::new(),
            pchannels: HashSet::new(),
            schannels: HashSet::new(),
            sender: tx_sender,
            type_of_node_connections: node_role,
            handshake: false,
        }
    }

    /// Obtiene el sender del cliente
    ///
    /// # Retorna
    /// - Sender del cliente
    pub fn get_sender(&mut self) -> Sender<String> {
        self.sender.clone()
    }

    /// Envia un mensaje al sender
    ///
    /// # Parametros
    /// * `mensaje`: mensaje a enviar
    ///
    /// # Retorna
    /// - Sender del cliente
    pub fn send_sender(&self, mensaje: String) -> Result<(), DatoRedis> {
        self.sender
            .send(mensaje)
            .map_err(|e| DatoRedis::new_simple_error("Err".to_string(), format!("{e}")))?;
        Ok(())
    }

    /// Obtiene el id del cliente
    ///
    /// # Retorna
    /// - id del cliente
    pub fn client_id(&self) -> String {
        self.id.to_string()
    }

    /// Agrega un canal disponible para el cliente
    ///
    /// # Parametros
    /// - canal: canala a agregar
    pub fn add_channel(&mut self, canal: Canal) {
        self.channels.insert(canal);
    }
    pub fn add_pchannel(&mut self, canal: Canal) {
        self.pchannels.insert(canal);
    }
    pub fn add_schannel(&mut self, canal: Canal) {
        self.schannels.insert(canal);
    }

    /// Obtiene los canales a los que esta suscripto el cliente
    ///
    /// # Retorna
    /// - canales del cliente
    pub fn get_channels(&self) -> &HashSet<String> {
        &self.channels
    }
    pub fn get_pchannels(&self) -> &HashSet<String> {
        &self.pchannels
    }
    pub fn get_schannels(&self) -> &HashSet<String> {
        &self.schannels
    }

    /// Elimina un canal disponible para el cliente
    ///
    /// # Parametros
    /// - canal: canal a eliminar
    pub fn remove_channel(&mut self, canal: &Canal) {
        self.channels.remove(canal);
    }
    pub fn remove_pchannel(&mut self, canal: &Canal) {
        self.pchannels.remove(canal);
    }
    pub fn remove_schannel(&mut self, canal: &Canal) {
        self.schannels.remove(canal);
    }

    /// Obtiene el stream del cliente
    ///
    /// # Retorna
    /// - stream del cliente
    pub fn get_stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Retorna si el cliente se encuentra en modo pub/sub
    ///
    /// # Retorna
    /// - verdadero si el cliente se encuentra en modo pub/sub, falso
    ///   en otro caso
    pub fn get_modo_pub_sub(&self) -> bool {
        !self.channels.is_empty()
    }

    /// Determina si el estado de handshake entre nodo y cliente es verdadero
    ///
    /// # Retorna
    /// - Verdadero si el handshake fue exitoso, falso en otro caso
    pub fn get_handshake(&self) -> bool {
        self.handshake
    }

    /// Asigna un estado para el handshake cliente-nodo
    ///
    /// # Parametros
    /// * `handshake`: valor a asignar
    pub fn set_handshake(&mut self, handshake: bool) {
        self.handshake = handshake;
    }

    pub fn get_type_of_node_connections(&self) -> Arc<RwLock<NodeRole>> {
        self.type_of_node_connections.clone()
    }
}

/// Intenta clonar un stream
///
/// # Parametros
/// * `stream`: stream a clonar
/// * `logger`: logger donde escribir en caso de ser necesario
///
/// # Retorna
/// - Result del stream clonado
fn intentar_clonar_stream(stream: &TcpStream, logger: &Logger) -> Result<TcpStream, ()> {
    match clonar_stream(stream, logger) {
        Some(s) => Ok(s),
        None => Err(()),
    }
}

/// Realiza el spawn del hilo escritor
///
/// # Parametros
/// * `rx_receiver`: donde se recibiran las respuestas a escribir
/// * `logger`: logger donde escribir en caso de ser necesario
/// * `stream_writer`: stream donde escribir
fn spawn_hilo_escritor(
    rx_receiver: Receiver<String>,
    logger: Logger,
    mut stream_writer: TcpStream,
) {
    spawn(move || {
        while let Ok(respuesta) = rx_receiver.recv() {
            log_writer_response_send(&logger, &respuesta, stream_writer.peer_addr().ok());
            if let Err(result) = resp_server_command_write(&respuesta, &mut stream_writer) {
                log_writer_error(&logger, &result.convertir_resp_a_string());
                break;
            }
        }
    });
}
