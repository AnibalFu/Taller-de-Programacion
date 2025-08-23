//! Este modulo contiene la implementacion del envio de mensajes frecuentes
//! al logger
use crate::client_struct::client::Client;
use crate::node::Node;
use crate::node_id::NodeId;
use logger::logger::Logger;
use std::io::{Error as IoError, Error};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, RwLockReadGuard};

/// Envia un error al aceptar conexion al logger
///
/// # Parámetros
/// * `id`: id del nodo actual
/// * `logger`: estructura logger donde se envia el error
/// * `e`: error a enviar
pub fn log_error_accepting_connection(id: String, logger: &Logger, e: std::io::Error) {
    let err_msg = format!("[{id}] Error accepting connection: {e}");
    logger.error(&err_msg, "Node");
}

/// Envia un error al fallar el bind con un socket address
///
/// # Parámetros
/// * `id`: id del nodo actual
/// * `addr`: address del socket cuya conexion fallo
/// * `logger`: estructura logger donde se envia el error
/// * `err`: error a enviar
pub fn log_bind_error(id: &str, addr: &SocketAddr, logger: &Logger, err: &IoError) {
    let msg = format!("Node {id} failed to bind to {addr}: {err}");
    logger.info(&msg, "Node");
}

/// Envia un error al obtener peer address
///
/// # Parámetros
/// * `id`: id del nodo actual
/// * `logger`: estructura logger donde se envia el error
/// * `err`: error a enviar
pub fn log_peer_addr_error(id: &str, logger: &Logger, err: &IoError) {
    let msg = format!("[{id}] Failed to get peer address: {err}");
    logger.error(&msg, "Node");
}

/// Envia un mensaje de aceptacion de conexion a un ip
///
/// # Parámetros
/// * `id`: id del nodo actual
/// * `ip`: ip de la conexion lograda con el nodo
/// * `logger`: estructura logger donde se envia el mensaje
pub fn log_connection_accepted(id: &str, ip: &str, logger: &Logger) {
    let msg = format!("[{id}] Accepted connection from [{ip}]");
    logger.info(&msg, "Node");
}

/// Envia un error al aceptar conexion al logger
///
/// # Parámetros
/// * `id`: id del nodo actual
/// * `logger`: estructura logger donde se envia el error
/// * `e`: error a enviar
pub fn log_max_clients_reached(id: &str, logger: &Logger, err_msg: &str) {
    let err_msg = format!("[{id}] {err_msg}");
    logger.info(&err_msg, "Node");
}

//////////////////////////////////////////////////////////////////////////////////////

/// Envia un error al obtener el lock de un cliente
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el error
pub fn log_failed_to_lock_client(logger: &Logger) {
    logger.error("No se pudo obtener lock del cliente", "Handle");
}

/// Envia un mensaje de cantidad de clientes al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el mensaje
/// * `count`: cantidad de clientes
pub fn log_client_count(logger: &Logger, count: usize) {
    logger.info(&format!("Cantidad clientes {count:?}"), "Handle");
}

/// Envia un mensaje de envio de respuesta al cliente al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el mensaje
/// * `respuesta`: respuesta a enviar
/// * `addr`: direccion donde se envia la respuesta
pub fn log_writer_response_send(logger: &Logger, respuesta: &str, addr: Option<SocketAddr>) {
    logger.info(
        &format!("Sending respuesta {respuesta:?} for [{addr:?}]"),
        "Writer",
    );
}

/// Envia un error de escritura al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el error
/// * `err`: error a enviar
pub fn log_writer_error(logger: &Logger, err: &str) {
    logger.error(&format!("{err:?}"), "Writer");
}

/// Envia un error al clonar un stream al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el error
/// * `e`: error a enviar
pub fn log_stream_clone_error(logger: &Logger, e: &std::io::Error) {
    logger.error(&format!("No se pudo clonar el stream: {e}"), "Stream");
}

/// Envia un error de lectura de stream al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el error
/// * `e`: error a enviar
pub fn log_stream_read_error(
    logger: &Logger,
    e: &std::sync::PoisonError<std::sync::RwLockReadGuard<'_, Client>>,
) {
    logger.error(&format!("No se pudo obtener el stream: {e}"), "Stream");
}

/// Envia un error de escritura al esperar a un hilo escritor al logger
///
/// # Parámetros
/// * `logger`: estructura logger donde se envia el error
/// * `err`: error a enviar
pub fn log_writer_join_error(logger: &Logger, e: Box<dyn std::any::Any + Send + 'static>) {
    logger.error(&format!("Error al esperar hilo escritor: {e:?}"), "Handle");
}

/// Envia un mensaje al logger de comando recibido
///
/// # Parametros
/// * `logger`: logger donde se envia el mensaje
/// * `cli`: cliente que envia el comando
/// * `comando_tokens`: comando completo recibido
pub fn log_cli_send_cmd_info(
    logger: &Logger,
    cli: RwLockReadGuard<'_, Client>,
    comando_tokens: &[String],
) {
    logger.info(
        &format!(
            "Comando de [{}] recibido: {:?}",
            cli.client_id(),
            comando_tokens
        ),
        "Procesar",
    )
}

/// Envia mensaje al logger si el nodo se inicio correctamente
///
/// # Parametros
/// * `logger`: logger donde se envia el mensaje
/// * `node`: nodo iniciado
pub fn log_nodo_start(logger: &Logger, node: &Node) {
    logger.info(&format!("Node correct init: {}", node.node_info()), "Node");
}

/// Envia al logger para un cliente puntual los canales a los que esta subscrito
///
/// # Parametros
/// * `logger`: logger donde se envia el mensaje
/// * `client`: cliente que a ver sus canales.
pub fn log_cli_subscribe_channels(logger: &Logger, client: &Arc<RwLock<Client>>) {
    if let Ok(cli) = client.read() {
        logger.info(
            &format!(
                "Client [{}] subscribe channels: {:?} pchannels: {:?} schannels {:?}",
                cli.client_id(),
                cli.get_channels(),
                cli.get_pchannels(),
                cli.get_schannels()
            ),
            "Procesar",
        );
    }
}

/// Escribe en el logger el error correspondiente
/// a un fallo en la conexion con el nodo semilla
///
/// Parametros
/// - `e`: error obtenido
/// - `s`: socket al cual conectarse
pub fn loggear_error_conectar_otro_nodo(logger: &Logger, e: Error, s: SocketAddr) {
    logger.info(&format!("No se pudo conectar al nodo [{s}]: {e}"), "INIT");
}

/// Loggea un error al realizar un handshake entre nodos
pub fn loggear_error_handhsake(logger: &Logger) {
    logger.error("Error en handshake", "INIT");
}

/// Loggea un error al leer el rol del nodo
///
/// # Parametros
/// * `modulo`: módulo del flujo del programa al que corresponde el error
pub fn loggear_error_lectura_rol(logger: &Logger, modulo: &str) {
    logger.error("Error al leer rol", modulo)
}

/// Loggea un error al leer el master del nodo
///
/// # Parametros
/// * `modulo`: módulo del flujo del programa al que corresponde el error
pub fn loggear_error_lectura_master(logger: &Logger, modulo: &str) {
    logger.error("Error al leer master", modulo)
}

pub fn loggear_error_recibir_replica(logger: &Logger, id: NodeId) {
    logger.error(&format!("Error al recibir replica [{id:?}]"), "INIT");
}
