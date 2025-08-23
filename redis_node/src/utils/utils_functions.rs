//! Este modulo tiene funciones auxiliares para redis_node
use crate::client_struct::client::Client;
use crate::comandos::comandos_list::{
    lindex, linsert, llen, lmove, lpop, lpush, lrange, lrem, lset, ltrim, rpop, rpush,
};
use crate::comandos::comandos_set::{sadd, scard, sismember, smembers, srem};
use crate::comandos::comandos_string::{append, decr, del, get, getdel, incr, set, strlen, substr};
use crate::comandos::const_cmd::*;
use crate::comandos::handshake::{auth, hello};
use crate::comandos::pub_sub_struct::{PubSubBroker, PubSubCore};
use crate::log_msj::log_mensajes::{log_stream_clone_error, log_stream_read_error};
use crate::storage::Storage;
use logger::logger::Logger;
use redis_client::tipos_datos::traits::DatoRedis;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock};

pub type CommandFunction = fn(&[String], &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis>;

type PubSubMethod =
    fn(&mut PubSubCore, &[String], Arc<RwLock<Client>>) -> Result<DatoRedis, DatoRedis>;

pub type HandshakeFunction = fn(
    &[String],
    &mut Arc<RwLock<Client>>,
    &HashMap<String, String>,
) -> Result<DatoRedis, DatoRedis>;

/// Clona un TCP stream recibido, escribe en el logger en caso de error
///
/// # Parámetros
/// * `stream`: stream a clonar
/// * `logger`: estructura logger donde se envia el error de haberlo
///  
/// # Retorna
/// - Option del nuevo stream en caso de obtenerlo, None en otro caso
pub fn clonar_stream(stream: &TcpStream, logger: &Logger) -> Option<TcpStream> {
    match stream.try_clone() {
        Ok(clone) => Some(clone),
        Err(e) => {
            log_stream_clone_error(logger, &e);
            None
        }
    }
}

/// Obtiene (y clona) un TCP stream recibido, escribe en el logger en
/// caso de error
///
/// # Parámetros
/// * `client`: cliente cuyo stream se quiere obtener
/// * `logger`: estructura logger donde se envia el error de haberlo
///  
/// # Retorna
/// - Option del nuevo stream en caso de obtenerlo, None en otro caso
pub fn obtener_stream(client: &Arc<RwLock<Client>>, logger: &Logger) -> Option<TcpStream> {
    match client.read() {
        Ok(guard) => clonar_stream(guard.get_stream(), logger),
        Err(e) => {
            log_stream_read_error(logger, &e);
            None
        }
    }
}

/// Obtiene un diccionario de funciones de handshake
///  
/// # Retorna
/// - Diccionario de funciones (pares de tipo (comando, funcion_asociada))
pub fn handshake_functions(cmd: &str) -> Option<HandshakeFunction> {
    match cmd {
        CMD_HELLO => Some(hello),
        CMD_AUTH => Some(auth),
        _ => None,
    }
}

/// Obtiene el metodo asociada a un nombre de comando de tipo pub sub
///
/// # Parámetros
/// - Nombre del comando de pub sub
///  
/// # Retorna
/// - Metodo de pub sub
pub fn obtener_fn_pub_sub(nombre: &str) -> Option<PubSubMethod> {
    match nombre {
        CMD_SUBSCRIBE | CMD_PSUBSCRIBE | CMD_SSUBSCRIBE => Some(PubSubCore::subscribe),
        CMD_PUBLISH | CMD_SPUBLISH => Some(PubSubCore::publish),
        CMD_UNSUBSCRIBE | CMD_PUNSUBSCRIBE | CMD_SUNSUBSCRIBE => Some(PubSubCore::unsubscribe),
        CMD_PUBSUB => Some(PubSubCore::pub_sub),
        _ => None,
    }
}

/// Obtiene la función asociada a un nombre de comando general
///  
/// # Parámetros
/// - Nombre del comando
///
/// # Retorna
/// - Función del comando
pub fn obtener_fn_normal(nombre_comando: &str) -> Option<CommandFunction> {
    match nombre_comando {
        CMD_GET => Some(get),
        CMD_SET => Some(set),
        CMD_DEL => Some(del),
        CMD_GETDEL => Some(getdel),
        CMD_APPEND => Some(append),
        CMD_STRLEN => Some(strlen),
        CMD_SUBSTR => Some(substr),
        CMD_GETRANGE => Some(substr),
        CMD_INCR => Some(incr),
        CMD_DECR => Some(decr),
        CMD_LINSERT => Some(linsert),
        CMD_LPUSH => Some(lpush),
        CMD_RPUSH => Some(rpush),
        CMD_LLEN => Some(llen),
        CMD_LPOP => Some(lpop),
        CMD_RPOP => Some(rpop),
        CMD_LRANGE => Some(lrange),
        CMD_LSET => Some(lset),
        CMD_LREM => Some(lrem),
        CMD_LTRIM => Some(ltrim),
        CMD_LINDEX => Some(lindex),
        CMD_LMOVE => Some(lmove),
        CMD_SADD => Some(sadd),
        CMD_SCARD => Some(scard),
        CMD_SISMEMBER => Some(sismember),
        CMD_SREM => Some(srem),
        CMD_SMEMBERS => Some(smembers),
        _ => None,
    }
}

/// Abre el aofile, escribe en el logger de haber error
///
/// # Parametros
/// * `path`: path al aofile
/// * `logger`: logger donde enviar mensajes
///
/// # Retorna
/// - Option del File de poder abirlo, None en otro caso
pub fn abrir_persistence_file(path: &str, logger: &Logger, tipo: String) -> Option<File> {
    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => Some(file),
        Err(e) => {
            logger.error(
                &format!("No se pudo abrir archivo {tipo} {path}: {e}"),
                "Procesar",
            );
            None
        }
    }
}

pub fn sumar_puerto(addr: &SocketAddr, delta: u16) -> SocketAddr {
    let new_port = addr.port() + delta;
    SocketAddr::new(addr.ip(), new_port)
}

/// Desconecta a un cliente del nodo, desuscribiendolo de todos los canales
///
/// # Parametros
/// * `client`: cliente a desconectar
/// * `channels`: canales del nodo
/// * `act_client_active`: cantidad de clientes conectados al nodo
/// * `logger`: logger donde enviar mensajes
pub fn limpiar_cliente_desconectado(
    client: Arc<RwLock<Client>>,
    pub_sub: &PubSubBroker,
    act_client_active: &Arc<AtomicUsize>,
    logger: &Logger,
) {
    pub_sub.borrar_canales_cliente(client.clone());
    if let Ok(mut cli) = client.write() {
        logger.info("Initialize shutdown", "Shutdown");
        let stream_guard = cli.get_stream();
        if let Ok(ip) = stream_guard.peer_addr() {
            logger.info(&format!("Client: [{ip:?}] disconnected"), "Shutdown");
        }
        let stream = cli.get_stream();
        let _ = stream.shutdown(Shutdown::Both);
        drop(cli.get_sender());
        logger.info("Finished shutdown", "Shutdown");
    }
    act_client_active.fetch_sub(1, SeqCst);
    logger.info(
        &format!("Number of clients {:?}", act_client_active.load(SeqCst)),
        "Shutdown",
    );
}
