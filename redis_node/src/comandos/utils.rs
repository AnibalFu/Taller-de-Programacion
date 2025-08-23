//! Este módulo contiene funciones generales para el manejo de comandos

use std::{
    collections::HashMap,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use logger::logger::Logger;
use redis_client::{
    protocol::protocol_resp::resp_server_command_read,
    tipos_datos::traits::{DatoRedis, TipoDatoRedis},
};

use crate::{client_struct::client::Client, storage::Storage};
use crate::{comandos::const_cmd::*, utils::utils_functions::handshake_functions};

/// Representa metadatos asociados a un comando Redis personalizado.
///
/// - `indices_datos`: los índices dentro del comando que corresponden a los datos (por ejemplo, claves o valores).
/// - `es_mutable`: indica si el comando modifica el estado del almacenamiento.
pub struct ComandoMetadata {
    pub(crate) indices_datos: Vec<usize>,
    pub(crate) es_mutable: bool,
}

/// Crea una nueva instancia de `ComandoMetadata`.
///
/// # Argumentos
/// - `indices`: Índices relevantes del vector de tokens que representan datos.
/// - `es_mutable`: `true` si el comando modifica el estado, `false` si es de solo lectura.
///
/// # Retorna
/// Una instancia de `ComandoMetadata` con los parámetros especificados.
fn create_comando_metadata(indices: Vec<usize>, es_mutable: bool) -> ComandoMetadata {
    ComandoMetadata {
        indices_datos: indices,
        es_mutable,
    }
}

/// Devuelve los metadatos correspondientes a un comando Redis.
///
/// Define qué posiciones del comando contienen datos importantes (`indices_datos`)
/// y si el comando modifica el estado (`es_mutable`).
///
/// # Argumentos
/// - `comando`: Nombre del comando en formato string.
/// - `tokens`: Lista de tokens que componen el comando completo (ej: `["SADD", "clave", "valor"]`).
///
/// # Retorna
/// Un `ComandoMetadata` con la información correspondiente al comando dado.
pub fn get_comando_metadata(comando: &str, tokens: &[String]) -> ComandoMetadata {
    match comando.to_uppercase().as_str() {
        CMD_SET | CMD_APPEND => create_comando_metadata(vec![2], true),
        CMD_DEL | CMD_GETDEL | CMD_INCR | CMD_DECR | CMD_LPOP | CMD_RPOP | CMD_LTRIM
        | CMD_LMOVE => create_comando_metadata(vec![], true),
        CMD_SUBSTR | CMD_GETRANGE | CMD_GET | CMD_STRLEN | CMD_LLEN | CMD_LRANGE | CMD_LINDEX
        | CMD_SCARD | CMD_SMEMBERS => create_comando_metadata(vec![], false),
        CMD_LINSERT => create_comando_metadata(vec![3, 4], true),
        CMD_LPUSH | CMD_RPUSH | CMD_SADD | CMD_SREM => {
            create_comando_metadata((2..tokens.len()).collect(), true)
        }
        CMD_LSET | CMD_LREM => create_comando_metadata(vec![3], true),
        CMD_SISMEMBER => create_comando_metadata(vec![2], false),
        _ => create_comando_metadata(vec![], false),
    }
}

/// Determina si el largo deseado (expected) concuerda con el valor obtenido
/// (len) y lanza un error de redis si no coinciden
///
/// # Parámetros
/// * `key`: clave que se busca acceder
/// * `expected`: largo deseado
/// * `len`: largo real
///
/// # Retorna
/// - () en caso de que el valor real sea menor al esperadp, error de
///   redis en otro caso
pub fn assert_correct_arguments_quantity(
    key: String,
    expected: usize,
    len: usize,
) -> Result<(), DatoRedis> {
    if len < expected {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            format!("wrong number of arguments for '{key}' command"),
        ));
    }
    Ok(())
}

/// Determina si el parametro greater concuerda con el valor obtenido
/// (len) y lanza un error de redis si no coinciden
///
/// # Parámetros
/// * `key`: clave que se busca acceder
/// * `greater`: largo deseado
/// * `len`: largo real
///
/// # Retorna
/// - () en caso de que ambos largos concuerden, error de redis en
///   otro caso
pub fn assert_number_of_arguments_distinct(
    key: String,
    greater: usize,
    len: usize,
) -> Result<(), DatoRedis> {
    if len != greater {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            format!("wrong number of arguments for '{key}' command"),
        ));
    }
    Ok(())
}

/// Envia un mensaje de cantidad de argumentos incorrecta al logger
///
/// # Parametros
/// * `client`: cliente cuya comando causo el error
/// * `logger`: logger donde enviar mensajes
pub fn send_msj_to_logger(client: &Arc<RwLock<Client>>, logger: &Logger) {
    send_msj(
        client.clone(),
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "wrong number of arguments for command".to_string(),
        ),
        logger,
    );
}

/// Envia un mensaje al cliente, escribe en el logger de ser necesario
///
/// # Parametros
/// * `client`: cliente a notificar
/// * `mensaje`: mensaje a enviar
/// * `logger`: logger donde enviar mensajes
pub fn send_msj(cli: Arc<RwLock<Client>>, mensaje: DatoRedis, logger: &Logger) {
    match cli.read() {
        Ok(cli_guard) => {
            let resp = mensaje.convertir_a_protocolo_resp();
            if cli_guard.send_sender(resp.to_string()).is_err() {
                logger.error(
                    &format!(
                        "Error al enviar mensaje al cliente: {}",
                        cli_guard.client_id()
                    ),
                    "Send",
                );
            }
        }
        Err(e) => {
            logger.error(
                &format!("Error obteniendo lock de cliente para enviar mensaje: {e}"),
                "Send",
            );
        }
    }
}

pub fn do_handshake(
    client: &Arc<RwLock<Client>>,
    comando_tokens: &[String],
    logger: &Logger,
    users: &HashMap<String, String>,
) {
    let data_to_send =
        if let Some(function) = handshake_functions(&comando_tokens[0].to_uppercase()) {
            function(comando_tokens, &mut client.clone(), users).unwrap_or_else(|error| error)
        } else {
            DatoRedis::new_simple_error("NOAUTH".to_string(), "Authentication required".to_string())
        };

    match data_to_send {
        DatoRedis::SimpleString(ref s) if s.contenido() == "OK" => {
            send_msj(client.clone(), data_to_send, logger);
        }
        _ => {
            send_msj(client.clone(), data_to_send, logger);
        }
    }
}

/// Obtiene un vector de tokens (strings) que conforman un comando
///
/// # Parametros
/// * `reader`: TcpStream donde se encuentra el comando
///
/// # Retorna
/// - Option de vector de strings en caso de exito, None en otro caso
pub fn leer_comando(reader: &mut TcpStream) -> Option<Vec<String>> {
    resp_server_command_read(reader).ok()
}

pub fn get_storage_write_lock(
    storage: &Arc<RwLock<Storage>>,
) -> Result<std::sync::RwLockWriteGuard<'_, Storage>, DatoRedis> {
    storage.write().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "No se pudo obtener el lock de escritura".to_string(),
        )
    })
}

pub fn get_storage_read_lock(
    storage: &Arc<RwLock<Storage>>,
) -> Result<std::sync::RwLockWriteGuard<'_, Storage>, DatoRedis> {
    storage.write().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "No se pudo obtener el lock de lectura".to_string(),
        )
    })
}
