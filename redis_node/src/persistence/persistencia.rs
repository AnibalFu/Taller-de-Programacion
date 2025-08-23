//! Este módulo contiene la lógica principal de persistencia de la metadata
//! y el storage del nodo
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    net::{SocketAddr, ToSocketAddrs},
    ops::Range,
    sync::{Arc, RwLock, atomic::AtomicUsize},
    thread,
    time::Duration,
};

use logger::logger::Logger;
use redis_client::protocol::{
    dataencryption::{decrypt_from_hex, encrypt_y_encode_hex},
    protocol_resp::parsear_comando,
};
use redis_client::tipos_datos::traits::DatoRedis;

use crate::{
    comandos::pub_sub_struct::PubSubBroker,
    utils::utils_functions::{obtener_fn_normal, sumar_puerto},
};
use crate::{comandos::utils::get_comando_metadata, node_builder::NodeBuilder};
use crate::{
    node::Node, node_id::NodeId, node_role::NodeRole, node_status::NodeStatus, storage::Storage,
};

/// Guarda el `NodeId` de un nodo en el archivo de persistencia.
///
/// # Parámetros
/// - `writer`: Writer con buffer hacia el archivo destino.
/// - `id`: Identificador del nodo a guardar.
/// - `logger`: Logger para registrar errores en caso de fallo.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la escritura del ID en el archivo.
pub fn guardar_id(
    writer: &mut BufWriter<File>,
    id: &NodeId,
    logger: &Logger,
) -> Result<(), io::Error> {
    match writer.write_all(&id.to_bytes()) {
        Ok(_) => Ok(()),
        Err(e) => {
            logger.error(
                "Error guardando ID del nodo en archivo de persistencia",
                "Persistence",
            );
            Err(e)
        }
    }
}

/// Restaura el `NodeId` de un nodo leyendo desde un archivo de persistencia.
///
/// # Parámetros
/// - `reader`: Reader con buffer desde el archivo fuente.
///
/// # Retorna
/// El `NodeId` reconstruido desde los bytes leídos.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la lectura del archivo o los datos son inválidos.
fn restaurar_id(reader: &mut BufReader<File>) -> Result<NodeId, io::Error> {
    let mut buffer = [0; 40]; // Tamaño del ID
    reader.read_exact(&mut buffer)?;
    Ok(NodeId::from_bytes(&buffer))
}

/// Guarda el rol del nodo (`NodeRole`) en el archivo de persistencia.
///
/// # Parámetros
/// - `writer`: Writer con buffer hacia el archivo destino.
/// - `role`: Rol del nodo a guardar.
/// - `logger`: Logger para registrar errores en caso de fallo.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la escritura del rol.
pub fn guardar_role(
    writer: &mut BufWriter<File>,
    role: &NodeRole,
    logger: &Logger,
) -> Result<(), io::Error> {
    match writer.write_all(&role.to_bytes()) {
        Ok(_) => Ok(()),
        Err(e) => {
            logger.error(
                "Error guardando rol del nodo en archivo de persistencia",
                "Persistence",
            );
            Err(e)
        }
    }
}

/// Restaura el rol del nodo (`NodeRole`) desde un archivo de persistencia.
///
/// # Parámetros
/// - `reader`: Reader con buffer desde el archivo fuente.
///
/// # Retorna
/// El rol reconstruido desde los bytes leídos.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la lectura o los datos son inválidos.
fn restaurar_role(reader: &mut BufReader<File>) -> Result<NodeRole, io::Error> {
    let mut buffer = [0; 7]; // Tamaño del rol
    reader.read_exact(&mut buffer)?;
    let role = NodeRole::from_bytes(&buffer)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid role"))?;
    Ok(role)
}

/// Guarda el estado (`NodeStatus`) del nodo en el archivo de persistencia.
///
/// # Parámetros
/// - `writer`: Writer con buffer hacia el archivo destino.
/// - `status`: Estado del nodo a guardar.
/// - `logger`: Logger para registrar errores en caso de fallo.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la escritura del estado.
pub fn guardar_status(
    writer: &mut BufWriter<File>,
    status: &NodeStatus,
    logger: &Logger,
) -> Result<(), io::Error> {
    match writer.write_all(&status.to_bytes()) {
        Ok(_) => Ok(()),
        Err(e) => {
            logger.error(
                "Error guardando estado del nodo en archivo de persistencia",
                "Persistence",
            );
            Err(e)
        }
    }
}

/// Restaura el estado del nodo (`NodeStatus`) desde un archivo de persistencia.
///
/// # Parámetros
/// - `reader`: Reader con buffer desde el archivo fuente.
///
/// # Retorna
/// El estado reconstruido desde los bytes leídos.
///
/// # Errores
/// Devuelve un error de tipo `io::Error` si falla la lectura o los datos son inválidos.
fn restaurar_status(reader: &mut BufReader<File>) -> Result<NodeStatus, io::Error> {
    let mut buffer = [0; 4]; // Tamaño del status
    reader.read_exact(&mut buffer)?;
    let status = NodeStatus::from_bytes(&buffer)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid status"))?;
    Ok(status)
}

/// Guarda el rango de slots de un nodo en el archivo de persistencia.
///
/// # Parámetros
/// - `writer`: Writer con buffer hacia el archivo de persistencia.
/// - `slot_range`: Rango de slots a guardar.
/// - `logger`: Logger para registrar errores en caso de fallo.
///
/// # Errores
/// Retorna un `io::Error` si ocurre un problema durante la escritura.
pub fn guardar_slot_range(
    writer: &mut BufWriter<File>,
    slot_range: Range<u16>,
    logger: &Logger,
) -> Result<(), io::Error> {
    match guardar_range(&slot_range, writer) {
        Ok(_) => Ok(()),
        Err(e) => {
            logger.error(
                "Error guardando rango de slots del nodo en archivo de persistencia",
                "Persistence",
            );
            Err(e)
        }
    }
}

/// Restaura un rango de slots (`<Range<u16>>`) desde un archivo de persistencia.
///
/// # Parámetros
/// - `reader`: Reader con buffer desde el archivo de persistencia.
///
/// # Retorna
/// El rango de slots restaurado o `None` si no había rango.
///
/// # Errores
/// Retorna un `io::Error` si ocurre un problema durante la lectura.
fn restaurar_slot_range(reader: &mut BufReader<File>) -> Result<Range<u16>, io::Error> {
    let mut buffer = [0; 2];
    reader.read_exact(&mut buffer)?;
    let start = u16::from_be_bytes(buffer);

    reader.read_exact(&mut buffer)?;
    let end = u16::from_be_bytes(buffer);

    Ok(start..end)
}

/// Guarda un rango `<&Range<u16>>` en el archivo usando un formato binario simple.
///
/// # Errores
/// Retorna un `io::Error` si ocurre un problema durante la escritura.
fn guardar_range(range: &Range<u16>, writer: &mut BufWriter<File>) -> Result<(), io::Error> {
    writer.write_all(&range.start.to_be_bytes())?;
    writer.write_all(&range.end.to_be_bytes())?;
    Ok(())
}

pub fn guardar_save_interval(
    writer: &mut BufWriter<File>,
    save_interval: u64,
) -> Result<(), io::Error> {
    writer.write_all(&save_interval.to_be_bytes())?;
    Ok(())
}

fn restaurar_save_interval(reader: &mut BufReader<File>) -> Result<u64, io::Error> {
    let mut buffer = [0; 8];
    reader.read_exact(&mut buffer)?;
    Ok(u64::from_be_bytes(buffer))
}

pub fn guardar_max_clients(
    writer: &mut BufWriter<File>,
    max_clients: usize,
) -> Result<(), io::Error> {
    writer.write_all(&max_clients.to_be_bytes())?;
    Ok(())
}

fn restaurar_max_clients(reader: &mut BufReader<File>) -> Result<usize, io::Error> {
    let mut buffer = [0; 8];
    reader.read_exact(&mut buffer)?;
    Ok(usize::from_be_bytes(buffer))
}

pub fn guardar_logger_path(
    writer: &mut BufWriter<File>,
    logger_path: &str,
) -> Result<(), io::Error> {
    let logger_path_bytes = logger_path.as_bytes();
    let logger_path_len = logger_path_bytes.len() as u32;
    writer.write_all(&logger_path_len.to_be_bytes())?;
    writer.write_all(logger_path_bytes)?;
    Ok(())
}

fn restaurar_logger_path(reader: &mut BufReader<File>) -> Result<String, io::Error> {
    let mut length_buf = [0; 4];
    reader.read_exact(&mut length_buf)?;
    let length = u32::from_be_bytes(length_buf) as usize;

    let mut path_buf = vec![0; length];
    reader.read_exact(&mut path_buf)?;
    let path = String::from_utf8_lossy(&path_buf).to_string();

    Ok(path)
}

pub fn guardar_node_timeout(
    writer: &mut BufWriter<File>,
    node_timeout: u64,
) -> Result<(), io::Error> {
    writer.write_all(&node_timeout.to_be_bytes())?;
    Ok(())
}

fn restaurar_node_timeout(reader: &mut BufReader<File>) -> Result<u64, io::Error> {
    let mut buffer = [0; 8];
    reader.read_exact(&mut buffer)?;
    let millis = u64::from_be_bytes(buffer);
    Ok(millis)
}

pub fn guardar_address(
    writer: &mut BufWriter<File>,
    cluster_addr: &SocketAddr,
) -> Result<(), io::Error> {
    let binding = cluster_addr.to_string();
    let addr_bytes = binding.as_bytes();
    let addr_len = addr_bytes.len() as u32;
    writer.write_all(&addr_len.to_be_bytes())?;
    writer.write_all(addr_bytes)?;
    Ok(())
}

fn restaurar_address(reader: &mut BufReader<File>) -> Result<SocketAddr, io::Error> {
    let mut length_buf = [0; 4];
    reader.read_exact(&mut length_buf)?;
    let length = u32::from_be_bytes(length_buf) as usize;

    let mut addr_buf = vec![0; length];
    reader.read_exact(&mut addr_buf)?;
    let addr = String::from_utf8_lossy(&addr_buf).to_string();

    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid address format"))?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No address found"))?;
    Ok(socket_addr)
}

/// Guarda el contenido completo del `Storage` del nodo en el archivo de persistencia.
///
/// # Errores
/// Retorna un `io::Error` si ocurre un problema durante la escritura.
pub fn guardar_storage(writer: &mut BufWriter<File>, storage: &Storage) -> Result<(), io::Error> {
    guardar_range(&storage.get_slot_range(), writer)?;

    for (slot, mapa) in storage.iter() {
        writer.write_all(&slot.to_be_bytes())?;

        let cantidad = mapa.len() as u32;
        writer.write_all(&cantidad.to_be_bytes())?;

        for (key, dato) in mapa {
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u32;
            writer.write_all(&key_len.to_be_bytes())?;
            writer.write_all(key_bytes)?;

            let dato_bytes = dato.to_bytes();
            let dato_len = dato_bytes.len() as u32;
            writer.write_all(&dato_len.to_be_bytes())?;
            writer.write_all(&dato_bytes)?;
        }
    }

    Ok(())
}

/// Restaura el contenido completo del `Storage` desde un archivo binario de persistencia.
///
/// # Parámetros
/// - `reader`: Reader con buffer desde el archivo de persistencia.
///
/// # Retorna
/// Una nueva instancia de `Storage` con el contenido restaurado.
///
/// # Errores
/// Retorna un `io::Error` si hay problemas de lectura, formato incorrecto o datos corruptos.
fn restaurar_storage_bin(reader: &mut BufReader<File>) -> Result<Storage, io::Error> {
    let slot_range = restaurar_slot_range(reader)?;

    let mut hashes_slots = HashMap::new();

    while reader.fill_buf().is_ok() {
        let mut slot_buf = [0u8; 2];
        if reader.read_exact(&mut slot_buf).is_err() {
            break;
        }
        let slot = u16::from_be_bytes(slot_buf);

        let mut cantidad_buf = [0u8; 4];
        reader.read_exact(&mut cantidad_buf)?;
        let cantidad = u32::from_be_bytes(cantidad_buf);

        let mut mapa = HashMap::new();
        for _ in 0..cantidad {
            reader.read_exact(&mut cantidad_buf)?;
            let key_len = u32::from_be_bytes(cantidad_buf) as usize;

            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf)?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            reader.read_exact(&mut cantidad_buf)?;
            let dato_len = u32::from_be_bytes(cantidad_buf) as usize;

            let mut dato_buf = vec![0u8; dato_len];
            reader.read_exact(&mut dato_buf)?;
            let dato = DatoRedis::from_encrypted_bytes(&dato_buf).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Error en DatoRedis: {e:?}"),
                )
            })?;

            mapa.insert(key, dato);
        }

        hashes_slots.insert(slot, mapa);
    }

    Ok(Storage::new_with_content(slot_range, hashes_slots))
}

/// Guarda una operación mutante en el archivo AOF.
///
/// Solo se guardan operaciones que no estén dentro del conjunto de operaciones no mutables.
/// La operación se convierte en una cadena y se guarda como una línea en el archivo.
///
/// # Parámetros
/// - `file`: Referencia mutable al archivo AOF donde se guarda la operación.
/// - `operacion`: Vector de strings que representa la operación ejecutada (ej: `["SET", "clave", "valor"]`).
/// - `operaciones_no_mutables`: Conjunto con los nombres de operaciones que no deben persistirse.
///
/// # Errores
/// Devuelve un error si falla al escribir en el archivo.
pub fn guardar_operacion(
    file: &Arc<RwLock<File>>,
    operacion: Vec<String>,
) -> Result<(), io::Error> {
    let metadata = get_comando_metadata(&operacion[0], &operacion);
    if !metadata.es_mutable {
        return Ok(());
    }
    let mut tokens_encriptados = operacion.to_vec();

    for &i in &metadata.indices_datos {
        match encrypt_y_encode_hex(&tokens_encriptados[i]) {
            Ok(hex) => tokens_encriptados[i] = hex,
            Err(_) => {
                io::Error::other("Error al encriptar datos AOF");
            }
        }
    }

    let op_string = tokens_encriptados.join(" ");
    let mut file_guard = file
        .write()
        .map_err(|_| io::Error::other("Error al obtener el lock del archivo AOF"))?;
    writeln!(file_guard, "{op_string}")?;
    Ok(())
}

/// Restaura las operaciones desde un archivo AOF.
///
/// Lee el archivo línea por línea, separando cada línea en tokens (palabras) para formar
/// una lista de comandos a ejecutar.
///
/// # Parámetros
/// - `file`: Archivo AOF a leer.
///
/// # Retorna
/// Un vector de operaciones, donde cada operación es un vector de strings.
///
/// # Errores
/// - Si ocurre un error de E/S.
/// - Si el archivo está vacío, se devuelve un error de `UnexpectedEof`.
fn restaurar_operaciones(file: File) -> Result<Vec<Vec<String>>, io::Error> {
    let reader = BufReader::new(file);
    let mut operaciones = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let linea = parsear_comando(line);

        let metadata = get_comando_metadata(&linea[0], &linea);
        let mut desencriptado = linea.to_vec();

        for &i in &metadata.indices_datos {
            if i < linea.len() {
                match decrypt_from_hex(&linea[i]) {
                    Ok(descifrado) => desencriptado[i] = descifrado,
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
        operaciones.push(desencriptado);
    }

    if operaciones.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "El archivo está vacío",
        ));
    }

    Ok(operaciones)
}

/// Reconstruye el estado del almacenamiento (`Storage`) aplicando las operaciones del archivo AOF.
///
/// Ejecuta las operaciones contenidas en el archivo AOF una por una sobre el almacenamiento.
///
/// # Parámetros
/// - `path`: Ruta del archivo AOF.
/// - `storage`: Referencia mutable al almacenamiento sobre el cual se aplican las operaciones.
///
/// # Errores
/// Retorna un error de `DatoRedis` si falla la apertura del archivo o la ejecución de una operación.
fn restaurar_storage_de_aof(
    path: &String,
    storage: &Arc<RwLock<Storage>>,
) -> Result<(), DatoRedis> {
    let file = File::open(path).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file".to_string(),
        )
    })?;

    let operaciones = restaurar_operaciones(file).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file".to_string(),
        )
    })?;

    for operacion in operaciones.iter() {
        let comando = operacion[0].to_uppercase();
        if let Some(f) = obtener_fn_normal(&comando) {
            f(operacion, storage)?;
        }
    }
    Ok(())
}

/// Guarda el estado binario del almacenamiento y reinicia el archivo AOF.
///
/// Persiste el almacenamiento actual en un archivo binario. Luego, vacía el archivo AOF
/// y posiciona el cursor al inicio para futuras escrituras.
///
/// # Parámetros
/// - `storage_path`: Ruta al archivo binario de persistencia.
/// - `storage`: Referencia al almacenamiento que se desea guardar.
/// - `aof`: Archivo AOF a truncar.
///
/// # Errores
/// Devuelve un `DatoRedis` si ocurre algún error al escribir en disco o al truncar el AOF.
pub fn guardar_storage_bin(
    storage_path: &str,
    storage: &Arc<RwLock<Storage>>,
    aof: &Option<Arc<RwLock<File>>>,
    logger: &Logger,
) -> Result<(), DatoRedis> {
    let storage_guard = storage.read().map_err(|_| {
        DatoRedis::new_simple_error("ERR".to_string(), "error reading storage".to_string())
    })?;

    let file = match File::create(storage_path) {
        Ok(file) => file,
        Err(_) => {
            logger.error("[SAVE-RDB THREAD] Error creating persistence file", "Node");
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error creating persistence file".to_string(),
            ));
        }
    };

    let mut writer_st = BufWriter::new(file);

    guardar_storage(&mut writer_st, &storage_guard).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error writing to persistence file".to_string(),
        )
    })?;

    if let Some(aof_arc) = aof {
        let mut aof_lock = match aof_arc.write() {
            Ok(lock) => lock,
            Err(_) => {
                logger.error(
                    "[SAVE-RDB THREAD] Error writing to persistence file (lock)",
                    "Node",
                );
                return Err(DatoRedis::new_simple_error(
                    "ERR".to_string(),
                    "error locking AOF file".to_string(),
                ));
            }
        };

        aof_lock.set_len(0).map_err(|_| {
            DatoRedis::new_simple_error("ERR".to_string(), "error truncating AOF file".to_string())
        })?;

        if let Err(e) = aof_lock.seek(SeekFrom::Start(0)) {
            logger.error(
                &format!("[SAVE-RDB THREAD] Error seeking AOF file: {e:?}"),
                "Node",
            );
        }
    }

    Ok(())
}

/// Restaura completamente un nodo a partir de archivos de persistencia.
///
/// Carga ID, rol, estado, rango de slots, réplicas y el almacenamiento del nodo
/// a partir de archivos binarios y AOF.
///
/// # Parámetros
/// - `path_meta`: Ruta del archivo con metadatos del nodo.
/// - `path_str_bin`: Ruta del archivo binario con el almacenamiento.
/// - `path_str_aof`: Ruta del archivo AOF con operaciones persistentes.
/// - `addr`: Dirección de red del nodo.
///
/// # Retorna
/// Un nodo completamente reconstruido a partir del estado persistido.
///
/// # Errores
/// Devuelve un `DatoRedis` si alguna sección del archivo de persistencia falla al leerse.
pub fn restaurar_nodo(
    path_meta: String,
    path_str_bin: String,
    aof: (bool, String),
    addr: SocketAddr,
) -> Result<Node, DatoRedis> {
    let file_meta = File::open(path_meta).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence metadata file".to_string(),
        )
    })?;
    let mut reader = BufReader::new(file_meta);

    let id = restaurar_id(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file id".to_string(),
        )
    })?;
    let role = restaurar_role(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file role".to_string(),
        )
    })?;
    let status = restaurar_status(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file status".to_string(),
        )
    })?;
    let slot_range = restaurar_slot_range(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file slot range".to_string(),
        )
    })?;

    let save_interval = restaurar_save_interval(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file save interval".to_string(),
        )
    })?;

    let max_clients = restaurar_max_clients(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file max clients".to_string(),
        )
    })?;

    let logger_path = restaurar_logger_path(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file logger path".to_string(),
        )
    })?;

    let node_timeout = restaurar_node_timeout(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file node timeout".to_string(),
        )
    })?;

    let cluster_addr = restaurar_address(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file cluster address".to_string(),
        )
    })?;

    let public_addr = restaurar_address(&mut reader).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "error reading from persistence file public address".to_string(),
        )
    })?;

    let (_, path_str_aof) = aof;

    let storage = restaurar_storage(&path_str_bin, &path_str_aof, slot_range.clone())?;

    let logger = Logger::new(&logger_path);
    let node = NodeBuilder::new()
        .id(id)
        .cli_addr(addr)
        .node_addr(sumar_puerto(&addr, 10000))
        .role(role)
        .status(status)
        .slot_range(slot_range.clone())
        .storage(storage)
        .replicas(Some(Vec::new()))
        .pub_sub(PubSubBroker::new(logger.clone(), slot_range))
        .max_client_capacity(max_clients)
        .act_client_active(Arc::new(AtomicUsize::new(0)))
        .save_interval(save_interval)
        .logger(logger)
        .node_timeout(node_timeout)
        .public_addr(public_addr)
        .cluster_addr(cluster_addr)
        .build();
    let result =
        node.map_err(|e| DatoRedis::new_simple_error("ERR".to_string(), format!("{e}")))?;
    Ok(result)
}

/// Intenta restaurar el almacenamiento desde el archivo binario y luego aplica el AOF.
///
/// Si el archivo binario no se puede restaurar, se inicializa el almacenamiento vacío y se aplica el AOF.
/// Si el binario se restauró correctamente, y el AOF falla, se ignora el error del AOF.
///
/// # Parámetros
/// - `path_str_bin`: Ruta del archivo binario de almacenamiento.
/// - `path_str_aof`: Ruta del archivo AOF con operaciones.
/// - `slot_range`: Rango de slots del almacenamiento.
///
/// # Retorna
/// El almacenamiento reconstruido con datos binarios y/o del AOF.
///
/// # Errores
/// Devuelve un `DatoRedis` si falla el binario y también el AOF.
fn restaurar_storage(
    path_str_bin: &str,
    path_str_aof: &String,
    slot_range: Range<u16>,
) -> Result<Arc<RwLock<Storage>>, DatoRedis> {
    let (storage, bin_ok) = match File::open(path_str_bin).and_then(|file| {
        let mut reader = BufReader::new(file);
        restaurar_storage_bin(&mut reader)
    }) {
        Ok(storage) => (storage, true),
        Err(_) => (Storage::new(slot_range), false),
    };

    let storage_arc: Arc<RwLock<Storage>> = Arc::new(RwLock::new(storage));
    match restaurar_storage_de_aof(path_str_aof, &storage_arc) {
        Ok(_) => Ok(storage_arc),
        Err(e) if !bin_ok => Err(e),
        Err(_) => Ok(storage_arc),
    }
}

/// Implementa lógica de guardado RBD cada save_interval segundos
/// Recibe:
/// * `path_bin`: path al archivo binario donde persistir
/// * `aof`: aof donde persistir
/// * `storage`: arc del storage a guardar
/// * `save_interval`: intervalo de guardado
/// * `logger`: logger para registrar errores
pub fn save_storage(
    path_bin: &str,
    aof: Option<Arc<RwLock<File>>>,
    storage: &Arc<RwLock<Storage>>,
    save_interval: &u64,
    logger: &Logger,
) {
    loop {
        thread::sleep(Duration::from_millis(*save_interval));
        match guardar_storage_bin(path_bin, storage, &aof, logger) {
            Ok(_) => logger.info("[SAVE-RDB THREAD] Guardado completado", "Node"),
            Err(e) => logger.error(
                &format!("[SAVE-RDB THREAD] Error {e:?} guardando en {path_bin}"),
                "Node",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;
    use logger::logger::Logger;
    use std::fs::File;
    use std::io::BufWriter;

    #[test]
    fn test_01_guardar_id() {
        let id = NodeId::new();
        let logger = Logger::new("test.txt");
        let file = File::create("test_id.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_id(&mut writer, &id, &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_id.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_id = restaurar_id(&mut reader).unwrap();

        assert_eq!(id, restored_id);
        std::fs::remove_file("test_id.bin").unwrap();
    }

    #[test]
    fn test_02_guardar_role_master() {
        let role = NodeRole::Master;
        let file = File::create("test_role.bin").unwrap();
        let logger = Logger::new("test.txt");
        let mut writer = BufWriter::new(file);
        guardar_role(&mut writer, &role, &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_role.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_role = restaurar_role(&mut reader).unwrap();

        assert_eq!(role, restored_role);
        std::fs::remove_file("test_role.bin").unwrap();
    }

    #[test]
    fn test_03_guardar_role_replica() {
        let role = NodeRole::Replica;
        let file = File::create("test_rrole.bin").unwrap();
        let logger = Logger::new("test.txt");
        let mut writer = BufWriter::new(file);
        guardar_role(&mut writer, &role, &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_rrole.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_role = restaurar_role(&mut reader).unwrap();

        assert_eq!(role, restored_role);
        std::fs::remove_file("test_rrole.bin").unwrap();
    }

    #[test]
    fn test_04_guardar_status_online() {
        let status = NodeStatus::Ok;
        let file = File::create("test_status.bin").unwrap();
        let logger = Logger::new("test.txt");
        let mut writer = BufWriter::new(file);
        guardar_status(&mut writer, &status, &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_status.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_stat = restaurar_status(&mut reader).unwrap();

        assert_eq!(status, restored_stat);
        std::fs::remove_file("test_status.bin").unwrap();
    }

    #[test]
    fn test_05_guardar_status_offline() {
        let status = NodeStatus::Fail;
        let file = File::create("test_fstatus.bin").unwrap();
        let logger = Logger::new("test.txt");
        let mut writer = BufWriter::new(file);
        guardar_status(&mut writer, &status, &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_fstatus.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_stat = restaurar_status(&mut reader).unwrap();

        assert_eq!(status, restored_stat);
        std::fs::remove_file("test_fstatus.bin").unwrap();
    }

    #[test]
    fn test_06_guardar_slot_range() {
        let range: Range<u16> = 0..100;
        let file = File::create("test_range.bin").unwrap();
        let logger = Logger::new("test.txt");
        let mut writer = BufWriter::new(file);
        guardar_slot_range(&mut writer, range.clone(), &logger).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_range.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_range = restaurar_slot_range(&mut reader).unwrap();

        assert_eq!(range, restored_range);
        std::fs::remove_file("test_range.bin").unwrap();
    }

    #[test]
    fn test_09_guardar_storage() {
        let slot_range = Range {
            start: 0,
            end: 16378,
        };
        let mut storage = Storage::new(slot_range);
        storage
            .set(
                "key1".to_string(),
                DatoRedis::new_bulk_string("value1".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key2".to_string(),
                DatoRedis::new_bulk_string("value2".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key3".to_string(),
                DatoRedis::new_bulk_string("value3".to_string()).unwrap(),
            )
            .unwrap();

        let file = File::create("test_storage.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_storage(&mut writer, &storage).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_storage.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_storage = restaurar_storage_bin(&mut reader).unwrap();

        assert_eq!(storage, restored_storage);
        std::fs::remove_file("test_storage.bin").unwrap();
    }

    #[test]
    fn test_10_guardar_nodo() {
        let logger = Logger::new("test.txt");
        let id = NodeId::new();
        let role = NodeRole::Master;
        let status = NodeStatus::Ok;
        let slot_range = Range {
            start: 0,
            end: 16378,
        };
        let mut storage = Storage::new(slot_range.clone());
        storage
            .set(
                "key1".to_string(),
                DatoRedis::new_bulk_string("value1".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key2".to_string(),
                DatoRedis::new_bulk_string("value2".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key3".to_string(),
                DatoRedis::new_bulk_string("value3".to_string()).unwrap(),
            )
            .unwrap();

        let file = File::create("test_node.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_id(&mut writer, &id, &logger).unwrap();
        guardar_role(&mut writer, &role, &logger).unwrap();
        guardar_status(&mut writer, &status, &logger).unwrap();
        guardar_slot_range(&mut writer, slot_range.clone(), &logger).unwrap();
        guardar_storage(&mut writer, &storage).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_node.bin").unwrap();
        let mut reader = BufReader::new(file);

        let restored_id = restaurar_id(&mut reader).unwrap();
        let restored_role = restaurar_role(&mut reader).unwrap();
        let restored_status = restaurar_status(&mut reader).unwrap();
        let restored_slot_range = restaurar_slot_range(&mut reader).unwrap();
        let restored_storage = restaurar_storage_bin(&mut reader).unwrap();

        assert_eq!(id, restored_id);
        assert_eq!(role, restored_role);
        assert_eq!(status, restored_status);
        assert_eq!(slot_range, restored_slot_range);
        assert_eq!(storage, restored_storage);

        std::fs::remove_file("test_node.bin").unwrap();
    }

    #[test]
    fn test_11_guardar_storage_bin_2_veces() {
        let slot_range = Range {
            start: 0,
            end: 16378,
        };
        let mut storage = Storage::new(slot_range);
        storage
            .set(
                "key1".to_string(),
                DatoRedis::new_bulk_string("value1".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key2".to_string(),
                DatoRedis::new_bulk_string("value2".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key3".to_string(),
                DatoRedis::new_bulk_string("value3".to_string()).unwrap(),
            )
            .unwrap();

        let file = File::create("test_storage.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_storage(&mut writer, &storage).unwrap();
        writer.flush().unwrap();

        storage
            .set(
                "key4".to_string(),
                DatoRedis::new_bulk_string("value4".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key5".to_string(),
                DatoRedis::new_bulk_string("value5".to_string()).unwrap(),
            )
            .unwrap();
        storage
            .set(
                "key6".to_string(),
                DatoRedis::new_bulk_string("value6".to_string()).unwrap(),
            )
            .unwrap();
        let file = File::create("test_storage.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_storage(&mut writer, &storage).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_storage.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_storage = restaurar_storage_bin(&mut reader).unwrap();

        assert_eq!(storage, restored_storage);
        std::fs::remove_file("test_storage.bin").unwrap();
    }

    #[test]
    fn test_12_guardar_save_interval() {
        let save_interval: u64 = 5000;
        let file = File::create("test_save_interval.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_save_interval(&mut writer, save_interval).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_save_interval.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_interval = restaurar_save_interval(&mut reader).unwrap();

        assert_eq!(save_interval, restored_interval);
        std::fs::remove_file("test_save_interval.bin").unwrap();
    }

    #[test]
    fn test_13_guardar_max_clients() {
        let max_clients: usize = 100;
        let file = File::create("test_max_clients.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_max_clients(&mut writer, max_clients).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_max_clients.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_max_clients = restaurar_max_clients(&mut reader).unwrap();

        assert_eq!(max_clients, restored_max_clients);
        std::fs::remove_file("test_max_clients.bin").unwrap();
    }

    #[test]
    fn test_14_guardar_logger_path() {
        let logger_path = "test_logger.log";
        let file = File::create("test_logger_path.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_logger_path(&mut writer, logger_path).unwrap();
        writer.flush().unwrap();
        let file = File::open("test_logger_path.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_logger_path = restaurar_logger_path(&mut reader).unwrap();
        assert_eq!(logger_path, restored_logger_path);
        std::fs::remove_file("test_logger_path.bin").unwrap();
    }

    #[test]
    fn test_15_guardar_node_timeout() {
        let node_timeout: u64 = 30000;
        let file = File::create("test_node_timeout.bin").unwrap();
        let mut writer = BufWriter::new(file);
        guardar_node_timeout(&mut writer, node_timeout).unwrap();
        writer.flush().unwrap();

        let file = File::open("test_node_timeout.bin").unwrap();
        let mut reader = BufReader::new(file);
        let restored_node_timeout = restaurar_node_timeout(&mut reader).unwrap();

        assert_eq!(node_timeout, restored_node_timeout);
        std::fs::remove_file("test_node_timeout.bin").unwrap();
    }

    #[test]
    fn test_16_aof_persistence() {
        use std::fs::File;
        use std::sync::{Arc, RwLock};

        let comando = vec!["SET".to_string(), "clave".to_string(), "valor".to_string()];

        let file = File::create("test_aof.log").expect("No se pudo crear archivo");
        let file_arc = Arc::new(RwLock::new(file));

        guardar_operacion(&file_arc, comando.clone()).expect("Error guardando operacion");

        {
            let f = file_arc.read().unwrap();
            f.sync_all().expect("Error sincronizando archivo");
        }

        let file_lectura =
            File::open("test_aof.log").expect("No se pudo abrir archivo para lectura");
        let operaciones_restauradas =
            restaurar_operaciones(file_lectura).expect("Error restaurando operaciones");

        assert_eq!(operaciones_restauradas.len(), 1);
        assert_eq!(operaciones_restauradas[0], comando);

        std::fs::remove_file("test_aof.log").expect("No se pudo borrar archivo");
    }
}
