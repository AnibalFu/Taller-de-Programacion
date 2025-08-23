//! Este modulo contiene la implementacion principal del nodo de redis

use crate::client_struct::client::Client;
use crate::cluster::neighboring_node::NeighboringNodeInfo;
use crate::cluster::node_message::TipoMensajeNode;
use crate::cluster_errors::ClusterError;
use crate::comandos::pub_sub_struct::*;
use crate::comandos::utils::send_msj;
use crate::config::config_parser::Config;
use crate::internal_protocol::node_flags::ClusterState;
use crate::log_msj::log_mensajes::{
    log_bind_error, log_client_count, log_connection_accepted, log_error_accepting_connection,
    log_max_clients_reached, log_nodo_start, log_peer_addr_error,
};
use crate::node_id::NodeId;
use crate::node_role::NodeRole;
pub(crate) use crate::node_status::NodeStatus;
use crate::persistence::persistencia::*;
use crate::storage::Storage;
use crate::utils::utils_functions::abrir_persistence_file;
use crate::utils::utils_functions::{limpiar_cliente_desconectado, sumar_puerto};
use logger::logger::Logger;
use redis_client::protocol::protocol_resp::*;
use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Range;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock, mpsc};
use std::thread::{sleep, spawn};
use std::time::Duration;

/// Definicion de un nodo. Tendra mas atributos pero es solo para probar.
#[derive(Debug)]
pub struct Node {
    pub(crate) id: NodeId,
    pub(crate) cli_addr: SocketAddr,
    pub(crate) node_addr: SocketAddr,
    pub(crate) cluster_addr: SocketAddr,
    pub(crate) public_addr: SocketAddr,
    pub(crate) role: Arc<RwLock<NodeRole>>,
    pub(crate) status: Arc<RwLock<NodeStatus>>, // Meterle un lock para setearlo en diferentes momentos
    pub(crate) slot_range: Range<u16>,          // Sacarle el option
    pub(crate) storage: Arc<RwLock<Storage>>,
    pub(crate) pub_sub: PubSubBroker,
    pub(crate) max_client_capacity: usize,
    pub(crate) act_client_active: Arc<AtomicUsize>,
    pub(crate) save_interval: u64,
    pub(crate) logger: Logger,
    /// Agregados (no se usan por ahora)
    pub(crate) config_epoch: Arc<AtomicU64>, // Cambiarlo a AtomicUsize
    pub(crate) current_epoch: Arc<AtomicU64>, // Cambiarlo a AtomicUsize
    pub(crate) replication_offset: Arc<AtomicU64>, // Nose
    pub(crate) master: Arc<RwLock<Option<NodeId>>>, // Quien es mi maestro
    pub(crate) replicas: Arc<RwLock<Option<Vec<NodeId>>>>, // Quines son mis replicas
    pub(crate) knows_nodes: Arc<RwLock<HashMap<NodeId, NeighboringNodeInfo>>>,
    pub(crate) cluster_state: Arc<RwLock<ClusterState>>,
    pub(crate) node_timeout: u64,
}

/// Esta estructura representa al nodo de Redis.
/// Posee los siguientes atributos:
/// * id
/// * address (SocketAddr)
/// * role: master/replica
/// * status: online/offline
/// * slot_range: rango de slots que administra
/// * storage: almacenamiento de los slots
/// * replicas: vector de replicas, de ser master
/// * channels: canales asociados
/// * max_client_capacity: maxima cantidad de clientes concurrentes aceptados
/// * act_client_active: cantidad de clientes activos al momento
/// * logger: logger donde registra los mensajes necesarios
impl Node {
    /// Crea un nuevo nodo maestro del clúster Redis.
    ///
    /// Este constructor se utiliza cuando el nodo no puede ser restaurado desde archivos persistentes
    /// o cuando se lanza por primera vez. Inicializa todos los componentes principales del nodo:
    /// almacenamiento, logging, estado de replicación y manejo de clientes.
    ///
    /// # Parámetros
    /// - `addr`: Dirección IP y puerto del nodo (será usado para clientes).
    /// - `slot_range`: Rango de slots hash que le corresponden al nodo en el clúster.
    /// - `max_clients`: Límite máximo de conexiones de clientes concurrentes permitidas.
    /// - `logger_path`: Ruta del archivo donde se crearán los logs del nodo.
    /// - `save_interval`: Intervalo de persistencia automática en segundos (snapshots o RBD).
    /// - `metadata_path`: Ruta del archivo donde se guardará la metadata del nodo.
    /// - `node_timeout`: Tiempo máximo en milisegundos que se esperará una respuesta de otro nodo antes de marcarlo como caído.
    ///
    /// # Retorna
    /// - Un `Node` configurado como **maestro**, con todos sus componentes inicializados y
    ///   listo para iniciar el servicio mediante `start_node(...)`.
    ///
    /// # Detalles de implementación
    /// - Se inicializa un `Logger` para auditoría y depuración.
    /// - Se genera un `NodeId` único.
    /// - Se calcula un `node_addr` interno sumando +10000 al puerto del `addr`, para comunicaciones internas.
    /// - Se crean las estructuras de sincronización necesarias para operar en concurrencia (`Arc<RwLock<...>>`).
    /// - Se llama a `guardar_metadata(...)` para persistir el estado inicial del nodo.
    pub fn new_master(config: &Config) -> Self {
        let logger = Logger::new(&config.get_node_log_file());
        let node = Self {
            cluster_addr: config.get_cluster_address(),
            public_addr: config.get_public_address(),
            id: NodeId::new(),
            cli_addr: config.get_node_address(),
            node_addr: sumar_puerto(&config.get_node_address(), 10000),
            role: Arc::new(RwLock::new(NodeRole::Master)),
            status: Arc::new(RwLock::new(NodeStatus::Ok)),
            slot_range: config.get_node_slot_range(),
            storage: Arc::new(RwLock::new(Storage::new(config.get_node_slot_range()))),
            pub_sub: PubSubBroker::new(logger.clone(), config.get_node_slot_range()),
            max_client_capacity: config.get_node_max_clients(),
            act_client_active: Arc::new(AtomicUsize::new(0)),
            save_interval: config.get_node_save_interval(),
            logger,

            // Por ahora los seteo en 0
            config_epoch: Arc::new(AtomicU64::new(0)),
            current_epoch: Arc::new(AtomicU64::new(0)),
            replication_offset: Arc::new(AtomicU64::new(0)),
            master: Arc::new(RwLock::new(None)),
            replicas: Arc::new(RwLock::new(None)),
            knows_nodes: Arc::new(RwLock::new(HashMap::new())),
            cluster_state: Arc::new(RwLock::new(ClusterState::Ok)),
            node_timeout: config.get_node_time_out(),
        };
        let _ = node.guardar_metadata(&config.get_node_metadata(), &config.get_node_log_file());
        node
    }

    /// Retorna la informacion del nodo formateada
    ///
    /// # Retorna
    /// - String con informacion del nodo
    pub fn node_info(&self) -> String {
        format!(
            "Id: [{:?}] socket cli: [{}] socket cluster: [{}] slot range: [{:?}]",
            self.id.get_id(),
            self.cli_addr,
            self.node_addr,
            self.slot_range.clone()
        )
    }

    ///////////////////////////////////////////////////////////////////////////////

    /// Inicia el nodo Redis dentro del clúster.
    ///
    /// Esta función habilita los servicios principales del nodo, incluyendo:
    /// - El socket para aceptar conexiones de clientes.
    /// - El socket interno de clúster para coordinar con otros nodos.
    /// - La carga y habilitación del archivo AOF si está activado.
    /// - La conexión con un nodo seed o configuración como réplica si se especifica.
    /// - El hilo de persistencia periódica (RBD).
    /// - Atención de conexiones de clientes.
    ///
    /// # Parámetros
    /// - `aof`: Tupla `(bool, String)` que indica si AOF está habilitado y la ruta al archivo.
    /// - `path_bin`: Ruta donde se guardará el snapshot binario (RBD).
    /// - `seed`: Dirección IP:puerto de un nodo seed para descubrimiento (opcional).
    /// - `replicaof`: Dirección IP:puerto de un nodo maestro del cual replicarse (opcional).
    /// - `users`: Mapa de usuarios válidos para autenticación (`username -> password`).
    ///
    /// # Retorna
    /// - `Ok(())` si el nodo fue iniciado correctamente.
    /// - `Err(())` si falla alguna etapa crítica, como apertura de sockets o carga de archivos.
    ///
    /// # Requisitos
    /// Esta función debe ser llamada con un `Arc<Self>` porque el nodo se comparte entre threads.
    pub fn start_node(
        self: Arc<Self>,
        aof: (bool, String),
        path_bin: String,
        seed: Option<SocketAddr>,
        replicaof: Option<SocketAddr>,
        users: HashMap<String, String>,
    ) -> Result<(), ClusterError> {
        let cli_listener = self
            .try_bind_listener(self.cli_addr)
            .map_err(|_| ClusterError::new_start_node_error("INIT"))?;
        let cluster_bus = self
            .try_bind_listener(self.node_addr)
            .map_err(|_| ClusterError::new_start_node_error("INIT"))?;

        let (supports_aof, path_aof) = aof;
        let file = match abrir_persistence_file(&path_aof, &self.logger, "AOF".to_string()) {
            Some(f) => f,
            None => {
                let error = ClusterError::new_start_node_error("INIT");
                self.logger.error(&error.description, &error.module);
                return Err(error);
            }
        };

        let aof_arc = if supports_aof {
            Some(Arc::new(RwLock::new(file)))
        } else {
            None
        };

        let arc_node = self.clone();
        let tx_connect_cmd = arc_node
            .cluster_init(cluster_bus, seed, replicaof, aof_arc.clone())
            .map_err(|_| ClusterError::new_start_node_error("INIT"))?;

        self.handle_saving_thread(&path_bin, aof_arc.clone());

        log_nodo_start(&self.logger, &self);
        self.iniciar_recepcion_clientes(cli_listener, users, aof_arc, tx_connect_cmd);
        Ok(())
    }

    /// Ciclo principal que acepta conexiones de clientes y las delega al manejador.
    ///
    /// Esta función entra en un loop bloqueante que acepta conexiones TCP de clientes
    /// desde el socket `cli_listener`, y por cada conexión válida:
    /// - Crea una copia del nodo (`self.clone()`).
    /// - Llama a `handle_incoming_stream` con la conexión y los datos necesarios.
    ///
    /// # Parámetros
    /// - `cli_listener`: Socket TCP ya enlazado y escuchando en el puerto cliente del nodo.
    /// - `users`: Diccionario de credenciales válidas (usuarios y contraseñas).
    /// - `aof`: Referencia opcional al archivo AOF compartido y protegido por `Arc<RwLock<File>>`.
    /// - `tx_connect_cmd`: Canal para enviar mensajes internos a otros nodos del clúster.
    ///
    /// # Observaciones
    /// - Si una conexión falla al aceptarse, se registra el error y se continúa con el siguiente intento.
    /// - Este loop corre en el hilo principal del nodo y no retorna, salvo que el listener sea cerrado.
    fn iniciar_recepcion_clientes(
        self: Arc<Self>,
        cli_listener: TcpListener,
        users: HashMap<String, String>,
        aof: Option<Arc<RwLock<File>>>,
        tx_connect_cmd: Sender<TipoMensajeNode>,
    ) {
        for stream in cli_listener.incoming() {
            match stream {
                Ok(stream) => self.clone().handle_incoming_stream(
                    stream,
                    aof.clone(),
                    tx_connect_cmd.clone(),
                    &users,
                ),
                Err(e) => {
                    log_error_accepting_connection(self.id.get_id().to_string(), &self.logger, e)
                }
            }
        }
    }

    /// Inicializa la lógica de clúster del nodo Redis.
    ///
    /// Esta función configura todas las conexiones y hilos necesarios para que el nodo
    /// participe activamente en el clúster: comunicación entre nodos, descubrimiento de topología,
    /// replicación (si es esclavo), y procesamiento de mensajes de clúster.
    ///
    /// # Parámetros
    /// - `cluster_bus`: `TcpListener` ya enlazado en el puerto interno del nodo (`node_addr`),
    ///   destinado a las comunicaciones entre nodos del clúster.
    /// - `seed`: Dirección IP:puerto de un nodo seed (opcional) para el descubrimiento de clúster.
    /// - `replicaof`: Dirección IP:puerto del maestro del que este nodo debe replicarse (opcional).
    /// - `aof_arc`: Archivo AOF protegido por `Arc<RwLock>` (opcional), usado para persistencia desde comandos.
    ///
    /// # Comportamiento
    /// - Arranca el hilo receptor que escucha conexiones entrantes de otros nodos.
    /// - Intenta conectarse al nodo `seed`, si se especifica.
    /// - Si se especifica `replicaof`, se conecta al nodo maestro y llama a `meet_master`.
    /// - Inicia el hilo lector de mensajes de nodos (despacha a canal).
    /// - Inicia el hilo que procesa los comandos recibidos entre nodos (ejecución + AOF).
    /// - Inicia el hilo de `PING`/`PONG` periódico para verificar el estado de vecinos.
    ///
    /// # Retorna
    /// - `Ok(Sender<TipoMensajeNode>)`: Un canal `Sender` que permite enviar comandos internos al hilo de procesamiento de nodos.
    /// - `Err(())`: Si alguna conexión crítica falla (por ejemplo, la conexión al seed o la creación de hilos).
    ///
    /// # Notas
    /// - Este método encapsula toda la lógica de coordinación con el clúster.
    /// - La función debe ser invocada sólo una vez por nodo al momento de inicializar.
    fn cluster_init(
        self: Arc<Self>,
        cluster_bus: TcpListener,
        seed: Option<SocketAddr>,
        replicaof: Option<SocketAddr>,
        aof_arc: Option<Arc<RwLock<File>>>,
    ) -> Result<Sender<TipoMensajeNode>, ()> {
        let incoming_streams = Arc::new(RwLock::new(HashMap::new()));
        let outgoing_streams = Arc::new(RwLock::new(HashMap::new()));

        // === Conexiones iniciales ===
        let arc_node = self.clone();
        arc_node.iniciar_hilo_receptor_node(
            cluster_bus,
            incoming_streams.clone(),
            outgoing_streams.clone(),
        )?;

        sleep(Duration::from_millis(100));

        let arc_node = self.clone();
        let stream_seed =
            arc_node.conectar_a_seed(seed, incoming_streams.clone(), outgoing_streams.clone())?;

        if let Ok(replica_stream) = self.conectar_a_replicaof(
            replicaof,
            seed,
            stream_seed,
            incoming_streams.clone(),
            outgoing_streams.clone(),
        ) {
            self.meet_master(replica_stream, outgoing_streams.clone());
        }

        let (tx_connect, rx_connect) = mpsc::channel();
        let tx_connect_cmd = tx_connect.clone();
        let arc_node = self.clone();
        arc_node.iniciar_hilo_lector_node(incoming_streams.clone(), tx_connect);

        let arc_node = self.clone();
        arc_node.iniciar_hilo_procesar_cmd_node(
            rx_connect,
            incoming_streams.clone(),
            outgoing_streams.clone(),
            aof_arc.clone(),
        );

        let arc_node = self.clone();
        arc_node.iniciar_hilo_ping_pong(incoming_streams.clone());

        Ok(tx_connect_cmd)
    }

    /// Actualiza el rol del nodo a replica
    ///
    /// # Retorna
    /// - true si el cambio se realizó correctamente, false en otro caso
    pub(crate) fn actualizar_rol_a_replica(&self) -> bool {
        let role_result = self.role.write();
        if let Ok(mut role) = role_result {
            *role = NodeRole::Replica;
            true
        } else {
            self.logger
                .error("Error al actualizar rol de replica", "INIT");
            false
        }
    }

    /// Obtiene el id correspondiente a un stream
    ///
    /// # Argumentos
    /// - `stream`: stream cuyo id se busca averiguar
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    ///
    /// # Retorna
    /// - Option de NodeId del nodo correspondiente, en caso de encontrarlo
    pub(crate) fn get_id_from_stream(
        &self,
        stream: &TcpStream,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Option<NodeId> {
        let target_addr = match stream.peer_addr() {
            Ok(addr) => addr,
            Err(_) => return None,
        };

        let guarda_result = outgoing_streams.read();
        if let Ok(guarda) = guarda_result {
            for (id, node_stream) in guarda.iter() {
                if let Ok(addr) = node_stream.peer_addr() {
                    if addr == target_addr {
                        return Some(id.clone());
                    }
                }
            }
        }
        None
    }

    // CLUSTER FIN
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    /// Lanza un hilo encargado de persistir periódicamente el estado del nodo.
    ///
    /// Este hilo ejecuta la función `save_storage`, que guarda:
    /// - El contenido actual del almacenamiento (`storage`) en un archivo binario (`RBD`).
    /// - El estado del archivo AOF, si está habilitado.
    ///
    /// # Parámetros
    /// - `path_bin`: Ruta al archivo binario donde se guarda periódicamente el estado (`RBD`).
    /// - `aof_arc`: Referencia compartida y sincronizada al archivo AOF, si está habilitado.
    ///
    /// # Detalles
    /// - El intervalo de guardado es determinado por `self.save_interval`.
    /// - El logger se utiliza para registrar errores o eventos del proceso de guardado.
    /// - El hilo es lanzado en background y no se espera su finalización (no se hace `join`).
    ///
    /// # Notas
    /// - El guardado periódico permite tolerancia a fallos, ya que se puede restaurar
    ///   el estado del nodo desde los archivos `RBD` y/o `AOF`.
    fn handle_saving_thread(&self, path_bin: &str, aof_arc: Option<Arc<RwLock<File>>>) {
        let storage_clone = Arc::clone(&self.storage);
        let logger_clone = self.logger.clone();
        let path_bin_string = path_bin.to_string();
        let save_interval = self.save_interval;

        spawn(move || {
            save_storage(
                &path_bin_string,
                aof_arc,
                &storage_clone,
                &save_interval,
                &logger_clone,
            );
        });
    }

    /// Intenta conectarse a un stream TCP
    ///
    /// # Retorna
    /// - Option del TCP obtenido
    pub(crate) fn try_bind_listener(&self, socket_addr: SocketAddr) -> Result<TcpListener, ()> {
        match TcpListener::bind(socket_addr) {
            Ok(listener) => Ok(listener),
            Err(e) => {
                log_bind_error(self.id.get_id(), &socket_addr, &self.logger, &e);
                Err(())
            }
        }
    }

    /// Maneja una nueva conexión entrante desde un cliente.
    ///
    /// Esta función valida si el nodo puede aceptar más clientes, y en caso afirmativo,
    /// lanza un nuevo hilo para manejar la conexión.
    ///
    /// # Parámetros
    /// - `stream`: Objeto `TcpStream` asociado a la conexión entrante.
    /// - `aof_file`: Referencia opcional al archivo AOF protegido por `Arc<RwLock<File>>`.
    /// - `tx_connect`: Canal de comunicación para enviar mensajes del cliente al clúster.
    /// - `users`: Mapa con las credenciales válidas para autenticación de clientes.
    ///
    /// # Comportamiento
    /// - Si el número máximo de clientes (`max_client_capacity`) ha sido alcanzado,
    ///   se envía un mensaje de error RESP al cliente y se termina la conexión.
    /// - Si se puede aceptar la conexión, se delega al método `spawn_client_handler(...)`.
    ///
    /// # Notas
    /// - Esta función no bloquea indefinidamente: se ejecuta por cada cliente aceptado.
    /// - El `ip_client` se utiliza para loguear eventos e identificar clientes individualmente.
    fn handle_incoming_stream(
        self: Arc<Self>,
        mut stream: TcpStream,
        aof_file: Option<Arc<RwLock<File>>>,
        tx_connect: Sender<TipoMensajeNode>,
        users: &HashMap<String, String>,
    ) {
        let ip_client = match stream.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(e) => {
                log_peer_addr_error(self.id.get_id(), &self.logger, &e);
                return;
            }
        };
        if self.max_client_capacity <= self.act_client_active.load(SeqCst) {
            let res = DatoRedis::new_simple_error(
                "ERR".to_string(),
                "max number of clients reached".to_string(),
            );
            if let Err(result) =
                resp_server_command_write(&res.convertir_a_protocolo_resp(), &mut stream)
            {
                let msg = result.convertir_resp_a_string();
                log_max_clients_reached(&ip_client, &self.logger, &msg);
            }

            let msg = res.convertir_resp_a_string();
            log_max_clients_reached(&ip_client, &self.logger, &msg);
            return;
        }

        self.spawn_client_handler(
            stream,
            ip_client,
            aof_file.clone(),
            tx_connect,
            users.clone(),
        );
    }

    /// Crea y lanza un hilo para atender un cliente conectado.
    ///
    /// Esta función encapsula el `Client`, crea una referencia protegida (`Arc<RwLock<Client>>`)
    /// y delega la gestión de la conexión a la función `handle_connection(...)` dentro de un hilo separado.
    ///
    /// # Parámetros
    /// - `stream`: Objeto `TcpStream` de la conexión con el cliente.
    /// - `ip_client`: Dirección IP del cliente, usada para logs e identificación.
    /// - `aof_file`: Referencia opcional al archivo AOF, para persistir comandos ejecutados por el cliente.
    /// - `tx_connect`: Canal para enviar mensajes que afectan el estado del clúster.
    /// - `users`: Diccionario de usuarios autorizados para autenticación.
    ///
    /// # Comportamiento
    /// - Se crea un nuevo `Client` y se asocia al stream recibido.
    /// - Se lanza un nuevo hilo que invoca `handle_connection(...)` para gestionar las operaciones del cliente.
    ///
    /// # Notas
    /// - Esta función permite manejar múltiples clientes concurrentemente.
    /// - La autenticación y el procesamiento de comandos se realiza en `handle_connection(...)`.
    fn spawn_client_handler(
        self: Arc<Self>,
        stream: TcpStream,
        ip_client: String,
        aof_file: Option<Arc<RwLock<File>>>,
        tx_connect: Sender<TipoMensajeNode>,
        users: HashMap<String, String>,
    ) {
        let client = Arc::new(RwLock::new(Client::new(
            ip_client.to_string(),
            stream,
            self.logger.clone(),
            self.role.clone(),
        )));

        log_connection_accepted(self.id.get_id(), &ip_client, &self.logger);
        let node_arc = self.clone();
        spawn(move || {
            node_arc.handle_connection(client, aof_file, tx_connect, users);
        });
    }

    ///////////////////////////////////////////////////////////////////////////////

    /// Persiste la informacion del nodo en formato binario
    ///
    /// # Parametros
    /// * `metadata_path`: path al archivo binario donde persistir
    fn guardar_metadata(&self, metadata_path: &str, logger_path: &str) -> Result<(), DatoRedis> {
        let file = std::fs::File::create(metadata_path).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing metadata to persistence file".to_string(),
            )
        })?;
        let mut writer = BufWriter::new(file);

        guardar_id(&mut writer, &self.id, &self.logger).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing id to persistence file".to_string(),
            )
        })?;
        guardar_role(
            &mut writer,
            &self.role.read().unwrap().clone(),
            &self.logger,
        )
        .map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing role to persistence file".to_string(),
            )
        })?;
        guardar_status(&mut writer, &self.status.read().unwrap(), &self.logger).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing status to persistence file".to_string(),
            )
        })?;
        guardar_slot_range(&mut writer, self.slot_range.clone(), &self.logger).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing slot range to persistence file".to_string(),
            )
        })?;
        guardar_save_interval(&mut writer, self.save_interval).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing save interval to persistence file".to_string(),
            )
        })?;
        guardar_max_clients(&mut writer, self.max_client_capacity).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing max clients to persistence file".to_string(),
            )
        })?;
        guardar_logger_path(&mut writer, logger_path).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing logger path to persistence file".to_string(),
            )
        })?;
        guardar_node_timeout(&mut writer, self.node_timeout).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing node timeout to persistence file".to_string(),
            )
        })?;
        guardar_address(&mut writer, &self.cluster_addr).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing cluster address to persistence file".to_string(),
            )
        })?;
        guardar_address(&mut writer, &self.public_addr).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error writing public address to persistence file".to_string(),
            )
        })?;

        self.logger.info("Metadata guardada correctamente", "Node");
        Ok(())
    }

    /// Atiende el ciclo de vida completo de una conexión de cliente.
    ///
    /// Pasos principales:
    /// 1. Incrementa el contador global de clientes activos (`act_client_active`)
    ///    y registra el nuevo total en el logger.
    /// 2. Llama a `procesar_comandos` para leer, autenticar y ejecutar los
    ///    comandos que envíe el cliente; se le pasa el AOF (si existe) y el canal
    ///    al hilo de clúster para propagar los comandos que correspondan.
    /// 3. Una vez finalizada la sesión ‒ya sea por cierre normal o error‒,
    ///    invoca `limpiar_cliente_desconectado`, que:
    ///      - Libera suscripciones Pub/Sub.
    ///      - Decrementa el contador de clientes activos.
    ///      - Registra la desconexión en los logs.
    ///
    /// # Parámetros
    /// - `client`: Referencia compartida al cliente conectado.
    /// - `aof_file`: Archivo AOF sincronizado, usado para persistir los comandos que
    ///   modifican el estado si está habilitado.
    /// - `tx_connect`: Canal para enviar mensajes internos al hilo de procesamiento
    ///   de comandos de clúster.
    /// - `users`: Mapa de credenciales (`usuario → contraseña`) para autenticar al cliente.
    fn handle_connection(
        self: Arc<Self>,
        client: Arc<RwLock<Client>>,
        aof_file: Option<Arc<RwLock<File>>>,
        tx_connect: Sender<TipoMensajeNode>,
        users: HashMap<String, String>,
    ) {
        self.act_client_active.fetch_add(1, SeqCst);
        log_client_count(&self.logger, self.act_client_active.load(SeqCst));
        self.procesar_comandos(Arc::clone(&client), aof_file, tx_connect, users);

        limpiar_cliente_desconectado(client, &self.pub_sub, &self.act_client_active, &self.logger);
    }

    /// Evalúa si un `RwLock<T>` se encuentra en estado de fallo según un predicado.
    ///
    /// # Parámetros
    /// - `lock`: Referencia al `RwLock<T>` que encapsula el estado a verificar.
    /// - `is_fail`: Cierre que recibe `&T` y devuelve `true` cuando ese estado
    ///   debe considerarse como fallo.
    ///
    /// # Retorna
    /// - `true`  si el estado indica fallo o si ocurre un error al adquirir la lectura.
    /// - `false` en caso contrario.
    fn check_fail<T>(&self, lock: &RwLock<T>, is_fail: impl Fn(&T) -> bool) -> bool {
        match lock.read() {
            Ok(state) => is_fail(&*state),
            Err(_) => true,
        }
    }

    /// Envía al cliente un mensaje de error RESP con el estado de fallo indicado.
    ///
    /// # Parámetros
    /// - `client`: Referencia compartida al cliente destinatario.
    /// - `code`:   Código de error RESP (p. ej. `"CLUSTERDOWN"` o `"NODEDOWN"`).
    /// - `msg`:    Mensaje descriptivo que acompaña al código.
    ///
    /// El error se construye con `DatoRedis::new_simple_error` y se envía usando
    /// `send_msj`, registrándose además en el logger del nodo.
    fn enviar_fail_status(&self, client: &Arc<RwLock<Client>>, code: &str, msg: &str) {
        send_msj(
            client.clone(),
            DatoRedis::new_simple_error(code.into(), msg.into()),
            &self.logger,
        );
    }

    /// Comprueba los estados de clúster y nodo; si alguno está en fallo, notifica al cliente.
    ///
    /// Orden de verificación:
    /// 1. `cluster_state == ClusterState::Fail` → envía `"CLUSTERDOWN"`.
    /// 2. `status == NodeStatus::Fail`          → envía `"NODEDOWN"`.
    ///
    /// # Parámetros
    /// - `client`: Referencia compartida al cliente al que se notificará.
    ///
    /// # Retorna
    /// - `true`  si se envió algún mensaje de fallo.
    /// - `false` si ambos estados están operativos.
    pub(crate) fn verificar_y_enviar_fail(&self, client: &Arc<RwLock<Client>>) -> bool {
        if self.check_fail(&self.cluster_state, |s| *s == ClusterState::Fail) {
            self.enviar_fail_status(client, "CLUSTERDOWN", "The cluster is down");
            return true;
        }

        if self.check_fail(&self.status, |s| *s == NodeStatus::Fail) {
            self.enviar_fail_status(client, "NODEDOWN", "The node is down");
            return true;
        }

        false
    }

    /// Registra un `ClusterError` en el logger del nodo.
    ///
    /// # Parámetros
    /// - `e`: Error de clúster que contiene módulo y descripción.
    ///
    /// El método envía la descripción al logger con nivel **error**,
    /// facilitando la trazabilidad de incidentes durante la operación del nodo.
    pub(crate) fn loggear_cluster_error(&self, e: ClusterError) {
        self.logger.error(&e.description, &e.module);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
