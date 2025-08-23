//! Este módulo contiene la lógica de procesamiento de mensajes y escritura
//! a otros nodos del cluster
use crate::cluster::estructuras_failover::EstructurasFailover;
use crate::cluster::neighboring_node::NeighboringNodeInfo;
use crate::cluster::node_message::{InnerMensajeNode, TipoMensajeNode};
use crate::cluster_errors::ClusterError;
use crate::comandos::pub_sub_struct::BrokerCommand;
use crate::comandos::utils::send_msj;
use crate::constantes::CANT_INICIAL_MASTERS;
use crate::internal_protocol::fail_auth_req::FailOverAuthRequest;
use crate::internal_protocol::gossip::GossipEntry;
use crate::internal_protocol::header::MessageHeader;
use crate::internal_protocol::internal_protocol_msg::{
    ClusterMessage, ClusterMessagePayload, send_cluster_message,
};
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::internal_protocol::moved::Moved;
use crate::internal_protocol::moved_shard_pubsub::MovedShardPubSub;
use crate::internal_protocol::node_flags::{ClusterState, NodeFlags};
use crate::internal_protocol::redis_cmd::RedisCMD;
use crate::node::Node;
use crate::node_id::NodeId;
use crate::node_role::NodeRole;
use redis_client::tipos_datos::arrays::Arrays;
use redis_client::tipos_datos::traits::DatoRedis;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::net::TcpStream;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Sender;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, RwLock};
use std::thread::spawn;
use std::time::Instant;

type MapaStreams = Arc<RwLock<HashMap<NodeId, TcpStream>>>;

impl Node {
    /// Inicia un hilo dedicado a procesar todos los mensajes recibidos desde otros nodos del clúster.
    ///
    /// Este hilo escucha en el canal `rx_connect`, donde otros hilos (por ejemplo, el hilo lector) envían
    /// mensajes recibidos a través de las conexiones TCP. Los mensajes pueden ser externos (de otros nodos)
    /// o internos (generados localmente por el nodo).
    ///
    /// Según el tipo de mensaje, se invoca la función correspondiente para procesarlo:
    ///
    /// - `ClusterNode`: mensajes provenientes de otros nodos, como comandos Redis, gossip o pub/sub.
    /// - `InnerNode`: mensajes internos generados por el nodo mismo, como enviar comandos a réplicas
    ///   o redirecciones.
    ///
    /// # Parámetros
    /// - `self`: Referencia compartida (`Arc<Self>`) al nodo actual.
    /// - `rx_connect`: Canal receptor por donde llegan mensajes del hilo lector u otras fuentes.
    /// - `incoming_streams`: Tabla compartida con streams TCP entrantes (usada por gossip).
    /// - `outgoing_streams`: Tabla compartida con streams TCP salientes (para enviar mensajes a otros nodos).
    /// - `aof_arc`: Archivo AOF compartido opcional, utilizado al ejecutar comandos Redis persistentes.
    pub(crate) fn iniciar_hilo_procesar_cmd_node(
        self: Arc<Self>,
        rx_connect: Receiver<TipoMensajeNode>,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        aof_arc: Option<Arc<RwLock<File>>>,
    ) {
        spawn(move || {
            let mut estructuras_failover = EstructurasFailover::new();
            while let Ok(msj) = rx_connect.recv() {
                let result_msj = match msj {
                    TipoMensajeNode::ClusterNode(msj) => self.clone().procesar_mensaje_out_node(
                        msj,
                        incoming_streams.clone(),
                        outgoing_streams.clone(),
                        aof_arc.clone(),
                        &mut estructuras_failover,
                    ),
                    TipoMensajeNode::InnerNode(msj) => {
                        self.procesar_mensaje_inner_node(msj, outgoing_streams.clone())
                    }
                };
                if let Err(e) = result_msj {
                    self.loggear_cluster_error(e);
                    self.marcar_node_fail("CLUSTER");
                    break;
                }
            }
        });
    }

    /// Procesa un mensaje recibido desde otro nodo del clúster (`ClusterMessage`).
    ///
    /// Dependiendo del tipo de `payload`, se ejecutan diferentes acciones:
    /// - `Gossip`: Se actualiza el estado del clúster y se intenta conectar a nodos faltantes.
    /// - `Empty`: Indica un mensaje de "heartbeat" o sincronización; puede usarse para manejar réplicas.
    /// - `RedisCommand`: Ejecuta el comando Redis recibido.
    /// - `PubSub`: Procesa un mensaje de publicación o suscripción.
    ///
    /// # Parámetros
    /// - `msj`: Mensaje recibido de otro nodo.
    /// - `incoming_streams`: Streams TCP entrantes compartidos, usados para nuevas conexiones (Gossip).
    /// - `outgoing_streams`: Streams TCP salientes compartidos, usados para nuevas conexiones (Gossip).
    /// - `aof_arc`: Archivo AOF opcional para persistir comandos Redis.
    /// - `estructuras_failover`: contiene las estructuras auxiliares necesarias
    ///   para el proceso de replica promotion
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn procesar_mensaje_out_node(
        self: Arc<Self>,
        msj: ClusterMessage,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        aof_arc: Option<Arc<RwLock<File>>>,
        estructuras_failover: &mut EstructurasFailover,
    ) -> Result<(), ClusterError> {
        self.procesar_header(&msj, outgoing_streams.clone())?;
        self.procesar_payload(
            msj,
            incoming_streams,
            outgoing_streams,
            aof_arc,
            estructuras_failover,
        )
    }

    /// Procesa el encabezado (`header`) de un mensaje de clúster entrante.
    ///
    /// Evalúa el tipo de mensaje (`Ping`, `Pong`, etc.) y ejecuta la acción correspondiente:
    /// - Si es `Ping`, responde con un mensaje `Pong` al nodo emisor.
    /// - Si es `Pong`, actualiza el tiempo de último `pong` recibido desde ese nodo.
    ///
    /// # Parámetros
    /// - `msj`: Referencia al mensaje de clúster recibido.
    /// - `outgoing_streams`: Mapa compartido con las conexiones de salida hacia otros nodos.
    ///
    /// # Retorna
    /// - `Ok(())` si el mensaje fue procesado correctamente.
    /// - `Err(ClusterError)` si hubo problemas con el acceso a streams o el envío de respuestas.
    fn procesar_header(
        &self,
        msj: &ClusterMessage,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        match msj.header().get_type() {
            InternalProtocolType::Ping => {
                self.actualizar_current_epoch(msj.header());
                self.responder_con_pong(msj.header().node_id(), outgoing_streams)
            }

            InternalProtocolType::Pong => {
                self.actualizar_current_epoch(msj.header());
                self.registrar_pong(msj.header().node_id())
            }
            _ => Ok(()),
        }
    }

    /// Actualiza el `current_epoch` del nodo si el mensaje recibido contiene un valor mayor.
    ///
    /// Este campo (`epoch`) se utiliza para la coordinación de estado en el clúster.
    /// Un valor más alto indica un conocimiento más reciente de la topología.
    ///
    /// # Parámetros
    /// - `msj`: Encabezado del mensaje recibido, desde donde se extrae el nuevo `epoch`.
    fn actualizar_current_epoch(&self, msj: &MessageHeader) {
        let current_epoch = msj.current_epoch();
        let self_epoch = self.current_epoch.load(Relaxed);
        if current_epoch > self_epoch {
            self.current_epoch.store(current_epoch, Relaxed);
        }
    }

    /// Envía un mensaje `PONG` en respuesta a un `PING` recibido desde otro nodo.
    ///
    /// # Parámetros
    /// - `id`: Identificador del nodo al que debe enviarse el `PONG`.
    /// - `outgoing_streams`: Mapa con los `TcpStream` salientes hacia otros nodos del clúster.
    ///
    /// # Retorna
    /// - `Ok(())` si el mensaje fue enviado correctamente.
    /// - `Err(ClusterError)` si no se encontró el stream o falló el envío del mensaje.
    ///
    /// # Notas
    /// - Utiliza el método `pong_node()` para construir el mensaje `PONG`.
    /// - Usa un lock de escritura sobre `outgoing_streams` para acceder al stream correspondiente.
    fn responder_con_pong(
        &self,
        id: NodeId,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        let mut guard = outgoing_streams
            .write()
            .map_err(|_| ClusterError::new_lock_error("streams salientes", "HEARTBEAT"))?;
        let stream = guard
            .get_mut(&id)
            .ok_or_else(|| ClusterError::new_lock_error("streams salientes", "HEARTBEAT"))?;
        let pong_msj = self
            .pong_node()
            .ok_or_else(|| ClusterError::new_send_message_error("pong", "HEARTBEAT"))?;

        send_cluster_message(stream, &pong_msj)
            .map_err(|_| ClusterError::new_send_message_error("pong", "HEARTBEAT"))?;

        Ok(())
    }

    /// Registra que se ha recibido un `PONG` desde el nodo indicado.
    ///
    /// Actualiza el timestamp del último `pong` recibido desde el nodo vecino,
    /// permitiendo mantener el monitoreo de salud del clúster.
    ///
    /// # Parámetros
    /// - `id`: Identificador del nodo desde el cual se recibió el `PONG`.
    ///
    /// # Retorna
    /// - `Ok(())` si se pudo actualizar correctamente la información.
    /// - `Err(ClusterError)` si el nodo no es reconocido o hubo error con el lock.
    fn registrar_pong(&self, id: NodeId) -> Result<(), ClusterError> {
        let mut guard = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "HEARTBEAT"))?;

        let vecino = guard.get_mut(&id).ok_or_else(|| {
            ClusterError::new_send_message_error("registro pong: nodo no encontrado", "HEARTBEAT")
        })?;

        vecino.set_pong_received_time(Instant::now());

        Ok(())
    }

    /// Procesa el payload de un mensaje de clúster y ejecuta la acción correspondiente.
    ///
    /// Dependiendo del tipo de payload (`ClusterMessagePayload`), esta función maneja:
    /// - Intercambio de gossip.
    /// - Inicialización de réplica.
    /// - Ejecución de comandos Redis y PubSub.
    /// - Coordinación de fallos (failover), votaciones y transición de replica a master.
    ///
    /// # Parámetros
    /// - `msj`: Mensaje de clúster recibido que contiene encabezado y payload.
    /// - `incoming_streams`: Mapa compartido de streams entrantes (`NodeId → TcpStream`).
    /// - `outgoing_streams`: Mapa compartido de streams salientes (`NodeId → TcpStream`).
    /// - `aof_arc`: Referencia opcional al archivo AOF para persistencia de comandos Redis.
    /// - `estructuras_failover`: Estructuras necesarias para manejar eventos de failover, votos, etc.
    ///
    /// # Retorna
    /// - `Ok(())` si el payload fue procesado correctamente.
    /// - `Err(ClusterError)` si ocurrió algún error en el manejo del mensaje.
    ///
    /// # Comportamiento por tipo de payload
    /// - `Gossip`: Actualiza el estado del nodo emisor y procesa los entries de gossip.
    /// - `MeetMaster`: Registra este nodo como réplica del emisor.
    /// - `RedisCommand`: Ejecuta un comando Redis recibido de otro nodo.
    /// - `Fail`: Marca a un nodo como fallado y propaga el estado.
    /// - `PubSub`: Ejecuta una operación de publicación o suscripción.
    /// - `FailNegotiation`: Recibe un offset de replicación en contexto de failover.
    /// - `FailAuthReq`: Evalúa una solicitud de voto para promover a un nuevo maestro.
    /// - `FailAuthAck`: Registra el voto recibido de otro nodo.
    /// - `MeetNewMaster`: Acepta una transición de liderazgo en curso.
    /// - `Update`: Actualiza este nodo para convertirse en maestro.
    /// - Otros casos (`_`): No hacen nada.
    fn procesar_payload(
        self: Arc<Self>,
        msj: ClusterMessage,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        aof_arc: Option<Arc<RwLock<File>>>,
        estructuras_failover: &mut EstructurasFailover,
    ) -> Result<(), ClusterError> {
        match msj.payload() {
            ClusterMessagePayload::Gossip(entries) => {
                self.actualizar_info_emisor(&msj)?;
                self.procesar_gossip_entries(
                    entries,
                    incoming_streams.clone(),
                    outgoing_streams.clone(),
                    &mut estructuras_failover.marcador_fallas,
                    msj.header(),
                    &mut estructuras_failover.sender_rep_offset,
                )
            }
            ClusterMessagePayload::MeetMaster => self.recibir_replica(msj.clone()),
            ClusterMessagePayload::RedisCommand(comando_redis) => {
                self.ejecutar_redis_cmd(comando_redis, aof_arc.clone());
                Ok(())
            }
            ClusterMessagePayload::Fail(node_id) => self.clone().marcar_fail(
                node_id,
                (&incoming_streams, &outgoing_streams),
                &mut estructuras_failover.marcador_fallas,
                &mut estructuras_failover.sender_rep_offset,
                &mut estructuras_failover.sender_votos,
                &mut estructuras_failover.sender_nuevo_master,
            ),
            ClusterMessagePayload::PubSub(cmd) => {
                self.ejecutar_pubsub(cmd);
                Ok(())
            }
            ClusterMessagePayload::FailNegotiation(failover_auth_req) => self.recibir_rep_offset(
                msj.header().master_id(),
                failover_auth_req,
                &mut estructuras_failover.sender_rep_offset.clone(),
            ),
            ClusterMessagePayload::FailAuthReq(_) => self.evaluar_pedido_votacion(
                msj.header().node_id(),
                msj.header().master_id(),
                msj.header().current_epoch(),
                &mut estructuras_failover.votos,
                &outgoing_streams,
            ),
            ClusterMessagePayload::FailAuthAck(master_id) => {
                self.recibir_voto(master_id, &mut estructuras_failover.sender_votos);
                Ok(())
            }
            ClusterMessagePayload::MeetNewMaster => {
                // ver
                self.meet_new_master(msj.header(), &mut estructuras_failover.sender_nuevo_master);
                Ok(())
            }
            ClusterMessagePayload::Update => self.update_to_master(msj.header().node_id()),
            _ => Ok(()),
        }
    }

    /// Procesa un mensaje generado internamente dentro del nodo (`InnerMensajeNode`).
    ///
    /// Este tipo de mensaje no viene de otro nodo sino que se genera como parte de una acción local
    /// que debe reflejarse en otros nodos (por ejemplo, replicación o publicación).
    ///
    /// - `PubSub`: Reenvía el mensaje Pub/Sub a los demás nodos.
    /// - `RedisCommand`: Replica el comando Redis a los nodos configurados como réplicas.
    /// - `Moved`: Maneja una redirección de cliente a otro nodo (cuando se detecta que una clave no pertenece al nodo actual).
    ///
    /// # Parámetros
    /// - `msj`: Mensaje interno generado por lógica del nodo.
    /// - `outgoing_streams`: Streams TCP salientes compartidos para enviar datos a otros nodos.
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn procesar_mensaje_inner_node(
        &self,
        msj: InnerMensajeNode,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        match msj {
            InnerMensajeNode::PubSub(cmd) => self.enviar_pubsub(cmd, outgoing_streams.clone()),
            InnerMensajeNode::RedisCommand(cmd) => {
                self.enviar_comandos_a_replicas(cmd, outgoing_streams.clone())
            }
            InnerMensajeNode::Moved(moved) => self.redireccion_moved_hander(moved),
            InnerMensajeNode::MovedShard(resp) => self.redireccion_moved_pubsub_hander(resp),
        }
    }

    /// Ejecuta un comando Pub/Sub recibido desde otro nodo del clúster.
    ///
    /// El comando se encapsula como `BrokerCommand` y se reenvía al `PubSubBroker` local
    /// para su procesamiento.
    ///
    /// # Parámetros
    /// - `cmd`: Comando Redis que representa una operación de tipo Pub/Sub (`PUBLISH`, `SUBSCRIBE`, etc.).
    ///
    /// # Notas
    /// - No reenvía el comando a otros nodos. Solo lo ejecuta en el nodo actual.
    fn ejecutar_pubsub(&self, cmd: RedisCMD) {
        let comando_pubsub = BrokerCommand::new_node_cmd(cmd.get_command(), None);
        self.pub_sub.send_cmd(comando_pubsub);
    }

    /// Propaga un comando Pub/Sub a todos los nodos del clúster.
    ///
    /// Crea un `ClusterMessage` de tipo `PubSub` y lo envía a través de todos
    /// los streams salientes registrados en `outgoing_streams`.
    ///
    /// # Parámetros
    /// - `cmd`: Comando Pub/Sub (`RedisCMD`) a propagar.
    /// - `outgoing_streams`: Mapa compartido de conexiones salientes (`NodeId → TcpStream`).
    ///
    /// # Retorna
    /// - `Ok(())` si el comando se propagó (o si no se pudo generar el header, se ignora sin error).
    /// - `Err(ClusterError)` si hubo un error al adquirir el lock de streams.
    ///
    /// # Notas
    /// - Si un nodo falla al recibir el mensaje, se loguea un warning y se continúa con los demás.
    /// - Si no se pudo construir el header del mensaje, se registra un warning pero no es un error crítico.
    fn enviar_pubsub(
        &self,
        cmd: RedisCMD,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        let header = match self.message_header_node(InternalProtocolType::Publish) {
            Some(h) => h,
            None => {
                self.logger
                    .warn("No se pudo construir header PUBSUB", "PUBSUB CLUSTER");
                return Ok(());
            }
        };

        let msg = ClusterMessage::new(header, ClusterMessagePayload::PubSub(cmd));

        let mut streams = match outgoing_streams.write() {
            Ok(s) => s,
            Err(_) => {
                return Err(ClusterError::new_lock_error(
                    "streams salientes",
                    "PUBSUB CLUSTER",
                ));
            }
        };

        for (node_id, stream) in streams.iter_mut() {
            if let Err(e) = send_cluster_message(stream, &msg) {
                self.logger.warn(
                    &format!(
                        "No se pudo propagar pubsub a nodo {}: {}",
                        node_id.get_id(),
                        e
                    ),
                    "PUBSUB CLUSTER",
                );
            }
        }

        Ok(())
    }

    /// Propaga un comando Redis a las réplicas de este nodo.
    ///
    /// Este método se utiliza para replicar operaciones que modifican el estado
    /// (por ejemplo, `SET`, `DEL`, etc.) desde el nodo maestro a sus réplicas.
    ///
    /// # Parámetros
    /// - `cmd`: Comando Redis a replicar.
    /// - `outgoing_streams`: Mapa compartido de conexiones salientes (`NodeId → TcpStream`).
    ///
    /// # Retorna
    /// - `Ok(())` si el comando fue enviado correctamente a las réplicas.
    /// - `Err(ClusterError)` si ocurre un error al adquirir algún lock (`replicas` o `outgoing_streams`).
    ///
    /// # Notas
    /// - Si no hay réplicas registradas, la función retorna exitosamente sin hacer nada.
    /// - Si una réplica no tiene stream asociado, no se le envía el comando.
    fn enviar_comandos_a_replicas(
        &self,
        cmd: RedisCMD,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        let replicas_guard = match self.replicas.read() {
            Ok(g) => g,
            Err(_) => {
                return Err(ClusterError::new_lock_error("replicas", "CLUSTER"));
            }
        };

        let replicas = match &*replicas_guard {
            Some(reps) => reps,
            None => return Ok(()),
        };

        let mut streams = match outgoing_streams.write() {
            Ok(s) => s,
            Err(_) => {
                return Err(ClusterError::new_lock_error("streams salientes", "CLUSTER"));
            }
        };

        let header = match self.message_header_node(InternalProtocolType::RedisCMD) {
            Some(h) => h,
            None => return Ok(()),
        };

        let msg = ClusterMessage::new(header, ClusterMessagePayload::RedisCommand(cmd));

        for rep in replicas {
            if let Some(stream) = streams.get_mut(rep) {
                let _ = send_cluster_message(stream, &msg);
            }
        }

        Ok(())
    }

    /// Ejecuta un comando Redis recibido desde otro nodo (por ejemplo, desde un maestro a una réplica).
    ///
    /// Este método:
    /// 1. Ejecuta el comando localmente (en almacenamiento y Pub/Sub si corresponde).
    /// 2. Actualiza el offset de replicación si el comando lo requiere.
    ///
    /// # Parámetros
    /// - `comando_redis`: Comando Redis recibido, ya deserializado.
    /// - `aof_arc`: Referencia opcional al archivo AOF, utilizado para persistencia.
    ///
    /// # Notas
    /// - La ejecución se realiza con `ejecutar_comando_general_replica`.
    /// - El offset de replicación permite sincronizar el estado con las réplicas.
    fn ejecutar_redis_cmd(&self, comando_redis: RedisCMD, aof_arc: Option<Arc<RwLock<File>>>) {
        let comando = comando_redis.get_command();
        self.ejecutar_comando_general_replica(&comando[0], &comando, &aof_arc);
        self.actualizar_replication_offset(&comando[0].to_uppercase());
    }

    /// Maneja la redirección MOVED para un cliente que intenta acceder a un slot no asignado a este nodo.
    ///
    /// Cuando un cliente envía un comando que involucra un slot que no pertenece a este nodo,
    /// se le responde con un error `-MOVED`, indicando a qué nodo debe reenviar su comando.
    ///
    /// # Parámetros
    /// - `moved`: Objeto `Moved` que contiene el slot al que intentó acceder el cliente
    ///   y el cliente (`Arc<RwLock<Client>>`) que debe ser redirigido.
    ///
    /// # Retorna
    /// - `Ok(())` si la redirección fue enviada exitosamente al cliente.
    /// - `Err(ClusterError)` si ocurre un error al acceder al mapa de nodos conocidos.
    ///
    /// # Proceso
    /// - Se obtiene el slot objetivo y se busca entre los nodos conocidos (`knows_nodes`)
    ///   cuál es el nodo master responsable de ese slot.
    /// - Una vez encontrado, se construye una respuesta `-MOVED {slot} {host}:{port}`.
    /// - Se envía al cliente a través de su socket.
    ///
    /// # Notas
    /// - El mensaje `MOVED` es parte del protocolo de Redis Cluster.
    ///   Permite a los clientes redirigir sus comandos al nodo correcto.
    /// - Esta función no actualiza ningún estado del clúster, solo responde al cliente.
    fn redireccion_moved_hander(&self, moved: Moved) -> Result<(), ClusterError> {
        let slot = moved.get_slot();
        let client = moved.get_client();

        let respuesta = self.buscar_addr_slot(slot)?;
        send_msj(client, respuesta, &self.logger);
        Ok(())
    }

    /// Maneja la redirección MOVED para comandos Pub/Sub shard, transformando la respuesta
    /// para reenviar correctamente al cliente con la dirección correcta de los slots involucrados.
    ///
    /// # Parámetros
    /// - `moved_pubsub`: Objeto que contiene la respuesta RESP con posibles errores MOVED shard,
    ///   y el cliente que debe recibir la respuesta.
    ///
    /// # Retorno
    /// - `Ok(())` si la respuesta transformada fue enviada correctamente.
    /// - `Err(ClusterError)` si falla el acceso a nodos conocidos.
    ///
    /// # Descripción
    /// La función procesa el array RESP recibido, reemplazando cada error MOVED por la dirección correcta
    /// obtenida de los nodos conocidos, y envía la nueva respuesta al cliente.
    ///
    /// # Notas
    /// - Solo transforma errores MOVED en la respuesta, dejando otros datos intactos.
    fn redireccion_moved_pubsub_hander(
        &self,
        moved_pubsub: MovedShardPubSub,
    ) -> Result<(), ClusterError> {
        let client = moved_pubsub.get_client();
        let mut new_array = Arrays::new();

        if let DatoRedis::Arrays(array) = moved_pubsub.get_resp() {
            for item in array.iter() {
                match item {
                    DatoRedis::MovedError(moved) => {
                        let addr = self.buscar_addr_slot(moved.get_slot())?;
                        new_array.append(addr);
                    }
                    _ => new_array.append(item.clone()),
                }
            }
        }

        let respuesta = DatoRedis::new_array_con_contenido(new_array);
        send_msj(client, respuesta, &self.logger);
        Ok(())
    }

    /// Busca la dirección del nodo master responsable de un slot específico.
    ///
    /// # Parámetros
    /// - `slot`: El número de slot para el que se desea conocer el nodo asignado.
    ///
    /// # Retorno
    /// - `Ok(DatoRedis)` con un error MOVED formateado para Redis con la dirección del nodo.
    /// - `Err(ClusterError)` si falla el acceso concurrente a la lista de nodos.
    ///
    /// # Descripción
    /// Recorre los nodos conocidos buscando el que tenga asignado el slot y sea master.
    /// Construye la respuesta MOVED con el formato estándar `{slot} {host}:{port}`.
    ///
    /// # Notas
    /// - Si no encuentra el nodo correspondiente, la respuesta MOVED será incompleta.
    /// - La función es interna y usada por los handlers de redirección.
    fn buscar_addr_slot(&self, slot: u16) -> Result<DatoRedis, ClusterError> {
        let knows_nodes = self
            .knows_nodes
            .read()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "CLUSTER"))?;

        let mut respuesta = format!("{slot} ");
        for (_, node) in knows_nodes.iter() {
            let range = node.get_slot_range();
            if range.contains(&slot) && node.get_role() == NodeRole::Master {
                let cli_socket = node.get_cli_addr();
                respuesta.push_str(&cli_socket.to_string());
                break;
            }
        }

        Ok(DatoRedis::new_simple_error("MOVED".to_string(), respuesta))
    }

    /// Procesamiento de mensajes gossip obtenidos en un ping
    ///
    /// # Argumentos
    /// - `self`: Arc<Self> del nodo
    /// - `entries`: vector de entradas de tipo gossip
    /// - `incoming_streams`: Mapa compartido para registrar streams entrantes.
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    /// - `marcador_fallas`: contabiliza qué nodos creen que cada nodo está en estado pfail
    /// - `header`: header del mensaje heartbeat
    /// - `tx`: Option del sender por donde enviar el replication offset de otras replicas en replica promotion
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn procesar_gossip_entries(
        self: Arc<Self>,
        entries: Vec<GossipEntry>,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        marcador_fallas: &mut HashMap<NodeId, HashSet<NodeId>>,
        header: &MessageHeader,
        tx: &mut Option<Sender<FailOverAuthRequest>>,
    ) -> Result<(), ClusterError> {
        let es_master_emisor: bool = header.flags().is_master();
        let id_emisor = header.node_id();
        for entry in entries {
            // println!("{:?}", entry);
            let id = entry.node_id();
            let addr = entry.ip_port();
            let mut pfail = false;
            let fail = entry.flags().is_fail();

            let conocidos = self
                .knows_nodes
                .read()
                .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "CLUSTER"))?;
            let ya_conocido = conocidos.contains_key(id);
            let option_info = conocidos.get(id);
            if let Some(info) = option_info {
                pfail = info.get_flags().is_pfail();
            }
            drop(conocidos); // liberar lock antes de intentar conexión
            if !ya_conocido && &self.id != id && !fail {
                self.logger
                    .info(&format!("Connect to: [{addr}]"), "CLUSTER");
                let _ = self.conectar_otro_nodo(
                    *addr,
                    incoming_streams.clone(),
                    outgoing_streams.clone(),
                );
            }
            self.clone().verificar_fail(
                (&es_master_emisor, pfail),
                (&id_emisor, entry),
                marcador_fallas,
                &incoming_streams,
                &outgoing_streams,
                tx,
            )?;
        }
        Ok(())
    }

    /// Actualiza información de un nodo conocido de corresponder
    ///
    /// # Argumentos
    /// - `msj`: mensaje de cluster con la información a actualizar
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn actualizar_info_emisor(&self, msj: &ClusterMessage) -> Result<(), ClusterError> {
        let header = msj.header();
        let flags_header = header.flags();
        let id = header.node_id();
        {
            let conocidos = self
                .knows_nodes
                .read()
                .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "CLUSTER"))?;
            let conocido_option = conocidos.get(&id);
            if let Some(conocido) = conocido_option {
                let rol = conocido.get_role();
                if conocido.get_status() == &ClusterState::Ok
                    && rol_esta_actualizado(&rol, &flags_header)
                {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        self.update_flags(&id, flags_header)
    }

    /// Modifica flags/rol de un nodo conocido
    ///
    /// # Argumentos
    /// - `id`: id del nodo conocido
    /// - `flags_header`: nuevas flags del nodo
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn update_flags(&self, id: &NodeId, flags_header: NodeFlags) -> Result<(), ClusterError> {
        let mut conocidos = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "CLUSTER"))?;
        let conocido_option = conocidos.get_mut(id);
        if let Some(conocido) = conocido_option {
            conocido.update_role_and_flags(flags_header);
        }
        Ok(())
    }

    /// Agrega una réplica a un master
    ///
    /// # Argumentos
    /// - `msj`: mensaje meet de una réplica
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn recibir_replica(&self, msj: ClusterMessage) -> Result<(), ClusterError> {
        let remitent_info = NeighboringNodeInfo::from_cluster_msg(&msj);
        let id = remitent_info.get_id();
        {
            let mut knows_node_guard = self
                .knows_nodes
                .write()
                .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "INIT"))?;
            knows_node_guard.insert(remitent_info.get_id(), remitent_info);
            let mut replicas = self
                .replicas
                .write()
                .map_err(|_| ClusterError::new_accept_replica_error(&id, "INIT"))?;
            replicas.get_or_insert_with(Vec::new).push(id.clone());
        }
        self.logger
            .info(&format!("Nueva replica aceptada [{id}]"), "INIT");
        Ok(())
    }

    /// Verifica el estado FAIL de un nodo según protocolo de gossip
    ///
    /// # Argumentos
    /// - `self`: Arc<Self> del nodo
    /// - `es_master_emisor`: indica si el emisor del mensaje gossip es master
    /// - `pfail`: indica si el nodo self considera que el nodo a fallar está en pfail
    /// - `id_emisor`: id del nodo que denuncia una posible falla
    /// - `entry`: gossip entry que denuncia el fallo
    /// - `marcador_fallas`: contabiliza qué nodos creen que cada nodo está en estado pfail
    /// - `incoming_streams`: streams de entrada del nodo
    /// - `outgoing_streams`: streams donde enviar el mensaje
    /// - `tx`: Option del sender por donde enviar el replication offset de otras replicas en replica promotion
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn verificar_fail(
        self: Arc<Self>,
        condiciones: (&bool, bool),
        id_entry: (&NodeId, GossipEntry),
        marcador_fallas: &mut HashMap<NodeId, HashSet<NodeId>>,
        incoming_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        tx: &mut Option<Sender<FailOverAuthRequest>>,
    ) -> Result<(), ClusterError> {
        let (id_emisor, entry) = id_entry;
        let (es_master_emisor, pfail) = condiciones;
        let emisor_considera_pfail = entry.flags().is_pfail();
        if !es_master_emisor || !pfail || !emisor_considera_pfail {
            return Ok(());
        }
        let role = self
            .role
            .read()
            .map_err(|_| ClusterError::new_lock_error("rol", "FAILURE"))?;
        if *role == NodeRole::Master {
            let id_falla = entry.node_id();
            let pfails = marcador_fallas.entry(id_falla.clone()).or_default();
            pfails.insert(id_emisor.clone());
            if pfails.len() >= (CANT_INICIAL_MASTERS / 2) as usize {
                return self.clone().enviar_fail(
                    id_falla,
                    incoming_streams,
                    outgoing_streams.clone(),
                    marcador_fallas,
                    tx,
                );
            }
        }
        Ok(())
    }

    /// Envía un mensaje de FAIL a todos sus nodos conocidos
    ///
    /// # Argumentos
    /// - `self`: Arc<Self> del nodo
    /// - `id`: id del nodo que falló
    /// - `incoming_streams`: streams de entrada del nodo
    /// - `outgoing_streams`: streams donde enviar el mensaje
    /// - `marcador_fallas`: contabiliza qué nodos creen que cada nodo está en estado pfail
    /// - `tx`: Option del sender por donde enviar el replication offset de otras replicas en replica promotion
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn enviar_fail(
        self: Arc<Self>,
        id: &NodeId,
        incoming_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        marcador_fallas: &mut HashMap<NodeId, HashSet<NodeId>>,
        tx: &mut Option<Sender<FailOverAuthRequest>>,
    ) -> Result<(), ClusterError> {
        if let Some(header) = self.message_header_node(InternalProtocolType::Fail) {
            let fail_msg = ClusterMessage::new(header, ClusterMessagePayload::Fail(id.clone()));
            if let Ok(mut guarda_streams) = outgoing_streams.write() {
                for (node_id, stream) in guarda_streams.iter_mut() {
                    if id != node_id {
                        send_fail_msg_and_log(stream, &fail_msg)?;
                    }
                }
            }
            self.clone().marcar_fail(
                id.clone(),
                (incoming_streams, &outgoing_streams),
                marcador_fallas,
                tx,
                &mut None,
                &mut None,
            )?;
        }
        Ok(())
    }

    /// Marca a un nodo como FAIL dentro de sus nodos conocidos
    ///
    /// # Argumentos
    /// - `failed_id`: id del nodo que falló
    /// - `streams`: streams de entrada y salida del nodo
    /// - `marcador_fallas`: contabiliza qué nodos creen que cada nodo está en estado pfail
    /// - `sender_rep_off`: Option del sender por donde enviar el replication offset de otras replicas en replica promotion
    /// - `sender_votos`: Option del sender por donde enviar los votos recibidos por masters en replica promotion
    /// - `sender_nuevo_master`: Option del sender por donde enviar nuevo master a las replicas
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn marcar_fail(
        self: Arc<Self>,
        failed_id: NodeId,
        streams: (&MapaStreams, &MapaStreams),
        marcador_fallas: &mut HashMap<NodeId, HashSet<NodeId>>,
        sender_rep_off: &mut Option<Sender<FailOverAuthRequest>>,
        sender_votos: &mut Option<Sender<NodeId>>,
        sender_nuevo_master: &mut Option<Sender<MessageHeader>>,
    ) -> Result<(), ClusterError> {
        let (incoming_streams, outgoing_streams) = streams;
        let mut conocidos = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "FAILURE"))?;
        if let Some(info) = conocidos.get_mut(&failed_id) {
            if info.get_flags().is_fail() {
                return Ok(());
            }
            info.set_fail(true);
            self.logger.warn(
                &format!("Marcando a nodo {failed_id:?} como FAIL"),
                "FAILURE",
            );

            println!(
                "[DEMO] FAIL detectado para nodo [{:?}]",
                info.get_node_addr()
            );
            println!("{:#?}", info.get_flags());

            conocidos.remove(&failed_id);
        } else {
            return Ok(());
        }
        marcador_fallas.remove(&failed_id);
        self.borrar_streams_nodo(&failed_id, incoming_streams, outgoing_streams)?;
        let (tx, rx) = mpsc::channel::<FailOverAuthRequest>();
        *sender_rep_off = Some(tx);
        let (nuevo_sender_votos, receiver_votos) = mpsc::channel::<NodeId>();
        *sender_votos = Some(nuevo_sender_votos);
        let (nuevo_sender_master, receiver_master) = mpsc::channel::<MessageHeader>();
        *sender_nuevo_master = Some(nuevo_sender_master);
        self.clone().iniciar_replica_promotion(
            failed_id,
            outgoing_streams,
            rx,
            receiver_votos,
            receiver_master,
        );
        Ok(())
    }

    /// Borra streams de un nodo
    ///
    /// # Argumentos
    /// - `id`: id del nodo que falló
    /// - `incoming_streams`: streams de entrada del nodo
    /// - `outgoing_streams`: streams de salida del nodo
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn borrar_streams_nodo(
        &self,
        id: &NodeId,
        incoming_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        borrar_de_streams(id, incoming_streams)?;
        borrar_de_streams(id, outgoing_streams)
    }
}

/// Define si la información obtenida sobre rol y flags de un nodo
/// coincide con la disponible internamente
///
/// # Argumentos
/// - `rol`: rol conocido del nodo
/// - `flags_header`: flags obtenidas del nodo por cluster message
fn rol_esta_actualizado(rol: &NodeRole, flags_header: &NodeFlags) -> bool {
    if rol == &NodeRole::Master {
        if flags_header.is_master() || !flags_header.is_replica() {
            return true;
        }
    } else if !flags_header.is_master() || flags_header.is_replica() {
        return true;
    }
    false
}

/// Envía un mensaje de tipo FAIL y loggea el error de haberlo
///
/// # Argumentos
/// - `stream`: stream receptor del mensaje
/// - `fail_msg`: mensaje fail
/// - `logger`: logger del nodo que envía el mensaje
/// - `id`: id del nodo que falló
///
/// # Retorna
/// - () si no hay errores, ClusterError correspondiente en otro caso
fn send_fail_msg_and_log(
    stream: &mut TcpStream,
    fail_msg: &ClusterMessage,
) -> Result<(), ClusterError> {
    if send_cluster_message(stream, fail_msg).is_err() {
        return Err(ClusterError::new_send_message_error("FAIL", "FAIL"));
    }
    Ok(())
}

/// Borra el stream de un id de un diccionario de streams
///
/// # Argumentos
/// - `id`: id del nodo cuyo stream se desea borrar
/// - `streams`: diccionario de streams
/// - `logger`: logger donde escribir errores de haberlos
///
/// # Retorna
/// - () si no hay errores, ClusterError correspondiente en otro caso
fn borrar_de_streams(
    id: &NodeId,
    streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
) -> Result<(), ClusterError> {
    if let Ok(mut s) = streams.write() {
        s.remove(id);
        Ok(())
    } else {
        Err(ClusterError::new_lock_error("streams", "FAIL"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_protocol::header::HeaderParameters;
    use crate::node_builder::NodeBuilder;
    use std::io::Read;
    use std::time::Duration;
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
        sync::{Arc, RwLock},
        thread,
    };

    #[test]
    fn procesar_ping_registra_pong() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6502".parse().unwrap())
                .node_addr("127.0.0.1:16502".parse().unwrap())
                .cluster_addr("127.0.0.1:16502".parse().unwrap())
                .public_addr("127.0.0.1:6502".parse().unwrap())
                .build()
                .unwrap(),
        );

        let (tx, rx) = std::sync::mpsc::channel::<TipoMensajeNode>();

        let incoming_streams = Arc::new(RwLock::new(HashMap::new()));
        let outgoing_streams = Arc::new(RwLock::new(HashMap::new()));

        // Simulamos un nodo B conocido y conectado
        let mut info_b = NeighboringNodeInfo::from_cluster_msg(&ClusterMessage::new(
            node.message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        ));
        info_b.set_pong_received_time(Instant::now() - Duration::from_millis(200));
        node.knows_nodes
            .write()
            .unwrap()
            .insert(id_b.clone(), info_b);

        // Preparamos un listener TCP (loopback)
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let outgoing_streams_clone = outgoing_streams.clone();

        // Hilo que acepta conexión y lee (simula nodo remoto recibiendo pong)
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0; 512];
            let _ = stream.read(&mut buf);
        });

        // Conectamos el stream TCP (simula nodo B)
        let stream = TcpStream::connect(addr).unwrap();
        outgoing_streams
            .write()
            .unwrap()
            .insert(id_b.clone(), stream.try_clone().unwrap());

        // Iniciamos el hilo que procesa los mensajes
        node.clone().iniciar_hilo_procesar_cmd_node(
            rx,
            incoming_streams,
            outgoing_streams_clone,
            None,
        );

        // Construimos el header con tipo Ping
        let ping_header = MessageHeader::new(HeaderParameters {
            header_type: InternalProtocolType::Ping,
            node_id: id_b.clone(),
            current_epoch: 1,
            config_epoch: 1,
            flags: NodeFlags::new(true, false, false, false),
            hash_slots_bitmap: 0..1000,
            tcp_client_port: "127.0.0.1:6500".parse().unwrap(),
            cluster_node_port: "127.0.0.1:16500".parse().unwrap(),
            cluster_state: ClusterState::Ok,
            master_id: None,
        });

        // Creamos el mensaje ping con el payload correcto
        let ping_msg = ClusterMessage::new(ping_header, ClusterMessagePayload::Gossip(Vec::new()));

        // Enviamos el mensaje al hilo procesador
        tx.send(TipoMensajeNode::ClusterNode(ping_msg)).unwrap();

        // Esperamos un poco para que se procese
        thread::sleep(Duration::from_millis(150));
        handle.join().unwrap();

        // Validamos que se haya actualizado el pong_received_time
        let guard = node.knows_nodes.read().unwrap();
        let vecino = guard.get(&id_b).unwrap();

        // El pong_received_time debe haberse actualizado, es decir, debe ser reciente
        // Para verificar, comparamos que haya sido actualizado hace menos de 1 segundo
        let tiempo_pong = vecino.get_pong_received_time();

        assert!(tiempo_pong < Duration::from_secs(1));
    }

    #[test]
    fn test_borrar_streams() {
        let id_1 = NodeId::new();
        let id_2 = NodeId::new();

        let addr_1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8088);
        let listener_1 = TcpListener::bind(addr_1).unwrap();
        let listen_addr_1 = listener_1.local_addr().unwrap();
        let stream_1 = TcpStream::connect(listen_addr_1).unwrap();

        let addr_2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8089);
        let listener_2 = TcpListener::bind(addr_2).unwrap();
        let listen_addr_2 = listener_2.local_addr().unwrap();
        let stream_2 = TcpStream::connect(listen_addr_2).unwrap();

        let mut streams_map = HashMap::new();
        streams_map.insert(id_1.clone(), stream_1);
        streams_map.insert(id_2.clone(), stream_2);
        let map = &Arc::new(RwLock::new(streams_map));
        borrar_de_streams(&id_1, map).unwrap();
        let lock = map.read().unwrap();
        assert!(!lock.contains_key(&id_1));
        drop(lock);
        assert!(map.read().unwrap().contains_key(&id_2))
    }

    #[test]
    fn test_rol_actualizado() {
        let master = NodeRole::Master;
        let replica = NodeRole::Replica;

        let flags_master = NodeFlags::new(true, false, false, false);
        let flags_replica = NodeFlags::new(false, true, false, false);

        assert!(rol_esta_actualizado(&master, &flags_master));
        assert!(rol_esta_actualizado(&replica, &flags_replica));
        assert!(!rol_esta_actualizado(&master, &flags_replica));
        assert!(!rol_esta_actualizado(&replica, &flags_master));
    }

    #[test]
    fn test_marcar_fail() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();
        let id_c = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6502".parse().unwrap())
                .node_addr("127.0.0.1:16502".parse().unwrap())
                .cluster_addr("127.0.0.1:16502".parse().unwrap())
                .public_addr("127.0.0.1:6502".parse().unwrap())
                .build()
                .unwrap(),
        );

        let node_b = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6503".parse().unwrap())
                .node_addr("127.0.0.1:16503".parse().unwrap())
                .cluster_addr("127.0.0.1:16503".parse().unwrap())
                .public_addr("127.0.0.1:6503".parse().unwrap())
                .build()
                .unwrap(),
        );

        let incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Simulamos un nodo B conocido y conectado
        let mut info_b = NeighboringNodeInfo::from_cluster_msg(&ClusterMessage::new(
            node_b
                .message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        ));
        info_b.set_pfail(true);
        let mut lock = node.knows_nodes.write().unwrap();
        lock.insert(id_b.clone(), info_b);
        drop(lock);

        let mut marcador_fallas = HashMap::new();
        let mut fallas_b = HashSet::new();
        fallas_b.insert(id_c);
        marcador_fallas.insert(id_b.clone(), fallas_b);

        node.clone()
            .marcar_fail(
                id_b.clone(),
                (&incoming_streams, &outgoing_streams),
                &mut marcador_fallas,
                &mut None,
                &mut None,
                &mut None,
            )
            .unwrap();

        assert!(!node.knows_nodes.read().unwrap().contains_key(&id_b));
    }

    #[test]
    fn test_recibir_replica() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6502".parse().unwrap())
                .node_addr("127.0.0.1:16502".parse().unwrap())
                .cluster_addr("127.0.0.1:16502".parse().unwrap())
                .public_addr("127.0.0.1:6502".parse().unwrap())
                .build()
                .unwrap(),
        );

        let node_b = Arc::new(
            NodeBuilder::new()
                .id(id_b.clone())
                .cli_addr("127.0.0.1:6503".parse().unwrap())
                .node_addr("127.0.0.1:16503".parse().unwrap())
                .cluster_addr("127.0.0.1:16503".parse().unwrap())
                .public_addr("127.0.0.1:6503".parse().unwrap())
                .role(NodeRole::Replica)
                .master(Some(id_a.clone()))
                .build()
                .unwrap(),
        );

        let header = node_b
            .message_header_node(InternalProtocolType::MeetMaster)
            .unwrap();
        let mensaje_b = ClusterMessage::new(header, ClusterMessagePayload::MeetMaster);

        node.recibir_replica(mensaje_b).unwrap();
        assert!(
            node.replicas
                .read()
                .unwrap()
                .clone()
                .unwrap()
                .contains(&id_b.clone())
        );
        assert!(node.knows_nodes.read().unwrap().contains_key(&id_b.clone()));
    }
}
