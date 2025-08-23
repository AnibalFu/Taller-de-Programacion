//! Este módulo contiene lógica relacionada a replica promotion
use std::{
    collections::{HashMap, HashSet},
    net::TcpStream,
    sync::{
        Arc, RwLock,
        atomic::Ordering,
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    thread::{self, spawn},
    time::{Duration, Instant},
};

use rand::{Rng, rng};

use crate::{
    cluster::voto::LastVote,
    constantes::CANT_INICIAL_MASTERS,
    internal_protocol::{
        fail_auth_req::FailOverAuthRequest,
        header::MessageHeader,
        internal_protocol_msg::{ClusterMessage, ClusterMessagePayload, send_cluster_message},
        internal_protocol_type::InternalProtocolType,
        node_flags::NodeFlags,
    },
    node::Node,
    node_id::NodeId,
    node_role::NodeRole,
};
use crate::{
    cluster_errors::ClusterError,
    internal_protocol::internal_protocol_type::InternalProtocolType::FailoverNegotiation,
};

impl Node {
    /// Lanza un hilo que maneja el proceso de replica promotion para las réplicas
    ///
    /// # Parámetros
    /// - `self`: Arc<Self> del nodo
    /// - `id_master`: Id del nodo en fail
    /// - `outgoing_streams`: Streams salientes del nodo
    /// - `rx`: Receiver de replication offset de las otras réplicas
    /// - `receiver_votos`: Receiver por donde recibir los votos en replica promotion
    pub(crate) fn iniciar_replica_promotion(
        self: Arc<Self>,
        id_master: NodeId,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        rx: Receiver<FailOverAuthRequest>,
        receiver_votos: Receiver<NodeId>,
        receiver_nuevo_master: Receiver<MessageHeader>,
    ) {
        let streams = outgoing_streams.clone();
        // me falta loggear errores
        spawn(move || {
            if let Err(e) = self.clone().efectuar_replica_promotion(
                id_master,
                &streams,
                rx,
                receiver_votos,
                receiver_nuevo_master,
            ) {
                self.loggear_cluster_error(e);
            }
        });
    }

    /// Lógica de replica promotion
    ///
    /// # Parámetros
    /// - `self`: Arc<Self> del nodo
    /// - `id_master`: Id del nodo en fail
    /// - `streams`: Streams salientes del nodo
    /// - `rx`: Receiver de replication offset de las otras réplicas
    /// - `receiver_votos`: Receiver por donde recibir los votos en replica promotion
    /// - `receiver_nuevo_master`: Receiver por donde recibir un nuevo master
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn efectuar_replica_promotion(
        self: Arc<Self>,
        id_master: NodeId,
        streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        rx: Receiver<FailOverAuthRequest>,
        receiver_votos: Receiver<NodeId>,
        receiver_nuevo_master: Receiver<MessageHeader>,
    ) -> Result<(), ClusterError> {
        if !self.es_replica_del_nodo_caido(&id_master)? {
            return Ok(());
        }
        self.current_epoch.fetch_add(1, Ordering::SeqCst);
        let replicas = self.enviar_replication_offset(streams, id_master.clone())?;
        if replicas == 0 {
            // si soy la unica replica
            if self.promover(streams, &id_master) {
                return Ok(());
            }
            return Err(ClusterError::new_promoting_replica_error(
                "REPLICA PROMOTION",
            ));
        }
        let replica_rank = self.armar_rep_rank(replicas, &rx);
        self.pedir_votos(replica_rank, streams)?;
        let resultado_master = receiver_nuevo_master.try_recv();
        if let Ok(nuevo_master) = resultado_master {
            return self.promover_a_otra_replica(nuevo_master.node_id());
        }
        if self.gana_votacion(&receiver_votos) && self.promover(streams, &id_master) {
            self.logger.info("Promovido a master", "REPLICA PROMOTION");
            return Ok(());
        }

        let resultado_master =
            receiver_nuevo_master.recv_timeout(Duration::from_millis(self.node_timeout * 4));
        if self.debe_reiniciar_promocion(resultado_master, &id_master)? {
            self.clone().efectuar_replica_promotion(
                id_master,
                streams,
                rx,
                receiver_votos,
                receiver_nuevo_master,
            )?;
        }
        Ok(())
    }

    fn es_replica_del_nodo_caido(&self, id_master: &NodeId) -> Result<bool, ClusterError> {
        let role = self
            .role
            .read()
            .map_err(|_| ClusterError::new_lock_error("rol", "FAILOVER"))?;
        if *role != NodeRole::Replica {
            return Ok(false);
        }
        let master = self
            .master
            .read()
            .map_err(|_| ClusterError::new_lock_error("master", "FAILOVER"))?;
        if *master != Some(id_master.clone()) {
            return Ok(false);
        }
        Ok(true)
    }

    fn debe_reiniciar_promocion(
        &self,
        resultado_master: Result<MessageHeader, RecvTimeoutError>,
        id_master: &NodeId,
    ) -> Result<bool, ClusterError> {
        if let Ok(nuevo_master) = resultado_master {
            self.promover_a_otra_replica(nuevo_master.node_id())?;
            Ok(false)
        } else {
            let master = self
                .master
                .read()
                .map_err(|_| ClusterError::new_lock_error("master", "FAILOVER"))?;
            if *master != Some(id_master.clone()) {
                return Ok(false);
            }
            self.logger.info("Reinicio votacion", "REPLICA PROMOTION");
            Ok(true)
        }
    }

    fn promover_a_otra_replica(&self, nuevo_master: NodeId) -> Result<(), ClusterError> {
        self.logger.info("Intenta actualizar master", "DEBUG");
        self.actualizar_master(nuevo_master)?;
        self.logger.info("Nuevo master", "REPLICA PROMOTION");
        Ok(())
    }

    /// Envía un mensaje de failover negotiation
    ///
    /// # Parámetros
    /// - `outgoing_streams`: Streams salientes del nodo
    ///
    /// # Retorna
    /// - Cantidad de réplicas a las que se les envió el mensaje si
    ///   no hay errores, ClusterError correspondiente en otro caso
    fn enviar_replication_offset(
        &self,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        id_master: NodeId,
    ) -> Result<i32, ClusterError> {
        let mut conocidos = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "FAILOVER"))?;
        let mut replicas = 0;
        if let Some(mensaje) = self.mensaje_failover_neg() {
            for (id, info) in conocidos.iter_mut() {
                if info.get_role() == NodeRole::Replica
                    && info.master_id() == Some(id_master.clone())
                {
                    let mut streams = outgoing_streams.write().map_err(|_| {
                        ClusterError::new_lock_error("streams salientes", "FAILOVER")
                    })?;
                    let option_stream = (*streams).get_mut(id);
                    if let Some(stream) = option_stream {
                        send_cluster_message(stream, &mensaje).map_err(|_| {
                            ClusterError::new_send_message_error("Replication Offset", "FAILOVER")
                        })?;
                        replicas += 1;
                    }
                }
            }
        }
        Ok(replicas)
    }

    /// Formula un mensaje de failover negotiation
    ///
    /// # Retorna
    /// - Option del mensaje a enviar a otras réplicas
    fn mensaje_failover_neg(&self) -> Option<ClusterMessage> {
        let header = self.message_header_node(FailoverNegotiation);
        let replication_offset = self.replication_offset.load(Ordering::SeqCst);
        let payload = FailOverAuthRequest::new(replication_offset as u32);
        if let Some(header_failover) = header {
            return Some(ClusterMessage::new(
                header_failover,
                ClusterMessagePayload::FailNegotiation(payload),
            ));
        }
        None
    }

    /// Envia al hilo encargado de replica promotion el replication offset de
    /// otra replica
    ///
    /// # Parámetros
    /// - `master_emisor`: Master del replication offset a enviar
    /// - `failover_auth_req`: Replication offset a enviar
    /// - `tx`: Sender por donde enviar el replication offset
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    pub(crate) fn recibir_rep_offset(
        &self,
        master_emisor: Option<NodeId>,
        failover_auth_req: FailOverAuthRequest,
        tx: &mut Option<Sender<FailOverAuthRequest>>,
    ) -> Result<(), ClusterError> {
        let master = self
            .master
            .read()
            .map_err(|_| ClusterError::new_lock_error("master", "FAILOVER"))?;
        if *master != master_emisor {
            return Ok(());
        }
        if let Some(canal) = tx {
            if canal.send(failover_auth_req).is_err() {
                self.logger.error(
                    "Error al recibir replication offset de otra réplica",
                    "FAILOVER",
                );
            }
        } else {
            self.logger.error(
                "Error al recibir replication offset de otra réplica",
                "FAILOVER",
            );
        }
        Ok(())
    }

    /// Determina el rango de una réplica en función de su replication offset
    ///
    /// # Parámetros
    /// - `replicas`: Cantidad de réplicas del nodo en fail
    /// - `rx`: Receiver de replication offset de las otras réplicas
    ///
    /// # Retorna
    /// - Replica rank
    fn armar_rep_rank(&self, replicas: i32, rx: &Receiver<FailOverAuthRequest>) -> usize {
        let mut replication_offsets = HashSet::new();
        let replication_offset = self.replication_offset.load(Ordering::SeqCst) as u32;
        for _ in 0..replicas {
            match rx.recv_timeout(Duration::from_millis(self.node_timeout)) {
                Ok(other_rep_offset) => {
                    let other = other_rep_offset.get_offset();
                    if other > replication_offset {
                        replication_offsets.insert(other);
                    }
                }
                Err(_) => break,
            };
        }
        replication_offsets.len()
    }

    /// Envía mensajes de pedidos de votos a los masters
    ///
    /// # Parámetros
    /// - `replica_rank`: Prioridad de la réplica frente a otras
    /// - `outgoing_streams`: Streams salientes del nodo
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn pedir_votos(
        &self,
        replica_rank: usize,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        esperar_turno(replica_rank);
        let mensaje_option = self.armar_vote_request();
        if let Some(mensaje) = mensaje_option {
            let conocidos = self
                .knows_nodes
                .read()
                .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "FAILOVER"))?;
            let mut streams = outgoing_streams
                .write()
                .map_err(|_| ClusterError::new_lock_error("stremas salientes", "FAILOVER"))?;
            for (id, _) in conocidos.iter() {
                if let Some(stream) = streams.get_mut(id) {
                    send_cluster_message(stream, &mensaje).map_err(|_| {
                        ClusterError::new_send_message_error("pedido de votos", "FAILOVER")
                    })?;
                } else {
                    return Err(ClusterError::new_lock_error(
                        "streams salientes",
                        "FAILOVER",
                    ));
                }
            }
            return Ok(());
        }
        Err(ClusterError::new_req_vote_error("FAILOVER"))
    }

    /// Arma un mensaje de pedido de voto para un master
    ///
    /// # Retorna
    /// - Option del pedido de voto
    fn armar_vote_request(&self) -> Option<ClusterMessage> {
        let replication_offset = self.replication_offset.load(Ordering::SeqCst) as u32;
        if let Some(header) = self.message_header_node(InternalProtocolType::FailoverAuthRequest) {
            let failover_req = FailOverAuthRequest::new(replication_offset);
            let payload = ClusterMessagePayload::FailAuthReq(failover_req);
            return Some(ClusterMessage::new(header, payload));
        }
        None
    }

    /// Determina si debe votar a favor de una réplica
    ///
    /// # Parámetros
    /// - `node_id`: Nodo que pide el voto
    /// - `master_id`: Master en fail
    /// - `current_epoch`: current_epoch de la réplica que pide el voto
    /// - `votos`: registro de votos anteriores de self
    /// - `outgoing_streams`: Streams salientes del nodo
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    pub fn evaluar_pedido_votacion(
        &self,
        node_id: NodeId,
        master_id: Option<NodeId>,
        current_epoch: u64,
        votos: &mut HashMap<NodeId, LastVote>,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        let rol = self
            .role
            .read()
            .map_err(|_| ClusterError::new_lock_error("rol", "FAILOVER"))?;
        if *rol != NodeRole::Master {
            return Ok(());
        }
        drop(rol);
        let conocidos = self
            .knows_nodes
            .read()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "FAILOVER"))?;
        if let Some(master_fail) = master_id {
            if let Some(master) = conocidos.get(&master_fail) {
                if !master.get_flags().is_fail() {
                    return Ok(());
                }
            }
            let nuevo_voto = LastVote::new(node_id.clone(), Instant::now(), current_epoch);
            let ultimo_voto = votos.get(&master_fail);
            if nuevo_voto.es_pedido_valido(
                self.current_epoch.load(Ordering::SeqCst),
                ultimo_voto,
                self.node_timeout,
            ) {
                self.current_epoch
                    .store(nuevo_voto.current_epoch, Ordering::SeqCst);
                votos.insert(master_fail.clone(), nuevo_voto);
                return self.enviar_acknowledge(outgoing_streams, node_id);
            }
        }
        Ok(())
    }

    /// Envía un voto positivo
    ///
    /// # Parámetros
    /// - `outgoing_streams`: Streams salientes del nodo
    /// - `node_id`: Nodo que pide el voto
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn enviar_acknowledge(
        &self,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        node_id: NodeId,
    ) -> Result<(), ClusterError> {
        let mut streams = outgoing_streams
            .write()
            .map_err(|_| ClusterError::new_lock_error("stremas salientes", "FAILOVER"))?;
        if let Some(stream) = streams.get_mut(&node_id) {
            if let Some(mensaje) = self.armar_mensaje_ack() {
                self.logger
                    .info(&format!("LAST VOTE: {}", &node_id), "DEBUG");
                send_cluster_message(stream, &mensaje).map_err(|_| {
                    ClusterError::new_send_message_error("voto positivo", "FAILOVER")
                })?;
            } else {
                self.logger
                    .error("Error al armar voto positivo", "FAILOVER");
            }
        }
        Ok(())
    }

    /// Arma un mensaje de voto positivo para un master
    ///
    /// # Retorna
    /// - Option del voto
    fn armar_mensaje_ack(&self) -> Option<ClusterMessage> {
        if let Some(header) = self.message_header_node(InternalProtocolType::FailoverAuthACK) {
            let payload = ClusterMessagePayload::FailAuthAck(self.id.clone());
            return Some(ClusterMessage::new(header, payload));
        } else {
            self.logger
                .error("Error al pedir votos de masters", "FAILOVER");
        }
        None
    }

    /// Envía un voto positivo al hilo de replica promotion
    ///
    /// # Parámetros
    /// - `id_votante`: id del master que votó
    /// - `sender`: sender por donde enviar el voto
    pub fn recibir_voto(&self, id_votante: NodeId, sender: &mut Option<Sender<NodeId>>) {
        if let Some(tx) = sender {
            let _ = tx.send(id_votante);
        } else {
            self.logger.error("Error al recibir voto", "FAILOVER");
        }
    }

    /// Determina si el nodo ganó la votación
    ///
    /// # Parámetros
    /// - `receiver_votos`: Receiver de replication votos
    ///
    /// # Retorna
    /// - Verdadero si gana la votación, falso en otro caso
    pub fn gana_votacion(&self, receiver_votos: &Receiver<NodeId>) -> bool {
        let mut votantes = HashSet::new();
        for _ in 0..(CANT_INICIAL_MASTERS - 1) {
            if let Ok(id) = receiver_votos.recv_timeout(Duration::from_millis(self.node_timeout)) {
                votantes.insert(id);
            } else {
                break;
            }
        }
        self.logger.info(
            &format!("Votantes: {:?}", votantes.len()),
            "REPLICA PROMOTION",
        );
        votantes.len() == (CANT_INICIAL_MASTERS - 1) as usize
    }

    /// Promueve el nodo a master
    ///
    /// # Parámetros
    /// - `outgoing_streams`: Streams salientes del nodo
    /// - `master_id`: id del master anterior
    ///
    /// # Retorna
    /// - Verdadero si el nodo fue promovido con éxito, falso en otro caso
    fn promover(
        &self,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        master_id: &NodeId,
    ) -> bool {
        if let Ok(mut rol) = self.role.write() {
            *rol = NodeRole::Master;
        } else {
            return false;
        }
        if let Ok(mut master) = self.master.write() {
            *master = None;
        } else {
            return false;
        }
        let mut reps = Vec::new();
        if let Ok(mut replicas) = self.replicas.write() {
            self.logger.info("Replicas", "DEBUG");
            if let Ok(conocidos) = self.knows_nodes.read() {
                if let Ok(mut streams) = outgoing_streams.write() {
                    for (id, info) in conocidos.iter() {
                        if info.master_id() == Some(master_id.clone()) {
                            reps.push(id.clone());
                            if let Some(rep_stream) = streams.get_mut(id) {
                                self.logger.info("Nueva replica", "DEBUG");
                                let _ = self.send_meet_new_master(rep_stream);
                            }
                        }
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
            *replicas = Some(reps);
        }
        self.propagar_rol(outgoing_streams)
    }

    /// Notifica a otra réplica que fue promovido a master
    ///
    /// # Parámetros
    /// - `stream`: Streams de la otra réplica
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn send_meet_new_master(&self, stream: &mut TcpStream) -> Result<(), ClusterError> {
        if let Some(header) = self.message_header_node(InternalProtocolType::MeetNewMaster) {
            let msg = ClusterMessage::new(header, ClusterMessagePayload::MeetNewMaster);
            if send_cluster_message(stream, &msg).is_ok() {
                self.logger.info("Meet new master enviado", "DEBUG");
                return Ok(());
            }
        }
        Err(ClusterError::new_send_meet_new_master_error("FAILOVER"))
    }

    /// Actualiza el master al perder la votación
    ///
    /// # Parámetros
    /// - `nuevo_master`: id del nuevo master
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    fn actualizar_master(&self, nuevo_master: NodeId) -> Result<(), ClusterError> {
        if let Ok(mut master) = self.master.write() {
            *master = Some(nuevo_master);
            return Ok(());
        }
        Err(ClusterError::new_set_new_master_error("FAILOVER"))
    }

    /// Envía el mensaje de nuevo master al hilo de replica promotion
    ///
    /// # Parámetros
    /// - `id`: id del nuevo master
    /// - `sender`: sender por donde enviar el nuevo master
    pub fn meet_new_master(
        &self,
        header: &MessageHeader,
        sender: &mut Option<Sender<MessageHeader>>,
    ) {
        if let Some(sender_header) = sender {
            if sender_header.clone().send(header.clone()).is_err() {
                self.logger.info(
                    "Error al enviar master a hilo de replica promotion",
                    "FAILOVER",
                )
            }
        } else {
            self.logger.info("Sender None para master", "FAILOVER")
        }
    }

    /// Propaga su nuevo rol a los otros nodos
    ///
    /// # Parámetros
    /// - `outgoing_streams`: Streams salientes del nodo
    ///
    /// # Retorna
    /// - Verdadero si el mensaje se propaga con éxito, falso en otro caso
    fn propagar_rol(&self, outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>) -> bool {
        if let Some(mensaje) = self.armar_mensaje_update() {
            if let Ok(mut streams) = outgoing_streams.write() {
                for (_, stream) in streams.iter_mut() {
                    if send_cluster_message(stream, &mensaje).is_err() {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Arma un mensaje update
    ///
    /// # Retorna
    /// - Option del mensaje update
    fn armar_mensaje_update(&self) -> Option<ClusterMessage> {
        if let Some(header) = self.message_header_node(InternalProtocolType::Update) {
            return Some(ClusterMessage::new(header, ClusterMessagePayload::Update));
        }
        None
    }

    /// Modifica la información de un nodo conocido, promoviéndolo a master
    ///
    /// # Parámetros
    /// - `master`: id del nuevo master
    ///
    /// # Retorna
    /// - () si no hay errores, ClusterError correspondiente en otro caso
    pub fn update_to_master(&self, master: NodeId) -> Result<(), ClusterError> {
        let mut conocidos = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "REPLICA PROMOTION"))?;
        if let Some(info_nodo) = conocidos.get_mut(&master) {
            let flags = info_nodo.get_flags();
            let new_flags = NodeFlags::new(true, false, flags.is_fail(), flags.is_pfail());
            info_nodo.update_role_and_flags(new_flags);
        }
        Ok(())
    }
}

/// Genera un delay antes de comenzar a pedir votos
///
/// # Parámetros
/// - `replica_rank`: replica rank
fn esperar_turno(replica_rank: usize) {
    let mut rng = rng();
    let random_delay = rng.random_range(0..=500);
    let delay_total = 500 + random_delay + replica_rank * 1000;
    let delay = Duration::from_millis(delay_total as u64);
    thread::sleep(delay);
}
