//! Este módulo contiene la lógica principal de procesamiento de mensajes
//! HEARTBEAT, mandando pings periódicos a nodos aleatorios y registrando
//! posibles fallas ante la falta de pongs
use crate::cluster::neighboring_node::NeighboringNodeInfo;
use crate::cluster_errors::ClusterError;
use crate::constantes::{CANT_INICIAL_MASTERS, TOTAL_SLOTS};
use crate::internal_protocol::internal_protocol_msg::{ClusterMessage, send_cluster_message};
use crate::internal_protocol::node_flags::ClusterState;
use crate::node::Node;
use crate::node_id::NodeId;
use crate::node_role::NodeRole;
use rand::rng;
use rand::seq::IteratorRandom;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::net::TcpStream;
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

impl Node {
    /// Inicia el ping pong con otros nodos
    ///
    /// # Parámetros
    /// * `self`
    /// * `outgoing_streams`: mapa de streams salientes por donde enviar
    ///   mensajes de tipo pong
    pub(crate) fn iniciar_hilo_ping_pong(
        self: Arc<Self>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) {
        let node_clone = Arc::clone(&self);
        self.iniciar_hilo_pong();
        node_clone.iniciar_hilo_ping(outgoing_streams);
    }

    /// Lanza un hilo que cada determinada cantidad de tiempo, envía pings
    /// a nodos aleatorios
    ///
    /// # Parámetros
    /// * `self`
    /// * `outgoing_stremas`: streams salientes del nodo
    fn iniciar_hilo_ping(
        self: Arc<Self>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) {
        spawn(move || {
            loop {
                sleep(Duration::from_millis(self.node_timeout / 10));
                if let Err(e) = self.iniciar_ping_periodico(&outgoing_streams) {
                    self.logger.error(
                        &format!("Error en hilo de ping: {}", e.description),
                        "HEARTBEAT",
                    );
                    // Debo marcar como NodeStarus::Fail ya que ocurrio un error importante.
                    self.marcar_node_fail("HEARTBEAT");
                    break;
                }
                println!(
                    "[DEMO] Socket: [{:?}] role: [{}] vecinos: [{}] currentEpoch: [{}] ",
                    self.node_addr,
                    self.role.read().unwrap(),
                    self.knows_nodes.read().unwrap().len(),
                    self.current_epoch.load(Ordering::Relaxed),
                );
            }
        });
    }

    /// Lanza un hilo encargado de verificar si para un nodo pasó el tiempo
    /// establecido par recibir su pong
    ///
    /// # Parámetros
    /// * `self`
    fn iniciar_hilo_pong(self: Arc<Self>) {
        spawn(move || {
            let mut marcados = HashSet::new();
            loop {
                sleep(Duration::from_millis(100));
                if let Err(e) = self.verificar_pfail(&mut marcados) {
                    self.loggear_cluster_error(e);
                    break;
                }
                if let Err(e) = self.validar_estado_cluster() {
                    self.loggear_cluster_error(e);
                    break;
                }
            }
        });
    }

    /// Envía pings a la mitad de los nodos de forma aleatoria y a aquellos
    /// que no hayan recibido ping del nodo en timeout / 2, registrando
    /// el instante de emisión
    ///
    /// # Parámetros
    /// * `outgoing_streams`: streams salientes del nodo
    ///
    /// # Retorna
    /// - () en caso de éxito, ClusterError en otro caso
    fn iniciar_ping_periodico(
        &self,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ClusterError> {
        let ping = match self.ping_node() {
            Some(p) => p,
            None => return Ok(()),
        };

        // Enviar pings a N/2 nodos aleatorios
        let ya_pingueados = self.enviar_pings_random(&ping, outgoing_streams)?;

        // Enviar pings a nodos que pasaron NODETIMEOUT / 2 sin ping
        self.enviar_pings_timeout(&ping, outgoing_streams, &ya_pingueados)?;

        Ok(())
    }

    /// Envía un mensaje pings a nodos aleatorios
    ///
    /// # Parámetros
    /// * `ping`: mensaje ping a enviar
    /// * `outgoing_streams`: streams salientes del nodo
    ///
    /// # Retorna
    /// - Set de nodos pingueados en caso de éxito, ClusterError en otro caso
    fn enviar_pings_random(
        &self,
        ping: &ClusterMessage,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<HashSet<NodeId>, ClusterError> {
        let mut guard = outgoing_streams
            .write()
            .map_err(|_| ClusterError::new_lock_error("streams salientes", "HEARTBEAT"))?;
        let cantidad = max(guard.len() / 2, 1);

        let mut rng = rng();
        let seleccionados: Vec<_> = guard.iter_mut().choose_multiple(&mut rng, cantidad);

        let mut ids_pingueados = HashSet::new();
        for (node_id, stream) in seleccionados {
            self.enviar_ping_individual(node_id, stream, ping)?;
            ids_pingueados.insert(node_id.clone());
        }

        Ok(ids_pingueados)
    }

    /// Envía un mensaje pings a nodos a los que no se mandó en una cierta
    /// cantidad de tiempo
    ///
    /// # Parámetros
    /// * `ping`: mensaje ping a enviar
    /// * `outgoing_streams`: streams salientes del nodo
    /// * `ya_pingueados`: set de nodos a los que se les envió ping aleatoriamente
    ///
    /// # Retorna
    /// - () en caso de éxito, ClusterError en otro caso
    fn enviar_pings_timeout(
        &self,
        ping: &ClusterMessage,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        ya_pingueados: &HashSet<NodeId>,
    ) -> Result<(), ClusterError> {
        let vecinos_candidatos = self.vecinos_aleatorios(ya_pingueados)?;

        let mut guard = outgoing_streams
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "HEARTBEAT"))?;

        for node_id in vecinos_candidatos {
            if let Some(stream) = guard.get_mut(&node_id) {
                self.enviar_ping_individual(&node_id, stream, ping)?;
            }
        }

        Ok(())
    }

    /// Retorna un vector de vecinos a los que mandar ping aleatoriamente
    ///
    /// # Parámetros
    /// * `ya_pingueados`: set de nodos a los que se les envió ping aleatoriamente
    ///
    /// # Retorna
    /// - Vector de nodos a pinguear en caso de éxito, ClusterError en otro caso
    fn vecinos_aleatorios(
        &self,
        ya_pingueados: &HashSet<NodeId>,
    ) -> Result<Vec<NodeId>, ClusterError> {
        let vecinos_candidatos: Vec<NodeId> = {
            let vecinos = self
                .knows_nodes
                .read()
                .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "HEARTBEAT"))?;
            vecinos
                .iter()
                .filter_map(|(node_id, info)| {
                    if ya_pingueados.contains(node_id) {
                        return None;
                    }
                    if info.get_ping_sent_time() > Duration::from_millis(self.node_timeout / 2) {
                        Some(node_id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        Ok(vecinos_candidatos)
    }

    /// Envía un mensaje ping a un nodo
    ///
    /// # Parámetros
    /// * `node_id`: id del nodo al cual enviar el mensaje
    /// * `stream`: stream salientes al nodo a pinguear
    /// * `ping`: mensaje ping a enviar
    ///
    /// # Retorna
    /// - () en caso de éxito, ClusterError en otro caso
    fn enviar_ping_individual(
        &self,
        node_id: &NodeId,
        stream: &mut TcpStream,
        ping: &ClusterMessage,
    ) -> Result<(), ClusterError> {
        let ahora = Instant::now();

        let mut info_guard = self
            .knows_nodes
            .write()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "HEARTBEAT"))?;
        if let Some(info) = info_guard.get_mut(node_id) {
            info.set_ping_sent_time(ahora);
        }

        //println!("-----ENVIO PING");
        if let Err(e) = send_cluster_message(stream, ping) {
            self.logger.error(
                &format!("Error al enviar ping a {node_id:?}: {e}"),
                "HEARTBEAT",
            );
            self.marcar_como_posiblemente_fallado(node_id, &mut info_guard);
        }

        Ok(())
    }

    /// Si al intentar mandar un ping se recibe un error, se marca al nodo
    /// como PFAIL
    ///
    /// # Parámetros
    /// * `node_id`: id del nodo a analizar
    /// * `info_guard`: diccionario de nodos conocidos
    fn marcar_como_posiblemente_fallado(
        &self,
        node_id: &NodeId,
        info_guard: &mut HashMap<NodeId, NeighboringNodeInfo>,
    ) {
        if let Some(info) = info_guard.get_mut(node_id) {
            info.set_pfail(true);
            self.logger.warn(
                &format!("Marcando a nodo {node_id:?} como PFAIL"),
                "FAILURE",
            );
        }
    }

    /// Marca a los nodos sin respuesta pong como PFAIL
    ///
    /// # Parámetros
    /// * `marcado`: set de nodos considerados PFAIL
    ///
    /// # Retorna
    /// - () en caso de éxito, ClusterError en otro caso
    fn verificar_pfail(&self, marcados: &mut HashSet<NodeId>) -> Result<(), ClusterError> {
        let mut info_guard = match self.knows_nodes.write() {
            Ok(guard) => guard,
            Err(e) => {
                self.logger
                    .error(&format!("Error en hilo de pong: {e}"), "HEARTBEAT");
                // Debo marcar como NodeStarus::Fail ya que ocurrio un error importante.
                self.marcar_node_fail("HEARTBEAT");
                return Err(ClusterError::new_lock_error("nodos conocidos", "HEARTBEAT"));
            }
        };

        for (node_id, vecino) in info_guard.iter_mut() {
            if vecino.is_suspected_failed(Duration::from_millis(self.node_timeout))
                && !marcados.contains(node_id)
            {
                marcados.insert(node_id.clone());
                //println!("{:#?}", vecino.get_flags());
                vecino.set_pfail(true);
                self.logger
                    .warn(&format!("PFAIL Node [{node_id:?}]"), "FAILURE [PFAIL]");
                println!(
                    "[PFAIL DEMO] failover detectado para nodo [{:?}]",
                    vecino.get_node_addr()
                );
                //println!("{:#?}", vecino.get_flags());
                let b = vecino.get_ping_sent_time();
                let c = vecino.get_pong_received_time();
                println!("TIEMPO DEL ULTIMO PING {b:?} TIEMPO DEL ULTIMO PONG {c:?}");
            }
        }
        Ok(())
    }

    /// Verifica si hay una cantidad suficiente de masters conocidos y la cobertura de slots
    /// es completa, de no ser así, se marca CLUSTER FAIL
    ///
    /// # Retorna
    /// - () en caso de éxito, ClusterError en otro caso
    pub fn validar_estado_cluster(&self) -> Result<(), ClusterError> {
        match self.recolectar_rangos_y_masters() {
            Ok((rangos, masters_vivos)) => {
                let cobertura_ok = Self::verificar_cobertura_slots(&rangos);
                let quorum_ok = masters_vivos >= (CANT_INICIAL_MASTERS / 2 + 1) as usize;

                let soy_master = match self.role.read() {
                    Ok(role) => *role == NodeRole::Master,
                    Err(_) => {
                        return Err(ClusterError::new_lock_error("rol", "CLUSTER"));
                    }
                };

                let cluster_ok = if soy_master {
                    cobertura_ok && quorum_ok
                } else {
                    cobertura_ok
                };

                //println!("COBERTURA: {cluster_ok} {cobertura_ok} {quorum_ok}");

                if let Ok(mut state) = self.cluster_state.write() {
                    *state = if cluster_ok {
                        ClusterState::Ok
                    } else {
                        ClusterState::Fail
                    };
                } else {
                    return Err(ClusterError::new_lock_error("cluster state", "CLUSTER"));
                }
                Ok(())
            }
            Err(e) => Err(ClusterError::new_cluster_validation_error(e, "CLUSTER")),
        }
    }

    /// A partir de los nodos conocidos, retorn la cobertura de slots
    /// y la cantidad de masters
    ///
    /// # Retorna
    /// - Tupla de vector de rangos cubiertos y cantidad de masters en caso de
    ///   éxito, ClusterError en otro caso
    fn recolectar_rangos_y_masters(&self) -> Result<(Vec<Range<u16>>, usize), ClusterError> {
        let mut rangos = Vec::new();
        let mut masters_vivos = 0;

        let guard = self
            .knows_nodes
            .read()
            .map_err(|_| ClusterError::new_lock_error("nodos conocidos", "PING PONG"))?;
        for info in guard.values() {
            if info.get_role() == NodeRole::Master {
                masters_vivos += 1;
            }
            rangos.push(info.get_slot_range());
        }

        let r = self.slot_range.clone();
        let role = self
            .role
            .read()
            .map_err(|_| ClusterError::new_lock_error("rol", "PING PONG"))?;
        if *role == NodeRole::Master {
            masters_vivos += 1;
        }
        rangos.push(r);

        Ok((rangos, masters_vivos))
    }

    /// Verifica si todos los slots están cubiertos a partir de un slice
    /// de rangos
    ///
    /// # Parámetros
    /// * `rangos`: Slice de rangos cubiertos
    ///
    /// # Retorna
    /// - true si se cubren todos los slots, falso en otro caso
    fn verificar_cobertura_slots(rangos: &[Range<u16>]) -> bool {
        if rangos.is_empty() || rangos.iter().any(|r| r.start >= r.end) {
            return false;
        }

        let mut rangos = rangos.to_vec();
        rangos.sort_by_key(|r| r.start);
        let mut limite = 0;
        if rangos[0].start != 0 {
            return false;
        }

        for r in rangos {
            if r.start > limite {
                return false;
            }
            if r.end > limite {
                limite = r.end + 1;
            }
            if limite >= TOTAL_SLOTS {
                break;
            }
        }

        limite >= TOTAL_SLOTS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_protocol::internal_protocol_msg::ClusterMessagePayload;
    use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
    use crate::node_builder::NodeBuilder;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn hilo_ping_actualiza_ping_sent_time() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        // Nodo A (el que pingea)
        let node_a = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6400".parse().unwrap())
                .node_addr("127.0.0.1:16400".parse().unwrap())
                .cluster_addr("127.0.0.1:16400".parse().unwrap())
                .public_addr("127.0.0.1:6400".parse().unwrap())
                .build()
                .unwrap(),
        );

        // Nodo B y conectamos streams
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bus_addr = listener.local_addr().unwrap();
        let _stream_b = TcpStream::connect(bus_addr).unwrap();
        let (stream_a, _) = listener.accept().unwrap();

        // Registramos B en tabla de A
        let vecino_b = NeighboringNodeInfo::from_cluster_msg(&ClusterMessage::new(
            node_a
                .message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        ));
        node_a
            .knows_nodes
            .write()
            .unwrap()
            .insert(id_b.clone(), vecino_b);

        let outgoing = Arc::new(RwLock::new({
            let mut map = HashMap::new();
            map.insert(id_b.clone(), stream_a.try_clone().unwrap());
            map
        }));

        node_a.clone().iniciar_hilo_ping_pong(outgoing.clone());
        // Esperamos medio timeout
        sleep(Duration::from_millis(250));

        // El ping_sent_time para B debe haberse actualizado (<100 ms)
        let guard = node_a.knows_nodes.read().unwrap();
        let info_b = guard.get(&id_b).unwrap();
        assert!(info_b.get_ping_sent_time() < Duration::from_millis(100));
    }

    #[test]
    fn detectar_pfail_cuando_no_hay_pong() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6500".parse().unwrap())
                .node_addr("127.0.0.1:16500".parse().unwrap())
                .cluster_addr("127.0.0.1:16500".parse().unwrap())
                .public_addr("127.0.0.1:6500".parse().unwrap())
                .build()
                .unwrap(),
        );

        // Vecino B con pong_received_time viejo
        let mut info_b = NeighboringNodeInfo::from_cluster_msg(&ClusterMessage::new(
            node.message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        ));
        info_b.set_ping_sent_time(Instant::now() - Duration::from_millis(1000));
        info_b.set_pong_received_time(Instant::now() - Duration::from_millis(1500));

        node.knows_nodes
            .write()
            .unwrap()
            .insert(id_b.clone(), info_b);

        let mut marcados = HashSet::new();
        node.verificar_pfail(&mut marcados).unwrap();

        // Debe haberse marcado como PFAIL
        let guard = node.knows_nodes.read().unwrap();
        println!("marcados: {guard:#?}");
        assert!(guard.get(&id_b).unwrap().get_flags().is_pfail());
    }

    #[test]
    fn cluster_fail_sin_cobertura_de_slots() {
        let id = NodeId::new();
        let node = NodeBuilder::new()
            .id(id)
            .cli_addr("127.0.0.1:6600".parse().unwrap())
            .node_addr("127.0.0.1:16600".parse().unwrap())
            .cluster_addr("127.0.0.1:16600".parse().unwrap())
            .public_addr("127.0.0.1:6600".parse().unwrap())
            .slot_range(0..5000)
            .build()
            .unwrap();

        node.validar_estado_cluster().unwrap();
        assert_eq!(*node.cluster_state.read().unwrap(), ClusterState::Fail);
    }

    #[test]
    fn cobertura_slots_detecta_hueco() {
        let rangos = vec![0..5000, 6000..16383]; // hueco 5000..5999
        assert!(!Node::verificar_cobertura_slots(&rangos));
    }

    #[test]
    fn marcar_vecino_como_posiblemente_fallado_setea_pfail() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_a.clone())
                .cli_addr("127.0.0.1:6500".parse().unwrap())
                .node_addr("127.0.0.1:16500".parse().unwrap())
                .cluster_addr("127.0.0.1:16500".parse().unwrap())
                .public_addr("127.0.0.1:6500".parse().unwrap())
                .build()
                .unwrap(),
        );

        let cluster_msg = ClusterMessage::new(
            node.message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        );

        let info_b = NeighboringNodeInfo::from_cluster_msg(&cluster_msg);

        let mut info_guard = HashMap::new();
        info_guard.insert(id_b.clone(), info_b);

        assert!(!info_guard.get(&id_b).unwrap().get_flags().is_pfail());
        node.marcar_como_posiblemente_fallado(&id_b, &mut info_guard);
        assert!(info_guard.get(&id_b).unwrap().get_flags().is_pfail());
    }

    #[test]
    fn enviar_pings_timeout_envia_ping_a_vecinos_con_timeout_sobre_2() {
        let id_self = NodeId::new();
        let id_b = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(id_self.clone())
                .cli_addr("127.0.0.1:6501".parse().unwrap())
                .node_addr("127.0.0.1:16501".parse().unwrap())
                .cluster_addr("127.0.0.1:16501".parse().unwrap())
                .public_addr("127.0.0.1:6501".parse().unwrap())
                .node_timeout(1000)
                .build()
                .unwrap(),
        );

        // Preparamos el vecino con ping_sent_time viejo
        let cluster_msg = ClusterMessage::new(
            node.message_header_node(InternalProtocolType::Ping)
                .unwrap(),
            ClusterMessagePayload::Meet,
        );

        let mut info_b = NeighboringNodeInfo::from_cluster_msg(&cluster_msg);
        info_b.set_ping_sent_time(Instant::now() - Duration::from_millis(800)); // > timeout / 2

        node.knows_nodes
            .write()
            .unwrap()
            .insert(id_b.clone(), info_b);

        // Stream simulado (loopback)
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let outgoing_streams = Arc::new(RwLock::new(HashMap::new()));
        let ya_pingueados = HashSet::new();

        let handle = thread::spawn(move || listener.accept().unwrap());

        let stream = TcpStream::connect(addr).unwrap();
        outgoing_streams
            .write()
            .unwrap()
            .insert(id_b.clone(), stream.try_clone().unwrap());

        // Ejecutamos
        let ping_msg = node.ping_node().unwrap();
        node.enviar_pings_timeout(&ping_msg, &outgoing_streams, &ya_pingueados)
            .unwrap();

        handle.join().unwrap();

        let info_guard = node.knows_nodes.read().unwrap();
        let info_b_post = info_guard.get(&id_b).unwrap();
        let tiempo_ping = info_b_post.get_ping_sent_time();
        assert!(tiempo_ping < Duration::from_millis(100)); // Se actualizó hace poco
    }
}
