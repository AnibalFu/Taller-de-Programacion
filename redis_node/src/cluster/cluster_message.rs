//! Este módulo contiene las funciones para la creación de mensajes de comunicación
//! entre nodos más frecuentes, como gossip, y la creación del header genérico para cualquier mensaje
use crate::internal_protocol::gossip::GossipEntry;
use crate::internal_protocol::header::{HeaderParameters, MessageHeader};
use crate::internal_protocol::internal_protocol_msg::{ClusterMessage, ClusterMessagePayload};
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::internal_protocol::node_flags::NodeFlags;
use crate::log_msj::log_mensajes::loggear_error_lectura_master;
use crate::node::*;
use crate::node_role::NodeRole;
use rand::rng;
use rand::seq::IteratorRandom;
use std::sync::atomic::Ordering;

impl Node {
    /// Construye un `MessageHeader` para un mensaje de clúster saliente.
    ///
    /// Arma los campos del encabezado (tipo de protocolo, flags, epoch, direcciones, etc.)
    /// tal como lo hace Redis Cluster en cada mensaje entre nodos.
    ///
    /// # Argumentos
    /// - `tipo`: Tipo de mensaje interno (`PING`, `MEET`, `PONG`, etc.)
    ///
    /// # Retorna
    /// - Un Option de `MessageHeader` con la información actual del nodo en caso de exito
    pub(crate) fn message_header_node(&self, tipo: InternalProtocolType) -> Option<MessageHeader> {
        let rol = match self.role.read() {
            Ok(r) => r.clone(),
            Err(_) => {
                loggear_error_lectura_master(&self.logger, "INIT (role)");
                return None;
            }
        };

        let if_master = rol == NodeRole::Master;
        let flags = NodeFlags::new(if_master, !if_master, false, false);
        let slots = self.slot_range.clone();

        let node_status = match self.cluster_state.read() {
            Ok(s) => s.clone(),
            Err(_) => {
                loggear_error_lectura_master(&self.logger, "INIT (cluster_state)");
                return None;
            }
        };

        let master = match self.master.read() {
            Ok(m) => m.clone(),
            Err(_) => {
                loggear_error_lectura_master(&self.logger, "INIT (master)");
                return None;
            }
        };

        let header_parameters = HeaderParameters {
            header_type: tipo,
            node_id: self.id.clone(),
            current_epoch: self.current_epoch.load(Ordering::SeqCst),
            config_epoch: self.config_epoch.load(Ordering::SeqCst),
            flags,
            hash_slots_bitmap: slots,
            tcp_client_port: self.public_addr,
            cluster_node_port: self.cluster_addr,
            cluster_state: node_status,
            master_id: master,
        };
        Some(MessageHeader::new(header_parameters))
    }

    /// Crea un mensaje `PING` incluyendo información de gossip del nodo.
    ///
    /// Redis Cluster utiliza `PING` regularmente para verificar conectividad.
    /// El `PING` también transporta información de estado (gossip) sobre otros nodos.
    ///
    /// # Retorna
    /// - Un Option de `ClusterMessage` con tipo `PING` y payload `Gossip(...)` en caso de éxito.
    pub(crate) fn ping_node(&self) -> Option<ClusterMessage> {
        let header_option = self.message_header_node(InternalProtocolType::Ping);
        if let Some(header) = header_option {
            let gossip_section_result = self.gossip_node();
            if let Some(gossip_section) = gossip_section_result {
                return Some(ClusterMessage::new(header, gossip_section));
            }
        }
        None
    }

    /// Crea un mensaje `PONG` incluyendo información de gossip del nodo.
    ///
    /// Redis Cluster utiliza `PONG` regularmente para verificar conectividad.
    /// El `PONG` también transporta información de estado (gossip) sobre otros nodos.
    ///
    /// # Retorna
    /// - Un Option de `ClusterMessage` con tipo `PONG` y payload `Gossip(...)` en caso de éxito.
    pub(crate) fn pong_node(&self) -> Option<ClusterMessage> {
        let header_option = self.message_header_node(InternalProtocolType::Pong);
        if let Some(header) = header_option {
            let gossip_section_result = self.gossip_node();
            if let Some(gossip_section) = gossip_section_result {
                return Some(ClusterMessage::new(header, gossip_section));
            }
        }
        None
    }

    /// Genera la sección de gossip para un mensaje `PING` o `PONG`.
    ///
    /// Redis Cluster incluye en cada `PING` y `PONG` un subconjunto aleatorio
    /// de nodos conocidos, para propagar de forma progresiva la topología del clúster.
    ///
    /// Esta función elige aleatoriamente la mitad de los nodos conocidos (excluyéndose a sí mismo)
    /// y genera una lista de `GossipEntry` para incluir como payload del mensaje.
    ///
    /// # Retorna
    /// - Option de `ClusterMessagePayload::Gossip(Vec<GossipEntry>)` con las entradas seleccionadas en caso de éxito.
    pub fn gossip_node(&self) -> Option<ClusterMessagePayload> {
        let mut rng = rng();
        let guard_result = self.knows_nodes.read();

        match guard_result {
            Ok(guard) => {
                // Elegimos n/2 nodos random
                let cantidad = guard.len() / 2;
                let gossip_entries = guard
                    .values()
                    .filter(|n| n.get_id() != self.id) // excluimos a uno mismo
                    .choose_multiple(&mut rng, cantidad)
                    .into_iter()
                    .map(|n| n.to_gossip_entry())
                    .collect::<Vec<GossipEntry>>();

                // [DEBUG]

                Some(ClusterMessagePayload::Gossip(gossip_entries))
            }
            Err(e) => {
                self.logger.error(
                    &format!("Error {e:?} al obtener informacion de nodo para gossip"),
                    "GOSSIP",
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::neighboring_node::NeighboringNodeInfo;
    use crate::internal_protocol::node_flags::ClusterState;
    use crate::node_builder::NodeBuilder;
    use crate::node_id::NodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, RwLock};
    use std::time::Instant;

    fn dummy_node_id() -> NodeId {
        NodeId::new()
    }

    fn dummy_socket_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345)
    }

    fn dummy_neighbor(id: NodeId) -> NeighboringNodeInfo {
        NeighboringNodeInfo {
            id,
            cli_addr: dummy_socket_addr(),
            node_addr: dummy_socket_addr(),
            role: NodeRole::Master,
            slot_range: 0..100,
            status: ClusterState::Ok,
            flags: NodeFlags::new(true, false, false, false),
            ping_sent_time: Instant::now(),
            pong_received_time: Instant::now(),
            master: None,
        }
    }

    #[test]
    fn test_message_header_node_success() {
        let id = dummy_node_id();
        let cli_addr = dummy_socket_addr();
        let node_addr = dummy_socket_addr();
        let cluster_addr = dummy_socket_addr();

        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr(cli_addr)
            .node_addr(node_addr)
            .slot_range(0..1000)
            .cluster_addr(cluster_addr)
            .public_addr(cli_addr)
            .build()
            .expect("Node build failed");

        let header = node.message_header_node(InternalProtocolType::Ping);
        assert!(header.is_some());

        let header = header.unwrap();
        assert_eq!(header.node_id(), id);
        assert_eq!(header.client_node_addr(), cli_addr);
        assert_eq!(header.cluster_node_addr(), node_addr);
        assert_eq!(header.slot_range(), 0..1000);
        assert_eq!(header.get_type(), InternalProtocolType::Ping);
        assert_eq!(header.status(), ClusterState::Ok);
        assert!(header.master_id().is_none());
    }

    #[test]
    fn test_ping_node_and_pong_node_return_some() {
        let id = dummy_node_id();
        let cli_addr = dummy_socket_addr();
        let node_addr = dummy_socket_addr();
        let cluster_addr = dummy_socket_addr();

        let mut knows = std::collections::HashMap::new();
        knows.insert(dummy_node_id(), dummy_neighbor(dummy_node_id()));
        knows.insert(dummy_node_id(), dummy_neighbor(dummy_node_id()));

        let node = NodeBuilder::new()
            .id(id)
            .cli_addr(cli_addr)
            .node_addr(node_addr)
            .slot_range(0..1000)
            .cluster_addr(cluster_addr)
            .knows_nodes(Arc::new(RwLock::new(knows)))
            .public_addr(cli_addr)
            .build()
            .expect("Node build failed");

        let ping_msg_opt = node.ping_node();
        assert!(ping_msg_opt.is_some());

        let pong_msg_opt = node.pong_node();
        assert!(pong_msg_opt.is_some());

        // Validamos payload tipo Gossip con entradas
        if let Some(ping_msg) = ping_msg_opt {
            match ping_msg.payload() {
                ClusterMessagePayload::Gossip(gossip_entries) => {
                    assert!(!gossip_entries.is_empty());
                }
                _ => panic!("Payload de ping no es Gossip"),
            }
        }

        if let Some(pong_msg) = pong_msg_opt {
            match pong_msg.payload() {
                ClusterMessagePayload::Gossip(gossip_entries) => {
                    assert!(!gossip_entries.is_empty());
                }
                _ => panic!("Payload de pong no es Gossip"),
            }
        }
    }

    #[test]
    fn test_gossip_node_returns_some_and_excludes_self() {
        let id = dummy_node_id();

        let mut knows = std::collections::HashMap::new();

        let self_neighbor = dummy_neighbor(id.clone());
        let other_neighbor = dummy_neighbor(dummy_node_id());

        knows.insert(self_neighbor.get_id(), self_neighbor);
        knows.insert(other_neighbor.get_id(), other_neighbor);

        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr("127.0.0.1:6379".parse().unwrap())
            .node_addr("127.0.0.1:16379".parse().unwrap())
            .cluster_addr("127.0.0.1:16379".parse().unwrap())
            .public_addr("127.0.0.1:6379".parse().unwrap())
            .build()
            .unwrap();

        let gossip_payload_opt = node.gossip_node();

        assert!(gossip_payload_opt.is_some());

        if let Some(ClusterMessagePayload::Gossip(entries)) = gossip_payload_opt {
            for entry in &entries {
                assert_ne!(entry.node_id(), &id);
            }
        } else {
            panic!("Payload no es Gossip");
        }
    }
}
