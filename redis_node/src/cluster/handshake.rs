//! Modulo que se encarga del sistema de HandShake

use crate::cluster::neighboring_node::NeighboringNodeInfo;
use crate::internal_protocol::internal_protocol_msg::{
    ClusterMessage, ClusterMessagePayload, recv_cluster_message, send_cluster_message,
};
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::node::Node;
use crate::node_id::NodeId;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

impl Node {
    /// Crea un mensaje `ClusterMessage` de tipo `MEET` con payload vacío.
    ///
    /// Este tipo de mensaje se usa cuando un nodo intenta anunciarse a otro
    /// por primera vez en el clúster, según el protocolo de Redis Cluster.
    ///
    /// # Retorna
    /// - Un Option de `ClusterMessage` con tipo `MEET` y payload vacío en caso de exito.
    fn meet(&self, tipo: InternalProtocolType) -> Option<ClusterMessage> {
        let header_option = self.message_header_node(tipo);
        if let Some(header) = header_option {
            return Some(ClusterMessage::new(header, ClusterMessagePayload::Meet));
        }
        None
    }

    /// Envía un mensaje `MEET` al nodo conectado a través del stream.
    ///
    /// Es el primer mensaje que se envía luego de una conexión TCP saliente
    /// como parte del protocolo de descubrimiento en Redis Cluster.
    ///
    /// # Argumentos
    /// - `stream`: Conexión TCP abierta al nodo destino.
    ///
    /// # Retorna
    /// - () en caso de éxito, error de IO en caso de falla
    pub(crate) fn iniciar_meet(
        &self,
        stream: &mut TcpStream,
        tipo: InternalProtocolType,
    ) -> Result<(), std::io::Error> {
        let cluster_msg_option = self.meet(tipo);
        if let Some(cluster_msg) = cluster_msg_option {
            if let Err(e) = send_cluster_message(stream, &cluster_msg) {
                self.logger.error(
                    &format!("Error {e:?} al enviar meet a stream {stream:?}"),
                    "INIT",
                );
                return Err(e);
            }
            Ok(())
        } else {
            Err(std::io::ErrorKind::Interrupted.into())
        }
    }

    /// Recibe un mensaje `MEET` y actualiza los mapas de nodos y streams.
    ///
    /// Esta función simula el comportamiento de Redis Cluster cuando un nodo recibe
    /// un `MEET`: guarda la información del nuevo nodo y registra su stream.
    ///
    /// # Argumentos
    /// - `stream`: Conexión TCP del nodo que envió el `MEET`.
    /// - `incoming_streams`: Mapa compartido para registrar streams entrantes.
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    ///
    /// # Retorna
    /// - () en caso de éxito, error de IO en caso de falla
    pub(crate) fn recibir_meet(
        &self,
        stream: &mut TcpStream,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), std::io::Error> {
        let meet_result = recv_cluster_message(stream);
        match meet_result {
            Ok(meet) => {
                let remitent_info = NeighboringNodeInfo::from_cluster_msg(&meet);
                let id = remitent_info.get_id().clone();
                {
                    match self.knows_nodes.write() {
                        Ok(mut knows_node_guard) => {
                            println!(
                                "[DEMO] HANDSHAKE CON: [{:?}]",
                                remitent_info.get_node_addr()
                            );
                            knows_node_guard.insert(remitent_info.get_id(), remitent_info);
                        }
                        Err(_) => {
                            self.logger
                                .error("Error al tomar lock de knows_nodes", "HANDSHAKE");
                            return Err(std::io::Error::other("Lock poisoned en knows_nodes"));
                        }
                    }
                }
                self.agregar_stream_connection(
                    id.clone(),
                    stream,
                    &incoming_streams,
                    &outgoing_streams,
                )?;
                self.logger.info(
                    &format!("New node connection from [{:?}]", stream.peer_addr()?),
                    "HANDSHAKE",
                );
                Ok(())
            }
            Err(e) => {
                self.logger.error(
                    &format!("Error {e} al recibir meet de stream {stream:?}"),
                    "HANDSHAKE",
                );
                Err(e)
            }
        }
    }

    /// Actualiza al nodo a réplica, y notifica al master
    ///
    /// # Argumentos
    /// - `master_stream`: Option del tcp stream del master.
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    pub(crate) fn meet_master(
        &self,
        master_stream: Option<TcpStream>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) {
        if let Some(mut stream) = master_stream {
            // Cuando soy yo el que se conecta mando un meet y espero un meet
            let id = self.get_id_from_stream(&stream, outgoing_streams.clone());
            {
                if !self.set_master_id(id) {
                    return;
                }
                if !self.actualizar_rol_a_replica() {
                    return;
                }
            }
            if let Err(e) = self.iniciar_meet(&mut stream, InternalProtocolType::MeetMaster) {
                self.logger.error(
                    &format!("Error {e:?} al propagar estado de replica a master {stream:?}"),
                    "INIT",
                );
            }
        }
    }

    /// Actualiza el master del nodo
    ///
    /// # Argumentos
    /// - `id`: Option del id del nuevo master.
    ///
    /// # Retorna
    /// - true si el cambio se realizó correctamente, false en otro caso
    pub(crate) fn set_master_id(&self, id: Option<NodeId>) -> bool {
        let master_result = self.master.write();
        if let Ok(mut master) = master_result {
            *master = id;
            true
        } else {
            self.logger.error("Error al actualizar master", "INIT");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_protocol::node_flags::ClusterState;
    use crate::node_builder::NodeBuilder;
    use crate::node_role::NodeRole;
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
        sync::{Arc, RwLock},
        thread,
        time::Duration,
    };

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn new_node(id: NodeId) -> Node {
        NodeBuilder::new()
            .id(id)
            .cli_addr(addr(6379))
            .node_addr(addr(16379))
            .cluster_addr(addr(16379))
            .public_addr(addr(6379))
            .build()
            .unwrap()
    }

    #[test]
    fn message_header_node_ok() {
        let id = NodeId::new();
        let node = new_node(id.clone());

        let header = node
            .message_header_node(InternalProtocolType::Ping)
            .expect("esperaba Some");

        assert_eq!(header.node_id(), id);
        assert_eq!(header.get_type(), InternalProtocolType::Ping);
        assert_eq!(header.client_node_addr(), addr(6379));
        assert_eq!(header.cluster_node_addr(), addr(16379));
        assert_eq!(header.status(), ClusterState::Ok);
    }

    #[test]
    fn iniciar_y_recibir_meet() {
        let node_a = new_node(NodeId::new());
        let node_b = Arc::new(new_node(NodeId::new()));

        let listener = TcpListener::bind(addr(0)).unwrap();
        let destino = listener.local_addr().unwrap();

        let incoming = Arc::new(RwLock::new(HashMap::<NodeId, TcpStream>::new()));
        let outgoing = Arc::new(RwLock::new(HashMap::<NodeId, TcpStream>::new()));

        // receptor
        let clone_b = node_b.clone();
        let h = thread::spawn({
            let incoming = incoming.clone();
            let outgoing = outgoing.clone();
            move || {
                let (mut sock, _) = listener.accept().unwrap();
                clone_b.recibir_meet(&mut sock, incoming, outgoing).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(50)); // aseguramos listener

        // emisor
        let mut sock = TcpStream::connect(destino).unwrap();
        node_a
            .iniciar_meet(&mut sock, InternalProtocolType::Meet)
            .unwrap();

        h.join().unwrap();

        // node_b debería conocer a node_a
        assert_eq!(node_b.knows_nodes.read().unwrap().len(), 1);
    }

    #[test]
    fn meet_master_actualiza_replica() {
        let master_id = NodeId::new();
        let master = new_node(master_id.clone());
        let replica = new_node(NodeId::new());

        let listener = TcpListener::bind(addr(0)).unwrap();
        let destino = listener.local_addr().unwrap();

        let outgoing = Arc::new(RwLock::new(HashMap::<NodeId, TcpStream>::new()));

        // hilo que hace de master receptor
        let h = thread::spawn({
            let outgoing = outgoing.clone();
            move || {
                let (mut sock, _) = listener.accept().unwrap();
                master
                    .recibir_meet(&mut sock, Arc::new(RwLock::new(HashMap::new())), outgoing)
                    .unwrap();
                println!("{master:?}");
            }
        });

        thread::sleep(Duration::from_millis(50));
        let stream = TcpStream::connect(destino).unwrap();
        replica.meet_master(Some(stream), outgoing);
        replica.set_master_id(Some(master_id));
        h.join().unwrap();

        // replica debe tener rol Replica y master id seteado
        assert_eq!(*replica.role.read().unwrap(), NodeRole::Replica);
        assert!(replica.master.read().unwrap().is_some());
    }
}
