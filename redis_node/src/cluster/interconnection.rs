//! Este módulo contiene las funciones para la conexión del nodo a la
//! semilla, a sus réplicas y a otros nodos del cluster
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::log_msj::log_mensajes::loggear_error_conectar_otro_nodo;
use crate::node::*;
use crate::node_id::NodeId;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock};

impl Node {
    /// Intenta conectarse al nodo semilla especificado para inicializar el clúster.
    ///
    /// Si `seed_addr` es `Some`, se establece una conexión TCP con dicho nodo e
    /// inicia un intercambio de mensajes tipo `MEET`, como en Redis Cluster.
    /// Si es `None`, no hace nada (nodo aislado o inicializa un clúster nuevo).
    ///
    /// # Argumentos
    /// - `seed_addr`: Dirección del nodo semilla.
    /// - `incoming_streams`: Mapa compartido para registrar streams entrantes.
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    ///
    /// # Retorna
    /// - `Ok(())` si la conexión fue exitosa o no se proporcionó semilla.
    /// - `Err(())` si falló la conexión al nodo semilla.
    pub(crate) fn conectar_a_seed(
        self: Arc<Self>,
        seed_addr: Option<SocketAddr>,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<Option<TcpStream>, ()> {
        if let Some(seed_addr) = seed_addr {
            self.logger
                .info(&format!("Connect to seed [{seed_addr}]"), "INTERCONECTION");
            return self.conectar_otro_nodo(seed_addr, incoming_streams, outgoing_streams);
        }

        Ok(None)
    }

    /// Establece conexión TCP con otro nodo del clúster, enviando un mensaje tipo `MEET`.
    ///
    /// Tras conectarse, se envía un mensaje `MEET` y se espera la respuesta del otro nodo.
    /// Esta función representa el _handshake inicial_ entre dos nodos en Redis Cluster.
    /// Si el intercambio es exitoso, se actualiza el conjunto de nodos conocidos.
    ///
    /// # Argumentos
    /// - `socket_addr`: Dirección del nodo destino.
    /// - `incoming_streams`: Mapa de streams entrantes.
    /// - `outgoing_streams`: Mapa de streams salientes.
    ///
    /// # Retorna
    /// - `Ok(())` si se establece conexión y handshake correctamente.
    /// - `Err(())` si ocurre un error de conexión TCP.
    pub(crate) fn conectar_otro_nodo(
        &self,
        socket_addr: SocketAddr,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<Option<TcpStream>, ()> {
        match TcpStream::connect(socket_addr) {
            Ok(mut stream) => {
                // Cuando soy yo el que se conecta mando un meet y espero un meet
                if let Err(e) = self.iniciar_meet(&mut stream, InternalProtocolType::Meet) {
                    loggear_error_conectar_otro_nodo(&self.logger, e, socket_addr);
                    return Err(());
                }
                if let Err(e) = self.recibir_meet(&mut stream, incoming_streams, outgoing_streams) {
                    loggear_error_conectar_otro_nodo(&self.logger, e, socket_addr);
                    return Err(());
                }
                Ok(Some(stream))
            }
            Err(e) => {
                loggear_error_conectar_otro_nodo(&self.logger, e, socket_addr);
                Err(())
            }
        }
    }

    /// Conecta al nodo a su master, de tenerlo
    ///
    /// # Argumentos
    /// - `replicaof_addr`: Option del socket address del master (obtenido del archivo .conf).
    /// - `seed_add`: Option del socket address del nodo semilla, de existir.
    /// - `incoming_streams`: Mapa compartido para registrar streams entrantes.
    /// - `outgoing_streams`: Mapa compartido para registrar streams salientes.
    ///
    /// # Retorna
    /// - Option del tcp stream del master, en caso de ser una réplica
    pub(crate) fn conectar_a_replicaof(
        &self,
        replicaof_addr: Option<SocketAddr>,
        seed_add: Option<SocketAddr>,
        seed_stream: Option<TcpStream>,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<Option<TcpStream>, ()> {
        if let Some(addr) = replicaof_addr {
            if let Some(seed_addr) = seed_add {
                if addr == seed_addr {
                    return Ok(seed_stream);
                }
            }

            match self.conectar_otro_nodo(addr, incoming_streams, outgoing_streams) {
                Ok(Some(stream)) => {
                    self.logger
                        .info(&format!("Connect to master [{addr}]"), "INTERCONECTION");
                    Ok(Some(stream))
                }
                Ok(None) => Ok(None),
                Err(_) => {
                    self.logger.warn(
                        &format!("No se pudo conectar a master [{addr}]"),
                        "INTERCONECTION",
                    );
                    Err(())
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {

    use crate::internal_protocol::header::{HeaderParameters, MessageHeader};
    use crate::internal_protocol::internal_protocol_msg::{
        ClusterMessage, ClusterMessagePayload, recv_cluster_message, send_cluster_message,
    };
    use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
    use crate::internal_protocol::node_flags::{ClusterState, NodeFlags};
    use crate::node_builder::NodeBuilder;
    use crate::node_id::NodeId;
    use std::collections::HashMap;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_conectar_a_seed_exito_y_none() {
        let node_id_b = NodeId::new();
        let node = Arc::new(
            NodeBuilder::new()
                .id(NodeId::new())
                .cli_addr("127.0.0.1:7001".parse().unwrap())
                .node_addr("127.0.0.1:17001".parse().unwrap())
                .cluster_addr("127.0.0.1:17001".parse().unwrap())
                .public_addr("127.0.0.1:7001".parse().unwrap())
                .build()
                .unwrap(),
        );

        let incoming_streams = Arc::new(RwLock::new(HashMap::new()));
        let outgoing_streams = Arc::new(RwLock::new(HashMap::new()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = recv_cluster_message(&mut stream);

                // Construir y enviar un MEET válido de respuesta
                let header_resp = MessageHeader::new(HeaderParameters {
                    header_type: InternalProtocolType::Meet,
                    node_id: node_id_b.clone(),
                    current_epoch: 1,
                    config_epoch: 1,
                    flags: NodeFlags::new(true, false, false, false),
                    hash_slots_bitmap: 0..0,
                    tcp_client_port: "127.0.0.1:7002".parse().unwrap(),
                    cluster_node_port: "127.0.0.1:17002".parse().unwrap(),
                    cluster_state: ClusterState::Ok,
                    master_id: None,
                });
                let meet_msg = ClusterMessage::new(header_resp, ClusterMessagePayload::Meet);
                let _ = send_cluster_message(&mut stream, &meet_msg);
                let _ = stream.flush();
                thread::sleep(Duration::from_millis(100));
            }
        });

        // Caso exitoso: conectar a nodo semilla
        let result = node.clone().conectar_a_seed(
            Some(addr),
            incoming_streams.clone(),
            outgoing_streams.clone(),
        );
        assert!(result.is_ok());
        assert!(result.as_ref().unwrap().is_some());

        // Caso None: no hace nada, solo devuelve Ok(None)
        let result_none = node.conectar_a_seed(None, incoming_streams, outgoing_streams);
        assert!(result_none.is_ok());
        assert!(result_none.unwrap().is_none());

        server_handle.join().unwrap();
    }
}
