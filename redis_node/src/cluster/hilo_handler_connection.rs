//! Este módulo realiza el handshake con otro nodo entrante e inicia el hilo
//! de ping pong correspondiente
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::log_msj::log_mensajes::loggear_error_handhsake;
use crate::node::Node;
use crate::node_id::NodeId;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread::spawn;

impl Node {
    /// Lanza un hilo encargado de aceptar conexiones entrantes de otros nodos en el `ClusterBus`.
    ///
    /// Este hilo se mantiene escuchando en el socket interno (`node_addr`) y maneja el proceso
    /// de **handshake inicial entre nodos del clúster**. Este proceso se realiza mediante un
    /// intercambio obligatorio de mensajes `MEET` (tipo `ClusterMessage`), que permite a ambos nodos
    /// registrar sus `NodeId` mutuos y establecer las conexiones entrantes/salientes correspondientes.
    ///
    /// # Parámetros
    /// - `cluster_bus`: Socket `TcpListener` ya enlazado en `node_addr`, desde donde se reciben las conexiones de otros nodos.
    /// - `incoming_streams`: Mapa compartido `NodeId → TcpStream` para registrar las conexiones entrantes.
    /// - `outgoing_streams`: Mapa compartido `NodeId → TcpStream` para registrar las conexiones salientes.
    ///
    /// # Comportamiento
    /// - Por cada conexión entrante:
    ///     1. Se espera recibir un mensaje `MEET` desde el nodo que se conecta.
    ///     2. Si el mensaje es válido, se registra el `NodeId` correspondiente en `incoming_streams`.
    ///     3. Se envía a la contraparte un mensaje propio `MEET`, para que también nos registre.
    /// - Si cualquier paso del handshake falla:
    ///     - Se loguea el error con `loggear_error_handhsake`.
    ///     - Se marca al nodo como en fallo (`marcar_node_fail`) y se termina el hilo.
    ///
    /// # Retorna
    /// - `Ok(())` si el hilo se lanzó correctamente (la ejecución continúa en background).
    /// - `Err(())` si ocurre algún error crítico antes de iniciar el hilo.
    ///
    /// # Notas
    /// - Este hilo debe iniciarse al momento de levantar el nodo para permitir
    ///   conexiones de otros peers del clúster.
    /// - El protocolo de conexión entre nodos se basa en el intercambio simétrico
    ///   de mensajes `MEET`, lo que permite establecer una topología distribuida y conectada.
    pub(crate) fn iniciar_hilo_receptor_node(
        self: Arc<Self>,
        cluster_bus: TcpListener,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> Result<(), ()> {
        spawn(move || {
            for stream in cluster_bus.incoming() {
                match stream {
                    Ok(mut stream) => {
                        if self
                            .recibir_meet(
                                &mut stream,
                                incoming_streams.clone(),
                                outgoing_streams.clone(),
                            )
                            .is_err()
                        {
                            loggear_error_handhsake(&self.logger);
                            self.marcar_node_fail("NODE-CONNECTION");
                            break;
                        }
                        if self
                            .iniciar_meet(&mut stream, InternalProtocolType::Meet)
                            .is_err()
                        {
                            loggear_error_handhsake(&self.logger);
                            self.marcar_node_fail("NODE-CONNECTION");
                            break;
                        }
                    }
                    Err(e) => {
                        self.logger
                            .info(&format!("Error accept connection {e}"), "NODE-CONNECTION");
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_builder::NodeBuilder;
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

    /// Construye un nodo con los campos obligatorios mínimos.
    fn new_node(id: NodeId, cli: u16, bus: u16) -> Node {
        NodeBuilder::new()
            .id(id)
            .cli_addr(addr(cli))
            .node_addr(addr(bus))
            .cluster_addr(addr(bus))
            .public_addr(addr(cli))
            .build()
            .unwrap()
    }

    #[test]
    fn hilo_receptor_node_realiza_handshake_meet() {
        // ---------- nodo receptor (self) ----------
        let recv_id = NodeId::new();
        let recv_node = Arc::new(new_node(recv_id.clone(), 6400, 16400));

        // maps compartidos que el hilo receptor va a modificar
        let incoming = Arc::new(RwLock::new(HashMap::<NodeId, TcpStream>::new()));
        let outgoing = Arc::new(RwLock::new(HashMap::<NodeId, TcpStream>::new()));

        // listener para el Cluster Bus del receptor
        let listener = TcpListener::bind(addr(0)).unwrap();
        let listen_addr = listener.local_addr().unwrap();

        // levantamos el hilo receptor
        recv_node
            .clone()
            .iniciar_hilo_receptor_node(listener, incoming.clone(), outgoing.clone())
            .unwrap();

        // ---------- nodo emisor (nuevo nodo que quiere unirse) ----------
        let sender_id = NodeId::new();
        let sender_node = new_node(sender_id.clone(), 6500, 16500);

        // nos conectamos al cluster-bus del receptor
        let mut stream = TcpStream::connect(listen_addr).unwrap();

        // el emisor inicia el MEET
        sender_node
            .iniciar_meet(&mut stream, InternalProtocolType::Meet)
            .expect("sender iniciar_meet falló");

        // damos tiempo a que el hilo receptor procese
        thread::sleep(Duration::from_millis(200));

        let knows = recv_node.knows_nodes.read().unwrap();
        assert!(knows.contains_key(&sender_id));
        assert!(incoming.read().unwrap().contains_key(&sender_id));
        assert!(outgoing.read().unwrap().contains_key(&sender_id));
    }
}
