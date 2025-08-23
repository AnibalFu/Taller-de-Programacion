//! Este módulo contiene funciones auxiliares para el cluster
use crate::node::Node;
use crate::node_id::NodeId;
use crate::node_status::NodeStatus;
use std::collections::HashMap;
use std::io;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

impl Node {
    /// Marca el estado de un nodo como FAIL
    ///
    /// # Parámetros
    /// * `modulo`: módulo donde registrar el error de ser necesario
    pub(crate) fn marcar_node_fail(&self, modulo: &str) {
        match self.status.write() {
            Ok(mut status_guard) => {
                *status_guard = NodeStatus::Ok;
                self.logger.warn("Nodo marcado como FAIL", modulo);
            }
            Err(e) => {
                let mut poisoned_guard = e.into_inner();
                *poisoned_guard = NodeStatus::Ok;
                self.logger
                    .warn("Nodo marcado como FAIL a pesar de lock envenenado", modulo);
            }
        }
    }

    /// Agrega un stream a los diccionarios de streams entrantes/salientes del nodo
    ///
    /// # Parámetros
    /// * `node_id`: id del nodo a agregar
    /// * `stream`: stream del nodo
    /// * `incoming_streams`: streams entrantes del nodo
    /// * `outgoing_streams`: streams salientes del nodo
    /// * `ya_pingueados`: set de nodos a los que se les envió ping aleatoriamente
    ///
    /// # Retorna
    /// - io result
    pub fn agregar_stream_connection(
        &self,
        node_id: NodeId,
        stream: &mut TcpStream,
        incoming_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        outgoing_streams: &Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    ) -> io::Result<()> {
        stream.set_nonblocking(true)?;
        // stream.set_read_timeout(Some(Duration::from_millis(1000)))?;

        let lectura = stream.try_clone()?;
        let escritura = stream.try_clone()?;

        {
            let mut guard = incoming_streams
                .write()
                .map_err(|_| io::Error::other("Lock poisoned (incoming_streams)"))?;
            guard.insert(node_id.clone(), lectura);
        }

        {
            let mut guard = outgoing_streams
                .write()
                .map_err(|_| io::Error::other("Lock poisoned (outgoing_streams)"))?;
            guard.insert(node_id, escritura);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeStatus;
    use crate::node_builder::NodeBuilder;

    #[test]
    fn test_marcar_node_fail_actualiza_status() {
        let node = NodeBuilder::new()
            .id(NodeId::new())
            .cli_addr("127.0.0.1:7003".parse().unwrap())
            .node_addr("127.0.0.1:17003".parse().unwrap())
            .cluster_addr("127.0.0.1:17003".parse().unwrap())
            .public_addr("127.0.0.1:7003".parse().unwrap())
            .build()
            .unwrap();

        {
            let mut status_guard = node.status.write().unwrap();
            *status_guard = NodeStatus::Fail;
        }

        node.marcar_node_fail("TEST");
        let status_guard = node.status.read().unwrap();
        assert_eq!(*status_guard, NodeStatus::Ok);
    }
}
