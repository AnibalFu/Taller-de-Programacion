//! Este módulo contiene la lógica prinicipal para el hilo de lectura
//! de los nodos para mensajes de cluster
use crate::cluster::node_message::TipoMensajeNode;
use crate::internal_protocol::internal_protocol_msg::recv_cluster_message;
use crate::node::Node;
use crate::node_id::NodeId;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::Duration;

impl Node {
    /// Lanza un hilo dedicado a leer continuamente los mensajes entrantes desde otros nodos del clúster.
    ///
    /// Este hilo itera cada 100ms para evitar "busy waiting", adquiere el lock de escritura
    /// sobre `incoming_streams` y trata de leer datos de cada conexión TCP abierta.
    ///
    /// Si se recibe un mensaje válido en algún stream, se envía a través del canal `tx_connect`
    /// para que el hilo de procesamiento principal lo maneje.
    ///
    /// Si ocurre un error al adquirir el lock o al leer un stream, se loguea una advertencia usando el
    /// logger del nodo.
    ///
    /// # Parámetros
    /// - `self`: Referencia compartida (`Arc<Self>`) al nodo actual.
    /// - `incoming_streams`: Mapa compartido de streams entrantes desde otros nodos del clúster.
    /// - `tx_connect`: Canal de envío usado para comunicar mensajes leídos con el hilo de procesamiento.
    ///
    /// # Nota
    /// Este hilo **no** elimina conexiones rotas automáticamente del `incoming_streams`. Se recomienda
    /// implementar esa lógica si se desea robustez ante desconexiones.
    pub(crate) fn iniciar_hilo_lector_node(
        self: Arc<Self>,
        incoming_streams: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        tx_connect: Sender<TipoMensajeNode>,
    ) {
        spawn(move || {
            loop {
                sleep(Duration::from_millis(100));

                let result_guard = incoming_streams.write();

                if let Ok(mut guard) = result_guard {
                    for (id, stream) in guard.iter_mut() {
                        leer_mensaje(id, stream, tx_connect.clone());
                    }
                } else {
                    self.logger.error(
                        "No se pudo tomar el lock de conexiones entrantes. Marcando nodo como FAIL",
                        "READ-THREAD",
                    );
                    self.marcar_node_fail("READ-THREAD");
                    break; // Termina el hilo
                }
            }
        });
    }
}

/// Intenta leer un mensaje desde el stream TCP proporcionado y lo reenvía al hilo de procesamiento.
///
/// Esta función representa la lógica de lectura de un único stream de un nodo del clúster. Utiliza la función
/// `recv_cluster_message()` para decodificar un mensaje del stream.
///
/// Si la lectura es exitosa, se envía un `TipoMensajeNode::ClusterNode(msg)` al canal de procesamiento.
/// Si no hay datos disponibles en el stream (e.g., no bloqueante), no hace nada.
/// Si ocurre otro tipo de error (desconexión, fallo de red), se imprime el error en consola. A cambiar
///
/// # Parámetros
/// - `id`: Identificador del nodo remoto del cual proviene este stream.
/// - `stream_lectura`: Stream TCP mutablemente referenciado desde el cual se intentará leer un mensaje.
/// - `tx_connect`: Canal de comunicación para reenviar el mensaje leído al hilo de ejecución principal.
///
/// # Nota
/// Actualmente solo imprime errores graves, pero se podría extender para:
/// - Cerrar el stream si el error es permanente.
/// - Remover el nodo del mapa de streams.
/// - Reintentar conexión.
///
/// Se puede hacer metodo del nodo para acceder a informacion y no pasarla por parametro
fn leer_mensaje(_id: &NodeId, stream_lectura: &mut TcpStream, tx_connect: Sender<TipoMensajeNode>) {
    match recv_cluster_message(stream_lectura) {
        Ok(msg) => {
            let _ = tx_connect.send(TipoMensajeNode::ClusterNode(msg));
        }
        Err(_e) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_protocol::internal_protocol_msg::{
        ClusterMessage, ClusterMessagePayload, send_cluster_message,
    };
    use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
    use crate::node_builder::NodeBuilder;
    use std::collections::HashMap;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_iniciar_hilo_lector_node_envia_mensaje() {
        let node_id = NodeId::new();

        let node = Arc::new(
            NodeBuilder::new()
                .id(node_id.clone())
                .cli_addr("127.0.0.1:7000".parse().unwrap())
                .node_addr("127.0.0.1:17000".parse().unwrap())
                .cluster_addr("127.0.0.1:17000".parse().unwrap())
                .public_addr("127.0.0.1:7000".parse().unwrap())
                .build()
                .unwrap(),
        );

        let incoming_streams = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = channel::<TipoMensajeNode>();

        // Listener TCP simulado para aceptar conexiones entrantes
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Lanzamos hilo lector, que leerá de incoming_streams y enviará a rx
        node.clone()
            .iniciar_hilo_lector_node(incoming_streams.clone(), tx.clone());

        // Conectar cliente a listener
        let handle_accept = thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();

            // Enviar un mensaje serializado válido con la función que use el protocolo
            // Aquí asumimos que existe una función send_cluster_message o similar para enviar
            let header = node
                .message_header_node(InternalProtocolType::Ping)
                .unwrap();
            let msg = ClusterMessage::new(header, ClusterMessagePayload::Gossip(Vec::new()));

            send_cluster_message(&mut stream, &msg).unwrap();

            // Mantener el stream abierto un poco para que el hilo lector pueda leer
            thread::sleep(Duration::from_millis(200));
        });

        // Cliente conecta y lo insertamos en incoming_streams
        let client_stream = TcpStream::connect(addr).unwrap();

        // Insertamos en incoming_streams para que el hilo lector lo lea
        incoming_streams
            .write()
            .unwrap()
            .insert(node_id.clone(), client_stream.try_clone().unwrap());

        // Esperamos que el hilo lector reciba el mensaje y lo envie por el canal
        let recibido = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("No se recibió mensaje a tiempo");

        // Validamos que el mensaje recibido sea el ping que enviamos
        if let TipoMensajeNode::ClusterNode(cluster_msg) = recibido {
            assert_eq!(cluster_msg.header().get_type(), InternalProtocolType::Ping);
        } else {
            panic!("Tipo de mensaje inesperado recibido");
        }

        handle_accept.join().unwrap();
    }
}
