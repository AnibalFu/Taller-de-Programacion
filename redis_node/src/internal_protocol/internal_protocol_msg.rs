use super::fail_auth_req::FailOverAuthRequest;
use super::redis_cmd::RedisCMD;
use crate::internal_protocol::gossip::GossipEntry;
use crate::internal_protocol::header::MessageHeader;
use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use crate::internal_protocol::utils::read_exact;
use crate::node_id::NodeId;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Representa los distintos tipos de payload que puede llevar un mensaje de clúster.
#[derive(Debug, Clone)]
pub enum ClusterMessagePayload {
    /// Payload para mensajes PING y PONG: contiene una lista de entradas de gossip.
    Gossip(Vec<GossipEntry>),

    /// Payload para mensaje FAIL: contiene el ID del nodo que falló.
    Fail(NodeId),

    /// Payload para comandos Redis enviados internamente en el clúster.
    RedisCommand(RedisCMD),

    /// Payload para mensajes de publish (pub/sub).
    PubSub(RedisCMD),

    /// Payload para pedidos de votación de réplicas a masters.
    FailAuthReq(FailOverAuthRequest),

    /// Payload para votos positivos de masters a réplicas.
    FailAuthAck(NodeId),

    /// Payload para comunicacion de replication offset entre réplicas.
    FailNegotiation(FailOverAuthRequest),

    /// Payload vacío, usado en mensajes como MEET.
    Empty,

    Meet,

    MeetMaster,

    MeetNewMaster,

    Update,
}

/// Representa un mensaje completo enviado entre nodos del clúster.
#[derive(Debug, Clone)]
pub struct ClusterMessage {
    /// Encabezado con metainformación sobre el mensaje.
    header: MessageHeader,

    /// Cuerpo del mensaje, con información específica del tipo de mensaje.
    payload: ClusterMessagePayload,
}
impl ClusterMessage {
    /// Crea un nuevo mensaje de clúster con un header y un payload determinado.
    pub fn new(header: MessageHeader, payload: ClusterMessagePayload) -> Self {
        Self { header, payload }
    }

    pub fn header(&self) -> &MessageHeader {
        &self.header
    }

    pub fn payload(&self) -> ClusterMessagePayload {
        self.payload.clone()
    }
}

pub fn send_cluster_message(
    stream: &mut TcpStream,
    message: &ClusterMessage,
) -> std::io::Result<()> {
    let bytes = message.serialize();
    stream.write_all(&bytes)?;
    Ok(())
}

pub fn recv_cluster_message(stream: &mut TcpStream) -> std::io::Result<ClusterMessage> {
    ClusterMessage::deserialize(stream)
}

impl SerializeRIP for ClusterMessage {
    /// Serializa el mensaje de clúster en un vector de bytes siguiendo el protocolo interno.
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Serializa el encabezado del mensaje
        bytes.extend(self.header.serialize());

        // Serializa el payload según su tipo
        match &self.payload {
            ClusterMessagePayload::Gossip(gossip) => {
                bytes.extend(serialize_gossip(gossip));
            }
            ClusterMessagePayload::RedisCommand(redis_cmd)
            | ClusterMessagePayload::PubSub(redis_cmd) => {
                bytes.extend(RedisCMD::serialize(redis_cmd));
            }
            ClusterMessagePayload::Fail(node_id) | ClusterMessagePayload::FailAuthAck(node_id) => {
                bytes.extend(NodeId::serialize(node_id));
            }
            ClusterMessagePayload::FailAuthReq(failover_req)
            | ClusterMessagePayload::FailNegotiation(failover_req) => {
                bytes.extend(FailOverAuthRequest::serialize(failover_req));
            }

            ClusterMessagePayload::Meet => {}

            ClusterMessagePayload::MeetMaster => {}
            // Aquí se deben agregar serializaciones específicas para otros tipos de payload
            // como Fail, RedisCommand y PubSub
            _ => {} // Empty
        }

        bytes
    }
}

impl DeserializeRIP for ClusterMessage {
    /// Deserializa un mensaje de clúster desde un stream, devolviendo un `ClusterMessage`.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let header = MessageHeader::deserialize(stream)?;

        // Determina el tipo de payload según el tipo del mensaje
        let payload = match header.get_type() {
            InternalProtocolType::Ping | InternalProtocolType::Pong => {
                let gossip_section = deserialize_gossip_section(stream)?;
                ClusterMessagePayload::Gossip(gossip_section)
            }
            InternalProtocolType::Fail => {
                let node_id = NodeId::deserialize(stream)?;
                ClusterMessagePayload::Fail(node_id)
            }
            InternalProtocolType::RedisCMD => {
                let redis_cmd = RedisCMD::deserialize(stream)?;
                ClusterMessagePayload::RedisCommand(redis_cmd)
            }
            InternalProtocolType::Publish => {
                let redis_cmd = RedisCMD::deserialize(stream)?;
                ClusterMessagePayload::PubSub(redis_cmd)
            }
            InternalProtocolType::FailoverAuthRequest => {
                let fail_auth_req = FailOverAuthRequest::deserialize(stream)?;
                ClusterMessagePayload::FailAuthReq(fail_auth_req)
            }
            InternalProtocolType::FailoverAuthACK => {
                let rep_id = NodeId::deserialize(stream)?;
                ClusterMessagePayload::FailAuthAck(rep_id)
            }
            InternalProtocolType::FailoverNegotiation => {
                let rep_offset = FailOverAuthRequest::deserialize(stream)?;
                ClusterMessagePayload::FailNegotiation(rep_offset)
            }
            InternalProtocolType::Meet => ClusterMessagePayload::Meet,

            InternalProtocolType::MeetMaster => ClusterMessagePayload::MeetMaster,

            InternalProtocolType::MeetNewMaster => ClusterMessagePayload::MeetNewMaster,

            InternalProtocolType::Update => ClusterMessagePayload::Update,
            // Aquí se deben agregar deserializaciones específicas para otros tipos de payload
            // como Fail y PubSub
            _ => ClusterMessagePayload::Empty,
        };

        Ok(Self { header, payload })
    }
}

/// Serializa una sección de gossip (lista de entradas) a bytes.
/// Formato:
/// - u32 con el número de entradas
/// - cada entrada serializada secuencialmente
fn serialize_gossip(gossip: &Vec<GossipEntry>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend((gossip.len() as u32).to_be_bytes());
    for entry in gossip {
        bytes.extend(entry.serialize());
    }
    bytes
}

/// Deserializa una sección de gossip desde un stream.
/// Lee primero un u32 indicando cuántas entradas hay, luego las entradas.
fn deserialize_gossip_section<T: Read>(stream: &mut T) -> std::io::Result<Vec<GossipEntry>> {
    let gossip_len = u32::from_be_bytes(read_exact::<4, _>(stream)?);
    let mut gossip_section = Vec::with_capacity(gossip_len as usize);
    for _ in 0..gossip_len {
        gossip_section.push(GossipEntry::deserialize(stream)?);
    }
    Ok(gossip_section)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_protocol::header::HeaderParameters;
    use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
    use crate::internal_protocol::node_flags::{ClusterState, NodeFlags};
    use crate::node_id::NodeId;
    use std::io::Cursor;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
    use std::thread::spawn;

    fn setup_header(node_id: NodeId, tipo: InternalProtocolType) -> MessageHeader {
        let header_parameters = HeaderParameters {
            header_type: tipo,
            node_id,
            current_epoch: 1,
            config_epoch: 1,
            flags: NodeFlags::new(true, false, false, false),
            hash_slots_bitmap: 0..5000,
            tcp_client_port: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            cluster_node_port: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16379),
            cluster_state: ClusterState::Ok,
            master_id: None,
        };
        MessageHeader::new(header_parameters)
    }

    fn setup_gossip_entry(ip: [u8; 4], port: u16) -> GossipEntry {
        GossipEntry::new(
            NodeId::new(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), port),
            NodeFlags::new(false, true, false, false),
        )
    }

    fn setup_redis_cmd(cmd: Vec<String>) -> RedisCMD {
        RedisCMD::new(cmd)
    }

    fn exchange_and_assert(stream: &mut TcpStream, expected_msg: &ClusterMessage) {
        send_cluster_message(stream, expected_msg).unwrap();
        let received_msg = recv_cluster_message(stream).unwrap();

        match &received_msg.payload {
            ClusterMessagePayload::Gossip(gossip_list) => {
                assert_eq!(gossip_list.len(), 2);

                if let ClusterMessagePayload::Gossip(expected_gossip) = &expected_msg.payload {
                    assert_eq!(expected_gossip, gossip_list);
                } else {
                    panic!("El mensaje esperado no es de tipo Gossip");
                }
            }
            _ => panic!("Se esperaba payload de tipo Gossip"),
        }
    }

    #[test]
    fn test01_heartbeat_serialization_with_one_gossip() {
        let header = setup_header(NodeId::new(), InternalProtocolType::Ping);
        let gossip_entry = setup_gossip_entry([127, 0, 0, 2], 16380);

        let message =
            ClusterMessage::new(header, ClusterMessagePayload::Gossip(vec![gossip_entry]));

        let serialized = message.serialize();
        let mut cursor = Cursor::new(&serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test02_heartbeat_serialization_with_two_gossip() {
        let header = setup_header(NodeId::new(), InternalProtocolType::Pong);
        let gossip_entry1 = setup_gossip_entry([127, 0, 0, 2], 16380);
        let gossip_entry2 = setup_gossip_entry([127, 0, 0, 3], 16381);

        let message = ClusterMessage::new(
            header,
            ClusterMessagePayload::Gossip(vec![gossip_entry1, gossip_entry2]),
        );

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);

        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test03_real_cluster_message_exchange() {
        // Paso 1: Iniciar el servidor en un hilo
        spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:18088").unwrap();
            if let Ok((mut stream, _)) = listener.accept() {
                // Recibe mensaje del cliente
                let received_msg = recv_cluster_message(&mut stream).unwrap();

                // (Opcional) Validar el mensaje
                assert!(matches!(
                    received_msg.payload,
                    ClusterMessagePayload::Gossip(_)
                ));

                // Enviar de nuevo el mismo mensaje como respuesta
                send_cluster_message(&mut stream, &received_msg).unwrap();
            }
        });

        // Pequeña espera para asegurar que el servidor arranque
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Paso 2: Crear mensaje
        let header = setup_header(NodeId::new(), InternalProtocolType::Pong);
        let gossip_entry1 = setup_gossip_entry([127, 0, 0, 2], 16380);
        let gossip_entry2 = setup_gossip_entry([127, 0, 0, 3], 16381);

        let message = ClusterMessage::new(
            header,
            ClusterMessagePayload::Gossip(vec![gossip_entry1, gossip_entry2]),
        );

        // Paso 3: Cliente conecta y envía mensaje
        let mut stream = TcpStream::connect("127.0.0.1:18088").unwrap();
        exchange_and_assert(&mut stream, &message);
    }

    #[test]
    fn test04_real_cluster_message_exchange_multiple() {
        // Paso 1: Iniciar el servidor en un hilo
        spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:18089").unwrap();
            if let Ok((mut stream, _)) = listener.accept() {
                for _ in 0..2 {
                    let received_msg = recv_cluster_message(&mut stream).unwrap();
                    assert!(matches!(
                        received_msg.payload,
                        ClusterMessagePayload::Gossip(_)
                    ));
                    send_cluster_message(&mut stream, &received_msg).unwrap();
                }
            }
        });

        // Esperar a que el servidor inicie
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Crear mensaje
        let header = setup_header(NodeId::new(), InternalProtocolType::Pong);
        let gossip_entry1 = setup_gossip_entry([127, 0, 0, 2], 16380);
        let gossip_entry2 = setup_gossip_entry([127, 0, 0, 3], 16381);
        let message = ClusterMessage::new(
            header,
            ClusterMessagePayload::Gossip(vec![gossip_entry1, gossip_entry2]),
        );

        // Cliente se conecta
        let mut stream = TcpStream::connect("127.0.0.1:18089").unwrap();

        // Repetir dos veces el intercambio
        for _ in 0..2 {
            exchange_and_assert(&mut stream, &message);
        }
    }

    #[test]
    fn test05_redis_cmd_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::RedisCMD);
        let redis_cmd = setup_redis_cmd(vec![
            "set".to_string(),
            "hola".to_string(),
            "mundo".to_string(),
        ]);

        let message = ClusterMessage::new(header, ClusterMessagePayload::RedisCommand(redis_cmd));

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test06_pub_sub_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::Publish);
        let redis_cmd = setup_redis_cmd(vec![
            "publish".to_string(),
            "canal1".to_string(),
            "mensaje".to_string(),
        ]);

        let message = ClusterMessage::new(header, ClusterMessagePayload::PubSub(redis_cmd));

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test07_fail_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::Fail);
        let node_id = NodeId::new();

        let message = ClusterMessage::new(header, ClusterMessagePayload::Fail(node_id));

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test08_fail_auth_req_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::FailoverAuthRequest);
        let fail_auth_req = FailOverAuthRequest::new(111000_u32);

        let message =
            ClusterMessage::new(header, ClusterMessagePayload::FailAuthReq(fail_auth_req));

        let header2 = setup_header(NodeId::new(), InternalProtocolType::FailoverAuthRequest);
        let fail_auth_req2 = FailOverAuthRequest::new(112000_u32);

        let message2 =
            ClusterMessage::new(header2, ClusterMessagePayload::FailAuthReq(fail_auth_req2));

        let mut serialized = message.serialize();
        serialized.extend(message2.serialize());
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        let deserialized2 = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
        assert_eq!(
            format!("{message2:?}"),
            format!("{:?}", deserialized2.unwrap())
        );
    }

    #[test]
    fn test09_fail_auth_ack_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::FailoverAuthACK);
        let rep_id = NodeId::new();

        let message = ClusterMessage::new(header, ClusterMessagePayload::FailAuthAck(rep_id));

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test10_fail_negotiation_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::FailoverNegotiation);
        let fail_auth_req = FailOverAuthRequest::new(111000_u32);

        let message = ClusterMessage::new(
            header,
            ClusterMessagePayload::FailNegotiation(fail_auth_req),
        );

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test11_meet_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::Meet);

        let message = ClusterMessage::new(header, ClusterMessagePayload::Meet);

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }

    #[test]
    fn test12_meet_master_serialization_deserialization() {
        let header = setup_header(NodeId::new(), InternalProtocolType::MeetMaster);

        let message = ClusterMessage::new(header, ClusterMessagePayload::MeetMaster);

        let serialized = message.serialize();
        let mut cursor = Cursor::new(serialized);
        let deserialized = ClusterMessage::deserialize(&mut cursor);
        assert_eq!(
            format!("{message:?}"),
            format!("{:?}", deserialized.unwrap())
        );
    }
}
