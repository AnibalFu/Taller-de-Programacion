use crate::internal_protocol::internal_protocol_type::InternalProtocolType;
use crate::internal_protocol::node_flags::{ClusterState, NodeFlags};
use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use crate::internal_protocol::utils::read_exact;
use crate::node_id::NodeId;
use std::io::Read;
use std::net::SocketAddr;
use std::ops::Range;

/// Cabecera de un mensaje interno del protocolo Redis Cluster.
///
/// Contiene toda la información estructural necesaria para interpretar
/// un mensaje entre nodos del clúster, incluyendo identificadores, rangos
/// de slots, direcciones IP, epochs, y estado del nodo.
///
/// Esta cabecera precede a cualquier payload (como `Gossip`, `Publish`, etc.)
/// y permite a los nodos comprender el origen, intención y estado del emisor.
///
/// # Campos
/// - `header_type`: Tipo del mensaje (ej: `Ping`, `Pong`, `Fail`, etc.).
/// - `node_id`: Identificador único del nodo que envía el mensaje.
/// - `current_epoch`: Epoch global utilizado para coordinar decisiones.
/// - `config_epoch`: Epoch de configuración del nodo (propio).
/// - `flags`: Estado actual del nodo (master, replica, fail, etc.).
/// - `hash_slots_bitmap`: Rango de slots que maneja este nodo (start..end).
/// - `tcp_client_port`: Dirección para clientes (IP:puerto).
/// - `cluster_node_port`: Dirección para comunicación entre nodos.
/// - `cluster_state`: Estado general del nodo (`Ok`, `Fail`).
/// - `master_id`: ID del nodo maestro si este nodo es una réplica.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MessageHeader {
    header_type: InternalProtocolType, // Tipo de header según el mensaje (1 byte)
    node_id: NodeId,                   // Id nodo actual (20 bytes)
    current_epoch: u64,                // Epoch global usado para coordinar decisiones en el clúster
    config_epoch: u64, // Epoch del nodo actual, usado para evitar conflictos de configuración
    flags: NodeFlags,  // Estado del nodo (master, replica, fail, etc.)
    hash_slots_bitmap: Range<u16>, // Rango de slots que maneja este nodo
    tcp_client_port: SocketAddr, // Dirección para clientes (con IP y puerto)
    cluster_node_port: SocketAddr, // Dirección para comunicación de clúster (con IP y puerto)
    cluster_state: ClusterState, // Estado del nodo (ok, fail, etc.)
    master_id: Option<NodeId>, // Solo si este nodo es una réplica
}

impl MessageHeader {
    /// Crea una nueva cabecera de mensaje de clúster.
    ///
    /// # Argumentos
    /// - `header_type`: Tipo del mensaje.
    /// - `node_id`: ID del nodo emisor.
    /// - `current_epoch`: Epoch global actual.
    /// - `config_epoch`: Epoch de configuración del nodo emisor.
    /// - `flags`: Estado del nodo.
    /// - `hash_slots_bitmap`: Rango de slots del nodo.
    /// - `tcp_client_port`: Dirección del puerto cliente.
    /// - `cluster_node_port`: Dirección del puerto del clúster.
    /// - `cluster_state`: Estado del nodo (`Ok`, `Fail`).
    /// - `master_id`: Nodo maestro (si el emisor es réplica).
    ///
    /// # Retorna
    /// Nueva instancia de `MessageHeader`.
    pub fn new(header_parameters: HeaderParameters) -> Self {
        Self {
            header_type: header_parameters.header_type,
            node_id: header_parameters.node_id,
            current_epoch: header_parameters.current_epoch,
            config_epoch: header_parameters.config_epoch,
            flags: header_parameters.flags,
            hash_slots_bitmap: header_parameters.hash_slots_bitmap,
            tcp_client_port: header_parameters.tcp_client_port,
            cluster_node_port: header_parameters.cluster_node_port,
            cluster_state: header_parameters.cluster_state,
            master_id: header_parameters.master_id,
        }
    }

    /// Devuelve el tipo de mensaje (`PING`, `PONG`, etc.) de esta cabecera.
    pub fn get_type(&self) -> InternalProtocolType {
        self.header_type
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    pub fn client_node_addr(&self) -> SocketAddr {
        self.tcp_client_port
    }
    pub fn cluster_node_addr(&self) -> SocketAddr {
        self.cluster_node_port
    }

    pub fn slot_range(&self) -> Range<u16> {
        self.hash_slots_bitmap.clone()
    }

    pub fn status(&self) -> ClusterState {
        self.cluster_state.clone()
    }

    pub fn flags(&self) -> NodeFlags {
        self.flags.clone()
    }

    pub fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    pub fn master_id(&self) -> Option<NodeId> {
        self.master_id.clone()
    }
}

pub struct HeaderParameters {
    pub header_type: InternalProtocolType,
    pub node_id: NodeId,
    pub current_epoch: u64,
    pub config_epoch: u64,
    pub flags: NodeFlags,
    pub hash_slots_bitmap: Range<u16>,
    pub tcp_client_port: SocketAddr,
    pub cluster_node_port: SocketAddr,
    pub cluster_state: ClusterState,
    pub master_id: Option<NodeId>,
}

impl SerializeRIP for MessageHeader {
    /// Serializa la cabecera de mensaje en formato binario.
    ///
    /// # Formato de serialización
    /// 1. `header_type`: 1 byte
    /// 2. `node_id`: 20 bytes
    /// 3. `current_epoch`: 8 bytes (big-endian)
    /// 4. `config_epoch`: 8 bytes (big-endian)
    /// 5. `flags`: 1 byte
    /// 6. `hash_slots_bitmap`: 4 bytes (2 para inicio, 2 para fin)
    /// 7. `tcp_client_port`: serializado (IP + puerto)
    /// 8. `cluster_node_port`: serializado
    /// 9. `cluster_state`: 1 byte (0 = Ok, 1 = Fail)
    /// 10. `master_id`:
    ///     - 1 byte: 0 (sin master), 1 (con master)
    ///     - 40 bytes si existe master_id
    ///
    /// # Retorna
    /// Un `Vec<u8>` que representa los bytes serializados.
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.header_type.serialize());
        bytes.extend(self.node_id.serialize());
        bytes.extend(&self.current_epoch.to_be_bytes());
        bytes.extend(&self.config_epoch.to_be_bytes());
        bytes.extend(self.flags.serialize());

        bytes.extend(&(self.hash_slots_bitmap.start).to_be_bytes());
        bytes.extend(&(self.hash_slots_bitmap.end).to_be_bytes());

        bytes.extend(&self.tcp_client_port.serialize());
        bytes.extend(&self.cluster_node_port.serialize());

        bytes.push(match self.cluster_state {
            ClusterState::Ok => 0,
            ClusterState::Fail => 1,
        });

        if let Some(master_id) = &self.master_id {
            bytes.push(1);
            bytes.extend(master_id.to_bytes());
        } else {
            bytes.push(0);
        }

        bytes
    }
}

impl DeserializeRIP for MessageHeader {
    /// Deserializa una cabecera desde un stream de bytes.
    ///
    /// # Formato esperado
    /// Debe respetar exactamente el formato de serialización definido en
    /// `serialize()`.
    ///
    /// # Argumentos
    /// - `stream`: Stream binario desde el que se lee la cabecera.
    ///
    /// # Retorna
    /// Una instancia de `MessageHeader` si la deserialización es exitosa,
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let header_type = InternalProtocolType::deserialize(stream)?;
        let node_id = NodeId::deserialize(stream)?;

        let current_epoch = u64::from_be_bytes(read_exact::<8, T>(stream)?);
        let config_epoch = u64::from_be_bytes(read_exact::<8, T>(stream)?);

        let flags = NodeFlags::deserialize(stream)?;

        let start = u16::from_be_bytes(read_exact::<2, T>(stream)?);
        let end = u16::from_be_bytes(read_exact::<2, T>(stream)?);
        let hash_slots_bitmap = start..end;

        let tcp_client_port = SocketAddr::deserialize(stream)?;
        let cluster_node_port = SocketAddr::deserialize(stream)?;

        let cluster_state = match read_exact::<1, T>(stream)?[0] {
            0 => ClusterState::Ok,
            1 => ClusterState::Fail,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Estado inválido",
                ));
            }
        };

        let has_master = read_exact::<1, T>(stream)?[0];
        let master_id = if has_master == 1 {
            Some(NodeId::from_bytes(&read_exact::<40, T>(stream)?))
        } else {
            None
        };

        Ok(Self {
            header_type,
            node_id,
            current_epoch,
            config_epoch,
            flags,
            hash_slots_bitmap,
            tcp_client_port,
            cluster_node_port,
            cluster_state,
            master_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn sample_node_id() -> NodeId {
        NodeId::new()
    }

    #[test]
    fn test_message_header_serialize_deserialize() {
        let tipo = InternalProtocolType::Ping;
        let node_id = sample_node_id();
        let current_epoch = 42;
        let config_epoch = 100;
        let flags = NodeFlags::new(true, false, true, false);
        let hash_slots_bitmap = 1000..2000;
        let tcp_client_port: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let cluster_node_port: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 16379);
        let cluster_state = ClusterState::Ok;
        let master_id = Some(NodeId::new());

        let header_parameters = HeaderParameters {
            header_type: tipo,
            node_id: node_id.clone(),
            current_epoch,
            config_epoch,
            flags: flags.clone(),
            hash_slots_bitmap: hash_slots_bitmap.clone(),
            tcp_client_port,
            cluster_node_port,
            cluster_state,
            master_id: master_id.clone(),
        };

        let header = MessageHeader::new(header_parameters);

        // Usar trait serialize
        let bytes = header.serialize();

        // Usar trait deserialize
        let deserialized = MessageHeader::deserialize(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(deserialized.header_type, tipo);
        assert_eq!(deserialized.node_id, node_id);
        assert_eq!(deserialized.current_epoch, current_epoch);
        assert_eq!(deserialized.config_epoch, config_epoch);
        assert_eq!(deserialized.flags, flags); // chequeamos flags
        assert_eq!(deserialized.hash_slots_bitmap, hash_slots_bitmap);
        assert_eq!(deserialized.tcp_client_port, tcp_client_port);
        assert_eq!(deserialized.cluster_node_port, cluster_node_port);
        assert!(matches!(deserialized.cluster_state, ClusterState::Ok));
        assert_eq!(deserialized.master_id, master_id);
    }
}
