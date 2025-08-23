use crate::internal_protocol::node_flags::NodeFlags;
use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use crate::node_id::NodeId;
use std::io::Read;
use std::net::SocketAddr;

/// Representa una entrada de información de *gossip* utilizada en la comunicación
/// entre nodos del clúster Redis. Esta estructura es parte del payload de mensajes
/// tipo `PING` o `PONG` enviados entre nodos.
///
/// Cada entrada describe el estado de un nodo vecino observado por el nodo emisor.
///
/// # Campos
/// - `node_id`: Identificador único del nodo vecino (por ejemplo, SHA1 de su ID).
/// - `ip_port`: Dirección IP y puerto del nodo vecino utilizado para clientes.
/// - `flags`: Flags que describen el estado del nodo (ej. `master`, `replica`, `fail`, etc.).
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GossipEntry {
    node_id: NodeId,     // ID del nodo vecino
    ip_port: SocketAddr, // Dirección IP del nodo y puerto de del nodo vecino
    flags: NodeFlags,    // Estado del nodo vecino en cuestión
}

impl GossipEntry {
    /// Crea una nueva entrada de gossip.
    ///
    /// # Argumentos
    /// - `node_id`: ID del nodo vecino.
    /// - `ip_port`: Dirección IP y puerto del nodo.
    /// - `flags`: Estado actual del nodo.
    ///
    /// # Retorna
    /// Una nueva instancia de `GossipEntry`.
    pub fn new(node_id: NodeId, ip_port: SocketAddr, flags: NodeFlags) -> Self {
        Self {
            node_id,
            ip_port,
            flags,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn ip_port(&self) -> &SocketAddr {
        &self.ip_port
    }

    pub fn flags(&self) -> &NodeFlags {
        &self.flags
    }
}

impl SerializeRIP for GossipEntry {
    /// Serializa la entrada de gossip en formato binario según el protocolo interno
    /// de Redis Cluster. La serialización incluye:
    ///
    /// 1. El `node_id` (20 bytes).
    /// 2. La dirección IP y puerto en formato binario (`SocketAddr` serializado).
    /// 3. Los flags del nodo (1 byte).
    ///
    /// # Retorna
    /// Un `Vec<u8>` con los bytes serializados.
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.node_id.serialize());
        bytes.extend(&self.ip_port.serialize());
        bytes.extend(self.flags.serialize());
        bytes
    }
}

impl DeserializeRIP for GossipEntry {
    /// Deserializa una entrada de gossip desde un stream de bytes, leyendo
    /// los campos en el mismo orden en que fueron serializados.
    ///
    /// # Formato esperado
    /// 1. `node_id`: 20 bytes.
    /// 2. `ip_port`: Dirección IP y puerto serializados.
    /// 3. `flags`: 1 byte.
    ///
    /// # Argumentos
    /// - `stream`: Stream binario desde el que se lee.
    ///
    /// # Retorna
    /// Un `Result` con la entrada `GossipEntry` o un error de I/O.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let node_id = NodeId::deserialize(stream)?;
        let ip_port = SocketAddr::deserialize(stream)?;
        let flags = NodeFlags::deserialize(stream)?;

        Ok(Self {
            node_id,
            ip_port,
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_gossip_entry_serialization_deserialization() {
        let node_id = NodeId::new();
        let ip_port = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let flags = NodeFlags::new(true, false, true, false);
        let entry = GossipEntry::new(node_id.clone(), ip_port, flags);

        let bytes = entry.serialize();
        let mut cursor = Cursor::new(bytes);
        let deserialized = GossipEntry::deserialize(&mut cursor).unwrap();

        assert_eq!(entry.node_id().to_bytes(), deserialized.node_id.to_bytes());
        assert_eq!(entry.ip_port(), &deserialized.ip_port);
        assert_eq!(entry.flags().to_byte(), deserialized.flags.to_byte());
    }
}
