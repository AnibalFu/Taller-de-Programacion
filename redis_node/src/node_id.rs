//! Este modulo implementa la estructura NodeId

use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::fmt;
use std::io::Read;

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct NodeId {
    id: String,
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Esta estructura permite obtener, serialiar y deserializar
/// el NodeId para un nodo
impl NodeId {
    pub fn new() -> Self {
        let id = Self::generar_hex_160bits().to_string();
        NodeId { id }
    }

    pub fn new_with_id(id: String) -> Self {
        NodeId { id }
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn generar_hex_160bits() -> String {
        let mut bytes = [0u8; 20]; // 160 bits = 20 bytes
        rand::fill(&mut bytes); // llena con datos aleatorios
        hex::encode(bytes) // convierte a string hexadecimal
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.id.as_bytes().to_vec()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let id = String::from_utf8_lossy(bytes).to_string();
        NodeId { id }
    }
}

impl SerializeRIP for NodeId {
    fn serialize(&self) -> Vec<u8> {
        self.to_bytes()
    }
}

impl DeserializeRIP for NodeId {
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let mut buf = [0u8; 40];
        stream.read_exact(&mut buf)?;
        let id_str = String::from_utf8(buf.to_vec()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "ID no válido UTF-8")
        })?;
        Ok(NodeId::new_with_id(id_str))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}
