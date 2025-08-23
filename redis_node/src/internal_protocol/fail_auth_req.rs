use super::utils::read_exact;
use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::io::Read;

/// Representa un pedido de votación de una réplica a un master.
///
/// # Campos
/// - `replication_offset`: Replication offset de la réplica candidata
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FailOverAuthRequest {
    replication_offset: u32,
}

impl FailOverAuthRequest {
    /// Crea un nuevo failover_auth_req a enviar a un master.
    ///
    /// # Argumentos
    /// - `replication_offset`: Replication offset de la réplica candidata
    ///
    /// # Retorna
    /// Una nueva instancia de `FailOverAuthRequest`.
    pub fn new(replication_offset: u32) -> Self {
        Self { replication_offset }
    }

    pub fn get_offset(&self) -> u32 {
        self.replication_offset
    }
}

impl SerializeRIP for FailOverAuthRequest {
    /// Serializa el paquete en formato binario según el protocolo interno
    /// de Redis Cluster. La serialización incluye:
    ///
    /// 1. Replication offset de la réplica
    ///
    /// # Retorna
    /// Un `Vec<u8>` con los bytes serializados.
    fn serialize(&self) -> Vec<u8> {
        self.replication_offset.to_be_bytes().to_vec()
    }
}

impl DeserializeRIP for FailOverAuthRequest {
    /// Deserializa el failover_auth_req desde un stream de bytes
    ///
    /// # Formato esperado
    /// 1. Replication offset
    ///
    /// # Argumentos
    /// - `stream`: Stream binario desde el que se lee.
    ///
    /// # Retorna
    /// Un `Result` con la entrada `FailOverAuthRequest` o un error de I/O.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let replication_offset = u32::from_be_bytes(read_exact::<4, _>(stream)?);
        Ok(Self::new(replication_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_01_fail_auth_req_struct() {
        let entry = FailOverAuthRequest::new(11000_u32);
        assert_eq!(entry.replication_offset, 11000_u32);
    }

    #[test]
    fn test_02_fail_auth_req_serialization_deserialization() {
        let entry = FailOverAuthRequest::new(11000_u32);

        let bytes = entry.serialize();
        let mut cursor = Cursor::new(bytes);
        let deserialized = FailOverAuthRequest::deserialize(&mut cursor).unwrap();

        assert_eq!(entry.replication_offset, deserialized.replication_offset);
    }

    #[test]
    fn test_03_fail_auth_req_serialization_deserialization_mult() {
        let entry = FailOverAuthRequest::new(11000_u32);
        let entry2 = FailOverAuthRequest::new(12000_u32);

        let mut bytes = entry.serialize();
        bytes.extend(entry2.serialize());
        let mut cursor = Cursor::new(bytes);
        let deserialized = FailOverAuthRequest::deserialize(&mut cursor).unwrap();
        let deserialized2 = FailOverAuthRequest::deserialize(&mut cursor).unwrap();

        assert_eq!(entry.replication_offset, deserialized.replication_offset);
        assert_eq!(entry2.replication_offset, deserialized2.replication_offset);
    }
}
