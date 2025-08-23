use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::io::Read;

/// Representa los distintos tipos de mensajes internos utilizados en el protocolo del clúster.
/// Cada variante tiene una representación numérica (`u8`) asociada explícitamente mediante `#[repr(u8)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InternalProtocolType {
    /// Mensaje de tipo PING, utilizado para mantener conectados los nodos y compartir información de gossip.
    Ping, // 0

    /// Mensaje de tipo PONG, respuesta a un PING, también puede incluir gossip.
    Pong, // 1

    /// Tipo reservado para futuras actualizaciones de slots (actualmente no utilizado).
    Update, // 2

    /// Indica que un nodo ha sido marcado como fallado.
    Fail, // 3

    /// Solicitud de autorización para iniciar un failover.
    FailoverAuthRequest, // 4

    /// Confirmación de autorización para failover.
    FailoverAuthACK, // 5

    /// Comando de Redis reenviado entre nodos (por ejemplo, escritura replicada).
    RedisCMD, // 6

    /// Mensaje de publicación Pub/Sub.
    Publish, // 7

    /// Mensaje entre réplicas con fin de establecer el REPLICA_RANK.
    FailoverNegotiation, // 8

    /// Mensaje para inicializar la conexión entre 2 nodos.
    Meet, // 9

    /// En caso de replica, la replica le manda un meet al master.
    MeetMaster, // 10

    /// Luego de una promoción, se presenta a sus nuevas réplicas
    MeetNewMaster, // 11

    /// Tipo de error o desconocido. Usado como fallback ante un byte inválido.
    Error, // 12
}

impl InternalProtocolType {
    /// Convierte el tipo de protocolo en su representación en byte (`u8`).
    fn to_byte(self) -> u8 {
        self as u8
    }

    /// Devuelve un `InternalProtocolType` correspondiente a un byte dado.
    /// Si el byte no es válido, retorna `InternalProtocolType::Error`.
    fn from_byte(byte: u8) -> Self {
        match byte {
            0 => InternalProtocolType::Ping,
            1 => InternalProtocolType::Pong,
            2 => InternalProtocolType::Update,
            3 => InternalProtocolType::Fail,
            4 => InternalProtocolType::FailoverAuthRequest,
            5 => InternalProtocolType::FailoverAuthACK,
            6 => InternalProtocolType::RedisCMD,
            7 => InternalProtocolType::Publish,
            8 => InternalProtocolType::FailoverNegotiation,
            9 => InternalProtocolType::Meet,
            10 => InternalProtocolType::MeetMaster,
            11 => InternalProtocolType::MeetNewMaster,
            _ => InternalProtocolType::Error,
        }
    }
}

impl SerializeRIP for InternalProtocolType {
    /// Serializa el tipo de protocolo como un único byte.
    fn serialize(&self) -> Vec<u8> {
        vec![self.to_byte()]
    }
}

impl DeserializeRIP for InternalProtocolType {
    /// Deserializa un único byte desde el stream para determinar el tipo de mensaje interno.
    /// Si el byte no representa un valor válido, retorna `InternalProtocolType::Error`.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        Ok(InternalProtocolType::from_byte(byte[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_internal_protocol_type_conversion() {
        let all = [
            InternalProtocolType::Ping,
            InternalProtocolType::Pong,
            InternalProtocolType::Update,
            InternalProtocolType::Fail,
            InternalProtocolType::FailoverAuthRequest,
            InternalProtocolType::FailoverAuthACK,
            InternalProtocolType::RedisCMD,
            InternalProtocolType::Publish,
            InternalProtocolType::FailoverNegotiation,
            InternalProtocolType::Meet,
            InternalProtocolType::MeetMaster,
            InternalProtocolType::Error,
        ];

        for protocol in all.iter() {
            let bytes = protocol.serialize();
            let mut cursor = Cursor::new(bytes);
            let recovered = InternalProtocolType::deserialize(&mut cursor).unwrap();
            assert_eq!(*protocol, recovered);
        }

        // Test unknown value returns Error
        let mut invalid_cursor = Cursor::new(vec![99u8]);
        let invalid = InternalProtocolType::deserialize(&mut invalid_cursor).unwrap();
        assert_eq!(invalid, InternalProtocolType::Error);
    }
}
