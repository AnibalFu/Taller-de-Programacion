use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::io::Read;

/// - is_master:
///
/// Indica que este nodo actúa como maestro, es decir, posee al menos un slot del hash ring
/// (de los 16384 disponibles). Solo los maestros pueden aceptar escritura.
///
/// - is_replica:
///
/// Es una réplica de otro nodo maestro. Las réplicas son nodos secundarios que replican el
/// estado de un maestro. Si el maestro falla, puede promoverse una réplica.
///
/// - fail:
///
/// Este nodo ha sido declarado definitivamente muerto por el clúster. Para llegar a este estado,
/// varios nodos deben haberlo marcado como pfail. Se considera una decisión de consenso.
///
/// - pfail:
///
/// El nodo parece estar fallando desde el punto de vista local (es decir, no responde a pings),
/// pero aún no ha sido confirmado por otros nodos. Es un estado de precaución que puede escalar a fail.
///
/// - handshake:
///
/// El nodo está en la fase de "descubrimiento", es decir, todavía no ha intercambiado información
/// completa con el resto del clúster. Suele usarse justo después de que un nodo nuevo se conecta.
///
/// - noaddr:
///
/// El nodo está registrado en el clúster, pero no se le pudo determinar o conectar a su dirección IP.
/// Esto puede pasar si fue registrado pero nunca respondió, o si está detrás de NAT y no configurado
/// correctamente.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeFlags {
    is_master: bool,  // Nodo es maestro (tiene slots asignados)
    is_replica: bool, // Nodo es réplica de otro
    fail: bool,       // Nodo declarado en FAIL por consenso (ya no responde)
    pfail: bool,      // Nodo en estado "PFAIL" (aparentemente fallando, detectado localmente)
                      // pub handshake: bool,     // Nodo en proceso de "handshake" (descubrimiento inicial)
                      // pub noaddr: bool,        // Nodo sin dirección IP conocida (no se puede conectar)
}

impl NodeFlags {
    pub fn new(is_master: bool, is_replica: bool, fail: bool, pfail: bool) -> Self {
        Self {
            is_master,
            is_replica,
            fail,
            pfail,
        }
    }

    pub fn to_byte(&self) -> Vec<u8> {
        let mut flag = 0u8;
        if self.is_master {
            flag |= 1 << 0;
        }
        if self.is_replica {
            flag |= 1 << 1;
        }
        if self.fail {
            flag |= 1 << 2;
        }
        if self.pfail {
            flag |= 1 << 3;
        }
        vec![flag]
    }

    pub fn from_byte(byte: u8) -> Self {
        NodeFlags {
            is_master: byte & (1 << 0) != 0,
            is_replica: byte & (1 << 1) != 0,
            fail: byte & (1 << 2) != 0,
            pfail: byte & (1 << 3) != 0,
        }
    }

    pub fn is_master(&self) -> bool {
        self.is_master
    }
    pub fn is_replica(&self) -> bool {
        self.is_replica
    }
    pub fn is_fail(&self) -> bool {
        self.fail
    }
    pub fn is_pfail(&self) -> bool {
        self.pfail
    }
    pub fn set_fail(&mut self, value: bool) {
        self.fail = value
    }
    pub fn set_pfail(&mut self, value: bool) {
        self.pfail = value
    }
}

impl DeserializeRIP for NodeFlags {
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        Ok(Self::from_byte(byte[0]))
    }
}

impl SerializeRIP for NodeFlags {
    fn serialize(&self) -> Vec<u8> {
        self.to_byte()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ClusterState {
    Ok,
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_node_flags_serialization_deserialization() {
        let flags = vec![
            (false, false, false, false),
            (true, false, false, false),
            (false, true, false, false),
            (false, false, true, false),
            (false, false, false, true),
            (true, true, false, false),
            (true, false, true, true),
            (true, true, true, true),
        ];

        for (is_master, is_replica, fail, pfail) in flags {
            let original = NodeFlags::new(is_master, is_replica, fail, pfail);
            let serialized = original.to_byte();
            let deserialized = NodeFlags::from_byte(serialized[0]);

            assert_eq!(original.is_master(), deserialized.is_master());
            assert_eq!(original.is_replica(), deserialized.is_replica());
            assert_eq!(original.is_fail(), deserialized.is_fail());
            assert_eq!(original.pfail, deserialized.pfail);
        }
    }

    #[test]
    fn test_node_flags_cursor() {
        let original = NodeFlags::new(true, true, false, true);
        let mut cursor = Cursor::new(original.to_byte());
        let deserialized = NodeFlags::deserialize(&mut cursor).unwrap();

        assert_eq!(original.is_master(), deserialized.is_master());
        assert_eq!(original.is_replica(), deserialized.is_replica());
        assert_eq!(original.is_fail(), deserialized.is_fail());
        assert_eq!(original.pfail, deserialized.pfail);
    }
}
