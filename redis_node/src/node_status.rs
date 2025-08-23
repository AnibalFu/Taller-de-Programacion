//! Este modulo implementa la estructura NodeStatus
use redis_client::tipos_datos::traits::DatoRedis;

#[derive(Debug, PartialEq, Clone)]
pub enum NodeStatus {
    Ok,
    Fail,
}

/// Esta estructura permite la serializacion y deserializacion del
/// NodeStatus (Online/Offline)
impl NodeStatus {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            NodeStatus::Ok => b"okok".to_vec(),
            NodeStatus::Fail => b"fail".to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatoRedis> {
        match bytes {
            b"okok" => Ok(NodeStatus::Ok),
            b"fail" => Ok(NodeStatus::Fail),
            _ => Err(DatoRedis::new_null()),
        }
    }
}
