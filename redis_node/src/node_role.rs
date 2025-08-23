//! Este modulo implementa la estructura NodeRole

use redis_client::tipos_datos::traits::DatoRedis;
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum NodeRole {
    Master,
    Replica,
}

/// Esta estructura permite serializar y deserializar el NodeRole
impl NodeRole {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            NodeRole::Master => b"masterr".to_vec(),
            NodeRole::Replica => b"replica".to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatoRedis> {
        match bytes {
            b"masterr" => Ok(NodeRole::Master),
            b"replica" => Ok(NodeRole::Replica),
            _ => Err(DatoRedis::new_null()),
        }
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Master => write!(f, "master"),
            NodeRole::Replica => write!(f, "replica"),
        }
    }
}
