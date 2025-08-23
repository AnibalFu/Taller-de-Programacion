use std::collections::HashSet;

use crate::tipos_datos::traits::DatoRedis;

use super::redis_driver_error::{RedisDriverError, RedisDriverErrorKind};

/// Convierte un tipo de dato redis en una estructura de rust
pub trait FromRedis: Sized {
    fn from_redis(value: DatoRedis) -> Result<Self, RedisDriverError>;
}

impl FromRedis for String {
    fn from_redis(value: DatoRedis) -> Result<Self, RedisDriverError> {
        match value {
            DatoRedis::BulkString(bulk_string) => Ok(bulk_string.contenido().to_string()),
            DatoRedis::SimpleString(simple_string) => Ok(simple_string.contenido().to_string()),
            _ => Err(RedisDriverError::new(
                "Cannot convert to String".to_string(),
                RedisDriverErrorKind::DriverError,
            )),
        }
    }
}

impl FromRedis for i64 {
    fn from_redis(value: DatoRedis) -> Result<Self, RedisDriverError> {
        match value {
            DatoRedis::Integer(integer) => Ok(integer.valor()),
            _ => Err(RedisDriverError::new(
                "Cannot convert to i64".to_string(),
                RedisDriverErrorKind::DriverError,
            )),
        }
    }
}

impl<T: FromRedis> FromRedis for Vec<T> {
    fn from_redis(value: DatoRedis) -> Result<Self, RedisDriverError> {
        match value {
            DatoRedis::Arrays(arrays) => {
                let mut result = Vec::new();
                for item in arrays.iter() {
                    result.push(T::from_redis(item.clone())?);
                }
                Ok(result)
            }
            _ => Err(RedisDriverError::new(
                "Cannot convert to Vec<T>".to_string(),
                RedisDriverErrorKind::DriverError,
            )),
        }
    }
}

impl<T: FromRedis + Eq + std::hash::Hash> FromRedis for HashSet<T> {
    fn from_redis(value: DatoRedis) -> Result<Self, RedisDriverError> {
        match value {
            DatoRedis::Set(set) => {
                let mut result = HashSet::new();
                for item in set.iter() {
                    result.insert(T::from_redis(item.clone())?);
                }
                Ok(result)
            }
            _ => Err(RedisDriverError::new(
                "Cannot convert to HashSet<T>".to_string(),
                RedisDriverErrorKind::DriverError,
            )),
        }
    }
}
