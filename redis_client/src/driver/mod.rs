use redis_driver_error::RedisDriverError;

pub mod redis_driver;
pub mod redis_driver_error;
pub mod traits;
pub type RedisDriverResult<T> = Result<T, RedisDriverError>;
