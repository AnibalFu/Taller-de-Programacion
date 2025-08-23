//! Este modulo contiene la implementacion de errores del driver
//! de Redis implementado
use std::num::ParseIntError;

use crate::{tipos_datos::traits::DatoRedis, tipos_errores::errores};

#[derive(Debug, PartialEq)]
pub enum RedisDriverErrorKind {
    /// Error caused by a connections issues, wrong ports, etc.
    ConnectionError,
    /// Generical errors
    OtherError,
    /// Errors caused by the driver
    DriverError,
    /// Empty stream
    EmptyStreamError,
    /// ProtocolError,
    ProtocolError,
    /// Error caused by parsing an address
    InvalidAddress,
}

/// Error de driver de redis
#[derive(Debug, PartialEq)]
pub struct RedisDriverError {
    /// The error message
    pub message: String,
    /// The error kind
    pub kind: RedisDriverErrorKind,
}

impl RedisDriverError {
    /// Creates a new RedisDriverError
    pub fn new(message: String, kind: RedisDriverErrorKind) -> Self {
        RedisDriverError { message, kind }
    }

    pub fn other(message: String) -> Self {
        RedisDriverError::new(message, RedisDriverErrorKind::OtherError)
    }

    /// Returns the error message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the error kind
    pub fn kind(&self) -> &RedisDriverErrorKind {
        &self.kind
    }
}

impl std::fmt::Display for RedisDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedisDriverError: {} - {:?}", self.message, self.kind)
    }
}

impl From<std::io::Error> for RedisDriverError {
    fn from(err: std::io::Error) -> Self {
        RedisDriverError::new(err.to_string(), RedisDriverErrorKind::ConnectionError)
    }
}

impl From<errores::Error> for RedisDriverError {
    fn from(err: errores::Error) -> Self {
        RedisDriverError::new(err.to_string(), RedisDriverErrorKind::DriverError)
    }
}

impl From<DatoRedis> for RedisDriverError {
    fn from(err: DatoRedis) -> Self {
        match err {
            DatoRedis::SimpleError(err) => RedisDriverError::new(
                format!("{}, {}", err.mensaje(), err.tipo()),
                RedisDriverErrorKind::DriverError,
            ),
            _ => RedisDriverError::new(
                "Unknown error".to_string(),
                RedisDriverErrorKind::DriverError,
            ),
        }
    }
}

impl From<ParseIntError> for RedisDriverError {
    fn from(err: ParseIntError) -> Self {
        RedisDriverError::new(err.to_string(), RedisDriverErrorKind::DriverError)
    }
}
