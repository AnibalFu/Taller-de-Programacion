use std::{
    num::ParseIntError,
    sync::{MutexGuard, PoisonError},
};

use common::common_error::CommonError;
use redis_client::{
    driver::{redis_driver::RedisDriver, redis_driver_error::RedisDriverError},
    tipos_datos::traits::DatoRedis,
};

use crate::{documents::documents_tracker::DocumentTracker, sheets::sheet_tracker::SheetTracker};

/// Module incharge of error handling for the documents handler service.

// Enum representing different kinds of errors that can occur in the documents handler service.
#[derive(Debug, PartialEq)]
pub enum DocumentErrorKind {
    /// Error caused by a connections issues, wrong ports, etc.
    ConnectionError,
    /// Invalid pubsub message format.
    InvalidMessage,
    /// Invalid channel for the pubsub message.
    InvalidChannel,
    /// Error raised by the Redis driver.
    RedisError,
    /// Not found,
    NotFound,
    /// Error when trying to create a document that already exists.
    AlreadyExists,
    /// Other kinds of errors can be added here as needed.
    Other,
    /// Error caused by invalid row or column index in a sheet.
    InvalidRowOrColumnIndex,
    /// Error caused by invalid args.
    InvalidAmountOfArguments,
    /// Invalid arguments provided to a function or method.
    InvalidArgs,
}

/// Struct representing an error in the documents handler service.
#[derive(Debug, PartialEq)]
pub struct DocumentError {
    /// The error message
    pub message: String,
    /// The error kind
    pub kind: DocumentErrorKind,
}

impl DocumentError {
    /// Creates a new `DocumentError` with the given message and kind.
    pub fn new(message: String, kind: DocumentErrorKind) -> Self {
        DocumentError { message, kind }
    }
    /// Creates a new `DocumentError` with a other error kind.
    pub fn other(message: String) -> Self {
        DocumentError::new(message, DocumentErrorKind::Other)
    }
}

impl From<RedisDriverError> for DocumentError {
    fn from(err: RedisDriverError) -> Self {
        DocumentError::new(err.message, DocumentErrorKind::RedisError)
    }
}

impl From<DatoRedis> for DocumentError {
    fn from(err: DatoRedis) -> Self {
        match err {
            DatoRedis::SimpleError(err) => DocumentError::new(
                format!("{}, {}", err.mensaje(), err.tipo()),
                DocumentErrorKind::RedisError,
            ),
            _ => DocumentError::new("Unknown error".to_string(), DocumentErrorKind::RedisError),
        }
    }
}

impl std::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DocumentError: {} - {:?}", self.message, self.kind)
    }
}

impl From<CommonError> for DocumentError {
    fn from(err: CommonError) -> Self {
        DocumentError::new(err.message, DocumentErrorKind::Other)
    }
}

impl From<PoisonError<MutexGuard<'_, DocumentTracker>>> for DocumentError {
    fn from(err: PoisonError<MutexGuard<'_, DocumentTracker>>) -> Self {
        DocumentError::new(format!("Poison error: {err}"), DocumentErrorKind::Other)
    }
}

impl From<PoisonError<MutexGuard<'_, RedisDriver>>> for DocumentError {
    fn from(err: PoisonError<MutexGuard<'_, RedisDriver>>) -> Self {
        DocumentError::new(format!("Poison error: {err}"), DocumentErrorKind::Other)
    }
}
impl From<PoisonError<MutexGuard<'_, SheetTracker>>> for DocumentError {
    fn from(err: PoisonError<MutexGuard<'_, SheetTracker>>) -> Self {
        DocumentError::new(format!("Poison error: {err}"), DocumentErrorKind::Other)
    }
}

impl From<ParseIntError> for DocumentError {
    fn from(err: ParseIntError) -> Self {
        DocumentError::new(format!("Parse error: {err}"), DocumentErrorKind::Other)
    }
}
