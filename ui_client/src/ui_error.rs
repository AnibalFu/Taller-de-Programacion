use std::{
    env::VarError,
    num::ParseIntError,
    sync::mpsc::{RecvError, SendError},
};

use events::events::event::Event;
use redis_client::driver::redis_driver_error::RedisDriverError;

pub type UIResult<T> = Result<T, UIError>;

/// Enum representing different kinds of UI errors.
pub enum UIErrorKind {
    /// Represents an error related to the UI.
    Other,
    /// Connection error, e.g., network issues.
    ConnectionError,
    /// Error related to the driver
    DriverError,
}

/// Struct representing a UI error with a message and kind.
pub struct UIError {
    /// The error message describing the UI error.
    pub message: String,
    /// The kind of UI error.
    pub kind: UIErrorKind,
}

impl UIError {
    /// Creates a new `UIError` with the given message and kind.
    ///
    /// # Arguments
    /// * `message` - A string slice that holds the error message.
    /// * `kind` - The kind of UI error.
    ///
    /// # Returns
    /// A new instance of `UIError`.
    pub fn new(message: String, kind: UIErrorKind) -> Self {
        UIError { message, kind }
    }

    /// Creates a new `UIError` with a default kind.
    pub fn other(message: String) -> Self {
        UIError {
            message,
            kind: UIErrorKind::Other, // Assuming Other is a variant of UIErrorKind
        }
    }
}

impl From<VarError> for UIError {
    fn from(err: VarError) -> Self {
        UIError::new(
            format!("Environment variable error: {err}"),
            UIErrorKind::Other,
        )
    }
}

impl From<ParseIntError> for UIError {
    fn from(err: ParseIntError) -> Self {
        UIError::new(format!("Parse integer error: {err}"), UIErrorKind::Other)
    }
}

impl From<RecvError> for UIError {
    fn from(err: RecvError) -> Self {
        UIError::new(format!("Receive error: {err}"), UIErrorKind::Other)
    }
}

impl From<RedisDriverError> for UIError {
    fn from(err: RedisDriverError) -> Self {
        UIError::new(
            format!("Redis driver error: {err}"),
            UIErrorKind::DriverError,
        )
    }
}

impl From<SendError<String>> for UIError {
    fn from(err: SendError<String>) -> Self {
        UIError::new(format!("Send error: {err}"), UIErrorKind::Other)
    }
}

impl From<SendError<Vec<String>>> for UIError {
    fn from(err: SendError<Vec<String>>) -> Self {
        UIError::new(format!("Send error: {err}"), UIErrorKind::Other)
    }
}

impl From<SendError<Event>> for UIError {
    fn from(err: SendError<Event>) -> Self {
        UIError::new(format!("Send error: {err}"), UIErrorKind::Other)
    }
}
