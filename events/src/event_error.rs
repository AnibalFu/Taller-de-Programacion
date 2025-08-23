use std::num::ParseIntError;

use json::json::ExpresionJson;

/// Error handling module for common errors in the application.

#[derive(Debug)]
/// Enum representing common error kinds.
pub enum EventErrorKind {
    /// Represents an error related to pubsub operations.
    EventPubSub,
    /// Represents an error related to any error that is not necccesary to have an special kind.
    Other,
}

#[derive(Debug)]
/// Struct representing a common error in the application.
pub struct EventError {
    pub message: String,
    pub kind: EventErrorKind,
}

impl EventError {
    /// Creates a new `EventError` with the specified message and kind.
    pub fn new(message: String, kind: EventErrorKind) -> Self {
        EventError { message, kind }
    }

    pub fn other(message: String) -> Self {
        EventError::new(message, EventErrorKind::Other)
    }
}

impl From<ParseIntError> for EventError {
    fn from(err: ParseIntError) -> Self {
        EventError::new(err.to_string(), EventErrorKind::Other)
    }
}

impl std::fmt::Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventError: {} - {:?}", self.message, self.kind)
    }
}

impl From<ExpresionJson> for EventError {
    fn from(err: ExpresionJson) -> Self {
        EventError::new(err.armar_string(), EventErrorKind::Other)
    }
}
