use std::num::ParseIntError;

/// Error handling module for common errors in the application.

#[derive(Debug)]
/// Enum representing common error kinds.
pub enum CommonErrorKind {
    /// Represents an error related to pubsub operations.
    EventPubSub,
    /// Represents an error related to any error that is not necccesary to have an special kind.
    Other,
}

#[derive(Debug)]
/// Struct representing a common error in the application.
pub struct CommonError {
    pub message: String,
    pub kind: CommonErrorKind,
}

impl CommonError {
    /// Creates a new `CommonError` with the specified message and kind.
    pub fn new(message: String, kind: CommonErrorKind) -> Self {
        CommonError { message, kind }
    }

    pub fn other(message: String) -> Self {
        CommonError::new(message, CommonErrorKind::Other)
    }
}

impl From<ParseIntError> for CommonError {
    fn from(err: ParseIntError) -> Self {
        CommonError::new(err.to_string(), CommonErrorKind::Other)
    }
}

impl std::fmt::Display for CommonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommonError: {} - {:?}", self.message, self.kind)
    }
}
