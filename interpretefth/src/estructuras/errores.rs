use std::fmt;

/// Representa los distintos tipos de errores
/// al interpretar las lineas del archivo fth

#[derive(Debug)]
pub enum Error {
    OperationFail,
    StackUnderflow,
    StackOverflow,
    InvalidWord,
    DivisionByZero,
    WordNotFound,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::OperationFail => write!(f, "Error en la operacion"),
            Error::StackUnderflow => write!(f, "stack-underflow"),
            Error::StackOverflow => write!(f, "stack-overflow"),
            Error::InvalidWord => write!(f, "invalid-word"),
            Error::DivisionByZero => write!(f, "division-by-zero"),
            Error::WordNotFound => write!(f, "?"),
        }
    }
}
