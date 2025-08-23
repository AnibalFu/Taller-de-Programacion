use super::constantes::{CRLF, MOVED_ERROR_SIMBOL};
use super::traits::TipoDatoRedis;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct MovedError {
    slot: u16,
}

impl MovedError {
    pub fn new(slot: u16) -> Self {
        Self { slot }
    }

    pub fn get_slot(&self) -> u16 {
        self.slot
    }
}

impl TipoDatoRedis for MovedError {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!("{}{}{}", MOVED_ERROR_SIMBOL, self.slot, CRLF)
    }

    fn convertir_resp_a_string(&self) -> String {
        format!("(error) {}{}", self.slot, CRLF)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_moved_error_new_and_get_slot() {
        let moved = MovedError::new(42);
        assert_eq!(moved.get_slot(), 42);
    }

    #[test]
    fn test_moved_error_convertir_a_protocolo_resp() {
        let moved = MovedError::new(1234);
        let esperado = format!("{MOVED_ERROR_SIMBOL}1234{CRLF}");
        assert_eq!(moved.convertir_a_protocolo_resp(), esperado);
    }

    #[test]
    fn test_moved_error_convertir_resp_a_string() {
        let moved = MovedError::new(5678);
        let esperado = format!("(error) 5678{CRLF}");
        assert_eq!(moved.convertir_resp_a_string(), esperado);
    }

    #[test]
    fn test_moved_error_equality_and_clone() {
        let a = MovedError::new(1);
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(a.get_slot(), b.get_slot());
    }

    #[test]
    fn test_moved_error_hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let a = MovedError::new(10);
        let b = MovedError::new(10);
        set.insert(a);
        assert!(set.contains(&b));
    }
}
