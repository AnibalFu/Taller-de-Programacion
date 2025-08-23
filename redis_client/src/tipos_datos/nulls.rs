//! Este modulo contiene la implementacion del tipo de dato Null de Redis
use super::traits::DatoRedis;
use crate::tipos_datos::constantes::CRLF;
use crate::tipos_datos::constantes::NULL_SIMBOL;
use crate::tipos_datos::traits::TipoDatoRedis;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Null;

impl Default for Null {
    fn default() -> Self {
        Self::new()
    }
}

impl Null {
    pub fn new() -> Self {
        Null {}
    }

    /// Crea un dato de redis Null a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `null_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - null de redis en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(null_resp: String) -> Result<Self, DatoRedis> {
        if null_resp == "_\r\n" {
            Ok(Null {})
        } else {
            Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "expected '\r\n'".to_string(),
            ))
        }
    }
}

impl TipoDatoRedis for Null {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!("{NULL_SIMBOL}{CRLF}")
    }

    fn convertir_resp_a_string(&self) -> String {
        "(null)\r\n".to_string() //  Hay que ver que devuelve como tal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_default() {
        let n1 = Null::new();
        let n2: Null = Default::default();
        assert_eq!(
            n1.convertir_a_protocolo_resp(),
            n2.convertir_a_protocolo_resp()
        );
        assert_eq!(n1.convertir_resp_a_string(), n2.convertir_resp_a_string());
    }

    #[test]
    fn test_new_desde_resp_ok() {
        let resp = "_\r\n".to_string();
        let res = Null::new_desde_resp(resp);
        assert!(res.is_ok());
    }

    #[test]
    fn test_new_desde_resp_err() {
        let resp = "x\r\n".to_string();
        let res = Null::new_desde_resp(resp);
        assert!(res.is_err());
    }

    #[test]
    fn test_convertir_a_protocolo_resp() {
        let n = Null::new();
        assert_eq!(n.convertir_a_protocolo_resp(), "_\r\n");
    }

    #[test]
    fn test_convertir_resp_a_string() {
        let n = Null::new();
        assert_eq!(n.convertir_resp_a_string(), "(null)\r\n");
    }
}
