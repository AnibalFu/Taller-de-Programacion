//! Este modulo contiene la implementacion del tipo de dato redis Integer
use super::traits::DatoRedis;
use super::utils::agregar_elemento;
use crate::tipos_datos::constantes::{CRLF, INTEGER_SIMBOL};
use crate::tipos_datos::traits::TipoDatoRedis;
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Integer {
    valor: i64,
}

// Hay que ver si es necesario el [<+|->]
impl Integer {
    pub fn new(valor: i64) -> Self {
        Integer { valor }
    }

    /// Crea un dato de redis Integer a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `integer_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Integer de redis en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(integer_resp: String) -> Result<Self, DatoRedis> {
        if integer_resp.chars().nth(0) != Some(':') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let largo = integer_resp.len();
        if integer_resp.chars().nth(largo - 2) == Some('\r')
            && integer_resp.chars().nth(largo - 1) == Some('\n')
        {
            let mut numero = String::new();
            for i in 1..largo - 2 {
                agregar_elemento(&mut numero, &integer_resp, i);
            }
            if let Ok(n) = numero.parse::<i64>() {
                return Ok(Self { valor: n });
            }
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "expected '\r\n'".to_string(),
        ))
    }

    /// Retorna el valor del integer
    pub fn valor(&self) -> i64 {
        self.valor
    }
}

impl TipoDatoRedis for Integer {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!("{}{}{}", INTEGER_SIMBOL, self.valor, CRLF)
    }

    fn convertir_resp_a_string(&self) -> String {
        format!("(integer) {}{}", self.valor, CRLF)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tipos_datos::traits::TipoDatoRedis;

    #[test]
    fn test_01_integer_resp_formato_valido() {
        let numero = Integer::new(1000);
        let resultado_esperado = ":1000\r\n".to_string();
        let resultado_obtenido = numero.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_02_integer_negativo_formato_valido() {
        let numero = Integer::new(-42);
        let resultado_esperado = ":-42\r\n".to_string();
        let resultado_obtenido = numero.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_03_integer_cero_formato_valido() {
        let numero = Integer::new(0);
        let resultado_esperado = ":0\r\n".to_string();
        let resultado_obtenido = numero.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_04_integer_a_partir_de_resp_valido() {
        let integer = Integer::new_desde_resp(":23\r\n".to_string());
        assert_eq!(integer.unwrap().valor, 23);

        let integer_2 = Integer::new_desde_resp(":0\r\n".to_string());
        assert_eq!(integer_2.unwrap().valor, 0);

        let integer_3 = Integer::new_desde_resp(":-21456\r\n".to_string());
        assert_eq!(integer_3.unwrap().valor, -21456);
    }
}
