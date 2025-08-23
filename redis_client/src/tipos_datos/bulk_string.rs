//! Este modulo contiene la implementacion del tipo de dato redis Bulk String
use super::traits::TipoDatoRedis;
use crate::tipos_datos::traits::DatoRedis;

use crate::tipos_datos::constantes::{BULK_STRING_SIMBOL, CRLF, MAX_LEN_BULK_TYPE};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BulkString {
    largo: i32,
    contenido: String,
}

impl BulkString {
    pub fn new(contenido: String) -> Result<Self, DatoRedis> {
        let contenido = contenido.trim_matches('"').to_string();
        if contenido.len() > MAX_LEN_BULK_TYPE {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "string exceeds maximum allowed size (512MB)".to_string(),
            ));
        }
        Ok(BulkString {
            largo: contenido.len() as i32,
            contenido,
        })
    }

    /// Crea un dato de redis Bulk String a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `bulk_string_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Bulk String en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(bulk_string_resp: String) -> Result<Self, DatoRedis> {
        match Self::separar_bulk_string(bulk_string_resp) {
            Ok((largo, contenido)) => Ok(BulkString { largo, contenido }),
            Err(e) => Err(e),
        }
    }

    /// Detecta las partes de un Bulk String a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `bulk_string_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - tupla de largo y contenido del bulk string en caso de exito,
    ///   error simple de redis en otro caso
    fn separar_bulk_string(bulk_string_resp: String) -> Result<(i32, String), DatoRedis> {
        let largo_entrada = bulk_string_resp.chars().count();
        // borrar numeros magicos
        if bulk_string_resp == "$-1\r\n" {
            // Esto es para el null supongo
            return Ok((-1, String::new()));
        } else if largo_entrada < 6 || bulk_string_resp.chars().nth(0) != Some('$') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let resultado_largo = Self::obtener_largo(&bulk_string_resp, &largo_entrada);
        if let Ok((largo, indice_inicio_contenido)) = resultado_largo {
            let mut contenido = String::new();
            for i in indice_inicio_contenido..largo_entrada - 2 {
                if let Some(c) = bulk_string_resp.chars().nth(i) {
                    contenido.push(c);
                }
            }
            // esta feo
            if contenido.len() != largo
                || bulk_string_resp.chars().nth(largo_entrada - 2) != Some('\r')
                || bulk_string_resp.chars().nth(largo_entrada - 1) != Some('\n')
            {
                return Err(DatoRedis::new_simple_error(
                    "Protocol error".to_string(),
                    "expected '\r\n'".to_string(),
                ));
            }
            if let Ok(largo_contenido) = i32::try_from(largo) {
                return Ok((largo_contenido, contenido));
            }
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "too big bulk string".to_string(),
        ))
    }

    /// Obtiene el largo de un bulk string a partir de una cadena que
    /// lo representa
    ///
    /// # Parametros
    /// * `bulk_string_resp`: String resp a interpretar
    /// * `largo_entrada`: largo en chars de la entrada
    ///
    /// # Retorna
    /// - tupla de largo e indice de inicio del contendio del bulk string
    ///   en caso de exito, error simple de redis en otro caso
    fn obtener_largo(
        bulk_string_resp: &str,
        largo_entrada: &usize,
    ) -> Result<(usize, usize), DatoRedis> {
        let mut largo_bulk_string = String::new();
        for i in 1..*largo_entrada {
            let caracter = bulk_string_resp.chars().nth(i);
            if caracter == Some('\r') {
                break;
            }
            if let Some(caracter_valido) = caracter {
                largo_bulk_string.push(caracter_valido);
            }
        }
        let inicio_texto = 3 + largo_bulk_string.len();
        if let Ok(largo) = largo_bulk_string.trim().parse::<usize>() {
            return Ok((largo, inicio_texto));
        }
        Err(DatoRedis::new_null())
    }

    /// Retorna el largo del contenido del bulk error
    pub fn largo(&self) -> i32 {
        self.largo
    }

    /// Retorna el contenido del bulk error
    pub fn contenido(&self) -> String {
        self.contenido.to_string()
    }

    /// Concatena un string al contenido del bulk error
    pub fn concatenar(&mut self, otro: String) {
        self.contenido.push_str(&otro);
        self.largo = self.contenido.len() as i32;
    }
}

impl TipoDatoRedis for BulkString {
    fn convertir_a_protocolo_resp(&self) -> String {
        let mut respuesta = String::new();
        respuesta.push_str(BULK_STRING_SIMBOL);
        respuesta.push_str(&self.largo.to_string());
        respuesta.push_str(CRLF);
        if self.largo > -1 {
            respuesta.push_str(&self.contenido);
            respuesta.push_str(CRLF);
        }
        respuesta
    }

    fn convertir_resp_a_string(&self) -> String {
        if self.largo == -1 {
            "nil".to_string()
        } else {
            format!("\"{}\"{}", self.contenido, CRLF)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::bulk_string::BulkString;
    use crate::tipos_datos::traits::TipoDatoRedis;

    #[test]
    fn test_01_resp_a_bulk_string_valido() {
        let bulk_string = BulkString::new_desde_resp("$5\r\nhello\r\n".to_string());
        assert_eq!(bulk_string.unwrap().largo, 5);
        let bulk_string_2 = BulkString::new_desde_resp("$5\r\nhello\r\n".to_string());
        assert_eq!(bulk_string_2.unwrap().contenido, "hello".to_string());
        let bulk_string_3 = BulkString::new_desde_resp("$6\r\nhello\n\r\n".to_string());
        assert_eq!(bulk_string_3.unwrap().contenido, "hello\n".to_string());
        let bulk_string_4 = BulkString::new_desde_resp("$7\r\nhello\r\n\r\n".to_string());
        assert_eq!(bulk_string_4.unwrap().contenido, "hello\r\n".to_string());
        let bulk_string_5 = BulkString::new_desde_resp("$12\r\nhello world\n\r\n".to_string());
        assert_eq!(bulk_string_5.unwrap().largo, 12);
        let bulk_string_6 = BulkString::new_desde_resp("$7\r\nhello¡\r\n".to_string());
        assert_eq!(bulk_string_6.unwrap().largo, 7);
        let bulk_string_7 = BulkString::new_desde_resp("$7\r\nhello¡\r\n".to_string());
        assert_eq!(bulk_string_7.unwrap().contenido, "hello¡".to_string());
    }

    #[test]
    fn test_02_resp_a_bulk_string_invalido() {
        let bulk_string = BulkString::new_desde_resp("$5\r\nhello world\r\n".to_string());
        assert!(bulk_string.is_err());
        let bulk_string_2 = BulkString::new_desde_resp("$5\r\n\r\n".to_string());
        assert!(bulk_string_2.is_err());
        let bulk_string_3 = BulkString::new_desde_resp("$hola\r\nhello world\r\n".to_string());
        assert!(bulk_string_3.is_err());
        let bulk_string_4 = BulkString::new_desde_resp("+5\r\nhello\r\n".to_string());
        assert!(bulk_string_4.is_err());
        let bulk_string_4 = BulkString::new_desde_resp("$5hello\r\n".to_string());
        assert!(bulk_string_4.is_err());
    }

    #[test]
    fn test_03_bulk_string_a_resp_valido() {
        let bulk_string = BulkString::new_desde_resp("$5\r\nhello\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_a_protocolo_resp(),
            "$5\r\nhello\r\n".to_string()
        );
        let bulk_string = BulkString::new_desde_resp("$-1\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_a_protocolo_resp(),
            "$-1\r\n".to_string()
        );
        let bulk_string = BulkString::new_desde_resp("$7\r\nhello\r\n\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_a_protocolo_resp(),
            "$7\r\nhello\r\n\r\n".to_string()
        );
        let bulk_string = BulkString::new_desde_resp("$12\r\nhello world\n\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_a_protocolo_resp(),
            "$12\r\nhello world\n\r\n".to_string()
        );
        let bulk_string = BulkString::new_desde_resp("$0\r\n\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_a_protocolo_resp(),
            "$0\r\n\r\n".to_string()
        );
    }

    #[test]
    fn test_04_bulk_string_valido_de_resp_a_string() {
        let bulk_string = BulkString::new("hello".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_resp_a_string(),
            "\"hello\"\r\n".to_string()
        );
    }

    #[test]
    fn test_05_bulk_string_vacio_de_resp_a_string() {
        let bulk_string = BulkString::new("".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_resp_a_string(),
            "\"\"\r\n".to_string()
        );
    }

    #[test]
    fn test_06_bulk_string_nulo_de_resp_a_string() {
        let bulk_string = BulkString::new_desde_resp("$-1\r\n".to_string());
        assert_eq!(
            bulk_string.unwrap().convertir_resp_a_string(),
            "nil".to_string()
        );
    }

    #[test]
    fn test_07_largo_y_contenido() {
        let bulk = BulkString {
            largo: 5,
            contenido: "Hola!".to_string(),
        };

        assert_eq!(bulk.largo(), 5);
        assert_eq!(bulk.contenido(), "Hola!");
    }

    #[test]
    fn test_08_concatenar() {
        let mut bulk = BulkString {
            largo: 4,
            contenido: "Test".to_string(),
        };

        bulk.concatenar("123".to_string());

        assert_eq!(bulk.contenido(), "Test123");
        assert_eq!(bulk.largo(), 7);
    }
}
