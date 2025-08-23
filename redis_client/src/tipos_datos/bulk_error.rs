//! Este modulo contiene la implementacion del tipo de dato redis Bulk Error
use super::traits::{DatoRedis, TipoDatoRedis};
use crate::tipos_datos::constantes::{CRLF, MAX_LEN_BULK_TYPE};
use std::fmt::Debug;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BulkError {
    largo: i32,
    contenido: String,
}

impl BulkError {
    pub fn new(contenido: String) -> Result<Self, DatoRedis> {
        if contenido.len() > MAX_LEN_BULK_TYPE {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "error exceeds maximum allowed size (512MB)".to_string(),
            ));
        }

        Ok(BulkError {
            largo: contenido.len() as i32,
            contenido,
        })
    }

    /// Crea un dato de redis Bulk Error a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `bulk_error_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Bulk Error en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(bulk_error_resp: String) -> Result<Self, DatoRedis> {
        match Self::separar_bulk_error(bulk_error_resp) {
            Ok((largo, contenido)) => Ok(BulkError { largo, contenido }),
            Err(e) => Err(e),
        }
    }

    /// Detecta las partes de un Bulk Error a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `bulk_error_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - tupla de largo y contenido del bulk error en caso de exito,
    ///   error simple de redis en otro caso
    fn separar_bulk_error(bulk_error_resp: String) -> Result<(i32, String), DatoRedis> {
        let largo_entrada = bulk_error_resp.chars().count();
        // borrar numeros magicos
        if bulk_error_resp == "!-1\r\n" {
            return Ok((-1, String::new()));
        } else if largo_entrada < 5 || bulk_error_resp.chars().nth(0) != Some('!') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let resultado_largo = Self::obtener_largo(&bulk_error_resp, &largo_entrada);
        if let Ok((largo, indice_inicio_contenido)) = resultado_largo {
            let mut contenido = String::new();
            for i in indice_inicio_contenido..largo_entrada - 2 {
                if let Some(c) = bulk_error_resp.chars().nth(i) {
                    contenido.push(c);
                }
            }
            // esta feo
            if contenido.len() != largo
                || bulk_error_resp.chars().nth(largo_entrada - 2) != Some('\r')
                || bulk_error_resp.chars().nth(largo_entrada - 1) != Some('\n')
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
            "expected '\r\n'".to_string(),
        ))
    }

    /// Obtiene el largo de un bulk error a partir de una cadena que
    /// lo representa
    ///
    /// # Parametros
    /// * `bulk_error_resp`: String resp a interpretar
    /// * `largo_entrada`: largo en chars de la entrada
    ///
    /// # Retorna
    /// - tupla de largo e indice de inicio del contendio del bulk error
    ///   en caso de exito, error simple de redis en otro caso
    fn obtener_largo(
        bulk_error_resp: &str,
        largo_entrada: &usize,
    ) -> Result<(usize, usize), DatoRedis> {
        let mut largo_bulk_error = String::new();
        for i in 1..*largo_entrada {
            let caracter = bulk_error_resp.chars().nth(i);
            if caracter == Some('\r') {
                break;
            }
            if let Some(caracter_valido) = caracter {
                largo_bulk_error.push(caracter_valido);
            }
        }
        // borrar numero magico
        let inicio_texto = 3 + largo_bulk_error.len();
        if let Ok(largo) = largo_bulk_error.trim().parse::<usize>() {
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

impl TipoDatoRedis for BulkError {
    fn convertir_a_protocolo_resp(&self) -> String {
        let mut respuesta = String::new();
        respuesta.push('!');
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
            "(nil)".to_string()
        } else {
            format!("{}{}", self.contenido, CRLF)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::bulk_error::BulkError;
    use crate::tipos_datos::traits::TipoDatoRedis;
    #[test]
    fn test_01_resp_a_bulk_error_valido() {
        let bulk_error = BulkError::new_desde_resp("!21\r\nSYNTAX invalid syntax\r\n".to_string());
        assert_eq!(bulk_error.unwrap().largo, 21);
        let bulk_error_2 =
            BulkError::new_desde_resp("!21\r\nSYNTAX invalid syntax\r\n".to_string());
        assert_eq!(bulk_error_2.unwrap().contenido, "SYNTAX invalid syntax");
        let bulk_error_3 = BulkError::new_desde_resp("!15\r\nERR generic err\r\n".to_string());
        assert_eq!(bulk_error_3.unwrap().largo, 15);
        let bulk_error_4 = BulkError::new_desde_resp("!15\r\nERR generic err\r\n".to_string());
        assert_eq!(bulk_error_4.unwrap().contenido, "ERR generic err");
        let bulk_error_5 = BulkError::new_desde_resp("!17\r\nERR generic ¡err\r\n".to_string());
        assert_eq!(bulk_error_5.unwrap().contenido, "ERR generic ¡err");
        let bulk_error_6 = BulkError::new_desde_resp("!17\r\nERR generic ¡err\r\n".to_string());
        assert_eq!(bulk_error_6.unwrap().largo, 17);
    }

    #[test]
    fn test_02_resp_a_bulk_error_invalido() {
        let bulk_error = BulkError::new_desde_resp("!21\r\nhello world\r\n".to_string());
        assert!(bulk_error.is_err());
        let bulk_error_2 = BulkError::new_desde_resp("$3\r\nERR\r\n".to_string());
        assert!(bulk_error_2.is_err());
        let bulk_error_3 = BulkError::new_desde_resp("!hola\r\nERR\r\n".to_string());
        assert!(bulk_error_3.is_err());
        let bulk_error_4 = BulkError::new_desde_resp("!3\r\nERR\r".to_string());
        assert!(bulk_error_4.is_err());
        let bulk_error_5 = BulkError::new_desde_resp("!3ERR\r\n".to_string());
        assert!(bulk_error_5.is_err());
    }

    #[test]
    fn test_03_bulk_error_a_resp_valido() {
        let bulk_error = BulkError::new_desde_resp("!3\r\nERR\r\n".to_string());
        assert_eq!(
            bulk_error.unwrap().convertir_a_protocolo_resp(),
            "!3\r\nERR\r\n".to_string()
        );
        let bulk_error_2 =
            BulkError::new_desde_resp("!21\r\nSYNTAX invalid syntax\r\n".to_string());
        assert_eq!(
            bulk_error_2.unwrap().convertir_a_protocolo_resp(),
            "!21\r\nSYNTAX invalid syntax\r\n".to_string()
        );
        let bulk_error_3 =
            BulkError::new_desde_resp("!23\r\nSYNTAX invalid ¡syntax\r\n".to_string());
        assert_eq!(
            bulk_error_3.unwrap().convertir_a_protocolo_resp(),
            "!23\r\nSYNTAX invalid ¡syntax\r\n".to_string()
        );
    }

    #[test]
    fn test_04_bulk_string_valido_de_resp_a_string() {
        let bulk_error = BulkError::new("ERR".to_string());
        assert_eq!(
            bulk_error.unwrap().convertir_resp_a_string(),
            "ERR\r\n".to_string()
        );
        let bulk_error_2 = BulkError::new("ERR¡".to_string());
        assert_eq!(
            bulk_error_2.unwrap().convertir_resp_a_string(),
            "ERR¡\r\n".to_string()
        );
    }

    #[test]
    fn test_05_bulk_string_vacio_de_resp_a_string() {
        let bulk_error = BulkError::new("".to_string());
        assert_eq!(
            bulk_error.unwrap().convertir_resp_a_string(),
            "\r\n".to_string()
        );
    }
}
