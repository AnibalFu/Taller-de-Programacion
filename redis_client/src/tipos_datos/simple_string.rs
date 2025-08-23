//! Este modulo contiene la implementacion del tipo de dato redis Simple String
use super::utils::agregar_elemento;
use crate::tipos_datos::constantes::{CR, CRLF, LF, MAX_LEN_SIMPLE_STRING};
use crate::tipos_datos::traits::DatoRedis;
use crate::tipos_datos::traits::TipoDatoRedis;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct SimpleString {
    largo: i32,
    contenido: String,
}

impl SimpleString {
    pub fn new(contenido: String) -> Result<Self, DatoRedis> {
        if contenido.contains(CR) || contenido.contains(LF) {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "string have \n or \r".to_string(),
            ));
        }

        // String Safe
        if contenido.len() > MAX_LEN_SIMPLE_STRING {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "string exceeds maximum allowed size (100 Bytes)".to_string(),
            ));
        }

        Ok(SimpleString {
            largo: contenido.len() as i32,
            contenido: contenido.to_string(),
        })
    }

    /// Crea un dato de redis Simple String a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `simple_string_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Simple String en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(simple_string_resp: String) -> Result<Self, DatoRedis> {
        if simple_string_resp.chars().nth(0) != Some('+') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let largo = simple_string_resp.chars().count();
        if largo > MAX_LEN_SIMPLE_STRING + 3 {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "too big simple string".to_string(),
            ));
        }
        if simple_string_resp.chars().nth(largo - 2) == Some('\r')
            && simple_string_resp.chars().nth(largo - 1) == Some('\n')
        {
            let mut texto = String::new();
            for i in 1..largo - 2 {
                agregar_elemento(&mut texto, &simple_string_resp, i);
            }
            return Ok(SimpleString {
                largo: (largo - 3) as i32,
                contenido: texto,
            });
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "expected '\r\n'".to_string(),
        ))
    }

    /// Retorna el contenido del Simple String
    pub fn contenido(&self) -> &String {
        &self.contenido
    }
}

impl TipoDatoRedis for SimpleString {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!("+{}\r\n", self.contenido)
    }

    fn convertir_resp_a_string(&self) -> String {
        format!("{}{}", self.contenido, CRLF)
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::simple_string::SimpleString;
    use crate::tipos_datos::traits::TipoDatoRedis;
    #[test]
    fn test_01_resp_a_simple_string_valido() {
        let simple_string = SimpleString::new_desde_resp("+hola\r\n".to_string());
        assert_eq!(simple_string.unwrap().largo, 4);
        let simple_string_2 = SimpleString::new_desde_resp("+hola\r\n".to_string());
        assert_eq!(simple_string_2.unwrap().contenido, "hola".to_string());
        let simple_string_3 = SimpleString::new_desde_resp("+ok\r\n".to_string());
        assert_eq!(simple_string_3.unwrap().contenido, "ok".to_string());
        let simple_string_4 = SimpleString::new_desde_resp("+\r\n".to_string());
        assert_eq!(simple_string_4.unwrap().contenido, "".to_string());
        let simple_string_5 = SimpleString::new_desde_resp("+a¡\r\n".to_string());
        assert_eq!(simple_string_5.unwrap().contenido, "a¡".to_string());
    }

    #[test]
    fn test_02_string_a_simple_string_valido() {
        let simple_string = SimpleString::new("+hola".to_string());
        assert_eq!(simple_string.unwrap().largo, 5);
        let simple_string_2 = SimpleString::new("+hola".to_string());
        assert_eq!(simple_string_2.unwrap().contenido, "+hola".to_string());
        let simple_string_3 = SimpleString::new("ok".to_string());
        assert_eq!(simple_string_3.unwrap().contenido, "ok".to_string());
        let simple_string_4 = SimpleString::new("".to_string());
        assert_eq!(simple_string_4.unwrap().contenido, "".to_string());
        let simple_string_5 = SimpleString::new("a¡".to_string());
        assert_eq!(simple_string_5.unwrap().contenido, "a¡".to_string());
    }

    #[test]
    fn test_03_resp_a_simple_string_invalido() {
        let simple_string = SimpleString::new_desde_resp("+abd".to_string());
        assert!(simple_string.is_err());
        let simple_string_2 = SimpleString::new_desde_resp("$abc\r\n".to_string());
        assert!(simple_string_2.is_err());
    }

    #[test]
    fn test_04_simple_string_a_resp_valido() {
        let simple_string = SimpleString::new_desde_resp("+abc\r\n".to_string());
        assert_eq!(
            simple_string.unwrap().convertir_a_protocolo_resp(),
            "+abc\r\n".to_string()
        );
        let simple_string_2 = SimpleString::new_desde_resp("+\r\n".to_string());
        assert_eq!(
            simple_string_2.unwrap().convertir_a_protocolo_resp(),
            "+\r\n".to_string()
        );
        let simple_string_3 = SimpleString::new_desde_resp("+12a\r\n".to_string());
        assert_eq!(
            simple_string_3.unwrap().convertir_a_protocolo_resp(),
            "+12a\r\n".to_string()
        );
        let simple_string_4 = SimpleString::new_desde_resp("+a¡\r\n".to_string());
        assert_eq!(
            simple_string_4.unwrap().convertir_a_protocolo_resp(),
            "+a¡\r\n".to_string()
        );
    }

    #[test]
    fn test_05_simple_string_a_desde_resp_a_string_valido() {
        let simple_string = SimpleString::new_desde_resp("+hola\r\n".to_string());
        assert_eq!(
            simple_string.unwrap().convertir_resp_a_string(),
            "hola\r\n".to_string()
        );
        let simple_string = SimpleString::new("hello".to_string());
        assert_eq!(
            simple_string.unwrap().convertir_resp_a_string(),
            "hello\r\n".to_string()
        );
        let simple_string = SimpleString::new("hi¡".to_string());
        assert_eq!(
            simple_string.unwrap().convertir_resp_a_string(),
            "hi¡\r\n".to_string()
        );
    }
}
