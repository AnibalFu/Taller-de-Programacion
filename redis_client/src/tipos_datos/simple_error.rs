//! Este modulo contiene la implementacion del tipo de dato redis Simple Error
use super::traits::DatoRedis;
use super::utils::agregar_elemento;
use crate::tipos_datos::constantes::{CRLF, ERROR_SIMBOL};
use crate::tipos_datos::traits::TipoDatoRedis;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct SimpleError {
    largo: i32,
    tipo: String,
    mensaje: String,
}

impl SimpleError {
    pub fn new(tipo: String, mensaje: String) -> Self {
        SimpleError {
            largo: mensaje.len() as i32,
            tipo: tipo.to_uppercase(),
            mensaje,
        }
    }

    /// Crea un dato de redis Simple Error a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `simple_error_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Simple Error en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(simple_error_resp: String) -> Result<Self, DatoRedis> {
        if simple_error_resp.chars().nth(0) != Some('-') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let largo = simple_error_resp.chars().count();
        if simple_error_resp.chars().nth(largo - 2) == Some('\r')
            && simple_error_resp.chars().nth(largo - 1) == Some('\n')
        {
            let mut texto = String::new();
            for i in 1..largo - 2 {
                agregar_elemento(&mut texto, &simple_error_resp, i);
            }
            let (tipo, mensaje) = Self::obtener_tipo_y_mensaje(texto)?;
            return Ok(SimpleError {
                largo: mensaje.len() as i32,
                tipo,
                mensaje,
            });
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "expected '\r\n'".to_string(),
        ))
    }

    /// Retorna el mensaje del Simple Error
    pub fn mensaje(&self) -> String {
        self.mensaje.clone()
    }

    /// Retorna el tipo del Simple Error
    pub fn tipo(&self) -> String {
        self.tipo.clone()
    }

    /// Retorna el tipo y mensaje del Simple Error
    ///
    /// # Parametros
    /// * `texto`: String a interpretar
    ///
    /// # Retorna
    /// - Tupla de tipo y mensaje como Strings en caso de exito,
    ///   error simple de redis en otro caso
    fn obtener_tipo_y_mensaje(texto: String) -> Result<(String, String), DatoRedis> {
        let mut separacion = 0;
        for i in 0..texto.len() {
            if let Some(' ') = texto.chars().nth(i) {
                separacion = i;
                break;
            }
        }
        if separacion != 0 {
            return Ok((
                texto[..separacion].to_string().to_uppercase(),
                texto[separacion + 1..].to_string(),
            ));
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "invalid simple error".to_string(),
        ))
    }
}

impl TipoDatoRedis for SimpleError {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!("{}{} {}\r\n", ERROR_SIMBOL, self.tipo, self.mensaje)
    }

    fn convertir_resp_a_string(&self) -> String {
        format!("(error) {} {}{}", self.tipo, self.mensaje, CRLF) //dsp cambiar
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::simple_error::SimpleError;
    use crate::tipos_datos::traits::TipoDatoRedis;
    #[test]
    fn test_01_resp_a_simple_error_valido() {
        let simple_error = SimpleError::new_desde_resp("-ERR falla1\r\n".to_string());
        assert_eq!(simple_error.unwrap().tipo, "ERR".to_string());

        let simple_error = SimpleError::new_desde_resp("-ERR falla1\r\n".to_string());
        assert_eq!(simple_error.unwrap().mensaje, "falla1".to_string());

        let simple_error_2 = SimpleError::new_desde_resp("-ERR falla 2\r\n".to_string());
        assert_eq!(simple_error_2.unwrap().tipo, "ERR".to_string());

        let simple_error_3 = SimpleError::new_desde_resp("-ERR falla 2\r\n".to_string());
        assert_eq!(simple_error_3.unwrap().mensaje, "falla 2".to_string());

        let simple_error_4 = SimpleError::new_desde_resp("-ERR ¡falla 2\r\n".to_string());
        assert_eq!(simple_error_4.unwrap().mensaje, "¡falla 2".to_string());
    }

    #[test]
    fn test_02_string_error_a_simple_error_valido() {
        let simple_error = SimpleError::new("err".to_string(), "falla1".to_string());
        assert_eq!(simple_error.tipo, "ERR".to_string());
        let simple_error_2 = SimpleError::new("err".to_string(), "falla1".to_string());
        assert_eq!(simple_error_2.mensaje, "falla1".to_string());
        let simple_error_3 = SimpleError::new("err".to_string(), "falla 2".to_string());
        assert_eq!(simple_error_3.tipo, "ERR".to_string());
        let simple_error_4 = SimpleError::new("err".to_string(), "falla 2".to_string());
        assert_eq!(simple_error_4.mensaje, "falla 2".to_string());
        let simple_error_4 = SimpleError::new("err".to_string(), "¡falla 2".to_string());
        assert_eq!(simple_error_4.mensaje, "¡falla 2".to_string());
    }

    #[test]
    fn test_03_resp_a_simple_error_invalido() {
        let simple_error = SimpleError::new_desde_resp("+abd".to_string());
        assert!(simple_error.is_err());
        let simple_error_2 = SimpleError::new_desde_resp("+err abc\r\n".to_string());
        assert!(simple_error_2.is_err());
        let simple_error_3 = SimpleError::new_desde_resp("-errmensaje\r\n".to_string());
        assert!(simple_error_3.is_err());
    }

    #[test]
    fn test_04_simple_error_a_resp_valido() {
        let simple_error = SimpleError::new_desde_resp("-err falla1\r\n".to_string());
        assert_eq!(
            simple_error.unwrap().convertir_a_protocolo_resp(),
            "-ERR falla1\r\n".to_string()
        );
        let simple_error_2 = SimpleError::new_desde_resp("-err falla 2\r\n".to_string());
        assert_eq!(
            simple_error_2.unwrap().convertir_a_protocolo_resp(),
            "-ERR falla 2\r\n".to_string()
        );
        let simple_error_3 = SimpleError::new_desde_resp("-err ¡falla 2\r\n".to_string());
        assert_eq!(
            simple_error_3.unwrap().convertir_a_protocolo_resp(),
            "-ERR ¡falla 2\r\n".to_string()
        );
    }
}
