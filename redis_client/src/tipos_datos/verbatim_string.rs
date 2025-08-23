//! Este modulo contiene la implementacion del tipo de dato redis Verbatim String
use super::traits::DatoRedis;
use super::utils::agregar_elemento;
use crate::tipos_datos::constantes::VERBATIN_STRING_SIMBOL;
use crate::tipos_datos::traits::TipoDatoRedis;
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct VerbatimString {
    largo: i32,
    contenido: String,
    tipo: String,
}

impl VerbatimString {
    pub fn new(contenido: String, tipo: String) -> Result<Self, DatoRedis> {
        let largo = contenido.len() as i32;
        if Self::es_tipo_valido(&tipo) {
            return Ok(VerbatimString {
                largo,
                contenido,
                tipo,
            });
        }
        Err(DatoRedis::new_null())
    }

    /// Crea un dato de redis Verbatim String a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `verbatim_string_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Verbatim String en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(verbatim_string_resp: String) -> Result<Self, DatoRedis> {
        let (tipo, contenido_string) = Self::obtener_string(verbatim_string_resp)?;
        let largo = contenido_string.len() as i32;
        Ok(VerbatimString {
            largo,
            contenido: contenido_string,
            tipo,
        })
    }

    /// Devuelve tipo y contenido de un Verbatim String representado por el
    /// parametro contenido
    ///
    /// # Parametros
    /// * `contenido`: String resp a interpretar
    ///
    /// # Retorna
    /// - Tupla de tipo y contenido en caso de exito, error simple de
    ///   redis en otro caso
    fn obtener_string(contenido: String) -> Result<(String, String), DatoRedis> {
        if contenido.chars().nth(0) != Some('=') {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let mut largo_string = String::new();
        let mut indice_fin = 0;
        for i in 1..contenido.len() {
            if contenido.chars().nth(i) == Some('\r') && contenido.chars().nth(i + 1) == Some('\n')
            {
                indice_fin = i + 1;
                break;
            }
            agregar_elemento(&mut largo_string, &contenido, i);
        }
        if let Ok(numero) = largo_string.parse::<usize>() {
            let mut indice_final = indice_fin + 1;
            let mut texto = String::new();
            for i in indice_fin + 1..indice_fin + 1 + 3 {
                if let Some(n) = contenido.chars().nth(i) {
                    texto.push(n);
                    indice_final += 1;
                }
            }
            if contenido.chars().nth(indice_final) == Some(':') {
                return Self::verificar_contenido(&texto, indice_final, numero, contenido);
            }
        }
        Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "sintax error".to_string(),
        ))
    }

    /// Verifica si es un tipo valido de verbatim string
    fn es_tipo_valido(tipo: &String) -> bool {
        tipo == "txt" || tipo == "bin"
    }

    /// Verifica el contenido de un verbatim string
    ///
    /// # Parametros
    /// * `texto`: tipo a verificar
    /// * `indice_fin`: indice de fin del contenido
    /// * `numero`: largo del verbatim string
    /// * `contenido`: contenido a verificar
    ///
    /// # Retorna
    /// - Tupla de tipo y contenido caso de exito, error simple de redis
    ///   en otro caso
    fn verificar_contenido(
        texto: &String,
        indice_fin: usize,
        numero: usize,
        contenido: String,
    ) -> Result<(String, String), DatoRedis> {
        let mut indice_final = indice_fin;
        if Self::es_tipo_valido(texto) {
            let mut verbatim = String::new();
            let indice_fin = indice_final;
            for i in indice_fin + 1..indice_fin + 1 + numero {
                if let Some(n) = contenido.chars().nth(i) {
                    verbatim.push(n);
                    indice_final += 1;
                }
            }
            if indice_final == contenido.len() - 2 - 1
                && contenido.chars().nth(indice_final + 1) == Some('\r')
                && contenido.chars().nth(indice_final + 2) == Some('\n')
            {
                return Ok((texto.to_string(), verbatim));
            }
        }
        Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "sintax error".to_string(),
        ))
    }
}

impl TipoDatoRedis for VerbatimString {
    fn convertir_a_protocolo_resp(&self) -> String {
        format!(
            "{}{}\r\n{}:{}\r\n",
            VERBATIN_STRING_SIMBOL, self.largo, self.tipo, self.contenido
        )
    }

    fn convertir_resp_a_string(&self) -> String {
        format!("{}:{}", self.tipo, self.contenido) // devuelve cualq cosa
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::traits::TipoDatoRedis;
    use crate::tipos_datos::verbatim_string::VerbatimString;
    #[test]
    fn test_resp_a_verbatim_string_valido() {
        let verbatim_string = VerbatimString::new_desde_resp("=3\r\ntxt:abc\r\n".to_string());
        assert_eq!(verbatim_string.unwrap().contenido, "abc".to_string());
        let verbatim_string_2 = VerbatimString::new_desde_resp("=2\r\nbin:12\r\n".to_string());
        assert_eq!(verbatim_string_2.unwrap().contenido, "12".to_string());
    }

    #[test]
    fn test_string_a_verbatim_string_valido() {
        let verbatim_string = VerbatimString::new("abc".to_string(), "txt".to_string());
        assert_eq!(verbatim_string.unwrap().contenido, "abc");
        let verbatim_string_2 = VerbatimString::new("12".to_string(), "bin".to_string());
        assert_eq!(verbatim_string_2.unwrap().contenido, "12");
    }

    #[test]
    fn test_resp_a_verbatim_string_invalido() {
        let verbatim_string = VerbatimString::new_desde_resp("+abd".to_string());
        assert!(verbatim_string.is_err());
        let verbatim_string_2 = VerbatimString::new_desde_resp("=2\r\ntxt:abc\r\n".to_string());
        assert!(verbatim_string_2.is_err());
        let verbatim_string_3 = VerbatimString::new_desde_resp("=2\r\nabc:aa\r\n".to_string());
        assert!(verbatim_string_3.is_err());
    }

    #[test]
    fn test_verbatim_string_a_resp_valido() {
        let verbatim_string = VerbatimString::new_desde_resp("=3\r\ntxt:abc\r\n".to_string());
        assert_eq!(
            verbatim_string.unwrap().convertir_a_protocolo_resp(),
            "=3\r\ntxt:abc\r\n".to_string()
        );
        let verbatim_string_2 = VerbatimString::new_desde_resp("=2\r\nbin:12\r\n".to_string());
        assert_eq!(
            verbatim_string_2.unwrap().convertir_a_protocolo_resp(),
            "=2\r\nbin:12\r\n".to_string()
        );
    }
}
