//! Este modulo contiene la implementacion del tipo de dato redis Set
use super::utils::obtener_elemento;
use crate::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Set {
    contenido: HashSet<DatoRedis>,
    es_nulo: bool,
}

impl Hash for Set {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for item in &self.contenido {
            item.hash(state);
        }
        self.es_nulo.hash(state);
    }
}

impl Default for Set {
    fn default() -> Self {
        Self::new()
    }
}

impl Set {
    pub fn new() -> Self {
        Set {
            contenido: HashSet::new(),
            es_nulo: false,
        }
    }

    /// Crea un dato de redis Set a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `array_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Set en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(array_resp: String) -> Result<Self, DatoRedis> {
        let elementos = Self::obtener_set(array_resp)?;
        let largo = elementos.len();
        Ok(Set {
            contenido: elementos,
            es_nulo: largo == 0,
        })
    }

    /// Crea un dato de redis Set vacio
    ///
    /// # Retorna
    /// - Set vacio
    pub fn new_nulo() -> Self {
        Set {
            contenido: HashSet::new(),
            es_nulo: true,
        }
    }

    /// Determina si es un set nulo
    pub fn es_nulo(&self) -> bool {
        self.es_nulo
    }

    /// Inserta un valor al set
    ///
    /// # Parametros
    /// * `dato`: dato redis a insertar
    pub fn insert(&mut self, dato: DatoRedis) {
        self.contenido.insert(dato);
    }

    /// Retorna la cardinalidad del set
    pub fn len(&self) -> usize {
        self.contenido.len()
    }

    /// Determina si el set no tiene elementos
    pub fn is_empty(&self) -> bool {
        self.contenido.is_empty()
    }

    /// Retorna el iterador del set
    pub fn iter(&self) -> impl Iterator<Item = &DatoRedis> {
        self.contenido.iter()
    }

    /// A partir de un string en formato resp, sin el caracter inicial de set,
    /// retorna un hashset de datos redis
    ///
    /// # Parametros:
    /// * `set_resp`: Representacion resp del set
    ///
    /// # Retorna
    /// - un hashset de datos redis en caso de exito, error simple en otro caso
    fn obtener_set(set_resp: String) -> Result<HashSet<DatoRedis>, DatoRedis> {
        if set_resp.is_empty() {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "wrong number of elements in set".to_string(),
            ));
        }
        if set_resp.chars().nth(0) != Some('~') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }
        let mut largo_set_str = String::new();
        let caracteres = &set_resp[1..];

        for caracter in caracteres.chars() {
            if caracter.is_ascii_digit() {
                largo_set_str.push(caracter);
            } else {
                break;
            }
        }
        let digitos_largo = largo_set_str.len();
        if let Ok(largo) = largo_set_str.parse::<usize>() {
            let set = Self::obtener_elementos(set_resp, digitos_largo, largo)?;
            if set.len() == largo {
                return Ok(set);
            }
        }
        Err(DatoRedis::new_simple_error(
            "Protocol error".to_string(),
            "invalid set length".to_string(),
        ))
    }

    /// Obtiene los elementos de un set a partir de una cadena que
    /// lo representa
    ///
    /// # Parametros
    /// * `set_resp`: String resp a interpretar
    /// * `digitos_largo`: cantidad de digitos del largo del arreglo
    /// * `largo_arreglo`: largo del set
    ///
    /// # Retorna
    /// - un hashset de datos redis en caso de exito, error simple en otro caso
    fn obtener_elementos(
        set_resp: String,
        digitos_largo: usize,
        largo_arreglo: usize,
    ) -> Result<HashSet<DatoRedis>, DatoRedis> {
        let mut elementos: HashSet<DatoRedis> = HashSet::new();
        let mut indice_fin = digitos_largo + 3;
        for _ in 0..largo_arreglo {
            let resto = &set_resp[indice_fin..].to_string();
            if let Ok((elemento, indice_final)) = obtener_elemento(resto) {
                elementos.insert(elemento);
                indice_fin += indice_final;
            } else {
                return Err(DatoRedis::new_simple_error(
                    "Protocol error".to_string(),
                    "invalid bulk string".to_string(),
                ));
            }
        }
        Ok(elementos)
    }

    /// Determina si un dato pertenece al set
    pub fn contains_member(&self, dato: &DatoRedis) -> bool {
        self.contenido.contains(dato)
    }

    /// Elimina un elemento del set, retornando 1 si estaba presente en el
    /// mismo, y 0 en otro caso
    pub fn remove_member(&mut self, dato: &DatoRedis) -> isize {
        if self.contenido.remove(dato) { 1 } else { 0 }
    }
}

impl TipoDatoRedis for Set {
    fn convertir_a_protocolo_resp(&self) -> String {
        if self.es_nulo {
            return "~-1\r\n".to_string();
        }
        let mut resultado = format!("~{}\r\n", self.contenido.len());
        for valor in &self.contenido {
            resultado.push_str(&valor.convertir_a_protocolo_resp());
        }
        resultado
    }

    fn convertir_resp_a_string(&self) -> String {
        if self.contenido.is_empty() {
            return "(empty set)\r\n".to_string();
        }
        self.convertir_con_indentacion(0)
    }
}

impl Set {
    /// Transforma un Set en una representacion de String
    /// enumerada
    ///
    /// # Parametros:
    /// * `nivel`: nivel de anidamiento en el arreglo
    ///
    /// # Retorna
    /// - Representacion String del set
    fn convertir_con_indentacion(&self, nivel: usize) -> String {
        let mut resultado = String::new();

        for (i, dato) in self.contenido.iter().enumerate() {
            resultado.push_str(&"\t".repeat(nivel));
            resultado.push_str(format!("{}) ", i + 1).as_str());

            match dato {
                DatoRedis::Set(set) => {
                    resultado.push_str(&set.convertir_con_indentacion(nivel + 1));
                }
                _ => {
                    resultado.push_str(&dato.convertir_resp_a_string());
                }
            }
        }

        resultado
    }
}

#[cfg(test)]
mod tests {

    use crate::tipos_datos::set::Set;
    use crate::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
    #[test]
    fn test_01_set_vacio_formato_valido() {
        let set = Set::new();
        let resultado_esperado = "~0\r\n".to_string();
        let resultado_obtenido = set.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_02_set_nulo_formato_valido() {
        let set = Set::new_nulo();
        let resultado_esperado = "~-1\r\n".to_string();
        let resultado_obtenido = set.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_03_set_con_enteros_formato_valido() {
        let mut set = Set::new();

        set.insert(DatoRedis::new_integer(1));
        set.insert(DatoRedis::new_integer(42));
        set.insert(DatoRedis::new_integer(-5));

        let resultado_obtenido = set.convertir_a_protocolo_resp();
        let mut lineas: Vec<&str> = resultado_obtenido
            .split("\r\n")
            .filter(|l| !l.is_empty())
            .collect();
        lineas.remove(0); // sacar la línea ~3

        let mut esperado = vec![":1", ":42", ":-5"];
        lineas.sort();
        esperado.sort();

        assert_eq!(lineas, esperado);
    }

    #[test]
    fn test_04_set_con_bulk_strings_formato_valido() {
        let mut set = Set::new();

        set.insert(DatoRedis::new_bulk_string("hola".into()).unwrap());
        set.insert(DatoRedis::new_bulk_string("chau".into()).unwrap());

        let resultado_obtenido = set.convertir_a_protocolo_resp();
        let mut lineas: Vec<&str> = resultado_obtenido
            .split("\r\n")
            .filter(|l| !l.is_empty())
            .collect();
        lineas.remove(0); // ~2

        // Cada BulkString son 2 líneas, así que deberíamos tener 4 líneas
        assert_eq!(lineas.len(), 4);
        assert!(lineas.contains(&"$4"));
        assert!(lineas.contains(&"hola"));
        assert!(lineas.contains(&"chau"));
    }

    #[test]
    fn test_05_set_mixto_formato_valido() {
        let mut set = Set::new();

        set.insert(DatoRedis::new_integer(7));
        set.insert(DatoRedis::new_bulk_string("test".into()).unwrap());

        let resultado_obtenido = set.convertir_a_protocolo_resp();
        assert!(resultado_obtenido.contains(":7\r\n"));
        assert!(resultado_obtenido.contains("$4\r\ntest\r\n"));
    }

    #[test]
    fn test_06_set_con_elementos_duplicados_no_se_repite() {
        let mut set = Set::new();

        let val = DatoRedis::new_integer(999);
        let val2 = DatoRedis::new_integer(999);

        set.insert(val);
        set.insert(val2);

        let resultado_obtenido = set.convertir_a_protocolo_resp();
        let cantidad = resultado_obtenido.matches(":999\r\n").count();
        assert_eq!(cantidad, 1);
        assert!(resultado_obtenido.starts_with("~1\r\n"));
    }

    #[test]
    fn test_07_set_int_a_partir_de_resp() {
        let set = Set::new_desde_resp("~3\r\n:1\r\n:3\r\n:334\r\n".to_string()).unwrap();
        assert_eq!(set.len(), 3);
        let resultado_obtenido = set.convertir_a_protocolo_resp();
        assert!(resultado_obtenido.starts_with("~3\r\n"));
        assert!(resultado_obtenido.contains(":1\r\n"));
        assert!(resultado_obtenido.contains(":3\r\n"));
        assert!(resultado_obtenido.contains(":334\r\n"));
    }

    #[test]
    fn test_08_set_bulk_str_a_partir_de_resp() {
        let set = Set::new_desde_resp("~2\r\n$3\r\nabc\r\n$1\r\nd\r\n".to_string()).unwrap();
        let resultado_obtenido = set.convertir_a_protocolo_resp();
        assert!(resultado_obtenido.starts_with("~2\r\n"));
        assert!(resultado_obtenido.contains("$3\r\nabc\r\n"));
        assert!(resultado_obtenido.contains("$1\r\nd\r\n"));
    }

    #[test]
    fn test_09_set_simple_str_a_partir_de_resp() {
        let set =
            Set::new_desde_resp("~4\r\n+abc\r\n+123\r\n+qwert\r\n+aa\r\n".to_string()).unwrap();
        let resultado_obtenido = set.convertir_a_protocolo_resp();
        assert!(resultado_obtenido.starts_with("~4\r\n"));
        assert!(resultado_obtenido.contains("+abc\r\n"));
        assert!(resultado_obtenido.contains("+123\r\n"));
        assert!(resultado_obtenido.contains("+qwert\r\n"));
        assert!(resultado_obtenido.contains("+aa\r\n"));
    }

    #[test]
    fn test_10_set_mixto_a_partir_de_resp() {
        let set = Set::new_desde_resp("~4\r\n+abc\r\n:-123\r\n+qwert\r\n$2\r\nab\r\n".to_string())
            .unwrap();
        let resultado_obtenido = set.convertir_a_protocolo_resp();
        assert!(resultado_obtenido.starts_with("~4\r\n"));
        assert!(resultado_obtenido.contains("+abc\r\n"));
        assert!(resultado_obtenido.contains(":-123\r\n"));
        assert!(resultado_obtenido.contains("+qwert\r\n"));
        assert!(resultado_obtenido.contains("$2\r\nab\r\n"));
    }

    #[test]
    fn test_11_set_a_string() {
        let set = Set::new_desde_resp("~4\r\n+abc\r\n:-123\r\n+qwert\r\n$2\r\nab\r\n".to_string())
            .unwrap();
        let resultado = set.convertir_resp_a_string();
        assert_eq!(resultado.len(), 46);
        assert!(resultado.contains("\"ab\"\r\n"));
        assert!(resultado.contains("qwert\r\n"));
        assert!(resultado.contains("(integer) -123\r\n"));
        assert!(resultado.contains("abc\r\n"));
    }
}
