//! Este modulo contiene la implementacion del tipo de dato redis Map Reply
use std::{collections::HashMap, hash::Hash};

use super::{
    traits::{DatoRedis, TipoDatoRedis},
    utils::obtener_elemento,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MapReply {
    content: HashMap<DatoRedis, DatoRedis>,
    null: bool,
}

impl Hash for MapReply {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (key, value) in &self.content {
            key.hash(state);
            value.hash(state);
        }
        self.null.hash(state);
    }
}

impl Default for MapReply {
    fn default() -> Self {
        Self::new()
    }
}

impl MapReply {
    pub fn new() -> Self {
        Self {
            content: HashMap::new(),
            null: false,
        }
    }

    /// Inserta un par (clave, valor) en el mapa. Si la clave ya existia,
    /// actualiza su valor
    ///
    /// # Parametros
    /// * `key`: Clave, Dato Redis
    /// * `value`: Valor, Dato Redis
    pub fn insert(&mut self, key: DatoRedis, value: DatoRedis) {
        self.content.insert(key, value);
    }

    /// Verifica si el mapa contiene una clave
    ///
    /// # Parametros
    /// * `key`: Clave, Dato Redis
    ///
    /// # Retorna
    /// - verdadero si el mapa contiene la clave, falso en otro caso
    pub fn contains(&mut self, key: &DatoRedis) -> bool {
        self.content.contains_key(key)
    }

    /// Crea un dato de redis Map Reply a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `map_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Map Reply de redis en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(map_resp: String) -> Result<Self, DatoRedis> {
        if map_resp.is_empty() {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "wrong number of elements in map".to_string(),
            ));
        }
        if map_resp.chars().nth(0) != Some('%') {
            return Err(DatoRedis::new_simple_error(
                "Protocol error".to_string(),
                "invalid first byte".to_string(),
            ));
        }

        let mut map_len = String::new();
        let map_len_digits = &map_resp[1..];

        for caracter in map_len_digits.chars() {
            if caracter.is_ascii_digit() {
                map_len.push(caracter);
            } else {
                break;
            }
        }

        let digits_len = map_len.len();
        if let Ok(len) = map_len.parse::<usize>() {
            let hashmap = get_elements(map_resp, digits_len, len)?;
            let result_len = hashmap.len();
            if result_len == len {
                return Ok(Self {
                    content: hashmap,
                    null: result_len == len,
                });
            }
        }

        Err(DatoRedis::new_simple_error(
            "Protocol Error".to_string(),
            "Invalid hashmap length".to_string(),
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&DatoRedis, &DatoRedis)> {
        self.content.iter()
    }
}

impl TipoDatoRedis for MapReply {
    fn convertir_a_protocolo_resp(&self) -> String {
        if self.null {
            return "\r\n".to_string();
        }
        let mut result = format!("%{}\r\n", self.content.len());

        for (key, value) in &self.content {
            result.push_str(&key.convertir_a_protocolo_resp());
            result.push_str(&value.convertir_a_protocolo_resp());
        }
        result
    }

    fn convertir_resp_a_string(&self) -> String {
        let mut result = String::new();
        for (index, (key, value)) in self.content.iter().enumerate() {
            let key = key.convertir_resp_a_string();
            let key = key.strip_suffix("\r\n").unwrap_or_default();
            let value = value.convertir_resp_a_string();
            let value = value.strip_suffix("\r\n").unwrap_or_default();

            let line = format!("{}# {} => {}\n", index + 1, key, value);
            result.push_str(&line);
        }
        result
    }
}

fn get_elements(
    map_resp: String,
    digits_len: usize,
    map_len: usize,
) -> Result<HashMap<DatoRedis, DatoRedis>, DatoRedis> {
    let mut content = HashMap::new();
    let mut current_index = digits_len + 3;
    for _ in 0..map_len {
        let rest = &map_resp[current_index..].to_string();
        if let Ok((key, index_amount_to_increment)) = obtener_elemento(rest) {
            current_index += index_amount_to_increment;
            let rest = &map_resp[current_index..].to_string();
            if let Ok((value, index_amount_to_increment)) = obtener_elemento(rest) {
                content.insert(key, value);
                current_index += index_amount_to_increment;
            }
        }
    }
    Ok(content)
}

#[cfg(test)]
mod tests {

    use crate::tipos_datos::traits::{DatoRedis, TipoDatoRedis};

    use super::MapReply;

    #[test]
    fn test_01_hashmap_new_desde_resp() {
        let resp = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".to_string();
        let mut result = MapReply::new_desde_resp(resp).unwrap();
        let key_a = DatoRedis::new_simple_string("first".to_string()).unwrap();
        let key_b = DatoRedis::new_simple_string("second".to_string()).unwrap();
        assert!(result.contains(&key_a));
        assert!(result.contains(&key_b));
    }

    #[test]
    fn test_02_map_reply_as_string() {
        let resp = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".to_string();
        let result = MapReply::new_desde_resp(resp).unwrap();
        let string = result.convertir_resp_a_string();
        assert!(string.contains("first => (integer) 1\n"));
        assert!(string.contains("second => (integer) 2\n"));
    }
}
