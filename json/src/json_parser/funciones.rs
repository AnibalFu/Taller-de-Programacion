//! Este módulo contiene funciones y métodos para operar sobre expresiones json
use std::collections::HashMap;

use crate::json::JsonValue;
use crate::json::{ExpresionJson, LiteralJson, LiteralJson::BooleanJson};
use crate::json_parser::funciones::ExpresionJson::{Arreglo, Error, Literal, Objeto};

type ObjetoPares = HashMap<String, Box<ExpresionJson>>;
type ArregloElem = Vec<Box<ExpresionJson>>;

impl ExpresionJson {
    /// Crea una ExpresionJson
    ///
    /// # Retorna
    /// - Nueva ExpresionJson (objeto vacío)
    pub fn new() -> Self {
        let pares: HashMap<_, _> = HashMap::new();
        Objeto(pares)
    }

    /// Crea una ExpresionJson a partir de un diccionario
    ///
    /// # Parámetros
    /// - `pares`: Diccionario de String, Box<ExpresionJson>
    ///
    /// # Retorna
    /// - Nueva ExpresionJson (objeto)
    pub fn new_from_hashmap_json(pares: ObjetoPares) -> Self {
        Objeto(pares)
    }

    /// Crea una ExpresionJson a partir de un arreglo
    ///
    /// # Parámetros
    /// - `elem`: vector de Box<ExpresionJson>
    ///
    /// # Retorna
    /// - Nueva ExpresionJson (arreglo)
    pub fn new_from_arr_json(elem: ArregloElem) -> Self {
        Arreglo(elem)
    }

    /// Crea una ExpresionJson a partir de un string
    ///
    /// # Parámetros
    /// - `string`: contenido de la expresión
    ///
    /// # Retorna
    /// - Nueva ExpresionJson de tipo String
    pub fn new_from_string(string: String) -> Self {
        Literal(LiteralJson::StringJson(string))
    }

    /// Crea una ExpresionJson a partir de un número
    ///
    /// # Parámetros
    /// - `n`: contenido de la expresión
    ///
    /// # Retorna
    /// - Nueva ExpresionJson de tipo Número
    pub fn new_from_f64(n: f64) -> Self {
        Literal(LiteralJson::NumberJson(n))
    }

    /// Crea una ExpresionJson a partir de un booleano
    ///
    /// # Parámetros
    /// - `b`: booleano
    ///
    /// # Retorna
    /// - Nueva ExpresionJson de tipo Boolean
    pub fn new_from_bool(b: bool) -> Self {
        Literal(LiteralJson::BooleanJson(b))
    }

    /// Crea un String en formato Json a partir de un HashMap de JsonValues
    ///
    /// # Parámetros
    /// - `expresion`: HashMap de Strings y JsonValues
    ///
    /// # Retorna
    /// - String
    pub fn new_from_hashmap(expresion: HashMap<String, JsonValue>) -> String {
        Self::new_string_from_json_value(JsonValue::Object(expresion))
    }

    /// Crea un String en formato Json a partir de un Vector de JsonValues
    ///
    /// # Parámetros
    /// - `expresion`: Vector JsonValues
    ///
    /// # Retorna
    /// - String
    pub fn new_from_arr(expresion: Vec<JsonValue>) -> String {
        Self::new_string_from_json_value(JsonValue::Array(expresion))
    }

    /// Crea un String en formato Json a partir de un JsonValue
    ///
    /// # Parámetros
    /// - `expresion`: JsonValue
    ///
    /// # Retorna
    /// - String
    pub fn new_string_from_json_value(expresion: JsonValue) -> String {
        let json = Self::new_from_json_value(expresion);
        json.armar_string()
    }

    /// Crea una ExpresionJson en formato Json a partir de un JsonValue
    ///
    /// # Parámetros
    /// - `expresion`: JsonValue
    ///
    /// # Retorna
    /// - ExpresionJson
    pub fn new_from_json_value(expresion: JsonValue) -> Self {
        match expresion {
            JsonValue::Object(mapa) => {
                let mut pares = HashMap::new();
                for (k, v) in mapa {
                    let valor = Self::new_from_json_value(v);
                    pares.insert(k, Box::new(valor));
                }
                Self::new_from_hashmap_json(pares)
            }
            JsonValue::Array(arreglo) => {
                let mut arr = Vec::new();
                for elem in arreglo {
                    let elemento = Self::new_from_json_value(elem);
                    arr.push(Box::new(elemento));
                }
                Self::new_from_arr_json(arr)
            }
            _ => Self::new_literal_from_json_value(expresion),
        }
    }

    /// Crea una ExpresionJson en formato Json a partir de un JsonValue literal
    ///
    /// # Parámetros
    /// - `expresion`: JsonValue
    ///
    /// # Retorna
    /// - ExpresionJson
    fn new_literal_from_json_value(expresion: JsonValue) -> Self {
        match expresion {
            JsonValue::String(s) => Self::new_from_string(s),
            JsonValue::Boolean(b) => Self::new_from_bool(b),
            JsonValue::Number(n) => Self::new_from_f64(n),
            _ => Self::new_invalid_json_err(),
        }
    }

    /// Determina si existe la clave pedida a nivel raíz de un objeto Json
    ///
    /// # Parámetros
    /// - `key`: clave a buscar
    ///
    /// # Retorna
    /// - Verdadero si la clave existe, falso en otro caso
    pub fn exists_key(&self, key: &String) -> bool {
        match self {
            Objeto(pares) => pares.contains_key(key),
            _ => false,
        }
    }

    /// Determina si existe la clave pedida en algún nivel de un objeto Json
    ///
    /// # Parámetros
    /// - `key`: clave a buscar
    ///
    /// # Retorna
    /// - Verdadero si la clave existe, falso en otro caso
    pub fn exists_key_rec(&self, key: &String) -> bool {
        match self {
            Objeto(pares) => {
                if pares.contains_key(key) {
                    return true;
                }
                for v in pares.values() {
                    if (*v).exists_key_rec(key) {
                        return true;
                    }
                }
                false
            }
            Arreglo(elem) => {
                for e in elem {
                    if (*e).exists_key_rec(key) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// Retorna el valor de una clave a nivel raíz de un objeto Json
    ///
    /// # Parámetros
    /// - `key`: clave a buscar
    ///
    /// # Retorna
    /// - Valor de la clave en caso de existir, como String, Error de ExpresionJson en otro caso
    pub fn get_value(&self, key: &str) -> Result<String, ExpresionJson> {
        match self {
            Objeto(pares) => match pares.get(key) {
                Some(v) => Ok((*v).armar_string()),
                _ => Err(ExpresionJson::new_invalid_json_err()),
            },
            _ => Err(ExpresionJson::new_invalid_json_err()),
        }
    }

    /// Retorna el valor de una clave en algún nivel de un objeto Json
    ///
    /// # Parámetros
    /// - `key`: clave a buscar
    ///
    /// # Retorna
    /// - Valor de la clave en caso de existir, como String, Error de ExpresionJson en otro caso
    pub fn get_value_rec(&self, key: &str) -> Result<String, ExpresionJson> {
        match self {
            Objeto(pares) => match pares.get(key) {
                Some(v) => Ok((*v).armar_string()),
                _ => {
                    for v in pares.values() {
                        let res = (*v).get_value_rec(key);
                        match res {
                            Err(_) => continue,
                            _ => {
                                return res;
                            }
                        }
                    }
                    Err(ExpresionJson::new_invalid_json_err())
                }
            },
            Arreglo(elem) => {
                for e in elem {
                    let res = (*e).get_value_rec(key);
                    match res {
                        Err(_) => continue,
                        _ => {
                            return res;
                        }
                    }
                }
                Err(ExpresionJson::new_invalid_json_err())
            }
            _ => Err(ExpresionJson::new_invalid_json_err()),
        }
    }

    /// Retorna el valor de un índice de un arreglo Json
    ///
    /// # Parámetros
    /// - `idx`: valor a indexar
    ///
    /// # Retorna
    /// - Valor de la posición en caso de existir, como String, Error de ExpresionJson en otro caso
    pub fn get_index(&self, idx: usize) -> Result<String, ExpresionJson> {
        match self {
            Arreglo(elem) => match elem.get(idx) {
                Some(v) => Ok((*v).armar_string()),
                _ => Err(ExpresionJson::new_invalid_json_err()),
            },
            _ => Err(ExpresionJson::new_invalid_json_err()),
        }
    }

    /// Retorna el valor de una clave en algún nivel de un objeto Json, si es un booleano
    ///
    /// # Parámetros
    /// - `key`: clave a buscar
    ///
    /// # Retorna
    /// - Valor de la clave en caso de existir, como bool, Error de ExpresionJson en otro caso
    pub fn get_bool_rec(&self, key: &str) -> Result<bool, ExpresionJson> {
        match self {
            Objeto(pares) => match pares.get(key) {
                Some(v) => (*v).to_bool(),
                _ => {
                    for v in pares.values() {
                        let res = (*v).get_bool_rec(key);
                        match res {
                            Err(_) => continue,
                            _ => {
                                return res;
                            }
                        }
                    }
                    Err(ExpresionJson::new_invalid_json_err())
                }
            },
            Arreglo(elem) => {
                for e in elem {
                    let res = (*e).get_bool_rec(key);
                    match res {
                        Err(_) => continue,
                        _ => {
                            return res;
                        }
                    }
                }
                Err(ExpresionJson::new_invalid_json_err())
            }
            _ => Err(ExpresionJson::new_invalid_json_err()),
        }
    }

    /// Retorna el valor de una expresion si es un boolean
    ///
    /// # Retorna
    /// - Valor booleano, si corresponde, Error de ExpresionJson en otro caso
    fn to_bool(&self) -> Result<bool, ExpresionJson> {
        if let Literal(BooleanJson(b)) = self {
            Ok(*b)
        } else {
            Err(ExpresionJson::new_invalid_json_err())
        }
    }
}

impl Default for ExpresionJson {
    fn default() -> Self {
        Self::new()
    }
}

/// Agrega un par clave valor a un objeto json
///
/// # Parámetros
/// - `key`: clave a agregar
/// - `value`: valor correspondiente
///
/// # Retorna
/// - Nueva expresión Json con el par agregado
pub fn add_key(json: ExpresionJson, key: String, value: ExpresionJson) -> ExpresionJson {
    match json {
        Objeto(pares) => add_key_obj(pares, key, value),
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Agrega un par clave valor a un objeto json
///
/// # Parámetros
/// - `pares_originales`: pares donde agregar el par
/// - `key`: clave a agregar
/// - `value`: valor correspondiente
///
/// # Retorna
/// - Nueva expresión Json con el par agregado
pub fn add_key_obj(
    pares_originales: ObjetoPares,
    key: String,
    value: ExpresionJson,
) -> ExpresionJson {
    let mut pares = pares_originales.clone();
    pares.insert(key, Box::new(value));
    ExpresionJson::Objeto(pares)
}

/// Agrega un elemento al final de un arreglo json
///
/// # Parámetros
/// - `elem`: elemento a agregar
///
/// # Retorna
/// - Nueva expresión Json con el elemento agregado
pub fn add_item(json: ExpresionJson, elem: ExpresionJson) -> ExpresionJson {
    match json {
        Arreglo(elementos) => add_item_arr(elementos, elem),
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Agrega un elemento al final de un arreglo json
///
/// # Parámetros
/// - `elementos`: arreglo donde agregar el elemento
/// - `elem`: elemento a agregar
///
/// # Retorna
/// - Nueva expresión Json con el elemento agregado
pub fn add_item_arr(elementos: ArregloElem, elem: ExpresionJson) -> ExpresionJson {
    let mut elementos_nuevos = elementos.clone();
    elementos_nuevos.push(Box::new(elem));
    ExpresionJson::Arreglo(elementos_nuevos)
}

/// Borra un par clave valor a nivel raíz de un objeto json
///
/// # Parámetros
/// - `key`: clave a borrar
///
/// # Retorna
/// - Nueva expresión Json con el par borrado
pub fn delete_key(json: ExpresionJson, key: &String) -> ExpresionJson {
    match json {
        Objeto(pares) => {
            if pares.contains_key(key) {
                let mut nuevos_pares = pares.clone();
                nuevos_pares.remove(key);
                return ExpresionJson::new_from_hashmap_json(nuevos_pares);
            }
            ExpresionJson::new_invalid_json_err()
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Borra un par clave valor en algún nivel de un objeto json
///
/// # Parámetros
/// - `key`: clave a borrar
///
/// # Retorna
/// - Nueva expresión Json con el par borrado
pub fn delete_key_rec(json: ExpresionJson, key: &String) -> ExpresionJson {
    match json {
        Objeto(pares) => {
            if pares.contains_key(key) {
                let mut nuevos_pares = pares.clone();
                nuevos_pares.remove(key);
                return ExpresionJson::new_from_hashmap_json(nuevos_pares);
            }
            let mut nuevos_pares = HashMap::new();
            for (k, v) in pares {
                let res = delete_key_rec(*v.clone(), key);
                match res {
                    Error(_) => nuevos_pares.insert(k, v),
                    _ => nuevos_pares.insert(k, Box::new(res)),
                };
            }
            ExpresionJson::new_from_hashmap_json(nuevos_pares)
        }
        Arreglo(elem) => {
            for e in elem {
                let res = delete_key_rec(*e, key);
                match res {
                    Error(_) => continue,
                    _ => {
                        return res;
                    }
                }
            }
            ExpresionJson::new_invalid_json_err()
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Borra un elemento de un arreglo json
///
/// # Parámetros
/// - `idx`: posición a borrar
///
/// # Retorna
/// - Nueva expresión Json con el elemento borrado
pub fn delete_index(json: ExpresionJson, idx: usize) -> ExpresionJson {
    match json {
        Arreglo(elem) => {
            let mut elem_nuevos = elem.clone();
            elem_nuevos.remove(idx);
            ExpresionJson::new_from_arr_json(elem_nuevos)
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Edita el valor de una clave a nivel raíz de un objeto Json
///
/// # Parámetros
/// - `key`: clave a editar
/// - `nuevo_v`: nuevo valor de la clave, ExpresionJson
///
/// # Retorna
/// - Nueva expresión Json con el valor modificado
pub fn edit_value(json: ExpresionJson, key: &String, nuevo_v: ExpresionJson) -> ExpresionJson {
    match json {
        Objeto(pares) => {
            let mut nuevos_pares = pares.clone();
            nuevos_pares.insert(key.to_string(), Box::new(nuevo_v));
            ExpresionJson::new_from_hashmap_json(nuevos_pares)
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Edita el valor de una clave en algún nivel de un objeto Json
///
/// # Parámetros
/// - `key`: clave a editar
/// - `nuevo_v`: nuevo valor de la clave, ExpresionJson
///
/// # Retorna
/// - Nueva expresión Json con el valor modificado
pub fn edit_value_rec(json: ExpresionJson, key: &String, nuevo_v: ExpresionJson) -> ExpresionJson {
    match json.clone() {
        Objeto(pares) => {
            if json.exists_key(key) {
                return edit_value(json, key, nuevo_v);
            }
            for (_, v) in pares {
                let res = edit_value_rec(*v, key, nuevo_v.clone());
                match res {
                    Error(_) => continue,
                    _ => {
                        return res;
                    }
                }
            }
            ExpresionJson::new_invalid_json_err()
        }
        Arreglo(elem) => {
            for e in elem {
                let res = edit_value_rec(*e, key, nuevo_v.clone());
                match res {
                    Error(_) => continue,
                    _ => {
                        return res;
                    }
                }
            }
            ExpresionJson::new_invalid_json_err()
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

/// Edita el valor de una posición de un arreglo Json
///
/// # Parámetros
/// - `idx`: índice a editar
/// - `nuevo_v`: nuevo valor del índice, ExpresionJson
///
/// # Retorna
/// - Nueva expresión Json con la posición modificada
pub fn edit_index(json: ExpresionJson, idx: usize, nuevo_v: ExpresionJson) -> ExpresionJson {
    match json {
        Arreglo(elem) => {
            let mut nuevos_elem = elem.clone();
            nuevos_elem[idx] = Box::new(nuevo_v);
            ExpresionJson::new_from_arr_json(nuevos_elem)
        }
        _ => ExpresionJson::new_invalid_json_err(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        json::{ExpresionJson, JsonValue},
        json_parser::{
            funciones::{
                add_item, add_key, delete_index, delete_key, delete_key_rec, edit_index,
                edit_value, edit_value_rec,
            },
            parser::obtener_json_raw,
        },
    };

    fn get_json_string() -> String {
        r#"{
        "candidates": [
          {
            "content": {
              "parts": [
                { "text": "Prueba\n" }
              ],
              "role": "model"
            },
            "finishReason": "STOP"
          }
        ],
        "modelVersion": "gemini-1.5-flash",
        "responseId": "X"
    }"#
        .to_string()
    }

    #[test]
    fn test_01_add_key() {
        let json = ExpresionJson::new();
        let value = ExpresionJson::new_from_string("chau".to_string());
        let json_2 = add_key(json, "hola".to_string(), value.clone());
        let key = &("hola".to_string());
        assert!(json_2.exists_key(key));
        assert!(json_2.exists_key_rec(key));
        assert_eq!(json_2.get_value(key).unwrap(), "\"chau\"".to_string());
        assert_eq!(json_2.get_value_rec(key).unwrap(), "\"chau\"".to_string());
    }

    #[test]
    fn test_02_add_item() {
        let e1 = ExpresionJson::new_from_f64(22.006);
        let e2 = ExpresionJson::new_from_string("h".to_string());
        let arr = vec![Box::new(e1), Box::new(e2)];
        let json = ExpresionJson::new_from_arr_json(arr);
        let e3 = ExpresionJson::new_from_f64(-15.96);
        let json_2 = add_item(json, e3.clone());
        assert_eq!(json_2.get_index(2).unwrap(), e3.armar_string());
    }

    #[test]
    fn test_03_exists_key() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        assert!(json.exists_key(&"candidates".to_string()));
        assert!(json.exists_key(&"modelVersion".to_string()));
        assert!(json.exists_key(&"responseId".to_string()));
        assert!(!json.exists_key(&"content".to_string()));
        assert!(!json.exists_key(&"role".to_string()));
        assert!(!json.exists_key(&"text".to_string()));
        assert!(!json.exists_key(&"STOP".to_string()));
        assert!(!json.exists_key(&"X".to_string()));
    }

    #[test]
    fn test_04_exists_key_rec() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        assert!(json.exists_key_rec(&"candidates".to_string()));
        assert!(json.exists_key_rec(&"modelVersion".to_string()));
        assert!(json.exists_key_rec(&"responseId".to_string()));
        assert!(json.exists_key_rec(&"content".to_string()));
        assert!(json.exists_key_rec(&"role".to_string()));
        assert!(json.exists_key_rec(&"text".to_string()));
        assert!(!json.exists_key_rec(&"STOP".to_string()));
        assert!(!json.exists_key_rec(&"X".to_string()));
    }

    #[test]
    fn test_05_get_value() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        assert_eq!(
            json.get_value("modelVersion").unwrap(),
            "\"gemini-1.5-flash\"".to_string()
        );
        assert_eq!(json.get_value("responseId").unwrap(), "\"X\"".to_string());
    }

    #[test]
    fn test_06_get_value_rec() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let val_1 = "\"Prueba\\n\"".to_string();
        let val_2 = "\"model\"".to_string();
        let val_3 = "\"X\"".to_string();
        assert_eq!(json.get_value_rec("text").unwrap(), val_1);
        assert_eq!(json.get_value_rec("role").unwrap(), val_2);
        assert_eq!(json.get_value_rec("responseId").unwrap(), val_3);
    }

    #[test]
    fn test_07_get_value_rec() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let val_1 = "\"Prueba\\n\"".to_string();
        let val_2 = "\"model\"".to_string();
        let val_3 = "\"X\"".to_string();
        assert_eq!(json.get_value_rec("text").unwrap(), val_1);
        assert_eq!(json.get_value_rec("role").unwrap(), val_2);
        assert_eq!(json.get_value_rec("responseId").unwrap(), val_3);
    }

    #[test]
    fn test_08_get_index() {
        let string = r#"["hola", 23.0, [1, 2, 4]]"#;
        let json = obtener_json_raw(string.to_string()).unwrap();
        let val_1 = "\"hola\"".to_string();
        let val_2 = "23".to_string();
        let arr_2 = r#"[1, 2, 4]"#.to_string();
        assert_eq!(json.get_index(0).unwrap(), val_1);
        assert_eq!(json.get_index(1).unwrap(), val_2);
        assert_eq!(json.get_index(2).unwrap(), arr_2);
        assert!(json.get_index(3).is_err());
    }

    #[test]
    fn test_09_delete_key() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let json_2 = delete_key(json, &"candidates".to_string());
        assert!(!json_2.exists_key(&"candidates".to_string()));
        assert!(json_2.exists_key(&"modelVersion".to_string()));
        assert!(json_2.exists_key(&"responseId".to_string()));
    }

    #[test]
    fn test_10_delete_key_rec() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let json_2 = delete_key_rec(json, &"parts".to_string());
        assert!(json_2.exists_key_rec(&"candidates".to_string()));
        assert!(json_2.exists_key_rec(&"modelVersion".to_string()));
        assert!(json_2.exists_key_rec(&"responseId".to_string()));
        assert!(!json_2.exists_key_rec(&"parts".to_string()));
    }

    #[test]
    fn test_11_delete_index() {
        let string = r#"["hola", 23.0, [1, 2, 4]]"#;
        let json = obtener_json_raw(string.to_string()).unwrap();
        let val_1 = "\"hola\"".to_string();
        let arr_2 = r#"[1, 2, 4]"#.to_string();
        let json_2 = delete_index(json, 1);
        assert_eq!(json_2.get_index(0).unwrap(), val_1);
        assert_eq!(json_2.get_index(1).unwrap(), arr_2);
        assert!(json_2.get_index(2).is_err());
    }

    #[test]
    fn test_12_edit_value() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let val_2 = ExpresionJson::new_from_string("nuevo valor".to_string());
        let json_2 = edit_value(json, &"modelVersion".to_string(), val_2.clone());
        assert_eq!(
            json_2.get_value("modelVersion").unwrap(),
            "\"nuevo valor\"".to_string()
        );
    }

    #[test]
    fn test_13_edit_value_rec() {
        let string = get_json_string();
        let json = obtener_json_raw(string).unwrap();
        let val_2 = ExpresionJson::new_from_string("nuevo valor".to_string());
        let json_2 = edit_value_rec(json, &"text".to_string(), val_2.clone());
        assert_eq!(
            json_2.get_value("text").unwrap(),
            "\"nuevo valor\"".to_string()
        );
    }

    #[test]
    fn test_14_edit_index() {
        let string = r#"["hola", 23.0, [1, 2, 4]]"#;
        let json = obtener_json_raw(string.to_string()).unwrap();
        let val_1 = ExpresionJson::new_from_string("chau".to_string());
        let json_2 = edit_index(json, 2, val_1.clone());
        assert_eq!(json_2.get_index(2).unwrap(), "\"chau\"".to_string());
    }

    #[test]
    fn test_15_get_bool_rec() {
        let string = r#"{
        "key_1": "true",
        "key_2": true,
        "key_3": [
                { "key_4": true }
        ],
        "key_5": false
    }"#
        .to_string();
        let json = obtener_json_raw(string).unwrap();
        assert!(json.get_bool_rec("key_1").is_err());
        assert!(json.get_bool_rec("key_2").unwrap());
        assert!(json.get_bool_rec("key_3").is_err());
        assert!(json.get_bool_rec("key_4").unwrap());
        assert!(!json.get_bool_rec("key_5").unwrap());
    }

    #[test]
    fn test_16_new_from_string() {
        let string = "hola".to_string();
        let json_value = JsonValue::String(string);
        let json = ExpresionJson::new_string_from_json_value(json_value);
        assert_eq!(json, "\"hola\"".to_string());
    }

    #[test]
    fn test_17_new_from_f64() {
        let n = 23.2;
        let json_value = JsonValue::Number(n);
        let json = ExpresionJson::new_string_from_json_value(json_value);
        assert_eq!(json, "23.2".to_string());
    }

    #[test]
    fn test_18_new_from_bool() {
        let b = true;
        let json_value = JsonValue::Boolean(b);
        let json = ExpresionJson::new_string_from_json_value(json_value);
        assert_eq!(json, "true".to_string());
    }

    #[test]
    fn test_19_new_from_str_arr() {
        let arr = vec![
            JsonValue::Number(23_f64),
            JsonValue::String("hola".to_string()),
            JsonValue::Array(vec![JsonValue::Number(2.3), JsonValue::Boolean(false)]),
        ];
        let json_value = JsonValue::Array(arr);
        let json = ExpresionJson::new_string_from_json_value(json_value);
        assert_eq!(json, "[23, \"hola\", [2.3, false]]".to_string());
    }

    #[test]
    fn test_20_new_from_str_obj() {
        let v_4 = HashMap::from([
            ("k_5".to_string(), JsonValue::Number(2.33_f64)),
            ("k_6".to_string(), JsonValue::String("hola_2".to_string())),
        ]);
        let pares = HashMap::from([
            ("k_1".to_string(), JsonValue::Number(23_f64)),
            ("k_2".to_string(), JsonValue::String("hola".to_string())),
            (
                "k_3".to_string(),
                JsonValue::Array(vec![JsonValue::Number(2.3), JsonValue::Boolean(false)]),
            ),
            ("k_4".to_string(), JsonValue::Object(v_4)),
        ]);
        let json_value = JsonValue::Object(pares);
        let json = ExpresionJson::new_string_from_json_value(json_value);
        let json_exp = obtener_json_raw(json).unwrap();
        assert_eq!(json_exp.get_value_rec("k_1").unwrap(), "23".to_string());
        assert!(json_exp.get_value("k_5").is_err());
        assert_eq!(json_exp.get_value_rec("k_5").unwrap(), "2.33".to_string());
    }

    #[test]
    fn test_21_new_from_arr() {
        let arr = vec![
            JsonValue::Number(23_f64),
            JsonValue::String("hola".to_string()),
            JsonValue::Array(vec![JsonValue::Number(2.3), JsonValue::Boolean(false)]),
        ];
        let json = ExpresionJson::new_from_arr(arr);
        assert_eq!(json, "[23, \"hola\", [2.3, false]]".to_string());
    }

    #[test]
    fn test_22_new_from_hashmap() {
        let v_4 = HashMap::from([
            ("k_5".to_string(), JsonValue::Number(2.33_f64)),
            ("k_6".to_string(), JsonValue::String("hola_2".to_string())),
        ]);
        let pares = HashMap::from([
            ("k_1".to_string(), JsonValue::Number(23_f64)),
            ("k_2".to_string(), JsonValue::String("hola".to_string())),
            (
                "k_3".to_string(),
                JsonValue::Array(vec![JsonValue::Number(2.3), JsonValue::Boolean(false)]),
            ),
            ("k_4".to_string(), JsonValue::Object(v_4)),
        ]);
        let json = ExpresionJson::new_from_hashmap(pares);
        let json_exp = obtener_json_raw(json).unwrap();
        assert_eq!(json_exp.get_value_rec("k_1").unwrap(), "23".to_string());
        assert!(json_exp.get_value("k_5").is_err());
        assert_eq!(json_exp.get_value_rec("k_5").unwrap(), "2.33".to_string());
    }
}
