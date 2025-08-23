use crate::from_raw_string;
use crate::json::ExpresionJson::{self, Error};
use crate::json_parser::funciones::{
    add_item, add_key, delete_index, delete_key, delete_key_rec, edit_index, edit_value,
    edit_value_rec,
};
use crate::json_parser::parser::obtener_json_raw;

/// Dado un string representando un json, obtiene el valor de una clave
///
/// # Parámetros
/// - `json_entrada`: string representando un json
/// - `campo`: clave pedida
///
/// # Retorna
/// - Valor del campo en caso de encontarlo, "" en otro caso
pub fn obtener_campo_rec(json_entrada: String, campo: &str) -> String {
    let json_result = obtener_json_raw(json_entrada);
    if let Ok(json) = json_result {
        let res = json.get_value_rec(campo);
        match res {
            Err(_) => return "".to_string(),
            Ok(r) => {
                return r;
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un json, agrega una clave
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a agregar
/// - `value`: valor de la clave
///
/// # Retorna
/// - String representando el nuevo Json en caso de éxito, "" en otro caso
pub fn add_key_json(json_raw: String, key: &str, value: String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        if let Ok(val) = obtener_json_raw(value) {
            let res = add_key(json, key.to_string(), val);
            match res {
                Error(_) => return "".to_string(),
                _ => {
                    return retornar_salida_string(res);
                }
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un arreglo json, agrega un elemento
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `elem`: elemento a agregar
///
/// # Retorna
/// - String representando el nuevo Json en caso de éxito, "" en otro caso
pub fn add_item_json(json_raw: String, elem: String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        if let Ok(elemento) = obtener_json_raw(elem) {
            let res = add_item(json, elemento);
            match res {
                Error(_) => return "".to_string(),
                _ => {
                    return retornar_salida_string(res);
                }
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un json, determina si existe una clave
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a buscar
///
/// # Retorna
/// - Verdadero si existe la clave a nivel raíz, falso en otro caso
pub fn exists_key_json(json_raw: String, key: &String) -> bool {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        return json.exists_key(key);
    }
    false
}

/// Dado un string representando un json, determina si existe una clave
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a buscar
///
/// # Retorna
/// - Verdadero si existe la clave, falso en otro caso
pub fn exists_key_rec_json(json_raw: String, key: &String) -> bool {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        return json.exists_key_rec(key);
    }
    false
}

/// Dado un string representando un objeto json, obtiene el valor de una clave
/// si existe
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a obtener
///
/// # Retorna
/// - String representando el valor en caso de éxito, "" en otro caso
pub fn get_value_json(json_raw: String, key: &str) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        let res = json.get_value_rec(key);
        match res {
            Err(_) => return "".to_string(),
            Ok(r) => {
                return r;
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un arreglo json, obtiene el valor de un elemento
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `index`: posición del elemento
///
/// # Retorna
/// - String representando el valor en caso de éxito, "" en otro caso
pub fn get_index_json(json_raw: String, index: usize) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        let res = json.get_index(index);
        match res {
            Err(_) => return "".to_string(),
            Ok(r) => {
                return r;
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un objeto json, borra una clave si existe
/// a nivel raíz
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a obtener
///
/// # Retorna
/// - String representando el nuevo Json sin la clave en caso de éxito, "" en otro caso
pub fn delete_key_json(json_raw: String, key: &String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        let res = delete_key(json, key);
        match res {
            Error(_) => return "".to_string(),
            _ => {
                return retornar_salida_string(res);
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un objeto json, borra una clave si existe
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a obtener
///
/// # Retorna
/// - String representando el nuevo Json sin la clave en caso de éxito, "" en otro caso
pub fn delete_key_rec_json(json_raw: String, key: &String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        let res = delete_key_rec(json, key);
        match res {
            Error(_) => return "".to_string(),
            _ => {
                return retornar_salida_string(res);
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un arreglo json, borra una posición si existe
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a obtener
///
/// # Retorna
/// - String representando el nuevo Json sin la posición en caso de éxito, "" en otro caso
pub fn delete_index_json(json_raw: String, index: usize) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        let res = delete_index(json, index);
        match res {
            Error(_) => return "".to_string(),
            _ => {
                return retornar_salida_string(res);
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un objeto json, edita el valor de una clave
/// si existe a nivel raíz
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a editar
/// - `value`: nuevo valor de la clave
///
/// # Retorna
/// - String representando el nuevo Json editado en caso de éxito, "" en otro caso
pub fn edit_value_json(json_raw: String, key: &String, value: String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        if let Ok(val) = obtener_json_raw(value) {
            let res = edit_value(json, key, val);
            match res {
                Error(_) => return "".to_string(),
                _ => {
                    return retornar_salida_string(res);
                }
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un objeto json, edita el valor de una clave
/// si existe
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `key`: clave a editar
/// - `value`: nuevo valor de la clave
///
/// # Retorna
/// - String representando el nuevo Json editado en caso de éxito, "" en otro caso
pub fn edit_value_rec_json(json_raw: String, key: &String, value: String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        if let Ok(val) = obtener_json_raw(value) {
            let res = edit_value_rec(json, key, val);
            match res {
                Error(_) => return "".to_string(),
                _ => {
                    return retornar_salida_string(res);
                }
            }
        }
    }
    "".to_string()
}

/// Dado un string representando un arreglo json, edita el valor de una posición
/// si existe
///
/// # Parámetros
/// - `json_raw`: string representando un json
/// - `index`: posición a editar
/// - `value`: nuevo valor del índice
///
/// # Retorna
/// - String representando el nuevo Json editado en caso de éxito, "" en otro caso
pub fn edit_index_json(json_raw: String, index: usize, value: String) -> String {
    let json_result = obtener_json_raw(json_raw);
    if let Ok(json) = json_result {
        if let Ok(val) = obtener_json_raw(value) {
            let res = edit_index(json, index, val);
            match res {
                Error(_) => return "".to_string(),
                _ => {
                    return retornar_salida_string(res);
                }
            }
        }
    }
    "".to_string()
}

/// Dada una ExpresionJson, la transforma en un String
///
/// # Parámetros
/// - `json`: ExpresionJson a transformar
///
/// # Retorna
/// - String representando el Json
fn retornar_salida_string(json: ExpresionJson) -> String {
    let string = json.armar_string();
    let largo = string.len();
    if string.starts_with('\"') && string.ends_with('\"') {
        return from_raw_string(&string[1..largo - 1]);
    }
    from_raw_string(&string)
}

#[cfg(test)]
mod test {

    use crate::{
        json_parser::parser::obtener_json_raw,
        libreria_json::{
            add_item_json, add_key_json, delete_index_json, delete_key_json, delete_key_rec_json,
            edit_index_json, edit_value_json, edit_value_rec_json, exists_key_json,
            exists_key_rec_json, get_index_json, get_value_json, obtener_campo_rec,
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
                "finishReason": "ST\\\"OP"
              }
            ],
            "modelVersion": "gemini-1.5-flash",
            "responseId": "X"
        }"#
        .to_string()
    }

    #[test]
    fn test_01_obtener_campo() {
        let string = get_json_string();
        assert_eq!(obtener_campo_rec(string, "text"), "\"Prueba\\n\"");
    }

    #[test]
    fn test_02_obtener_campo_from_string() {
        let string = "{\n    \"candidates\": [\n        {\n            \"content\": {\n                \"parts\": [\n                    { \"text\": \"Prue,ba\\n\" }\n                ],\n                \"role\": \"model\"\n,\n                \"float\": -0.2345\n},\n            \"finishReason\": \"STO\\\"P\"\n        }\n    ],\n    \"modelVersion\": \"gemini-1.5-flash\",\n    \"responseId\": \"X\"\n}\n";
        assert_eq!(
            obtener_campo_rec(string.to_string(), "finishReason"),
            "\"STO\\\"P\""
        );
    }

    #[test]
    fn test_03_obtener_campo_from_string_2() {
        let string = "{\n    \"candidates\": [\n -0.234],\n    \"modelVersion\": \"gemini-1.5-flash\",\n    \"responseId\": \"X\"\n}\n";
        assert_eq!(
            obtener_campo_rec(string.to_string(), "modelVersion"),
            "\"gemini-1.5-flash\""
        );
    }

    #[test]
    fn test_04_add_key() {
        let json = get_json_string();
        let key = &("hola".to_string());
        let val = &("20".to_string());
        let json_2 = add_key_json(json, key, val.to_string());
        assert_eq!(obtener_campo_rec(json_2.clone(), key), "20".to_string());
    }

    #[test]
    fn test_05_add_item() {
        let string = r#"["hola", 23.0, [1, 2, 4]]"#;
        let val_1 = &("\"chau\"".to_string());
        let res = add_item_json(string.to_string(), val_1.to_string());
        let json = obtener_json_raw(res).unwrap();
        assert_eq!(json.get_index(3).unwrap(), "\"chau\"".to_string());
    }

    #[test]
    fn test_06_exists_key() {
        let json = get_json_string();
        assert!(exists_key_json(json.to_string(), &"candidates".to_string()));
        assert!(exists_key_json(
            json.to_string(),
            &"modelVersion".to_string()
        ));
        assert!(!exists_key_json(json.to_string(), &"text".to_string()));
    }

    #[test]
    fn test_07_exists_key_rec() {
        let json = get_json_string();
        assert!(exists_key_rec_json(
            json.to_string(),
            &"candidates".to_string()
        ));
        assert!(exists_key_rec_json(
            json.to_string(),
            &"modelVersion".to_string()
        ));
        assert!(exists_key_rec_json(json.to_string(), &"text".to_string()));
    }

    #[test]
    fn test_08_get_value() {
        let string = get_json_string();
        assert_eq!(get_value_json(string, "responseId"), "\"X\"");
    }

    #[test]
    fn test_09_get_index() {
        let string = r#"["hola", 23.0, [1, 2, 4]]"#;
        assert_eq!(
            get_index_json(string.to_string(), 0),
            "\"hola\"".to_string()
        );
    }

    #[test]
    fn test_10_delete_key() {
        let string = get_json_string();
        let json = delete_key_json(string, &"modelVersion".to_string());
        assert!(!exists_key_json(json, &"modelVersion".to_string()));
    }

    #[test]
    fn test_11_delete_key_rec() {
        let string = get_json_string();
        let json = delete_key_rec_json(string.to_string(), &"role".to_string());
        assert!(!exists_key_rec_json(json, &"role".to_string()));
    }

    #[test]
    fn test_12_delete_index() {
        let string = r#"["hola", 23, [1, 2, 4]]"#;
        let res = delete_index_json(string.to_string(), 0);
        assert_eq!(get_index_json(res.clone(), 0), "23".to_string());
    }

    #[test]
    fn test_13_edit_value() {
        let string = get_json_string();
        let json = edit_value_json(string, &"responseId".to_string(), "\"Y\"".to_string());
        assert_eq!(get_value_json(json.to_string(), "responseId"), "\"Y\"");
    }

    #[test]
    fn test_14_edit_value_rec() {
        let string = get_json_string();
        let json = edit_value_rec_json(string, &"text".to_string(), "\"Y\"".to_string());
        assert_eq!(get_value_json(json.to_string(), "text"), "\"Y\"");
    }

    #[test]
    fn test_15_edit_index() {
        let string = r#"["hola", 23, [1, 2, 4]]"#;
        let json = edit_index_json(string.to_string(), 2, "\"Y\"".to_string());
        assert_eq!(get_index_json(json.to_string(), 2), "\"Y\"");
    }
    #[test]
    fn test_16_strings_con_comillas() {
        let string = "{\"hola\":\"cha\\\"\\\"\\\"u\"}";
        assert_eq!(
            obtener_campo_rec(string.to_string(), "hola"),
            "\"cha\\\"\\\"\\\"u\"".to_string()
        );
    }
}
