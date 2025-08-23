//! Este modulo contiene la implementacion de los comandos
//! del tipo set de redis
use std::sync::Arc;
use std::sync::RwLock;

use crate::comandos::utils::{
    assert_correct_arguments_quantity, assert_number_of_arguments_distinct,
};
use crate::storage::Storage;
use redis_client::tipos_datos::traits::DatoRedis;

use super::utils::get_storage_read_lock;
use super::utils::get_storage_write_lock;

/// Inserta un elemento en una lista antes o despues
/// de un elemento de referencia (pivote)
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave del
///   set donde insertar y elementos a insertar
/// * `storage`: storage del nodo donde se encuentra el set donde
///   insertar
///
/// # Retorna
/// - en caso de insercion exitosa, la cantidad de valores insertados,
///   error simple de redis en otros casos
pub fn sadd(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;

    let mut guard = get_storage_write_lock(storage)?;

    let key = tokens[1].to_string();
    let elementos = tokens[2..].to_vec();

    let set_ref = match guard.get_mutable(key.clone()) {
        Ok(DatoRedis::Set(set)) => set,
        Ok(_) => {
            return Err(DatoRedis::new_simple_error(
                "WRONGTYPE".to_string(),
                "Operation against a key holding the wrong kind of value".to_string(),
            ));
        }
        Err(e) => match e {
            DatoRedis::MovedError(_) => return Err(e),
            _ => {
                let nuevo_set = DatoRedis::new_set();
                guard.set(key.clone(), nuevo_set)?;
                match guard.get_mutable(key.clone()) {
                    Ok(DatoRedis::Set(set)) => set,
                    _ => {
                        return Err(DatoRedis::new_simple_error(
                            "ERR".to_string(),
                            "No se pudo acceder al set recién creado".to_string(),
                        ));
                    }
                }
            }
        },
    };

    let mut nuevos_insertados = 0;
    for valor in elementos {
        set_ref.insert(DatoRedis::new_bulk_string(valor)?);
        nuevos_insertados += 1;
    }

    Ok(DatoRedis::new_integer(nuevos_insertados))
}

/// Devuelve la cantidad de elementos de un set
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave del
///   set
/// * `storage`: storage del nodo donde se encuentra el set
///
/// # Retorna
/// - cardinalidad del set, 0 de estar vacio o no existir
pub fn scard(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_number_of_arguments_distinct(tokens[0].to_string(), 2, tokens.len())?;

    let key = tokens[1].to_string();
    let mut guard = get_storage_write_lock(storage)?;
    match guard.get_mutable(key.to_string()) {
        Ok(DatoRedis::Set(set)) => Ok(DatoRedis::new_integer(set.len() as i64)),
        Ok(_) => Ok(DatoRedis::new_integer(0)),
        Err(e) => {
            if let DatoRedis::MovedError(_) = e {
                return Err(e);
            }
            Ok(DatoRedis::new_integer(0))
        }
    }
}

/// Determina si un elemento es parte de un set
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave del
///   set y elemento a buscar
/// * `storage`: storage del nodo donde se encuentra el set
///
/// # Retorna
/// - 1 si el elemento pertenece al set, 0 en otro caso
pub fn sismember(
    tokens: &[String],
    storage: &Arc<RwLock<Storage>>,
) -> Result<DatoRedis, DatoRedis> {
    assert_number_of_arguments_distinct(tokens[0].to_string(), 3, tokens.len())?;

    let guard = get_storage_read_lock(storage)?;
    let key = tokens[1].to_string();
    let element = tokens[2].to_string();
    let set_ref = guard.get(key).map_err(|_| DatoRedis::new_integer(0))?;

    // I don't like this, but it will return 0 either if the key or the element doesn't exist
    if let DatoRedis::Set(set) = set_ref {
        if set.contains_member(&DatoRedis::new_bulk_string(element)?) {
            Ok(DatoRedis::new_integer(1))
        } else {
            Ok(DatoRedis::new_integer(0))
        }
    } else {
        Ok(DatoRedis::new_integer(0))
    }
}

/// Remueve un elemento de un set
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave del
///   set y elemento a eliminar
/// * `storage`: storage del nodo donde se encuentra el set
///
/// # Retorna
/// - cantidad de elementos removidos del set
pub fn srem(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;
    let mut guard = get_storage_write_lock(storage)?;

    let key = tokens[1].to_string();
    let set_ref = guard.get_mutable(key.to_string()).map_err(|e| {
        if let DatoRedis::MovedError(_) = e {
            e
        } else {
            DatoRedis::new_integer(0)
        }
    })?;

    if let DatoRedis::Set(set) = set_ref {
        if set.is_empty() {
            return Ok(DatoRedis::new_integer(0));
        }
        let element_to_remove = DatoRedis::new_bulk_string(tokens[2].to_string())?;
        let response = set.remove_member(&element_to_remove);
        Ok(DatoRedis::new_integer(response as i64))
    } else {
        Ok(DatoRedis::new_integer(0))
    }
}

/// Devuelve una copia del set con los datos almacenados en el set
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave del set
/// * `storage`: storage del nodo donde se encuentra el set
/// # Retorna
/// - Una copia del ser original, o un error si el set no existe
pub fn smembers(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_number_of_arguments_distinct(tokens[0].to_string(), 2, tokens.len())?;

    let key = tokens[1].to_string();
    let guard = storage.read().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "No se pudo obtener el lock de lectura".to_string(),
        )
    })?;
    let set = guard.get(key).map_err(|_| DatoRedis::new_set())?;

    if let DatoRedis::Set(set_esperado) = set.clone() {
        Ok(DatoRedis::new_set_con_contenido(set_esperado))
    } else {
        Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{sadd, scard, sismember, smembers, srem};
    use crate::storage::Storage;
    use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
    use std::{
        ops::Range,
        sync::{Arc, RwLock},
    };

    const RANGE: Range<u16> = Range {
        start: 0,
        end: 16378,
    };

    fn convert_to_tokens(vector: Vec<&str>) -> Vec<String> {
        vector.into_iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_sadd_with_one_argument() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = convert_to_tokens(["sadd", "key1", "value1"].to_vec());

        let response = sadd(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(1));
    }

    #[test]
    fn test_sadd_with_n_arguments() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> =
            convert_to_tokens(["sadd", "key1", "value1", "value2", "value3"].to_vec());

        let response = sadd(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(3));
    }

    #[test]
    fn test_scard_with_one_argument() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["scard", "key1"].to_vec());
        let response = scard(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(1));
    }

    #[test]
    fn test_scard_with_no_arguments() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["scard", "key1"].to_vec());

        let response = scard(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(0));
    }

    #[test]
    fn test_scard_with_n_arguments() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1", "value2", "value3"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["scard", "key1"].to_vec());
        let response = scard(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(3));
    }

    #[test]
    fn test_is_member_with_member() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["sismember", "key1", "value1"].to_vec());
        let response = sismember(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(1));
    }

    #[test]
    fn test_is_member_with_non_member() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["sismember", "key1", "value2"].to_vec());
        let response = sismember(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(0));
    }

    #[test]
    fn test_remove_member() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["srem", "key1", "value1"].to_vec());
        let response = srem(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(1));
    }

    #[test]
    fn test_remove_member_with_non_member() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["srem", "key1", "value2"].to_vec());
        let response = srem(&tokens, &storage).unwrap();
        assert_eq!(response, DatoRedis::new_integer(0));
    }

    #[test]
    fn test_smembers_empty_set() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));

        let tokens = convert_to_tokens(["smembers", "key1"].to_vec());
        let response = smembers(&tokens, &storage);
        assert!(response.is_err());
        let result = response.err().unwrap();
        assert!(result.convertir_resp_a_string().contains("(empty set)\r\n"));
    }

    #[test]
    fn test_smembers() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens = convert_to_tokens(["sadd", "key1", "value1", "value2"].to_vec());
        sadd(&tokens, &storage).unwrap();

        let tokens = convert_to_tokens(["smembers", "key1"].to_vec());
        let response = smembers(&tokens, &storage).unwrap();
        assert!(response.convertir_resp_a_string().contains("value1"));
        assert!(response.convertir_resp_a_string().contains("value2"));
    }
}
