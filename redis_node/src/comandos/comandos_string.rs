//! Este modulo contiene la implementacion de los comandos para strings de redis
use std::sync::{Arc, RwLock};

use crate::comandos::utils::assert_correct_arguments_quantity;
use crate::{comandos::const_cmd::OPERACION_EXITOSA, storage::Storage};
use redis_client::tipos_datos::traits::DatoRedis;

use super::utils::{get_storage_read_lock, get_storage_write_lock};

/// Obtiene el valor (string) de una clave
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave del elemento
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Valor de la clave en caso de exito, null en otro caso
pub fn get(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;
    let guard = get_storage_read_lock(storage)?;

    match guard.get(tokens[1].to_string()) {
        Ok(val) => {
            if let DatoRedis::BulkString(_) = val {
                Ok(val)
            } else {
                Err(DatoRedis::new_null())
            }
        }
        Err(e) => Err(e),
    }
}

/// Borra el valor (string) de una clave
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y
///   claves de los elementos a borrar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Cantidad de elementos eliminados, error de redis en otro caso
pub fn del(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;
    let mut eliminados = 0;
    let mut guard = get_storage_write_lock(storage)?;
    for token in tokens.iter() {
        match guard.remove(token.to_string()) {
            Ok(_) => eliminados += 1,
            Err(e) => match e {
                DatoRedis::MovedError(_) => return Err(e),
                _ => continue,
            },
        }
    }
    Ok(DatoRedis::new_integer(eliminados))
}

/// Asigna un valor (string) de una clave. De existir la clave, modifica
/// su valor si la misma ya almacenaba un string
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave y elemento
///   a insertar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - OK en caso de exito, error de redis en otro caso
pub fn set(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;
    let valor_a_guardar = DatoRedis::new_bulk_string(tokens[2].to_string())?;
    let mut guard = get_storage_write_lock(storage)?;

    if let Ok(valor_existente) = guard.get(tokens[1].to_string()) {
        if !matches!(valor_existente, DatoRedis::BulkString(_)) {
            return Err(DatoRedis::new_simple_error(
                "WRONGTYPE".to_string(),
                "Operation against a key holding the wrong kind of value".to_string(),
            ));
        }
    }
    guard.set(tokens[1].to_string(), valor_a_guardar)?;
    DatoRedis::new_simple_string(OPERACION_EXITOSA.to_string())
}

/// Borra una clave y devuelve su valor (string)
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave a eliminar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Valor borrado en caso de exito, null en otro caso
pub fn getdel(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;
    let mut guard = get_storage_write_lock(storage)?;

    match guard.remove(tokens[1].to_string()) {
        Ok(val) => Ok(val),
        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e),
            _ => Err(DatoRedis::new_null()),
        },
    }
}

/// Extiende el valor de una clave con el parametro valor
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave a modificar
///   y valor a appendear
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Nuevo largo del valor de la clave en caso de exito, 0 en otro caso
pub fn append(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;
    let mut guard = get_storage_write_lock(storage)?;

    match guard.get_mutable(tokens[1].to_string()) {
        Ok(valor) => {
            if let DatoRedis::BulkString(bulk_string) = &mut *valor {
                bulk_string.concatenar(tokens[2].to_string());
                let longitud_cadena = DatoRedis::new_integer(bulk_string.largo() as i64);
                return Ok(longitud_cadena);
            }
        }
        Err(e) => match e {
            DatoRedis::MovedError(_) => return Err(e),
            _ => {
                set(
                    &[
                        "set".to_string(),
                        tokens[1].to_string(),
                        tokens[2].to_string(),
                    ],
                    storage,
                )?;
                return Ok(DatoRedis::new_integer(tokens[2].len() as i64));
            }
        },
    }
    Err(DatoRedis::new_integer(0))
}

/// Obtiene el largo del valor para una clave
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave a analizar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Largo del valor de la clave en caso de exito, error de redis
///   en caso de tipo invalido, 0 en otro caso
pub fn strlen(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;
    let guard = get_storage_read_lock(storage)?;

    match guard.get(tokens[1].to_string()) {
        Ok(DatoRedis::BulkString(bulk_string)) => {
            Ok(DatoRedis::new_integer(bulk_string.largo() as i64))
        }
        Ok(_) => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),
        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e),
            _ => Ok(DatoRedis::new_integer(0)),
        },
    }
}

/// Obtiene un substring del valor de una clave
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, clave a
///   analizar, valor start de inicio del substring y valor stop de fin
///   del mismo. De ser algun valor menor a 0, se considera desde el final
///   del string (por ejemplo, -1 representa el ultimo caracter)
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Largo del valor de la clave en caso de exito, error de redis
///   en caso de tipo invalido, 0 en otro caso
pub fn substr(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 4, tokens.len())?;
    let guard = get_storage_read_lock(storage)?;

    match guard.get(tokens[1].to_string()) {
        Ok(DatoRedis::BulkString(bulk_string)) => {
            let len = bulk_string.largo() as isize;

            let inicio = parse_index(tokens[2].to_string())?;
            let fin = parse_index(tokens[3].to_string())?;
            let (inicio, fin) = adjust_indices(inicio, fin, len);

            if inicio > fin {
                return DatoRedis::new_bulk_string("".to_string());
            }

            let slice_cadena = bulk_string.contenido()[inicio..fin].to_string();
            DatoRedis::new_bulk_string(slice_cadena)
        }

        Ok(_) => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),

        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e), // propagás el MOVED
            _ => Ok(DatoRedis::new_bulk_string("".to_string())?), // clave no encontrada u otros errores
        },
    }
}

/// Incrementa el valor de una clave en 1 unidad, dada que la misma
/// pueda interpretarse como valor numerico
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave a
///   analizar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Valor incrementado en caso de exito, error de redis en otro caso
pub fn incr(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    modificar_valor(tokens, storage, |x| x + 1)
}

/// Decrementa el valor de una clave en 1 unidad, dada que la misma
/// pueda interpretarse como valor numerico
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave a
///   analizar
/// * `storage`: storage del nodo donde se encuentra la clave
///
/// # Retorna
/// - Valor decrementado en caso de exito, error de redis en otro caso
pub fn decr(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    modificar_valor(tokens, storage, |x| x - 1)
}

/// Aplica una operacion numerica sobre una clave, de ser posible
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y clave a
///   analizar
/// * `storage`: storage del nodo donde se encuentra la clave
/// * `operacion`: funcion a aplicar
///
/// # Retorna
/// - Valor modificado en caso de exito, error de redis en otro caso
fn modificar_valor(
    tokens: &[String],
    storage: &Arc<RwLock<Storage>>,
    operacion: impl Fn(i64) -> i64,
) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;
    let error = DatoRedis::new_simple_error(
        "ERR".to_string(),
        "value is not an integer or out of range".to_string(),
    );

    let mut guard = get_storage_write_lock(storage)?;

    match guard.get(tokens[1].to_string()) {
        Ok(DatoRedis::BulkString(valor_bstring)) => {
            let valor_actual = valor_bstring
                .contenido()
                .parse::<i64>()
                .map_err(|_| error.clone())?;
            let nuevo_valor = operacion(valor_actual);

            if let Ok(valor_a_guardar) = DatoRedis::new_bulk_string(nuevo_valor.to_string()) {
                let _ = guard.set(tokens[1].to_string(), valor_a_guardar);
                Ok(DatoRedis::new_integer(nuevo_valor))
            } else {
                Err(error)
            }
        }
        Ok(_) => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),
        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e),
            _ => {
                if let Ok(valor_cero) = DatoRedis::new_bulk_string("0".to_string()) {
                    let _ = guard.set(tokens[1].to_string(), valor_cero);
                    drop(guard);
                    modificar_valor(tokens, storage, operacion)
                } else {
                    Err(error)
                }
            }
        },
    }
}

/// Obtiene el valor numerico de un string, transformandolo en un
/// posible indice
///
/// # Parámetros
/// * `index_str`: String a transformar
///
/// # Retorna
/// - Valor indexable en caso de exito, null en otro caso
fn parse_index(index_str: String) -> Result<isize, DatoRedis> {
    index_str.parse().map_err(|_| DatoRedis::new_null())
}

/// Obtiene valores indexables para un string a partir de los
/// parametros inicio y fin
///
/// # Parámetros
/// * `inicio`: indice de inicio deseado
/// * `fin`: indice de fin deseado
/// * `len`: largo del string que se buscara indexar
///
/// # Retorna
/// - Tupla de los indices ajustados a valores positivos
fn adjust_indices(mut inicio: isize, mut fin: isize, len: isize) -> (usize, usize) {
    if inicio < 0 && fin >= 0 {
        return (0, 0);
    }

    if inicio < 0 {
        inicio += len
    }

    if fin < 0 {
        fin += len - fin;
    }

    let inicio = inicio.clamp(0, len) as usize;
    let fin = fin.clamp(0, len) as usize;

    (inicio, fin)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::comandos::comandos_string::*;
    use crate::constantes::CLAVE_ELIMINADA;

    const RANGE: Range<u16> = Range {
        start: 0,
        end: 16378,
    };

    #[test]
    fn test_set() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let result = set(&tokens, &storage).unwrap();
        if let DatoRedis::BulkString(bulk_string) = result {
            assert_eq!(bulk_string.contenido(), "OK");
            assert_eq!(bulk_string.largo(), 2);
        }
    }

    #[test]
    fn test_get() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));

        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["get", "key1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let value = get(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(bulk_string) = value {
            assert_eq!(bulk_string.contenido(), "value1");
            assert_eq!(bulk_string.largo(), 6);
        }
    }

    #[test]
    fn test_del() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));

        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["del", "key1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = del(&tokens.clone(), &storage).unwrap();

        if let DatoRedis::Integer(valor) = res {
            assert_eq!(valor.valor(), CLAVE_ELIMINADA as i64);
        }

        let value = get(&tokens, &storage);
        assert!(value.is_err());
    }

    #[test]
    fn test_getdel() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));

        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["getdel", "key1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = getdel(&tokens.clone(), &storage).unwrap();

        if let DatoRedis::BulkString(bulk_string) = res {
            assert_eq!(bulk_string.contenido(), "value1".to_string());
        }

        let value = get(&tokens, &storage);
        assert!(value.is_err());
    }

    #[test]
    fn test_append() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);
        let tokens: Vec<String> = vec!["append", "key1", "value2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = append(&tokens.clone(), &storage).unwrap();
        if let DatoRedis::Integer(integer) = res {
            assert_eq!(integer.valor(), 12); // 6 + 6
        }

        let valor = get(&tokens, &storage).unwrap();
        if let DatoRedis::BulkString(bulk_string) = valor {
            assert_eq!(bulk_string.contenido(), "value1value2".to_string());
        }
    }

    #[test]
    fn test_strlen() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens, &storage);

        let res = strlen(&["strlen".to_string(), "key1".to_string()], &storage).unwrap();
        if let DatoRedis::Integer(long) = res {
            assert_eq!(long.valor(), 6);
        }
    }

    #[test]
    fn test_substr_in_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "0", "3"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "val");
        }
    }

    #[test]
    fn test_substr_off_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "0", "7"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "value1");
        }
    }

    #[test]
    fn test_substr_neg_index_init() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "-1", "3"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "");
        }
    }

    #[test]
    fn test_substr_neg_index_end() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "0", "-1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "value1");
        }
    }

    #[test]
    fn test_substr_index() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "0", "2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "va");
        }
    }

    #[test]
    fn test_substr_ini_neg_and_fin_neg_index() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "value1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["substr", "key1", "-2", "-1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = substr(&tokens, &storage).unwrap();

        if let DatoRedis::BulkString(valor) = res {
            assert_eq!(valor.contenido(), "e1");
        }
    }

    #[test]
    fn test_incr() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["incr", "key1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = incr(&tokens, &storage);
        assert!(res.is_ok());

        let valor = get(&["get".to_string(), "key1".to_string()], &storage).unwrap();
        if let DatoRedis::BulkString(valor_bstring) = valor {
            assert_eq!(valor_bstring.contenido(), "2");
        }
    }

    #[test]
    fn test_decr() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens_a_guardar: Vec<String> = vec!["set", "key1", "1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let _ = set(&tokens_a_guardar, &storage);

        let tokens: Vec<String> = vec!["incr", "key1"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let res = decr(&tokens, &storage);
        assert!(res.is_ok());

        let valor = get(&["get".to_string(), "key1".to_string()], &storage).unwrap();
        if let DatoRedis::BulkString(valor_bstring) = valor {
            assert_eq!(valor_bstring.contenido(), "0");
        }
    }
}
