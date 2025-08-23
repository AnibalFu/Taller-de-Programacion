//! Este modulo contiene la implementacion de los comandos de redis
//! basicos para listas
use std::{
    cmp::Ordering,
    sync::{Arc, RwLock},
};

use crate::comandos::utils::assert_correct_arguments_quantity;
use crate::storage::Storage;
use redis_client::tipos_datos::{arrays::Arrays, bulk_string::BulkString, traits::DatoRedis};

use super::utils::{get_storage_read_lock, get_storage_write_lock};

/// Inserta un elemento en una lista antes o despues
/// de un elemento de referencia (pivote)
///
/// # Parámetros
/// * tokens: lista conteniendo nombre del comandos, nombre de la lista donde insertar, direccion a insertar, elemento a insertar y
///   elemento pivote
/// * storage: storage del nodo donde se encuentra la lista donde insertar
///
/// # Retorna
/// - en caso de insercion exitosa, el nuevo largo del arreglo, error simple de redis en otros casos
pub fn linsert(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 5, tokens.len())?;

    let posicion = tokens[2].to_uppercase();
    if posicion != "BEFORE" && posicion != "AFTER" {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "Syntax error: expected BEFORE or AFTER".to_string(),
        ));
    }

    let pivot = BulkString::new(tokens[3].to_string())?;
    let nuevo_elemento = DatoRedis::new_bulk_string(tokens[4].to_string())?;

    let mut guard = get_storage_write_lock(storage)?;

    let valor = guard.get_mutable(tokens[1].to_string())?;

    match valor {
        DatoRedis::Arrays(arrays) => {
            procesar_insercion(arrays, posicion.as_str(), pivot, nuevo_elemento)
        }
        _ => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),
    }
}

/// Inserta los elementos recibidos al inicio de la lista
///
/// # Parámetros
/// * tokens: lista conteniendo nombre del comando, nombre de la lista donde insertar
///   y elementos a insertar
/// * storage: storage del nodo donde se encuentra la lista donde
///   insertar
///
/// # Retorna
/// - en caso de insercion exitosa, el nuevo largo del arreglo,
///   error simple de redis en otros casos
pub fn lpush(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    push_elements(tokens, storage, |arrays, element| {
        arrays.insert(0, element).map_err(|_| DatoRedis::new_null())
    })
}

/// Inserta los elementos recibidos al final de la lista
///
/// # Parámetros
/// * tokens: lista conteniendo nombre del comando, lista donde insertar
///   y elementos a insertar
/// * storage: storage del nodo donde se encuentra la lista donde
///   insertar
///
/// # Retorna
/// - en caso de insercion exitosa, el nuevo largo del arreglo,
///   error simple de redis en otros casos
pub fn rpush(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    push_elements(tokens, storage, |arrays, element| {
        arrays.append(element);
        Ok(())
    })
}

/// Devuelve el largo de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando y el nombre de la lista
///   cuyo largo se quiere calcular
/// * `storage`: storage del nodo donde se encuentra la lista
///
/// # Retorna
/// - en caso de calculo exitoso, el largo del arreglo,
///   error simple de redis en otros casos
pub fn llen(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;

    let guard = get_storage_read_lock(storage)?;

    match guard.get(tokens[1].to_string()) {
        Ok(DatoRedis::Arrays(list)) => Ok(DatoRedis::new_integer(list.len() as i64)),
        Ok(_) => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),
        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e),
            _ => Ok(DatoRedis::new_integer(0)), // Clave no existe
        },
    }
}

/// Elimina los primeros elementos de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista
///   a reducir y la cantidad de elementos a borrar
/// * `storage`: storage del nodo donde se encuentra la lista donde
///   eliminar
///
/// # Retorna
/// - en caso de calculo exitoso, el nuevo largo del arreglo,
///   error simple de redis en otros casos
pub fn lpop(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    pop_elements_with_action(tokens, storage, |list: &mut Arrays| {
        if list.is_empty() {
            return Err(DatoRedis::new_null());
        }
        let removed = list.remove(0).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "No se pudo remover el elemento".to_string(),
            )
        })?;
        Ok(Some(removed))
    })
}

/// Elimina los primeros últimos de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista
///   a reducir y la cantidad de elementos a borrar
/// * `storage`: storage del nodo donde se encuentra la lista donde
///   eliminar
///
/// # Retorna
/// - en caso de calculo exitoso, el nuevo largo del arreglo,
///   error simple de redis en otros casos
pub fn rpop(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    pop_elements_with_action(tokens, storage, |list: &mut Arrays| {
        if let Some(e) = list.get(list.len() - 1) {
            let _ = list
                .remove(list.len() - 1)
                .map_err(|_| DatoRedis::new_null());
            Ok(Some(e))
        } else {
            Ok(None)
        }
    })
}

/// Muestra el rango de elementos indicados de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista
///   a mostrar, indice de inicio e indice de fin
/// * `storage`: storage del nodo donde se encuentra la lista
///
/// # Retorna
/// - en caso de exito, los elementos, en el rango indicado, arreglo
///   vacio en otros casos (por ejemplo, indices invalidos)
pub fn lrange(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 4, tokens.len())?;

    let key = tokens[1].to_string();
    let start = parse_index(&tokens[2])?;
    let stop = parse_index(&tokens[3])?;

    let guard = get_storage_read_lock(storage)?;

    match guard.get(key.clone()) {
        Ok(DatoRedis::Arrays(arrays)) => obtener_rango(arrays, start, stop),
        Ok(_) => Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        )),
        Err(e) => match e {
            DatoRedis::MovedError(_) => Err(e),
            _ => Ok(DatoRedis::new_array()), // clave no existe, Redis devuelve array vacío
        },
    }
}

/// Inserta un elemento en una posicion determinada
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista,
///   un indice y un elemento
/// * `storage`: storage del nodo donde se encuentra la lista donde
///   insertar
///
/// # Retorna
/// - en caso de insercion exitosa, OK,
///   error simple de redis en otros casos
pub fn lset(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 4, tokens.len())?;

    let key = tokens[1].to_string();
    let index = parse_index(&tokens[2])?;
    let nuevo_elemento = DatoRedis::new_bulk_string(tokens[3].to_string())?;

    let mut guard = get_storage_write_lock(storage)?;

    let lista = match guard.get_mutable(key.clone()) {
        Ok(valor) => valor,
        Err(e) => match e {
            DatoRedis::MovedError(_) => return Err(e),
            _ => {
                return Err(DatoRedis::new_simple_error(
                    "ERR".to_string(),
                    "no such key".to_string(),
                ));
            }
        },
    };

    if let DatoRedis::Arrays(arrays) = &mut *lista {
        setear_elemento(arrays, index, nuevo_elemento)
    } else {
        Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        ))
    }
}

/// Reduce una lista a un rango determinado
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista,
///   un indice de inicio y un indice de fin
/// * `storage`: storage del nodo donde se encuentra la lista
///
/// # Retorna
/// - en caso de modificacion exitosa, OK, lista vacia en caso de
///   error (por ejemplo, indices invalidos)
pub fn ltrim(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 4, tokens.len())?;

    let key = tokens[1].to_string();
    let start = parse_index(&tokens[2])?;
    let end = parse_index(&tokens[3])?;

    let mut guard = get_storage_write_lock(storage)?;

    let lista = match guard.get_mutable(key.clone()) {
        Ok(valor) => valor,
        Err(e) => match e {
            DatoRedis::MovedError(_) => return Err(e),
            _ => {
                // Si no existe la clave, Redis igual responde "OK" sin modificar nada.
                return DatoRedis::new_simple_string("OK".to_string());
            }
        },
    };

    if let DatoRedis::Arrays(arrays) = &mut *lista {
        recortar_lista(arrays, start, end)?;
        Ok(DatoRedis::new_simple_string("OK".to_string())?)
    } else {
        Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        ))
    }
}

/// Retorna el elemento de una posicion determinada de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista,
///   y un indice
/// * `storage`: storage del nodo donde se encuentra la lista donde
///   indexar
///
/// # Retorna
/// - en caso de insercion exitosa, el elemento, nil para indices
///   invalidos y error simple en caso de que no haya una lista en la key
///   indicada
pub fn lindex(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;

    let index = parse_index(&tokens[2])?;
    let guard = get_storage_read_lock(storage)?;

    let lista = guard.get(tokens[1].to_string())?;

    if let DatoRedis::Arrays(arrays) = lista {
        obtener_elemento(&arrays, index)
    } else {
        Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        ))
    }
}

/// Elimina una cantidad de ocurrencias de un elemento de una lista
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de la lista,
///   parametro count y un elemento
/// * `storage`: storage del nodo donde se encuentra la lista
///
/// De ser count > 0, se eliminan las primeras count ocurrencias empezando
/// desde el inicio de la lista. De ser count < 0, se inicia en el final de
/// la lista y de ser count = 0, se borran todas las apariciones del elemento
///
/// # Retorna
/// - en caso de eliminacion exitosa, la cantidad de elementos
///   removidos y error simple en caso de que no haya una lista en la key
///   indicada
pub fn lrem(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 4, tokens.len())?;

    let key = tokens[1].to_string();
    let count = parse_index(&tokens[2])?;
    let elemento = DatoRedis::new_bulk_string(tokens[3].to_string())?;

    let mut guard = get_storage_write_lock(storage)?;

    let valor = match guard.get_mutable(key.clone()) {
        Ok(v) => v,
        Err(DatoRedis::MovedError(e)) => return Err(DatoRedis::MovedError(e)),
        Err(_) => return Ok(DatoRedis::new_integer(0)),
    };

    if let DatoRedis::Arrays(arrays) = valor {
        let removed = remover_elementos(arrays, &elemento, count);
        Ok(DatoRedis::new_integer(removed))
    } else {
        Err(DatoRedis::new_simple_error(
            "WRONGTYPE".to_string(),
            "Operation against a key holding the wrong kind of value".to_string(),
        ))
    }
}

/// Mueve un elemento de una lista a otra
///
/// # Parámetros
/// * `tokens`: lista conteniendo nombre del comando, el nombre de
///   dos listas, un parametro from y un paramentro wherefrom y un parametro
///   whereto. wherefrom determina si se remueve el elemento
///   del inicio o del final de la primera lista, whereto determina si el
///   elemento se inserta al inicio o al final de la segunda lista
/// * `storage`: storage del nodo donde se encuentra la lista
///
/// De ser count > 0, se eliminan las primeras count ocurrencias empezando
/// desde el inicio de la lista. De ser count < 0, se inicia en el final de
/// la lista y de ser count = 0, se borran todas las apariciones del elemento
///
/// # Retorna
/// - en caso de movimiento exitosa, el elemento y error simple en
///   otros casos
pub fn lmove(tokens: &[String], storage: &Arc<RwLock<Storage>>) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].clone(), 5, tokens.len())?;

    let source_key = tokens[1].clone();
    let dest_key = tokens[2].clone();
    let wherefrom = tokens[3].to_uppercase();
    let whereto = tokens[4].to_uppercase();

    let mut guard = get_storage_write_lock(storage)?;

    // Obtener lista origen
    let array_origen = match guard.get_mutable(source_key.clone()) {
        Ok(DatoRedis::Arrays(arr)) => arr,
        Ok(_) => {
            return Err(DatoRedis::new_simple_error(
                "WRONGTYPE".to_string(),
                "Operation against a key holding the wrong kind of value".to_string(),
            ));
        }
        Err(e @ DatoRedis::MovedError(_)) => return Err(e),
        Err(_) => {
            return Err(DatoRedis::new_null());
        }
    };

    if array_origen.is_empty() {
        return Err(DatoRedis::new_null());
    }

    let elemento = extraer_elemento(array_origen, &wherefrom)?;

    // Obtener o crear lista destino
    let array_destino = match guard.get_mutable(dest_key.clone()) {
        Ok(DatoRedis::Arrays(arr)) => arr,
        Ok(_) => {
            return Err(DatoRedis::new_simple_error(
                "WRONGTYPE".to_string(),
                "Operation against a key holding the wrong kind of value".to_string(),
            ));
        }
        Err(e @ DatoRedis::MovedError(_)) => return Err(e),
        Err(_) => {
            guard.set(dest_key.clone(), DatoRedis::new_array())?;
            match guard.get_mutable(dest_key.clone()) {
                Ok(DatoRedis::Arrays(arr)) => arr,
                Ok(_) => {
                    return Err(DatoRedis::new_simple_error(
                        "WRONGTYPE".to_string(),
                        "Operation against a key holding the wrong kind of value".to_string(),
                    ));
                }
                Err(e @ DatoRedis::MovedError(_)) => return Err(e),
                Err(_) => {
                    return Err(DatoRedis::new_simple_error(
                        "ERR".to_string(),
                        "No se pudo acceder a la lista recién creada".to_string(),
                    ));
                }
            }
        }
    };

    insertar_elemento(array_destino, &whereto, elemento.clone())?;
    Ok(elemento)
}

/// Extrae un elemento del inicio o el final de un arreglo
///
/// # Parámetros
/// * `arrays`: array de donde se remueve el elemento
/// * `wherefrom`: determina si se remueve el elemento del inicio
///   (LEFT) o del final (RIGHT)
///
/// # Retorna
/// - en caso de movimiento exitosa, el elemento y error simple en
///   otros casos
fn extraer_elemento(arrays: &mut Arrays, wherefrom: &str) -> Result<DatoRedis, DatoRedis> {
    let index = match wherefrom {
        "LEFT" => 0,
        "RIGHT" => arrays.len() - 1,
        _ => {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "syntax error".to_string(),
            ));
        }
    };

    let elemento = arrays.get(index).ok_or_else(DatoRedis::new_null)?;

    match wherefrom {
        "LEFT" => {
            arrays.remove(0)?;
        }
        "RIGHT" => {
            arrays.pop()?;
        }
        _ => unreachable!(),
    };

    Ok(elemento)
}

/// Inserta un elemento al inicio o el final de un arreglo
///
/// # Parámetros
/// * `arrays`: array donde se inserta el elemento
/// * `whereto`: determina si se inserrta el elemento al inicio
///   (LEFT) o al final (RIGHT)
/// * `elemento`: elemento a insertar
///
/// # Retorna
/// - unit en caso de exito, error simple en otros casos
fn insertar_elemento(
    arrays: &mut Arrays,
    whereto: &str,
    elemento: DatoRedis,
) -> Result<(), DatoRedis> {
    match whereto {
        "LEFT" => arrays.insert(0, elemento),
        "RIGHT" => {
            arrays.append(elemento);
            Ok(())
        }
        _ => Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "sintax error".to_string(),
        )),
    }?;
    Ok(())
}

/// Inserta un elemento en antes o despues de un pivote
/// en un arreglo
///
/// # Parámetros
/// * `arrays`: array donde se inserta el elemento
/// * `posicion`: BEFORE/AFTER relativa al elemento pivote
/// * `pivot`: un elemento del arreglo
/// * `nuevo_elemento`: elemento a insertar
///
/// # Retorna
/// - nuevo largo del arreglo en caso de exito, error simple en
///   otros casos
fn procesar_insercion(
    arrays: &mut Arrays,
    posicion: &str,
    pivot: BulkString,
    nuevo_elemento: DatoRedis,
) -> Result<DatoRedis, DatoRedis> {
    if arrays.is_empty() {
        return Ok(DatoRedis::new_integer(0));
    }

    if let Some(index) = encontrar_pivot(arrays, &pivot) {
        let insert_index = calcular_indice_insercion(posicion, index)?;
        arrays.insert(insert_index, nuevo_elemento)?;
        Ok(DatoRedis::new_integer(arrays.len() as i64))
    } else {
        Ok(DatoRedis::new_integer(-1))
    }
}

/// Encuentra la posicion de un elemento en un arreglo
///
/// # Parámetros
/// * `arrays`: array donde se busca el elemento
/// * `pivot`: un elemento del arreglo
///
/// # Retorna
/// - Option de la posicion del pivote en el arreglo, de existir
///   en el mismo
fn encontrar_pivot(arrays: &Arrays, pivot: &BulkString) -> Option<usize> {
    arrays.iter().position(|dato| {
        if let DatoRedis::BulkString(bulk) = dato {
            bulk == pivot
        } else {
            false
        }
    })
}

/// Transforma un parametro BEFORE / AFTER en un indice para una
/// posicion del pivote en un arreglo
///
/// # Parámetros
/// * `posicion`: BEFORE / AFTER
/// * `index`: posicion del pivote
///
/// # Retorna
/// - Posicion correspondiente al parametro pasado en el arreglo,
///   error simple de redis en otro caso
fn calcular_indice_insercion(posicion: &str, index: usize) -> Result<usize, DatoRedis> {
    match posicion.to_uppercase().as_str() {
        "BEFORE" => Ok(index),
        "AFTER" => Ok(index + 1),
        _ => Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "wrong number of arguments for 'linsert' command".to_string(),
        )),
    }
}

/// Transforma un token (&str) en un indice del arreglo (i32)
///
/// # Parámetros
/// * `token`: string a transformar
///
/// # Retorna
/// - Posicion correspondiente al parametro pasado en el arreglo,
///   error simple de redis en otro caso
fn parse_index(token: &str) -> Result<i32, DatoRedis> {
    token.parse::<i32>().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR".to_string(),
            "value is not an integer or out of range".to_string(),
        )
    })
}

/// Obtiene un rango de un arreglo
///
/// # Parámetros
/// * `arrays`: arreglo del cual obtener un rango
/// * `start`: posicion de inicio (inclusive, inicia en 0)
/// * `stop`: posicion de fin (inclusive, inicia en 0)
///
/// Posiciones menores a 0 se consideran desde el final del arreglo
/// (por ejemplo, posicion -1 referencia al ultimo elemento)
///
/// # Retorna
/// - Rango del arreglo entre las posiciones start y stop, error
///   simple de redis en otro caso
fn obtener_rango(arrays: Arrays, start: i32, stop: i32) -> Result<DatoRedis, DatoRedis> {
    let len = arrays.len() as i32;

    if len == 0 {
        return Ok(DatoRedis::new_array());
    }

    let mut real_start = ajustar_indice(start, len);
    let mut real_stop = ajustar_indice(stop, len);

    real_start = real_start.clamp(0, len);
    real_stop = real_stop.clamp(0, len - 1);

    if real_start > real_stop {
        Ok(DatoRedis::new_array())
    } else {
        let rango = arrays.range(real_start as usize, real_stop as usize);
        Ok(DatoRedis::new_array_con_contenido(rango))
    }
}

/// Obtiene un indice valido para indexar a partir de un entero
///
/// # Parámetros
/// * `indice`: i32 a convertir
/// * `len`: largo del arreglo a indexar
///
/// Posiciones menores a 0 se consideran desde el final del arreglo
/// (por ejemplo, posicion -1 referencia al ultimo elemento)
///
/// # Retorna
/// - Valor positivo para indexar el arreglo
fn ajustar_indice(indice: i32, len: i32) -> i32 {
    if indice < 0 { len + indice } else { indice }
}

/// Obtiene el parametro count de la funcion pop_with_action
///
/// # Parámetros
/// * `tokens`: arreglo cuyo tercer elemento es el valor
///   a transformar. De no tener largo 3 o mayor, se deduce count = 1
///
/// # Retorna
/// - el valor del parametro buscado como usize en caso de exito,
///   error simple de redis en otros casos
fn parse_count(tokens: &[String]) -> Result<usize, DatoRedis> {
    if tokens.len() > 2 {
        tokens[2].parse::<usize>().map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR".to_string(),
                "value is not an integer or out of range".to_string(),
            )
        })
    } else {
        Ok(1)
    }
}

/// Inserta una lista de elementos en un arreglo
///
/// # Parámetros
/// * `tokens`: lista incluyendo nombre del comando, nombre de la lista
///   donde insertar y secuencia de elementos a insertar
/// * `storage`: storage del nodo donde se encuentra la lista indicada
/// * `push`: funcion que inserta un elemento a un arreglo redis
///
/// # Retorna
/// - El nuevo largo del arreglo en caso de exito, error de redis en
///   otro caso
fn push_elements(
    tokens: &[String],
    storage: &Arc<RwLock<Storage>>,
    push: impl Fn(&mut Arrays, DatoRedis) -> Result<(), DatoRedis>,
) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].to_string(), 3, tokens.len())?;

    let key = &tokens[1];
    let mut guard = get_storage_write_lock(storage)?;

    let lista = match guard.get_mutable(key.to_string()) {
        Ok(valor) => valor,
        Err(_) => {
            guard.set(key.to_string(), DatoRedis::new_array())?;
            guard.get_mutable(key.to_string())?
        }
    };

    if let DatoRedis::Arrays(arrays) = &mut *lista {
        for token in tokens.iter().skip(2) {
            let nuevo_elemento = DatoRedis::new_bulk_string(token.to_string()).map_err(|_| {
                DatoRedis::new_simple_error("ERR".to_string(), "invalid bulk string".to_string())
            })?;
            push(arrays, nuevo_elemento)?;
        }
        return Ok(DatoRedis::new_integer(arrays.len() as i64));
    }

    Err(DatoRedis::new_simple_error(
        "WRONGTYPE".to_string(),
        "Operation against a key holding the wrong kind of value".to_string(),
    ))
}

/// Inserta una lista de elementos en un arreglo
///
/// # Parámetros
/// * `tokens`: lista incluyendo nombre del comando, nombre de la lista
///   donde insertar y secuencia de elementos a insertar
/// * `storage`: storage del nodo donde se encuentra la lista indicada
/// * `push`: funcion que inserta un elemento a un arreglo redis
///
/// # Retorna
/// - El nuevo largo del arreglo en caso de exito, error de redis en
///   otro caso
fn pop_elements_with_action(
    tokens: &[String],
    storage: &Arc<RwLock<Storage>>,
    action: impl Fn(&mut Arrays) -> Result<Option<DatoRedis>, DatoRedis>,
) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity(tokens[0].clone(), 2, tokens.len())?;
    let mut guard = get_storage_write_lock(storage)?;
    let lista = guard.get_mutable(tokens[1].clone())?;
    let count = parse_count(tokens)?;
    let elements = pop_elements(lista, count, action)?;
    Ok(DatoRedis::new_array_con_contenido(elements))
}

/// Devuelve los elementos de un arreglo que cumplen con la funcion de
/// evaluacion
///
/// # Parámetros
/// * `lista`: arreglo de donde eliminar los elementos
/// * `count`: cantidad de elementos donde aplicar la funcion
/// * `action`: funcion a evaluar con los elementos a eliminar
///
/// # Retorna
/// - Nuevo array con los elementos restantes en caso de exito, error
///   simple de redis en otros casos
fn pop_elements(
    lista: &mut DatoRedis,
    count: usize,
    action: impl Fn(&mut Arrays) -> Result<Option<DatoRedis>, DatoRedis>,
) -> Result<Arrays, DatoRedis> {
    if let DatoRedis::Arrays(list) = lista {
        if list.is_empty() {
            return Err(DatoRedis::new_null());
        }

        let mut elements_to_return = Arrays::new();
        let limit = count.min(list.len());

        for _ in 0..limit {
            let element = action(list)?;

            if let Some(e) = element {
                elements_to_return.append(e);
            } else {
                return Err(DatoRedis::new_null());
            }
        }

        Ok(elements_to_return)
    } else {
        Err(DatoRedis::new_null())
    }
}

/// Devuelve los elementos de un arreglo que cumplen con la funcion de
/// evaluacion
///
/// # Parámetros
/// * `lista`: arreglo de donde eliminar los elementos
/// * `count`: cantidad de elementos donde aplicar la funcion
/// * `action`: funcion a evaluar con los elementos a eliminar
///
/// # Retorna
/// - Nuevo array con los elementos restantes en caso de exito, error
///   simple de redis en otros casos
fn setear_elemento(
    arrays: &mut Arrays,
    index: i32,
    nuevo_elemento: DatoRedis,
) -> Result<DatoRedis, DatoRedis> {
    let len = arrays.len() as i32;
    let real_index = ajustar_indice(index, len);

    if real_index < 0 || real_index >= len {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "index out of range".to_string(),
        ));
    }

    let _ = arrays.set(real_index as usize, nuevo_elemento);
    DatoRedis::new_simple_string("OK".to_string())
}

/// Devuelve los elementos de un arreglo que se encuentran entre los
/// parametros start y stop
///
/// # Parámetros
/// * `arrays`: arreglo de donde recortar los elementos
/// * `start`: indice de inicio del recorte
/// * `end`: indice de fin del recorte
///
/// # Retorna
/// - Nuevo array con los elementos restantes en caso de exito, error
///   simple de redis en otros casos
fn recortar_lista(arrays: &mut Arrays, start: i32, end: i32) -> Result<DatoRedis, DatoRedis> {
    let len = arrays.len() as i32;
    if len == 0 {
        return Ok(DatoRedis::new_null());
    }

    let real_start = ajustar_indice(start, len).clamp(0, len);
    let real_end = ajustar_indice(end, len).clamp(0, len - 1);

    if real_start > real_end {
        arrays.clear();
    } else {
        let mut start_usize = real_start as usize;
        let mut end_usize = real_end as usize;

        if end_usize >= arrays.len() {
            end_usize = arrays.len() - 1;
        }

        while start_usize > 0 {
            let _ = arrays.remove(0);
            start_usize -= 1;
        }

        while end_usize < arrays.len() - 1 {
            let _ = arrays.remove(arrays.len() - 1);
        }
    }

    DatoRedis::new_simple_string("OK".to_string())
}

/// Devuelve el elemento de un arreglo que se encuentran en una
/// determinada posicion
///
/// # Parámetros
/// * `arrays`: arreglo de donde obtener el elemento
/// * `index`: indice del elemento
///
/// # Retorna
/// - Elemento indexado de existir, error con elemento nulo en otro
///   caso
fn obtener_elemento(arrays: &Arrays, index: i32) -> Result<DatoRedis, DatoRedis> {
    let len = arrays.len() as i32;
    let idx = ajustar_indice(index, len);

    if idx >= 0 && idx < len {
        if let Some(elemento) = arrays.get(idx as usize) {
            Ok(elemento)
        } else {
            Err(DatoRedis::new_null())
        }
    } else {
        Err(DatoRedis::new_null())
    }
}

// no la miren esta fea
/// Elimina las primeras count apariciones del parametro elementos
/// en el parametro arrays
///
/// # Parámetros
/// * `arrays`: arreglo de donde eliminar el elemento
/// * `elemento`: elemento a eliminar
/// * `count`: cantidad de apariciones a borrar
///
/// # Retorna
/// - Cantidad de elementos eliminados
fn remover_elementos(arrays: &mut Arrays, elemento: &DatoRedis, mut count: i32) -> i64 {
    let mut removed = 0;

    match count.cmp(&0) {
        Ordering::Less => {
            let mut i = arrays.len() as i32 - 1;
            while count < 0 && i >= 0 {
                if let Some(e) = arrays.get(i as usize) {
                    if e == *elemento {
                        arrays.remove(i as usize).ok();
                        count += 1;
                        removed += 1;
                    }
                }
                i -= 1;
            }
        }
        Ordering::Greater => {
            let mut i = 0;
            while i < arrays.len() {
                if let Some(e) = arrays.get(i) {
                    if e == *elemento {
                        arrays.remove(i).ok();
                        count -= 1;
                        removed += 1;
                        if count == 0 {
                            break;
                        }
                        continue;
                    }
                }
                i += 1;
            }
        }
        Ordering::Equal => {
            let mut i = 0;
            while i < arrays.len() {
                if let Some(e) = arrays.get(i) {
                    if e == *elemento {
                        arrays.remove(i).ok();
                        removed += 1;
                        continue;
                    }
                }
                i += 1;
            }
        }
    }

    removed
}

#[cfg(test)]
mod tests {
    use crate::comandos::comandos_list::*;
    use redis_client::tipos_datos::integer::Integer;
    use redis_client::tipos_datos::simple_string::SimpleString;
    use std::ops::Range;

    const RANGE: Range<u16> = Range {
        start: 0,
        end: 16378,
    };

    fn obtener_tokens(vector: Vec<&str>) -> Vec<String> {
        vector.into_iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_llen() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        lpush(&tokens, &storage).unwrap();
        if let DatoRedis::Integer(numero) = llen(&tokens, &storage).unwrap() {
            assert_eq!(numero, Integer::new(4));
        }
    }

    #[test]
    fn test_lpush_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        if let DatoRedis::Integer(numero) = lpush(&tokens, &storage).unwrap() {
            assert_eq!(numero, Integer::new(1));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_rpush_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["rpush", "key", "element2"]);

        if let DatoRedis::Integer(numero) = rpush(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 2);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
                }
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("element2".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lpush_elements() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        if let DatoRedis::Integer(numero) = lpush(&tokens, &storage).unwrap() {
            assert_eq!(numero, Integer::new(4));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 4);
                let esperados = ["element4", "element3", "element2", "element"];
                for (i, esperado) in esperados.iter().enumerate() {
                    if let DatoRedis::BulkString(bulk) = array.get(i).unwrap() {
                        assert_eq!(bulk, BulkString::new(esperado.to_string()).unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn test_rpush_elements() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens1: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens1, &storage).unwrap();
        let tokens2: Vec<String> =
            obtener_tokens(vec!["rpush", "key", "element2", "element3", "element4"]);

        if let DatoRedis::Integer(numero) = rpush(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(4));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 4);
                let esperados = ["element", "element2", "element3", "element4"];
                for (i, esperado) in esperados.iter().enumerate() {
                    if let DatoRedis::BulkString(bulk) = array.get(i).unwrap() {
                        assert_eq!(bulk, BulkString::new(esperado.to_string()).unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn test_lset() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();

        let tokens2: Vec<String> = obtener_tokens(vec!["lset", "key", "0", "new_element"]);

        if let DatoRedis::SimpleString(check) = lset(&tokens2, &storage).unwrap() {
            assert_eq!(check, SimpleString::new("OK".to_string()).unwrap());
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("new_element".to_string()).unwrap());
                }
                assert_eq!(array.len(), 1);
            }
        }
    }

    #[test]
    fn test_lindex() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lindex", "key", "0"]);

        if let DatoRedis::BulkString(bulk) = lindex(&tokens2, &storage).unwrap() {
            assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
        }
    }

    #[test]
    fn test_lindex_out_of_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lindex", "key", "1"]);
        assert!(lindex(&tokens2, &storage).is_err());
    }

    #[test]
    fn test_lindex_negative_index() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lindex", "key", "-1"]);

        if let DatoRedis::BulkString(bulk) = lindex(&tokens2, &storage).unwrap() {
            assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
        }
    }

    #[test]
    fn test_linsert_with_after() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> =
            obtener_tokens(vec!["linsert", "key", "AFTER", "element", "new_element"]);

        if let DatoRedis::Integer(numero) = linsert(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("new_element".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_linsert_with_before() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> =
            obtener_tokens(vec!["linsert", "key", "BEFORE", "element", "new_element"]);

        if let DatoRedis::Integer(numero) = linsert(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("new_element".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_linsert_with_nonexistent_pivot() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec![
            "linsert",
            "key",
            "AFTER",
            "nonexistent",
            "new_element",
        ]);

        if let DatoRedis::Integer(numero) = linsert(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(-1));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 1);
            }
        }
    }

    #[test]
    fn test_lpop_with_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lpop", "key"]);

        if let DatoRedis::Arrays(array) = lpop(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 1);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
            }
        }

        let guard = storage.read().unwrap();
        if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
            assert_eq!(array.len(), 0);
        }
    }

    #[test]
    fn test_lpop_with_elements() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lpop", "key", "2"]);

        if let DatoRedis::Arrays(array) = lpop(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 2);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element4".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("element3".to_string()).unwrap());
            }
        }
        let guard = storage.read().unwrap();
        if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
            assert_eq!(array.len(), 2);
        }
    }

    #[test]
    fn test_lrange_index_in_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrange", "key", "0", "3"]);

        if let DatoRedis::Arrays(array) = lrange(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 4);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element4".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("element3".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(2).unwrap() {
                assert_eq!(bulk, BulkString::new("element2".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(3).unwrap() {
                assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
            }
        }
    }

    #[test]
    fn test_lrange_neg_index_end() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrange", "key", "0", "-1"]);

        if let DatoRedis::Arrays(array) = lrange(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 4);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element4".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("element3".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(2).unwrap() {
                assert_eq!(bulk, BulkString::new("element2".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(3).unwrap() {
                assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
            }
        }
    }

    #[test]
    fn test_lrange_neg_index_star() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrange", "key", "-3", "2"]);

        if let DatoRedis::Arrays(array) = lrange(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 3);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("one".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("two".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(2).unwrap() {
                assert_eq!(bulk, BulkString::new("three".to_string()).unwrap());
            }
        }
    }

    #[test]
    fn test_lrange_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrange", "key", "0", "0"]);

        if let DatoRedis::Arrays(array) = lrange(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 1);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("one".to_string()).unwrap());
            }
        }
    }

    #[test]
    fn test_lrange_off_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrange", "key", "5", "100"]);

        if let DatoRedis::Arrays(array) = lrange(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 0);
        }
    }

    #[test]
    fn test_linsert_with_empty_list() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens: Vec<String> = obtener_tokens(vec!["lpop", "key"]);
        lpop(&tokens, &storage).unwrap();

        let tokens2: Vec<String> = obtener_tokens(vec![
            "linsert",
            "key",
            "AFTER",
            "nonexistent",
            "new_element",
        ]);

        if let DatoRedis::Integer(numero) = linsert(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(0));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 0);
            }
        }
    }

    #[test]
    fn test_ltrim_index_in_range() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["ltrim", "key", "0", "1"]);

        if let DatoRedis::SimpleString(ok) = ltrim(&tokens2, &storage).unwrap() {
            assert_eq!(ok, SimpleString::new("OK".to_string()).unwrap());
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 2);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("one".to_string()).unwrap());
                }
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("two".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_ltrim_neg_index_end() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["ltrim", "key", "1", "-1"]);

        if let DatoRedis::SimpleString(ok) = ltrim(&tokens2, &storage).unwrap() {
            assert_eq!(ok, SimpleString::new("OK".to_string()).unwrap());
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 2);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("two".to_string()).unwrap());
                }
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("three".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_ltrim_neg_index_start() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["ltrim", "key", "-1", "1"]);

        if let DatoRedis::SimpleString(ok) = ltrim(&tokens2, &storage).unwrap() {
            assert_eq!(ok, SimpleString::new("OK".to_string()).unwrap());
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 0);
            }
        }
    }

    #[test]
    fn test_rpop_with_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["lpush", "key", "element"]);
        lpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["rpop", "key"]);

        if let DatoRedis::Arrays(array) = rpop(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 1);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element".to_string()).unwrap());
            }
        }
        let guard = storage.read().unwrap();
        if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
            assert_eq!(array.len(), 0);
        }
    }

    #[test]
    fn test_rpop_with_elements() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec![
            "lpush", "key", "element", "element2", "element3", "element4",
        ]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["rpop", "key", "2"]);

        if let DatoRedis::Arrays(array) = rpop(&tokens2, &storage).unwrap() {
            assert_eq!(array.len(), 2);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("element4".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("element3".to_string()).unwrap());
            }
        }
        let guard = storage.read().unwrap();
        if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
            assert_eq!(array.len(), 2);
        }
    }

    #[test]
    fn test_lrem_neg_count() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key", "hello", "foo", "hello"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrem", "key", "-2", "hello"]);

        if let DatoRedis::Integer(numero) = lrem(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 1);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("foo".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lrem_neg_count_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key", "hello", "foo", "hello"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrem", "key", "-1", "hello"]);

        if let DatoRedis::Integer(numero) = lrem(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(1));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 2);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("hello".to_string()).unwrap());
                }
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("foo".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lrem_pos_count_one_element() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key", "hello", "foo", "hello"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrem", "key", "1", "hello"]);

        if let DatoRedis::Integer(numero) = lrem(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(1));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 2);
                if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                    assert_eq!(bulk, BulkString::new("hello".to_string()).unwrap());
                }
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("foo".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lrem_pos_count() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key", "hello", "foo", "hello"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrem", "key", "2", "hello"]);

        if let DatoRedis::Integer(numero) = lrem(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 1);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("foo".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lrem_remove_all() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key", "hello", "foo", "hello"]);
        rpush(&tokens, &storage).unwrap();
        let tokens2: Vec<String> = obtener_tokens(vec!["lrem", "key", "0", "hello"]);

        if let DatoRedis::Integer(numero) = lrem(&tokens2, &storage).unwrap() {
            assert_eq!(numero, Integer::new(2));
            let guard = storage.read().unwrap();
            if let DatoRedis::Arrays(array) = guard.get("key".to_string()).unwrap() {
                assert_eq!(array.len(), 1);
                if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                    assert_eq!(bulk, BulkString::new("foo".to_string()).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_lmove() {
        let storage = Arc::new(RwLock::new(Storage::new(RANGE)));
        let tokens: Vec<String> = obtener_tokens(vec!["rpush", "key1", "one", "two", "three"]);
        rpush(&tokens, &storage).unwrap();

        let tokens3: Vec<String> = obtener_tokens(vec!["lmove", "key1", "key2", "RIGHT", "LEFT"]);
        if let DatoRedis::BulkString(element) = lmove(&tokens3, &storage).unwrap() {
            assert_eq!(element, BulkString::new("three".to_string()).unwrap());
        }

        let tokens3: Vec<String> = obtener_tokens(vec!["lmove", "key1", "key2", "LEFT", "RIGHT"]);
        if let DatoRedis::BulkString(element) = lmove(&tokens3, &storage).unwrap() {
            assert_eq!(element, BulkString::new("one".to_string()).unwrap());
        }
        let guard = storage.read().unwrap();
        if let DatoRedis::Arrays(array) = guard.get("key1".to_string()).unwrap() {
            assert_eq!(array.len(), 1);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("two".to_string()).unwrap());
            }
        }

        if let DatoRedis::Arrays(array) = guard.get("key2".to_string()).unwrap() {
            assert_eq!(array.len(), 2);
            if let DatoRedis::BulkString(bulk) = array.get(0).unwrap() {
                assert_eq!(bulk, BulkString::new("three".to_string()).unwrap());
            }
            if let DatoRedis::BulkString(bulk) = array.get(1).unwrap() {
                assert_eq!(bulk, BulkString::new("one".to_string()).unwrap());
            }
        }
    }
}
