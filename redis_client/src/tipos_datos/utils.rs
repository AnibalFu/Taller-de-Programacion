//! Este modulo contiene funciones auxiliares para los datos redis
use super::arrays::Arrays;
use super::bulk_string::BulkString;
use super::simple_string::SimpleString;
use super::traits::DatoRedis;
use crate::tipos_datos::constantes::*;
use crate::tipos_datos::integer::Integer;
use crate::tipos_datos::moved_error::MovedError;
use crate::tipos_datos::simple_error::SimpleError;
use crate::tipos_errores::errores::Error;

/// Para un string que representa un arreglo en formato resp, obtiene su
/// primer elemento (dato redis) y el indice del resp donde inicia el proximo
/// elemento
///
/// # Parametros:
/// * `arreglo_resp`: representacion resp del arreglo
///
/// # Retorna
/// - tupla de dato redis e indice de inicio del proximo elemento en caso de
///   exito, error en otro caso
pub fn obtener_elemento(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    if let Some(token_inicio) = arreglo_resp.chars().next() {
        match token_inicio.to_string().as_str() {
            INTEGER_SIMBOL => obtener_integer(arreglo_resp),
            BULK_STRING_SIMBOL => obtener_bulk_string(arreglo_resp),
            SIMPLE_STRING_SIMBOL => obtener_simple_string(arreglo_resp),
            ARRAY_SIMBOL => obtener_array(arreglo_resp),
            ERROR_SIMBOL => obtener_simple_error(arreglo_resp),
            MOVED_ERROR_SIMBOL => obtener_moved_error(arreglo_resp),
            _ => Err(Error::DatoIngresadoEsInvalido),
        }
    } else {
        Err(Error::DatoIngresadoEsInvalido)
    }
}

/// Dado que el primer elemento de una cadena resp represene un entero,
/// lo obtiene
///
/// # Parametros:
/// * `arreglo_resp`: representacion resp
///
/// # Retorna
/// - tupla de dato redis (integer) e indice de inicio del proximo elemento
///   en caso de exito, error en otro caso
fn obtener_integer(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let mut numero_string = String::new();
    let mut indice_fin = 0;
    for i in 1..arreglo_resp.len() {
        if arreglo_resp.chars().nth(i) == Some('\r')
            && arreglo_resp.chars().nth(i + 1) == Some('\n')
        {
            indice_fin = i + 1;
            break;
        }
        agregar_elemento(&mut numero_string, arreglo_resp, i);
    }
    if let Ok(numero) = numero_string.parse::<i64>() {
        return Ok((DatoRedis::Integer(Integer::new(numero)), indice_fin + 1));
    }
    Err(Error::DatoIngresadoEsInvalido)
}

/// Dado que el primer elemento de una cadena resp represene un bulk string,
/// lo obtiene
///
/// # Parametros:
/// * `arreglo_resp`: representacion resp
///
/// # Retorna
/// - tupla de dato redis (bulk string) e indice de inicio del proximo elemento
///   en caso de exito, error en otro caso
fn obtener_bulk_string(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let mut numero_string = String::new();
    let mut indice_fin = 0;
    for i in 1..arreglo_resp.len() {
        if arreglo_resp.chars().nth(i) == Some('\r')
            && arreglo_resp.chars().nth(i + 1) == Some('\n')
        {
            indice_fin = i + 1;
            break;
        }
        agregar_elemento(&mut numero_string, arreglo_resp, i);
    }
    if let Ok(numero) = numero_string.parse::<usize>() {
        let mut indice_final = indice_fin + 1;
        let mut texto = String::new();
        for i in indice_fin + 1..indice_fin + 1 + numero {
            if indice_final == indice_fin + 1 + numero {
                break;
            }
            if let Some(n) = arreglo_resp.chars().nth(i) {
                texto.push(n);
                indice_final += n.to_string().len();
            }
        }
        let indice_acceso = indice_fin + 1 + texto.chars().count();
        if arreglo_resp.chars().nth(indice_acceso) == Some('\r')
            && arreglo_resp.chars().nth(indice_acceso + 1) == Some('\n')
        {
            if let Ok(bulk_string) = BulkString::new(texto) {
                return Ok((DatoRedis::BulkString(bulk_string), indice_final + 2));
            }
        }
    }
    Err(Error::DatoIngresadoEsInvalido)
}

/// Dado que el primer elemento de una cadena resp represene un simple string,
/// lo obtiene
///
/// # Parametros:
/// * `arreglo_resp`: representacion resp
///
/// # Retorna
/// - tupla de dato redis (simple string) e indice de inicio del proximo elemento
///   en caso de exito, error en otro caso
fn obtener_simple_string(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let mut texto = String::new();
    for i in 1..arreglo_resp.len() {
        if arreglo_resp.chars().nth(i) == Some('\r')
            && arreglo_resp.chars().nth(i + 1) == Some('\n')
        {
            break;
        }
        agregar_elemento(&mut texto, arreglo_resp, i);
    }
    let indice_prox = texto.len() + 2;
    if let Ok(simple_string) = SimpleString::new(texto) {
        return Ok((DatoRedis::SimpleString(simple_string), indice_prox + 1));
    }
    Err(Error::DatoIngresadoEsInvalido)
}

/// Dado que el primer elemento de una cadena resp represene un arreglo,
/// lo obtiene
///
/// # Parametros:
/// * `arreglo_resp`: representacion resp
///
/// # Retorna
/// - tupla de dato redis (arreglo) e indice de inicio del proximo elemento
///   en caso de exito, error en otro caso
fn obtener_array(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let mut numero_string = String::new();
    let mut indice_fin = 0;
    for i in 1..arreglo_resp.len() {
        if arreglo_resp.chars().nth(i) == Some('\r')
            && arreglo_resp.chars().nth(i + 1) == Some('\n')
        {
            indice_fin = i + 1;
            break;
        }
        agregar_elemento(&mut numero_string, arreglo_resp, i);
    }
    if let Ok(numero) = numero_string.parse::<usize>() {
        let mut array = Vec::new();
        let mut indice_acceso = indice_fin + 1;
        for _ in 0..numero {
            let (dato, nuevo_indice) = obtener_elemento(&arreglo_resp[indice_acceso..])?;
            array.push(dato);
            indice_acceso += nuevo_indice;
        }
        return Ok((
            DatoRedis::Arrays(Arrays::new_con_contenido(array)),
            indice_acceso,
        ));
    }
    Err(Error::DatoIngresadoEsInvalido)
}

fn obtener_simple_error(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let end = arreglo_resp
        .find("\r\n")
        .ok_or(Error::DatoIngresadoEsInvalido)?;
    let linea = &arreglo_resp[1..end];

    let (head, cuerpo) = match linea.find(' ') {
        Some(idx) => linea.split_at(idx),
        None => return Err(Error::DatoIngresadoEsInvalido),
    };
    let cuerpo = &cuerpo[1..];
    let simple = SimpleError::new(head.to_string(), cuerpo.to_string());
    let siguiente_indice = end + 2;

    Ok((DatoRedis::SimpleError(simple), siguiente_indice))
}

fn obtener_moved_error(arreglo_resp: &str) -> Result<(DatoRedis, usize), Error> {
    let end = arreglo_resp
        .find("\r\n")
        .ok_or(Error::DatoIngresadoEsInvalido)?;
    let linea = &arreglo_resp[1..end];
    let slot: u16 = linea.parse().map_err(|_| Error::DatoIngresadoEsInvalido)?;
    let moved = MovedError::new(slot);

    Ok((DatoRedis::MovedError(moved), end + 2))
}

/// Concatena un char a un string
///
/// # Parametros:
/// * `elementos`: String donde concatenar
/// * `arreglo_resp`: representacion resp
/// * `i`: posicion de arreglo_resp donde se encuentra el char a concatenar
pub fn agregar_elemento(elementos: &mut String, arreglo_resp: &str, i: usize) {
    if let Some(n) = arreglo_resp.chars().nth(i) {
        elementos.push(n);
    }
}
