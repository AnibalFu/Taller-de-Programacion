//! Este modulo contiene la operatoria para funciones logicas

use super::operaciones_stack::desapilar_n_arg;
use crate::constantes::{FALSO, VERDADERO};
use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;

/// Compara si los dos ultimos elementos de la pila son iguales, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn igualdad(pila: &mut Pila<i16>) -> Result<String, Error> {
    let condicion: fn(&i16, &i16) -> bool = |a, b| a == b;
    comparacion_elementos(pila, condicion)
}

/// Determina si el anteultimo elemento de la pila es mayor al ultimo, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn mayor(pila: &mut Pila<i16>) -> Result<String, Error> {
    let condicion: fn(&i16, &i16) -> bool = |a, b| a < b;
    comparacion_elementos(pila, condicion)
}

/// Determina si el anteultimo elemento de la pila es menor al ultimo, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn menor(pila: &mut Pila<i16>) -> Result<String, Error> {
    let condicion: fn(&i16, &i16) -> bool = |a, b| a > b;
    comparacion_elementos(pila, condicion)
}

/// Determina si los dos ultimos elementos son VERDADERO (-1), apilando el resultado.
/// De haber elementos en las ultimas dos posiciones de la pila que no correspondan a "booleanos",
/// el resultado es inesperado (comportamiento de Easy Forth)
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn and(pila: &mut Pila<i16>) -> Result<String, Error> {
    let condicion: fn(&i16, &i16) -> bool = |a, b| *a == VERDADERO && *b == VERDADERO;
    comparacion_elementos(pila, condicion)
}

/// Determina si alguno de los dos ultimos elementos son VERDADERO (-1), apilando el resultado.
/// De haber elementos en las ultimas dos posiciones de la pila que no correspondan a "booleanos",
/// el resultado es inesperado (comportamiento de Easy Forth)
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn or(pila: &mut Pila<i16>) -> Result<String, Error> {
    let condicion: fn(&i16, &i16) -> bool = |a, b| *a == VERDADERO || *b == VERDADERO;
    comparacion_elementos(pila, condicion)
}

/// Determina si los dos ultimos elementos de la pila cumplen una determinada condicion, apila el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
/// - 'f': condicion que deben cumplir los ultimos dos elementos, por ejemplo, ser iguales
///
/// De haber error, lo devuelve
fn comparacion_elementos(pila: &mut Pila<i16>, f: fn(&i16, &i16) -> bool) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => {
            if f(&vec[0], &vec[1]) {
                pila.apilar(VERDADERO).map(|_| String::new())
            } else {
                pila.apilar(FALSO).map(|_| String::new())
            }
        }
        Err(e) => Err(e),
    }
}

/// Niega el ultimo elemento de la pila, el cual debe ser booleano (0, -1) y apila el resultado
/// De no tener un ultimo elemento que no corresponda a "booleanos",
/// el resultado es inesperado (comportamiento de Easy Forth)
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn not(pila: &mut Pila<i16>) -> Result<String, Error> {
    match pila.desapilar() {
        Ok(valor) => {
            let mut res = 0;
            let tope = *valor;
            if tope == FALSO {
                res = VERDADERO;
            }
            pila.apilar(res).map(|_| String::new())
        }
        Err(e) => Err(e),
    }
}

#[test]
pub fn test_igualdad() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(igualdad(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(igualdad(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = igualdad(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(0);
    let _ = igualdad(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));
}

#[test]
pub fn test_mayor() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(mayor(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(mayor(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = mayor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(-3);
    let _ = mayor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));

    let _ = pila.apilar(-1);
    let _ = mayor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));
}

#[test]
pub fn test_menor() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(menor(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(menor(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = menor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));

    let _ = pila.apilar(-3);
    let _ = menor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(0);
    let _ = menor(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));
}

#[test]
pub fn test_and() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(and(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(and(&mut pila).is_err());

    let _ = pila.apilar(0);
    let _ = pila.apilar(0);
    let _ = and(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(-1);
    let _ = and(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(-1);
    let _ = pila.apilar(-1);
    let _ = and(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));
}

#[test]
pub fn test_or() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(or(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(or(&mut pila).is_err());

    let _ = pila.apilar(0);
    let _ = pila.apilar(0);
    let _ = or(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));

    let _ = pila.apilar(-1);
    let _ = or(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));

    let _ = pila.apilar(-1);
    let _ = or(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));
}

#[test]
pub fn test_not() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(not(&mut pila).is_err());

    let _ = pila.apilar(0);
    let _ = not(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-1));

    let _ = not(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(0));
}
