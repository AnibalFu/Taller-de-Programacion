//! Este modulo contiene la operatoria para funciones aritmeticas

use super::operaciones_stack::desapilar_n_arg;
use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;

/// Suma los ultimos dos elementos de la pila, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn suma(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => pila.apilar(vec[0] + vec[1]).map(|_| String::new()),
        _ => Err(Error::StackUnderflow),
    }
}

/// Resta los ultimos dos elementos de la pila, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn resta(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => pila.apilar(vec[1] - vec[0]).map(|_| String::new()),
        _ => Err(Error::StackUnderflow),
    }
}

/// Multiplica los ultimos dos elementos de la pila, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn multiplicacion(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => pila.apilar(vec[0] * vec[1]).map(|_| String::new()),
        _ => Err(Error::StackUnderflow),
    }
}

/// Divide los ultimos dos elementos de la pila, apilando el resultado
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn division(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => {
            if vec[0] == 0 {
                Err(Error::DivisionByZero)
            } else {
                pila.apilar(vec[1] / vec[0]).map(|_| String::new())
            }
        }
        _ => Err(Error::StackUnderflow),
    }
}

#[test]
pub fn test_suma() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(suma(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(suma(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = suma(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(4));

    let _ = pila.apilar(-6);
    let _ = suma(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-2));
}

#[test]
pub fn test_resta() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(resta(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(resta(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = resta(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-10));

    let _ = pila.apilar(-6);
    let _ = resta(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-4));
}

#[test]
pub fn test_multiplicacion() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(multiplicacion(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(multiplicacion(&mut pila).is_err());

    let _ = pila.apilar(-3);
    let _ = pila.apilar(7);
    let _ = multiplicacion(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(-21));

    let _ = pila.apilar(-3);
    let _ = multiplicacion(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(63));
}

#[test]
pub fn test_division() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(division(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(division(&mut pila).is_err());

    let _ = pila.apilar(8);
    let _ = pila.apilar(3);
    let _ = division(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(2));

    let _ = pila.apilar(1);
    let _ = division(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(2));

    let _ = pila.apilar(0);
    let _ = division(&mut pila);
    assert!(division(&mut pila).is_err());
}
