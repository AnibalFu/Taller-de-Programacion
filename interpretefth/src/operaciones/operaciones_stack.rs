//! Este modulo contiene la operatoria para funciones de stack

use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;

/// Desapila los ultimos n elementos de la pila, y los devuelve en un vectors,
/// ordenados desde el ultimo apilado al primero
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
/// - 'n': argumentos a desapilar
///
/// De haber error, lo devuelve
pub fn desapilar_n_arg(pila: &mut Pila<i16>, n: usize) -> Result<Vec<i16>, Error> {
    let mut args: Vec<i16> = Vec::with_capacity(n);
    for _ in 0..n {
        match pila.desapilar() {
            Ok(num) => args.push(*num),
            _ => return Err(Error::StackUnderflow),
        }
    }
    Ok(args)
}

/// Vuelve a apilar el tope de la pila
///
/// # Parametros:
/// - 'pila': pila de donde obtener el operador y apilar el resultado
///
/// De haber error, lo devuelve
pub fn dup(pila: &mut Pila<i16>) -> Result<String, Error> {
    let tope = pila.ver_tope();
    match tope {
        Ok(num) => {
            let valor = *num;
            pila.apilar(valor).map(|_| String::new())
        }
        Err(e) => Err(e),
    }
}

/// Desapila el ultimo elemento de la pila
///
/// # Parametros:
/// - 'pila': pila de donde obtener el operador
///
/// De haber error, lo devuelve
pub fn drop(pila: &mut Pila<i16>) -> Result<String, Error> {
    match pila.desapilar() {
        Ok(num) => Ok(format!("{}", *num)),
        Err(e) => Err(e),
    }
}

/// Invierte el orden de los ultimos dos elementos de la pila
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn swap(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => {
            for elemento in vec.iter().take(2) {
                pila.apilar(*elemento)?;
            }
            Ok("".to_string())
        }
        Err(e) => Err(e),
    }
}

/// Apila el anteultimo elemento de la pila
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores y apilar el resultado
///
/// De haber error, lo devuelve
pub fn over(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 2);
    match args {
        Ok(vec) => {
            let elementos = [vec[1], vec[0], vec[1]];
            for elemento in &elementos {
                pila.apilar(*elemento)?;
            }
            Ok("".to_string())
        }
        Err(e) => Err(e),
    }
}

/// Para una pila cuyos ultimos tres elementos son
/// A B C -> tope
/// se modifica a
/// B C A -> tope
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores
///
/// De haber error, lo devuelve
pub fn rot(pila: &mut Pila<i16>) -> Result<String, Error> {
    let args = desapilar_n_arg(pila, 3);
    match args {
        Ok(vec) => {
            let elementos = [vec[1], vec[0], vec[2]];
            for elemento in &elementos {
                pila.apilar(*elemento)?;
            }
            Ok("".to_string())
        }
        Err(e) => Err(e),
    }
}

#[test]
pub fn test_dup() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(dup(&mut pila).is_err());

    let _ = pila.apilar(2);
    let _ = dup(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(2));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(2));
}

#[test]
pub fn test_drop() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(drop(&mut pila).is_err());

    let _ = pila.apilar(2);
    let _ = pila.apilar(3);
    let _ = pila.apilar(4);
    let _ = drop(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(3));
    let _ = drop(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(2));
    let _ = drop(&mut pila);
    assert!(pila.esta_vacia());
}

#[test]
pub fn test_swap() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(swap(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(swap(&mut pila).is_err());

    let _ = pila.apilar(2);
    let _ = pila.apilar(3);

    let _ = swap(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(2));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(3));
}

#[test]
pub fn test_over() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(over(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(over(&mut pila).is_err());

    let _ = pila.apilar(4);
    let _ = pila.apilar(3);

    let _ = over(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(4));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(3));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(4));
}

#[test]
pub fn test_rot() {
    let mut pila: Pila<i16> = Pila::crear(15);
    assert!(rot(&mut pila).is_err());

    let _ = pila.apilar(2);
    assert!(rot(&mut pila).is_err());

    let _ = pila.apilar(1);
    let _ = pila.apilar(2);
    assert!(rot(&mut pila).is_err());

    let _ = pila.apilar(1);
    let _ = pila.apilar(2);
    let _ = pila.apilar(3);

    let _ = rot(&mut pila);
    assert_eq!(pila.ver_tope().unwrap(), &(1));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(3));
    let _ = pila.desapilar();
    assert_eq!(pila.ver_tope().unwrap(), &(2));
}
