//! Este modulo contiene la operatoria para funciones de output

use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;

/// Desapila el ultimo elemento de la pila
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores
///
/// De haber error, lo devuelve, sino, devuelve el numero a imprimir
pub fn imprimir_ult(pila: &mut Pila<i16>) -> Result<String, Error> {
    let tope = pila.desapilar();
    match tope {
        Ok(num) => Ok(format!("{}", *num)),
        Err(e) => Err(e),
    }
}

/// Desapila el ultimo elemento de la pila y lo devuelve como ascii
///
/// # Parametros:
/// - 'pila': pila de donde obtener los operadores
///
/// De haber error, lo devuelve, sino, devuelve el caracter a imprimir
pub fn emit(pila: &mut Pila<i16>) -> Result<String, Error> {
    let tope = pila.desapilar();
    match tope {
        Ok(num) => Ok((*num as u8 as char).to_string()),
        Err(e) => Err(e),
    }
}

/// Devuelve un salto de linea
///
/// # Parametros:
/// - 'pila': pila de i16
///
/// De haber error, lo devuelve, sino, devuelve un salto de linea
pub fn cr(_pila: &mut Pila<i16>) -> Result<String, Error> {
    Ok("\n".to_string())
}
