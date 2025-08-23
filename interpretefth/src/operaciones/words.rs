//! Este modulo se encarga de agregar nuevas definiciones a words a partir de una
//! secuencia de instrucciones

use crate::estructuras::errores::Error;
use crate::estructuras::palabra::Palabra;
use std::collections::HashMap;

/// Define si una secuencia corresponde a la definicion de una word, devolviendo verdadero
/// si lo es, falso en otro caso
///
/// # Parametros:
/// - 'secuencia': instrucciones a analizar
pub fn es_definir_word(secuencia: &[String]) -> bool {
    match secuencia.first() {
        Some(c) => c == ":",
        _ => false,
    }
}

/// Define si una secuencia corresponde a la definicion de una word, devolviendo verdadero
/// si lo es, falso en otro caso
///
/// # Parametros:
/// - 'secuencia': instrucciones a analizar
/// - 'dicc_words': diccionatio de pares (nombre_de_palabra, palabra)
///
/// Devuelve las instrucciones que sean externas a la definicion de la word, o error de haberlo
pub fn definir_word(
    secuencia: &[String],
    dicc_words: &mut HashMap<String, Palabra>,
) -> Result<Vec<String>, Error> {
    let mut definicion: Vec<String> = Vec::new();
    let mut resto: Vec<String> = Vec::new();
    let mut fin_word = false;
    let mut contador = 0;
    let mut referencias: HashMap<String, isize> = HashMap::new();

    for elem in secuencia.iter() {
        let elemento = elem.to_string();
        contador += 1;
        if contador < 3 {
            continue;
        }
        actualizar_definicion(
            &mut fin_word,
            elemento,
            &mut definicion,
            &mut resto,
            dicc_words,
            &mut referencias,
        );
    }

    if !fin_word {
        return Err(Error::InvalidWord);
    }

    if let Some(nombre) = secuencia.get(1) {
        match parsear_numero(nombre) {
            Ok(_) => Err(Error::InvalidWord),
            _ => {
                agregar_definicion(
                    &(nombre.to_lowercase()),
                    dicc_words,
                    definicion,
                    referencias,
                );
                Ok(resto)
            }
        }
    } else {
        Err(Error::InvalidWord)
    }
}

/// Define si un elemento de tipo &String representa un entero, de ser
/// asi, lo devuelve como i16, sino devuelve error
///
/// # Parametros:
/// - 'elemento': cadena a analizar
pub fn parsear_numero(elemento: &String) -> Result<i16, Error> {
    match elemento.to_string().parse::<i16>() {
        Ok(num) => Ok(num),
        Err(_) => {
            if elemento.chars().next() == "-".chars().next() {
                return match elemento.get(1..) {
                    Some(num) => match num.parse::<i16>() {
                        Ok(num) => Ok(-num),
                        Err(_) => Err(Error::OperationFail),
                    },
                    _ => Err(Error::OperationFail),
                };
            }
            Err(Error::OperationFail)
        }
    }
}

/// Define si un elemento es parte de la definicion de la palabra, de ser asi, lo agrega a su definicion,
/// en otro caso lo agrega a resto
///
/// # Parametros:
/// - 'fin_word': indica si termino la definicion de la palabra
/// - 'elemento': cadena a analizar
/// - 'definicion': construccion de la definicion de la palabra
/// - 'resto': construccion de las instrucciones que no son parte de la definicion de la palabra
/// - 'dicc_words': diccionario de pares (nombre_de_palabra, palabra)
/// - 'dicc_ref': diccionario de pares (nombre_palabra, version) que incluye las palabras a las que
///   referencia aquella word que se esta definiendo y en que version
fn actualizar_definicion(
    fin_word: &mut bool,
    elemento: String,
    definicion: &mut Vec<String>,
    resto: &mut Vec<String>,
    dicc_words: &mut HashMap<String, Palabra>,
    dicc_ref: &mut HashMap<String, isize>,
) {
    if elemento == ";" {
        *fin_word = true;
    } else if *fin_word {
        agregar(resto, elemento, dicc_words, dicc_ref);
    } else {
        agregar(definicion, elemento, dicc_words, dicc_ref);
    }
}

/// Agrega la definicion a la palabra, y la crea de no existir. Actualiza las referencias a otras palabras
///
/// # Parametros:
/// - 'nombre': nombre de la palabra a definir
/// - 'dicc_words': diccionario de pares (nombre_de_palabra, palabra)
/// - 'definicion': definicion de la palabra
/// - 'dicc_ref': diccionario de pares (nombre_palabra, version) que incluye las palabras a las que
///   referencia aquella word que se esta definiendo y en que version
fn agregar_definicion(
    nombre: &String,
    dicc_words: &mut HashMap<String, Palabra>,
    definicion: Vec<String>,
    dicc_ref: HashMap<String, isize>,
) {
    if let Some(palabra) = dicc_words.get_mut(nombre) {
        palabra.agregar_definicion(definicion);
        for (nombre_ref, version_ref) in dicc_ref {
            palabra.agregar_referencia(nombre_ref.to_string(), version_ref);
            palabra.es_referenciada();
        }
    } else {
        let palabra = Palabra::new(definicion, dicc_ref);
        dicc_words.insert(nombre.to_string(), palabra);
    }
}

/// Agrega una instruccion a un vector, de ser esta una palabra actualiza sus referencias
///
/// # Parametros:
/// - 'vec': vector donde agregar la instruccion
/// - 'elemento': instruccion a agregar
/// - 'dicc_words': diccionario de pares (nombre_de_palabra, palabra)
/// - 'dicc_ref': diccionario de pares (nombre_palabra, version) que incluye las palabras a las que
fn agregar(
    vec: &mut Vec<String>,
    elemento: String,
    dicc_words: &mut HashMap<String, Palabra>,
    dicc_ref: &mut HashMap<String, isize>,
) {
    if let Some(palabra) = dicc_words.get_mut(&(elemento.to_lowercase())) {
        dicc_ref.insert(elemento.to_string(), palabra.versiones);
        palabra.es_referenciada();
    }
    vec.push(elemento);
}
