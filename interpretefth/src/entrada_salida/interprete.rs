//! Este modulo continiene las funciones que ejecutan una secuencia de instrucciones
//! definida en el modulo lectura

use crate::estructuras::errores::Error;
use crate::estructuras::palabra::Palabra;
use crate::estructuras::pila::Pila;
use crate::operaciones::operaciones_mapeo::Operaciones;
use crate::operaciones::words::{definir_word, es_definir_word, parsear_numero};
use std::collections::HashMap;

/// A partir del primer elemento de una secuencia de Strings, llama a la funcion correspondiente
///
/// # Parametros
/// - 'secuencia': las instrucciones que conforman una linea del programa a interpretar
/// - 'pila': estructura de tipo Pila con los numeros apilados hasta el momento
/// - 'dicc_operaciones': diccionario cuyas claves son nombres de instrucciones
///   que el interprete puede resolver y sus claves son funciones que a partir de la pila ejecutan
///   las mismas
/// - 'dicc_words': diccionario de pares (nombre_de_palabra, palabra)
/// - 'resultado': string de resultado de la ejecucion
///
/// De haber un error, se corta la ejecucion y se devuelve como option
pub fn interpretar(
    secuencia: &[String],
    pila: &mut Pila<i16>,
    dicc_operaciones: &mut Operaciones,
    dicc_words: &mut HashMap<String, Palabra>,
    resultado: &mut String,
) -> Option<Error> {
    if secuencia.is_empty() {
        return None;
    }
    if es_definir_word(secuencia) {
        match definir_word(secuencia, dicc_words) {
            Ok(resto) => {
                return interpretar_comandos(
                    &resto,
                    pila,
                    dicc_operaciones,
                    dicc_words,
                    None,
                    resultado,
                );
            }
            Err(_) => {
                return Some(Error::InvalidWord);
            }
        };
    }
    interpretar_comandos(
        secuencia,
        pila,
        dicc_operaciones,
        dicc_words,
        None,
        resultado,
    )
}

/// Dada la secuencia, determina si se pide evaluar un condicional,
/// una palabra ya definida, imprimir texto, o realizar una operacion predeterminada
/// y ejecuta la accion correspondiente
///
/// # Parametros
/// - 'secuencia': las instrucciones que conforman una linea del programa a interpretar
/// - 'pila': estructura de tipo Pila con los numeros apilados hasta el momento
/// - 'dicc_operaciones': diccionario cuyas claves son nombres de instrucciones
///   que el interprete puede resolver y sus claves son funciones que a partir de la pila ejecutan
///   las mismas
/// - 'dicc_words': diccionatio de pares (nombre_de_palabra, palabra)
/// - 'word_actual': de estar ejectuando la definicion de una palabra previamente definida,
///   se mantiene el nombre de la misma para conocer que versiones de otras palabras referencia
/// - 'resultado': string de resultado de la ejecucion
///
/// De haber un error, se corta la ejecucion y se devuelve como option
fn interpretar_comandos(
    secuencia: &[String],
    pila: &mut Pila<i16>,
    dicc_operaciones: &mut Operaciones,
    dicc_words: &mut HashMap<String, Palabra>,
    word_actual: Option<&String>,
    resultado: &mut String,
) -> Option<Error> {
    for elemento in secuencia.iter() {
        if elemento.to_lowercase() == "if" {
            let (mut caso_verdadero, mut caso_falso, mut resto) =
                (Vec::new(), Vec::new(), Vec::new());
            separar_clausulas(secuencia, &mut caso_verdadero, &mut caso_falso, &mut resto);
            let casos = [&caso_falso, &caso_verdadero];
            let tope = pila.desapilar();
            if let Ok(n) = tope {
                let interpretacion = interpretar_comandos(
                    casos[((*n).abs() % 2) as usize],
                    pila,
                    dicc_operaciones,
                    dicc_words,
                    None,
                    resultado,
                );
                if let Some(e) = interpretacion {
                    return Some(e);
                }
                return interpretar_comandos(
                    &resto,
                    pila,
                    dicc_operaciones,
                    dicc_words,
                    None,
                    resultado,
                );
            } else if let Err(e) = tope {
                return Some(e);
            }
        } else if !imprimir_texto(elemento, resultado) {
            let version = version(&(elemento.to_lowercase()), word_actual, dicc_words);
            if let Some(palabra) = dicc_words.get(&(elemento.to_lowercase())) {
                if let Some(instrucciones) = palabra.obtener_version(version) {
                    let mut comandos = Vec::new();
                    instruccion(instrucciones, &mut comandos);
                    if let Some(e) = interpretar_comandos(
                        &comandos,
                        pila,
                        dicc_operaciones,
                        dicc_words,
                        Some(&(elemento.to_lowercase())),
                        resultado,
                    ) {
                        return Some(e);
                    }
                }
            } else if let Some(e) = operar(pila, elemento, dicc_operaciones, resultado) {
                return Some(e);
            }
        }
    }
    None
}

/// Dada la secuencia, determina si se pide evaluar un condicional,
/// una palabra ya definida, imprimir texto, o realizar una operacion predeterminada
/// y ejecuta la accion correspondiente
///
/// # Parametros
/// - 'pila': estructura de tipo Pila con los numeros apilados hasta el momento
/// - 'elemento': token a analizar (numero u operacion predeterminada, de no haber error)
/// - 'dicc_operaciones': diccionario cuyas claves son nombres de instrucciones
///   que el interprete puede resolver y sus claves son funciones que a partir de la pila ejecutan
///   las mismas
/// - 'resultado': string de resultado de la ejecucion
///
/// De haber un error, se corta la ejecucion y se devuelve como option
fn operar(
    pila: &mut Pila<i16>,
    elemento: &String,
    dicc_operaciones: &mut Operaciones,
    resultado: &mut String,
) -> Option<Error> {
    match parsear_numero(elemento) {
        Ok(num) => {
            if let Err(e) = pila.apilar(num) {
                return Some(e);
            }
        }
        Err(_) => match dicc_operaciones.get(&(elemento.to_lowercase())) {
            Some(f) => match f(pila) {
                Ok(res) => {
                    resultado.push_str(&res);
                    if !res.is_empty() && res != "\n" {
                        resultado.push(' ');
                    }
                }
                Err(e) => {
                    return Some(e);
                }
            },
            _ => {
                return Some(Error::WordNotFound);
            }
        },
    };
    None
}

/// Determina si un token es del tipo ." " y de serlo, imprime el texto correspondiente
///
///  # Parametros
///  - 'elemento': token a analizar
///  - 'resultado': string de resultado de la ejecucion
///
/// De ser impreso el texto devuelve verdadero, sino falso
fn imprimir_texto(elemento: &str, resultado: &mut String) -> bool {
    if es_imprimir_texto(elemento) {
        if let Some(t) = obtener_texto(elemento) {
            resultado.push_str(&format!("{t} "));
        }
        true
    } else {
        false
    }
}

/// Determina si un token es del tipo ." "
///
///  # Parametros
///  - 'elemento': token a analizar
///
/// De ser correcto el formato devuelve verdadero, sino falso
fn es_imprimir_texto(elemento: &str) -> bool {
    let largo = elemento.len();
    if largo < 3 {
        return false;
    }
    elemento.starts_with('.')
        && elemento.chars().nth(1) == Some('"')
        && elemento.chars().nth(elemento.len() - 1) == Some('"')
}

/// Dado un token de tipo ." ", obtiene el texto correspondiente
///
///  # Parametros
///  - 'elemento': token a analizar
///
/// De ser correcta la extension del elemento devuelve el texto, sino None
fn obtener_texto(elemento: &str) -> Option<&str> {
    let largo = elemento.len();
    elemento.get(3..(largo - 1))
}

/// Obtiene las instrucciones de una definicion
///
///  # Parametros
///  - 'definicion': definicion de una palabra
///  - 'res': vector donde se almacenaran las instrucciones
///
/// El resultado se almacena en res
fn instruccion<'a>(definicion: &'a [String], res: &'a mut Vec<String>) -> &'a Vec<String> {
    for elem in definicion.iter() {
        res.push(elem.to_string());
    }
    res
}

/// Obtiene la versin a utilizar de una palabra
///
///  # Parametros
///  - 'elemento': nombre de la palabra que se quiere llamar a ejectuar
///  - 'word_actual': nombre de la palabra que esta llamando a la palabra de elemento
///  - 'dicc_words': diccionatio de pares (nombre_de_palabra, palabra)
///
/// Devuelve el numero de version que referencia la palabra que se esta ejecutando,
/// de no estar ejecutando una palabra, se devuelve la ultima version
fn version(
    elemento: &String,
    word_actual: Option<&String>,
    dicc_words: &mut HashMap<String, Palabra>,
) -> isize {
    if let Some(nombre_actual) = word_actual {
        if let Some(palabra_actual) = dicc_words.get(nombre_actual) {
            if let Some(version) = palabra_actual.version_referencia(elemento.to_string()) {
                return version;
            }
            return palabra_actual.versiones;
        }
    }
    -1
}

/// Obtiene las instrucciones correspondientes a las clausulas de una secuencia de tipo if .. else .. then
///
///  # Parametros
///  - 'secuencia': vector de strings que forman un condicional
///  - 'caso_verdadero': vector que almacenara las instrucciones correspondientes a ejecutar la clausula if
///  - 'caso_falso': vector que almacenara las instrucciones correspondientes a ejecutar la clausula else
///  - 'caso_resto': vector que almacenara las instrucciones correspondientes a ejecutar la clausula then
fn separar_clausulas(
    secuencia: &[String],
    caso_verdadero: &mut Vec<String>,
    caso_falso: &mut Vec<String>,
    resto: &mut Vec<String>,
) {
    let mut cant_if = 0;
    let mut dentro_if = false;
    let mut dentro_else = false;
    let mut fin_if = false;
    let mut casos = vec![caso_verdadero, caso_falso, resto];
    for elemento in secuencia.iter() {
        let elem = elemento.to_lowercase();

        if !es_parte_del_if(
            &elem,
            &mut cant_if,
            &mut dentro_if,
            &mut dentro_else,
            &mut fin_if,
        ) {
            continue;
        }

        actualizar_casos(&mut casos, elem, &dentro_else, &fin_if);
    }
    for caso in casos.iter_mut().take(3) {
        if !caso.is_empty() {
            caso.remove(0);
        }
    }
}

/// Determina en que seccion de la secuencia if .. else .. then se encuentra
/// una instruccion
///
/// # Parametros
/// - 'elem': instruccion a analizar
/// - 'cant_if': cantidad de ifs anidados hasta el momento, sin su clausula then
/// - 'dentro_if': indica si se esta analizando la clausula if
/// - 'dentro_else': indica si se esta analizando la clausula else
/// - 'fin_if': indica si se esta analizando la clausula then
///
/// Devuelve true si la instruccion forma parte de la secuencia if .. else .. then, false de ser
/// previa a la misma
fn es_parte_del_if(
    elem: &String,
    cant_if: &mut i32,
    dentro_if: &mut bool,
    dentro_else: &mut bool,
    fin_if: &mut bool,
) -> bool {
    if elem == "if" {
        *dentro_if = true;
        *cant_if += 1;
    } else if !*dentro_if {
        return false;
    } else if elem == "then" {
        *cant_if -= 1;
    }
    if *cant_if == 1 && elem == "else" {
        *dentro_else = true;
    } else if *cant_if == 0 {
        *fin_if = true;
        *dentro_else = false;
    }
    true
}

/// Actualiza eñl vector de la clausula correspondiente para un elemento dado
/// # Parametros
/// - 'casos': vector que incluye al caso verdadero, caso falso, caso then en ese orden
/// - 'elem': instruccion a analizar
/// - 'dentro_else': indica si se esta analizando la clausula else
/// - 'fin_if': indica si se esta analizando la clausula then
fn actualizar_casos(
    casos: &mut [&mut Vec<String>],
    elem: String,
    dentro_else: &bool,
    fin_if: &bool,
) {
    if *dentro_else {
        casos[1].push(elem);
    } else if *fin_if {
        casos[2].push(elem);
    } else {
        casos[0].push(elem);
    }
}
