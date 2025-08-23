//! Este modulo se encarga de la lectura del archivo fth a ejectuar,
//! la separacion del mismo en lineas de instrucciones, la impresion de errores
//! y la muestra del stack al final de la ejecucion

use super::interprete::interpretar;
use super::utils::{separar_por_espacios, unir, vaciar_pila};
use crate::constantes::TAM_PRED_STACK;
use crate::estructuras::errores::Error;
use crate::estructuras::palabra::Palabra;
use crate::estructuras::pila::Pila;
use crate::operaciones::operaciones_mapeo::{Operaciones, crear_dicc_op};
use std::collections::HashMap;

/// Llama a la procesion de cada linea, sin almacenarlas en memoria
///
/// # Parametros
/// - 'text': lineas a ejecutar
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
///
/// De haber algun error, falla
pub(crate) fn interpretar_archivo(text: Vec<String>, armando_word: &mut bool) -> (String, String) {
    let tam_stack = TAM_PRED_STACK;
    let (mut pila, mut operaciones, mut words) = crear_estructuras(tam_stack);
    let mut linea_final: Vec<String> = Vec::new();
    let mut resultado: String = String::new();

    for linea in text {
        if let Some(e) = interpretar_linea(
            linea.to_string(),
            armando_word,
            &mut linea_final,
            &mut pila,
            &mut operaciones,
            &mut words,
            &mut resultado,
        ) {
            resultado.push_str(&format!("{e}"));
            break;
        }
    }
    let pila_final = vaciar_pila(&mut pila, &tam_stack, &mut resultado);
    (resultado, pila_final)
}

/// Llama a la procesion de cada linea, sin almacenarlas en memoria
///
/// # Parametros
/// - 'tam_stack': cantidad de elementos en el stack deseada
///
/// Devuelve una pila vacia, un diccionario con todas las operaciones
/// predeterminadas ya definidas y un diccionario vacio para almacenar futuras words con sus nombres
fn crear_estructuras(tam_stack: usize) -> (Pila<i16>, Operaciones, HashMap<String, Palabra>) {
    let pila: Pila<i16> = Pila::crear(tam_stack);
    let operaciones: Operaciones = crear_dicc_op();
    let words: HashMap<String, Palabra> = HashMap::new();
    (pila, operaciones, words)
}

/// Llama a la procesion de cada linea, sin almacenarlas en memoria
///
/// # Parametros
/// - 'comando': linea del archivo .fth
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
/// - 'linea_final': lineas anteriores si se estaba definiendo una word, vector vacio en otro caso
/// - 'pila': pila de i16 con los resultados acumulados
/// - 'operaciones': diccionario cuyas claves son nombres de instrucciones
///   que el interprete puede resolver y sus claves son funciones que a partir de la pila ejecutan
///   las mismas
/// - 'words': diccionatio de pares (nombre_de_palabra, palabra)
/// - 'resultado': string de resultado de la ejecucion
///
/// De haber algun error, lo devuelve
fn interpretar_linea(
    comando: String,
    armando_word: &mut bool,
    linea_final: &mut Vec<String>,
    pila: &mut Pila<i16>,
    operaciones: &mut Operaciones,
    words: &mut HashMap<String, Palabra>,
    resultado: &mut String,
) -> Option<Error> {
    if !*armando_word {
        linea_final.clear();
    }
    let mut linea_actual: Vec<String> = Vec::new();
    if let Err(e) = separar_por_espacios(comando, &mut linea_actual, armando_word) {
        return Some(e);
    }
    unir(linea_final, linea_actual);
    if !*armando_word {
        if let Some(e) = interpretar(linea_final, pila, operaciones, words, resultado) {
            return Some(e);
        }
    }
    None
}
