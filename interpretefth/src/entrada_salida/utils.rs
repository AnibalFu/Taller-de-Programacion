//! Este modulo contiene funciones auxiliares para el modulo lectura

use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;

/// Llama a la procesion de cada linea, sin almacenarlas en memoria
///
/// # Parametros
/// - 'comando': linea a analizar
/// - 'linea_final': lineas anteriores si se estaba definiendo una word, vector vacio en otro caso
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
///
/// De haber algun error, lo devuelve, sino, devuelve un vector de los tokens preprocesados
pub fn separar_por_espacios(
    comando: String,
    linea_actual: &mut Vec<String>,
    armando_word: &mut bool,
) -> Result<Option<Vec<String>>, Error> {
    let mut dentro_de_str = false;
    let mut expresion = String::new();

    if let Some(e) = armar_expresion(
        &comando,
        armando_word,
        &mut expresion,
        &mut dentro_de_str,
        linea_actual,
    ) {
        return Err(e);
    }
    if !expresion.is_empty() {
        linea_actual.push((*expresion).to_string());
    }
    if dentro_de_str {
        return Err(Error::OperationFail);
    }
    Ok(None)
}

/// Une dos vectores en el primero
///
/// # Parametros
/// - 'linea_1': vector a extender
/// - 'linea_2': instrucciones a agregar a linea_1
///
/// Modifica linea_1 in place
pub fn unir(linea_1: &mut Vec<String>, linea_2: Vec<String>) {
    for elem in linea_2.iter() {
        linea_1.push(elem.to_string());
    }
}

/// Vacia una pila y escribe sus contenidos en un archivo dado
///
/// # Parametros
/// - 'pila': Pila de i16 a vaciar
/// - 'tam_stack': cantidad de elementos posibles en la pila
/// - 'resultado': string de resultado de la ejecucion
///
/// # Retorna
/// - String representando la pila
pub fn vaciar_pila(pila: &mut Pila<i16>, tam_stack: &usize, resultado: &mut String) -> String {
    let mut pila_final = String::new();
    let mut pila_volteada: Pila<i16> = Pila::crear(*tam_stack);
    llenar_pila(pila, &mut pila_volteada, resultado);
    while !pila_volteada.esta_vacia() {
        if let Ok(elem) = pila_volteada.desapilar() {
            pila_final.push_str(&format!("{} ", *elem));
        }
    }

    pila_final
}

/// Pasa los elementos de una pila a otra, invirtiendolos
///
/// # Parametros
/// - 'pila_1': pila a invertir
/// - 'pila_2': pila invertida
/// - 'resultado': string de resultado de la ejecucion
fn llenar_pila(pila_1: &mut Pila<i16>, pila_2: &mut Pila<i16>, resultado: &mut String) {
    while !pila_1.esta_vacia() {
        if let Ok(elem) = pila_1.desapilar() {
            if let Err(e) = pila_2.apilar(*elem) {
                resultado.push_str(&format!("{e}"));
            }
        }
    }
}

/// Analiza un comando caracter a caracter para dividirlo en instrucciones de forth
///
/// # Parametros
/// - 'comando': linea a analizar
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
/// - 'expresion': formacion del token actual hasta el momento
/// - 'dentro_de_str': determina si se esta analizando por dentro una expresion de tipo ." "
/// - 'linea_actual': vector que almacena el resultados
///
/// De haber algun error, lo devuelve
fn armar_expresion(
    comando: &str,
    armando_word: &mut bool,
    expresion: &mut String,
    dentro_de_str: &mut bool,
    linea_actual: &mut Vec<String>,
) -> Option<Error> {
    for caracter in comando.chars() {
        if let Some(e) = verificar_armando_word(armando_word, &caracter) {
            return Some(e);
        }
        if expresion == ".\"" && caracter == ' ' {
            *dentro_de_str = true;
        }
        if es_espacio(&caracter) && !*dentro_de_str {
            actualizar_linea(expresion, linea_actual, armando_word);
            continue;
        } else if caracter == '"' && *dentro_de_str {
            cerrar_string(dentro_de_str, expresion, linea_actual, &caracter);
            continue;
        }
        expresion.push(caracter);
    }
    None
}

/// Verfica si se esta definiendo una word
///
/// # Parametros
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
/// - 'caracter': caracter a analizar para determinar si se esta definiendo una word
///
/// De haber algun error, lo devuelve
fn verificar_armando_word(armando_word: &mut bool, caracter: &char) -> Option<Error> {
    if (*caracter == ':' && !*armando_word) || (*caracter == ';' && *armando_word) {
        *armando_word = !*armando_word;
        return None;
    } else if *caracter == ':' || *caracter == ';' {
        return Some(Error::InvalidWord);
    }
    None
}

/// Verfica si un caracter es algun tipo de espacio
///
/// # Parametros
/// - 'caracter': caracter a analizar
///
/// Devuelve verdadero de ser espacio, falso en otro caso
fn es_espacio(caracter: &char) -> bool {
    *caracter == '\t' || *caracter == '\n' || *caracter == ' '
}

/// Actualiza la linea actual con una expresion de ser correspondiente
///
/// # Parametros
/// - 'expresion': formacion del token actual hasta el momento
/// - 'linea_actual': vector que almacena el resultados
/// - 'armando_word': booleano indicando si en la linea anterior se estaba definiendo una
///   palabra, para continuar con la definicion de la misma
///
/// De haber error, lo devuelve
fn actualizar_linea(
    expresion: &mut String,
    linea_actual: &mut Vec<String>,
    armando_word: &mut bool,
) -> Option<Error> {
    if !expresion.is_empty() {
        if (expresion.to_lowercase() == "if"
            || expresion.to_lowercase() == "then"
            || expresion.to_lowercase() == "else")
            && !*armando_word
        {
            return Some(Error::OperationFail);
        }
        actualizar_linea_actual(expresion, linea_actual);
    }
    None
}

/// Cierra un token de tipo ". "
///
/// # Parametros
/// - 'dentro_de_str': determina si se esta analizando por dentro una expresion de tipo ." "
/// - 'expresion': formacion del token actual hasta el momento
/// - 'linea_actual': vector que almacena el resultados
/// - 'caracter': caracter " que cierra la expresion
fn cerrar_string(
    dentro_de_str: &mut bool,
    expresion: &mut String,
    linea_actual: &mut Vec<String>,
    caracter: &char,
) {
    expresion.push(*caracter);
    actualizar_linea_actual(expresion, linea_actual);
    *dentro_de_str = false;
}

fn actualizar_linea_actual(expresion: &mut String, linea_actual: &mut Vec<String>) {
    linea_actual.push((*expresion).to_string());
    expresion.clear();
}
