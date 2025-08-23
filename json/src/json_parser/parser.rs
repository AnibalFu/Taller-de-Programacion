//! Este módulo contiene la funcionalidad para parsear un String
//! a una ExpresionJson
use std::collections::HashMap;

use crate::json::{ExpresionJson, LiteralJson};
use crate::pila::Pila;

/*
pub fn obtener_json(entrada_str: String) -> Result<ExpresionJson, ExpresionJson> {
  let entrada= entrada_str.trim().to_string();
  match entrada {
    val if val == "".to_string() => {
      Err(ExpresionJson::new_invalid_json_err())
    }
    val if es_objeto_json(&val) => {
      let pares = armar_objeto(val)?;
      let mut objeto = HashMap::new();
      for (k, v) in pares {
        let v_json = obtener_json(v)?;
        objeto.insert(k, Box::new(v_json));
      }
      Ok(ExpresionJson::Objeto(objeto))
    }
    val if es_arreglo_json(&val) => {
      let elementos = armar_arreglo(val)?;
      let mut arreglo = Vec::new();
      for elem in elementos {
        arreglo.push(Box::new(obtener_json(elem)?));
      }
      Ok(ExpresionJson::Arreglo(arreglo))
    }
    val => {
      armar_literal(val)
    }
  }
} */

/// Transforma un String en una ExpresionJson
///
/// # Parámetros
/// - `entrada_string`: entrada
///
/// # Retorna
/// - ExpresionJson resultante en caso de éxito, error de ExpresionJson en otro caso
pub fn obtener_json_raw(entrada_string: String) -> Result<ExpresionJson, ExpresionJson> {
    let entrada = entrada_string.trim().to_string();
    match entrada {
        val if val == *"" => Err(ExpresionJson::new_invalid_json_err()),
        val if es_objeto_json(&val) => {
            let pares = armar_objeto_raw(val)?;
            let mut objeto = HashMap::new();
            for (k, v) in pares {
                let v_json = obtener_json_raw(v)?;
                objeto.insert(k, Box::new(v_json));
            }
            Ok(ExpresionJson::Objeto(objeto))
        }
        val if es_arreglo_json(&val) => {
            let elementos = armar_arreglo(val)?;
            let mut arreglo = Vec::new();
            for elem in elementos {
                arreglo.push(Box::new(obtener_json_raw(elem)?));
            }
            Ok(ExpresionJson::Arreglo(arreglo))
        }
        val => armar_literal_raw(val),
    }
}

/// Determina si un str representa un objeto Json
///
/// # Parámetros
/// - `val`: entrada
///
/// # Retorna
/// - Verdadero si es un objeto, falso en otro caso
fn es_objeto_json(val: &str) -> bool {
    val.starts_with('{') && val.ends_with('}')
}

/// Determina si un str representa un arreglo Json
///
/// # Parámetros
/// - `val`: entrada
///
/// # Retorna
/// - Verdadero si es un arreglo, falso en otro caso
fn es_arreglo_json(val: &str) -> bool {
    val.starts_with('[') && val.ends_with(']')
}

/// Determina si un str representa un string Json
///
/// # Parámetros
/// - `val`: entrada
///
/// # Retorna
/// - Verdadero si es un string, falso en otro caso
fn es_string(val: &str) -> bool {
    val.starts_with('"') && val.ends_with('"')
}

/// A partir de un String en formato objeto Json, obtiene un mapa de claves valores
///
/// # Parámetros
/// - `entrada`: String a decodificar
///
/// # Retorna
/// - HashMap de claves valores en caso de éxito, error de ExpresionJson en otro caso
fn armar_objeto_raw(entrada: String) -> Result<HashMap<String, String>, ExpresionJson> {
    let chars: Vec<char> = entrada.chars().collect();
    let mut chars_medio = chars[1..chars.len() - 1].to_vec();
    let partes: Vec<String> = separar_por_caracter(
        &mut chars_medio,
        &mut Pila::crear(10000),
        false,
        String::new(),
        &',',
    )?;
    let mut pares = HashMap::new();
    for parte in partes {
        let par = separar_por_caracter(
            &mut parte.chars().collect(),
            &mut Pila::crear(10000),
            false,
            String::new(),
            &':',
        )?;
        if par.len() != 2 {
            return Err(ExpresionJson::new_invalid_json_err());
        }
        let key = par[0].trim();
        let largo = key.len();
        let s = &key[1..largo - 1];
        pares.insert(s.to_string(), par[1].to_string());
    }
    Ok(pares)
}

/// A partir de un String en formato arreglo Json, obtiene sus elementos
///
/// # Parámetros
/// - `entrada`: String a decodificar
///
/// # Retorna
/// - Vector de elementos en caso de éxito, error de ExpresionJson en otro caso
fn armar_arreglo(entrada: String) -> Result<Vec<String>, ExpresionJson> {
    let chars: Vec<char> = entrada.chars().collect();
    let mut chars_medio = chars[1..chars.len() - 1].to_vec();
    let elementos = separar_por_caracter(
        &mut chars_medio,
        &mut Pila::crear(10000),
        false,
        String::new(),
        &',',
    )?;
    Ok(elementos)
}

/// A partir de un String en formato literal de Json, obtiene su representación
///
/// # Parámetros
/// - `entrada`: String a decodificar
///
/// # Retorna
/// - ExpresionJson equivalente en caso de éxito, error de ExpresionJson en otro caso
fn armar_literal_raw(entrada: String) -> Result<ExpresionJson, ExpresionJson> {
    if let Ok(n) = entrada.parse::<f64>() {
        return Ok(ExpresionJson::Literal(LiteralJson::NumberJson(n)));
    }
    if entrada == "true" {
        return Ok(ExpresionJson::Literal(LiteralJson::BooleanJson(true)));
    }
    if entrada == "false" {
        return Ok(ExpresionJson::Literal(LiteralJson::BooleanJson(false)));
    }
    if es_string(&entrada) {
        let largo = entrada.len();
        let s = &entrada[1..largo - 1];
        return Ok(ExpresionJson::Literal(LiteralJson::StringJson(
            s.to_string(),
        )));
    }
    Err(ExpresionJson::new_invalid_json_err())
}

/// Dado un caracter, particiona a un String en secciones lógicas de
/// formato Json según dicho caracter
///
/// # Parámetros
/// - `cadena`: vector de chars a separar
/// - `pila`: pila auxiliar para detectar el correcto formato de la entrada
/// - `en_str`: indica si un caracter forma parte de un string json
/// - `act`: sección actual
/// - `separador`: caracter por el cual separar
///
/// # Retorna
/// - Vector de secciones en caso de éxito, error de ExpresionJson en otro caso
fn separar_por_caracter(
    cadena: &mut Vec<char>,
    pila: &mut Pila<char>,
    mut en_str: bool,
    mut act: String,
    separador: &char,
) -> Result<Vec<String>, ExpresionJson> {
    match cadena.first().copied() {
        None if !pila.esta_vacia() => {
            return Err(ExpresionJson::new_invalid_json_err());
        }
        None => {
            return Ok(vec![act]);
        }
        Some('{') if !en_str => {
            apilar_caracter(&mut act, pila, cadena)?;
        }
        Some('}') if !en_str => {
            desapilar_caracter(&mut act, pila, cadena, '{')?;
        }
        Some('[') if !en_str => {
            apilar_caracter(&mut act, pila, cadena)?;
        }
        Some(']') if !en_str => {
            desapilar_caracter(&mut act, pila, cadena, '[')?;
        }
        Some(c) if c == *separador && pila.esta_vacia() && !en_str => {
            cadena.remove(0);
            let mut res = vec![act];
            res.extend(separar_por_caracter(
                cadena,
                pila,
                en_str,
                String::new(),
                separador,
            )?);
            return Ok(res);
        }
        Some(' ') if !en_str => {
            cadena.remove(0);
        }
        Some('"') => {
            agregar_comilla(&mut act, cadena, &mut en_str);
        }
        Some(_) => {
            let i = acumular_caracteres(cadena, &mut act, pila.esta_vacia(), separador, &en_str);
            cadena.drain(0..i);
        }
    }
    separar_por_caracter(cadena, pila, en_str, act, separador)
}

/// Dado un caracter, determina si es algún tipo de separador dentro de una
/// expresión json
///
/// # Parámetros
/// - `c`: char a analizar
/// - `en_str`: indica si un caracter forma parte de un string json
/// - `pila_vacial`: indica si la pila auxiliar esta vacía
/// - `separador`: caracter por el cual separar
///
/// # Retorna
/// - Verdadero si es caracter especial, falso en otro caso
fn es_caracter_especial(c: &char, en_str: &bool, pila_vacia: &bool, separador: &char) -> bool {
    if (c == &'[' || c == &']' || c == &'{' || c == &'}' || c == &' ') && !*en_str {
        return true;
    }
    if c == separador && *pila_vacia && !*en_str {
        return true;
    }
    c == &'"'
}

/// Dado un vector de caracteres, determina si una posición corresponde a
/// una comilla escapada
///
/// # Parámetros
/// - `c`: vector de caracteres
/// - `i`: posición a analizar
///
/// # Retorna
/// - Verdadero si es comilla escapada, falso en otro caso
fn es_comilla_escapada(c: &[char], i: &mut usize) -> bool {
    if c[*i] == '\\' && *i + 1 < c.len() && c[*i + 1] == '\"' {
        *i += 1;
        return true;
    }
    false
}

/// Dado un vector de caracteres, los agrega en act hasta llegar a un
/// caracter especial
///
/// # Parámetros
/// - `c`: vector de caracteres
/// - `act`: String resultado
/// - `pila_vacia`: indica si la pila auxiliar esta vacía
/// - `separador`: caracter por el que se separara
/// - `en_str`: determina si se está analizando un literal string
///
/// # Retorna
/// - usize indicando la posición por donde se debe continuar el parseo
fn acumular_caracteres(
    c: &[char],
    act: &mut String,
    pila_vacia: bool,
    separador: &char,
    en_str: &bool,
) -> usize {
    let mut i = 0;
    while i < c.len() {
        if es_caracter_especial(&c[i], en_str, &pila_vacia, separador) {
            break;
        } else {
            if es_comilla_escapada(c, &mut i) {
                act.push(c[i - 1]);
                act.push(c[i]);
                i += 1;
                break;
            }
            act.push(c[i]);
        }
        i += 1;
    }
    i
}

/// Apila un caracter, y lo borra del vector de caracteres
///
/// # Parámetros
/// - `act`: String resultado
/// - `pila`: pila auxiliar
/// - `c`: vector de caracteres
///
/// # Retorna
/// - () en caso de éxito, error de ExpresionJson en otro caso
fn apilar_caracter(
    act: &mut String,
    pila: &mut Pila<char>,
    c: &mut Vec<char>,
) -> Result<(), ExpresionJson> {
    act.push(c[0]);
    pila.apilar(c[0])?;
    c.remove(0);
    Ok(())
}

/// Desapila un caracter, y lo borra del vector de caracteres
///
/// # Parámetros
/// - `act`: String resultado
/// - `pila`: pila auxiliar
/// - `c`: vector de caracteres
///
/// # Retorna
/// - () en caso de éxito, error de ExpresionJson en otro caso
fn desapilar_caracter(
    act: &mut String,
    pila: &mut Pila<char>,
    c: &mut Vec<char>,
    char_apertura: char,
) -> Result<(), ExpresionJson> {
    if char_apertura == *pila.ver_tope()? {
        pila.desapilar()?;
        act.push(c[0]);
        c.remove(0);
        Ok(())
    } else {
        Err(ExpresionJson::new_invalid_json_err())
    }
}

/// Agrega una comilla al String, y la borra del vector de caracteres
///
/// # Parámetros
/// - `act`: String resultado
/// - `c`: vector de caracteres
fn agregar_comilla(act: &mut String, c: &mut Vec<char>, en_str: &mut bool) {
    act.push(c[0]);
    c.remove(0);
    *en_str = !*en_str;
}

#[cfg(test)]
mod tests {
    use crate::{json_parser::parser::separar_por_caracter, pila::Pila};

    #[test]
    fn test_separar_por_comas() {
        let objeto = "\"hola\": \"chau\",\"avion\": 23";
        assert_eq!(
            separar_por_caracter(
                &mut objeto.chars().collect(),
                &mut Pila::crear(5),
                false,
                String::new(),
                &','
            )
            .unwrap(),
            ["\"hola\":\"chau\"", "\"avion\":23"]
        );
    }

    #[test]
    fn test_separar_por_puntos() {
        let objeto = "\"hola\": \"chau\"";
        assert_eq!(
            separar_por_caracter(
                &mut objeto.chars().collect(),
                &mut Pila::crear(5),
                false,
                String::new(),
                &':'
            )
            .unwrap(),
            ["\"hola\"", "\"chau\""]
        );
    }
}
