//! Este modulo almacena las operaciones basicas

use super::operaciones_artimeticas;
use super::operaciones_logicas;
use super::operaciones_output;
use super::operaciones_stack;
use crate::estructuras::errores::Error;
use crate::estructuras::pila::Pila;
use std::collections::HashMap;

pub type Operaciones = HashMap<String, Box<dyn Fn(&mut Pila<i16>) -> Result<String, Error>>>;

/// Almacena las operaciones predeterminadas en un diccionario junto a su respectiva operatoria
pub fn crear_dicc_op() -> Operaciones {
    let mut dicc: Operaciones = HashMap::new();
    dicc.insert("+".to_string(), Box::new(operaciones_artimeticas::suma));
    dicc.insert("-".to_string(), Box::new(operaciones_artimeticas::resta));
    dicc.insert(
        "*".to_string(),
        Box::new(operaciones_artimeticas::multiplicacion),
    );
    dicc.insert("/".to_string(), Box::new(operaciones_artimeticas::division));
    dicc.insert("dup".to_string(), Box::new(operaciones_stack::dup));
    dicc.insert("drop".to_string(), Box::new(operaciones_stack::drop));
    dicc.insert("swap".to_string(), Box::new(operaciones_stack::swap));
    dicc.insert("over".to_string(), Box::new(operaciones_stack::over));
    dicc.insert("rot".to_string(), Box::new(operaciones_stack::rot));
    dicc.insert(".".to_string(), Box::new(operaciones_output::imprimir_ult));
    dicc.insert("emit".to_string(), Box::new(operaciones_output::emit));
    dicc.insert("cr".to_string(), Box::new(operaciones_output::cr));
    dicc.insert("=".to_string(), Box::new(operaciones_logicas::igualdad));
    dicc.insert("<".to_string(), Box::new(operaciones_logicas::menor));
    dicc.insert(">".to_string(), Box::new(operaciones_logicas::mayor));
    dicc.insert("and".to_string(), Box::new(operaciones_logicas::and));
    dicc.insert("or".to_string(), Box::new(operaciones_logicas::or));
    dicc.insert("not".to_string(), Box::new(operaciones_logicas::not));
    dicc
}
