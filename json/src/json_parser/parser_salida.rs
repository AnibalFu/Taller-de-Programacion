//! Este módulo contiene la funcionalidad para transformar una ExpresionJson
//! en un String
use crate::json::ExpresionJson;
use crate::json::ExpresionJson::{Arreglo, Literal, Objeto};
use crate::json::LiteralJson::{BooleanJson, NumberJson, StringJson};

impl ExpresionJson {
    /// Parsea una ExpresionJson a un String equivalente
    ///
    /// # Retorna
    /// - String representando el Json
    pub fn armar_string(&self) -> String {
        match self {
            Objeto(pares) => {
                let mut string = String::new();
                let mut pares_str: Vec<String> = Vec::new();
                string.push('{');
                for (k, v) in pares {
                    let par = "\"".to_string() + k + "\"" + ":" + &v.armar_string();
                    pares_str.push(par)
                }
                string.push_str(&pares_str.join(", "));
                string.push('}');
                string
            }
            Arreglo(elementos) => {
                "[".to_string()
                    + &elementos
                        .iter()
                        .map(|n| n.armar_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                    + "]"
            }
            Literal(literal) => match literal {
                StringJson(s) => "\"".to_string() + s + "\"",
                BooleanJson(b) => b.to_string(),
                NumberJson(n) => n.to_string(),
            },
            _ => "Json Inválido".to_string(),
        }
    }
}
