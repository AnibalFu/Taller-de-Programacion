//! Este módulo contiene la estructura correspondiente al formato Json
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ExpresionJson {
    Arreglo(Vec<Box<ExpresionJson>>),
    Objeto(HashMap<String, Box<ExpresionJson>>),
    Literal(LiteralJson),
    Error(JsonError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralJson {
    StringJson(String),
    NumberJson(f64),
    BooleanJson(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonError {
    InvalidJsonFormat,
    InvalidJsonTypeForFunction,
}

impl ExpresionJson {
    pub fn new_invalid_json_err() -> ExpresionJson {
        ExpresionJson::Error(JsonError::InvalidJsonFormat)
    }

    pub fn new_invalid_json_type() -> ExpresionJson {
        ExpresionJson::Error(JsonError::InvalidJsonTypeForFunction)
    }
}
