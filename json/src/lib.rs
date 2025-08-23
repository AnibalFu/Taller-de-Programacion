pub mod json;
pub mod json_parser;
pub mod libreria_json;
mod pila;

pub fn from_raw_string(s: &str) -> String {
    s.replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\\\", "\\")
        .replace("\\\"", "\"")
}
