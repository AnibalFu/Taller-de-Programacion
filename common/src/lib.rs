pub mod char_entry;
pub mod common_error;
pub mod consts_resp;
pub mod cr16;
pub mod lcs;
pub mod pila;
pub mod sheet;
pub mod text;
pub mod thread_pool;

// This is a logical timestamp, it is not a real timestamp.
// It is used to order events in a distributed system.
// It is incremented every time an event is created.
// It is used to ensure that events are processed in the order they were created.
pub type LamportTimestamp = usize;
pub type CommonResult<T> = Result<T, common_error::CommonError>;

pub fn to_raw_string(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            '\\' => "\\\\".to_string(),
            '"' => "\\\"".to_string(),
            _ => c.to_string(),
        })
        .collect()
}

pub fn from_raw_string(s: &str) -> String {
    s.replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\\\", "\\")
        .replace("\\\"", "\"")
}

pub fn remove_quotes(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}
