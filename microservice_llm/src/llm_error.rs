use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};

#[derive(Debug)]
pub enum LLMError {
    IOError(std::io::Error),
    SetUpMicroserv(DatoRedis),
    Malformed,
    Api(String),
    Network(String),
}

impl std::fmt::Display for LLMError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LLMError::IOError(e) => write!(f, "IO error: {e}"),
            LLMError::SetUpMicroserv(e) => write!(f, "{}", e.convertir_resp_a_string()),
            LLMError::Malformed => write!(f, "Malformed expresion"),
            LLMError::Api(e) => write!(f, "API error: {e}"),
            LLMError::Network(e) => write!(f, "Network error: {e}"),
        }
    }
}
