use crate::llm_error::LLMError;
use json::libreria_json::get_value_json;
use reqwest::blocking::Client;

const URL_LLM: &str =
    "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key=";

/// Algo capaz de contestar un *prompt* utilizando un LLM.
///
/// *  `Send + Sync` permite compartir una misma instancia entre hilos.
pub trait LlmClient: Send + Sync + Clone + 'static {
    fn answer(&self, prompt: String) -> Result<String, LLMError>;
}

/// Cliente real
#[derive(Clone)]
pub struct RealLlm {
    endpoint: String,
    http: Client,
}
impl RealLlm {
    /// * `api_key`: Clave de API de Google AI Studio.
    pub fn new(api_key: String) -> Self {
        let endpoint = format!("{}{}", URL_LLM, api_key.trim());

        Self {
            endpoint,
            http: Client::new(),
        }
    }
}

impl LlmClient for RealLlm {
    /// Llama a **Gemini 1.5 Flash** con un prompt dado y devuelve la respuesta
    /// de texto del primer candidato.
    ///
    /// Construye el cuerpo JSON manualmente, envía la petición
    /// HTTP y extrae el campo `candidates[0].content.parts[0].text`.
    ///
    /// # Parámetros
    /// * `prompt`: Texto del usuario.
    ///
    /// # Errores
    /// Devuelve `Err` si falla la red, si la respuesta es error HTTP
    /// o si no puede extraer el campo `text`.
    fn answer(&self, prompt: String) -> Result<String, LLMError> {
        let body = format!(
            r#"{{
                "contents":[{{"role":"user","parts":[{{"text":"{prompt}"}}]}}]
            }}"#
        );

        let resp = self
            .http
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|e| LLMError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(LLMError::Api(resp.status().to_string()));
        }

        let text = resp.text().map_err(|e| LLMError::Network(e.to_string()))?;

        println!("[DEBUG]: [{text}]");
        let answer = get_value_json(text, "text");
        let json_answer = format!("{{\"status\": \"ok\", \"text\":{answer}}}");
        Ok(json_answer)
    }
}
