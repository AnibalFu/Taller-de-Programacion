// Autentificacion redis
pub(crate) const AUTH_CMD: &str = "AUTH user default";

// Filtros para parse
pub(crate) const REQUEST_MESSAGE_PREFIX: &str = "3)";

// Campos json esperados de las request de usuarios
pub(crate) const PROMPT_KEY: &str = "prompt";
pub(crate) const CHANNEL_RESPONSE_KEY: &str = "response_channel";
pub(crate) const REQUEST_ID_KEY: &str = "requestId";
pub(crate) const DOC_ID_KEY: &str = "docId";
pub(crate) const TYPE_REQUEST_KEY: &str = "type_request";

// Tipos esperados de request que puede mandar el usuario
pub(crate) const LOCAL_REQUEST: &str = "local";
pub(crate) const GLOBAL_REQUEST: &str = "global";
pub(crate) const DOC_TEXT_RESPONSE: &str = "doc_text";

// Tipos esperados de request para mandar al microservicio de control
pub(crate) const GET_DOC_TEXT: &str = "get";

// Campo de texto del doc para request globales
pub(crate) const DOC_TEXT_KEY: &str = "text";
pub(crate) const STATUS_KEY: &str = "status";

// Tiempo de espera para el texto del documento solicitado
pub(crate) const TIMEOUT_DOC_TEXT_SEC: u64 = 5;
