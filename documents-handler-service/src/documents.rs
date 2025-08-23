use crate::document_error::DocumentError;

pub mod document;
pub mod document_operations;
pub mod document_subscriptions_handler;
pub mod documents_tracker;
pub mod redis_document;
pub type DocumentResult<T> = Result<T, DocumentError>;
