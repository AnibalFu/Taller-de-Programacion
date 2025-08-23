use std::collections::HashSet;

use redis_client::driver::{redis_driver::RedisDriver, traits::FromRedis};

use crate::document_error::DocumentError;

use super::{DocumentResult, document::Document};

/// Stores a document in Redis.
/// # Arguments
/// * `conn` - A mutable reference to the Redis connection.
/// * `id` - The unique identifier for the document.
/// * `content` - The content of the document to be stored.
/// # Returns
/// A `DocumentResult<()>` which is an empty result if successful, or an error if the operation fails.
pub fn save_document(conn: &mut RedisDriver, id: usize, content: String) -> DocumentResult<()> {
    let document_key = format!("document:{id}");
    let command = vec!["SET".to_string(), document_key.clone(), content];
    conn.safe_command(command)?;
    Ok(())
}

/// Retrieves a document from Redis by its ID.
/// # Arguments
/// * `conn` - A mutable reference to the Redis connection.
/// * `id` - The unique identifier for the document.
/// # Returns
/// A `DocumentResult<String>` which contains the content of the document if successful, or an error if the operation fails.
pub fn get_document(conn: &mut RedisDriver, id: usize) -> DocumentResult<String> {
    let document_key = format!("document:{id}");
    let command = vec!["GET".to_string(), document_key.clone()];
    let response = conn.safe_command(command);

    let content: String = match response {
        Ok(data) => String::from_redis(data)?,
        Err(e) => {
            return Err(DocumentError::new(
                format!("Failed to get document {id}: {e}"),
                crate::document_error::DocumentErrorKind::NotFound,
            ));
        }
    };

    Ok(content)
}

/// Gets current users connected to a document.
/// # Arguments
/// * `conn` - A mutable reference to the Redis connection.
/// * `id` - The unique identifier for the document.
/// * `user_id` - The unique identifier for the user to be added.
/// # Returns
/// A `DocumentResult<()>` which is an empty result if successful, or an error if the operation fails.
pub fn get_current_users(conn: &mut RedisDriver, id: usize) -> DocumentResult<HashSet<String>> {
    let users_key = format!("document:{id}:users");
    let command = vec!["SMEMBERS".to_string(), users_key.clone()];
    let response = conn.safe_command(command);

    let users: HashSet<String> = match response {
        Ok(data) => HashSet::from_redis(data)?,
        Err(e) => {
            return Err(DocumentError::new(
                format!("Failed to get current users for document {id}: {e}"),
                crate::document_error::DocumentErrorKind::NotFound,
            ));
        }
    };

    Ok(users)
}

pub fn publish_document_creation_event(
    conn: &mut RedisDriver,
    document: &Document,
) -> DocumentResult<()> {
    let channel = format!("{}{}", "document_creation:", document.id);
    let message = format!(
        "{{\"id\": {}, \"content\": \"{}\"}}",
        document.id, document.content
    );
    let command = vec!["PUBLISH".to_string(), channel.clone(), message];
    conn.safe_command(command)?;

    Ok(())
}
