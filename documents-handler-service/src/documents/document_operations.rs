use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common::char_entry::CharEntry;
use events::events::{
    creation_event::CreationEvent, disconnect_event::DisconnectEvent,
    full_document_event::FullDocumentEvent, insertion_event::InsertionEvent, join_event::JoinEvent,
    list_event::ListEvent, operation_event::OperationEvent, operations_event::OperationsEvent,
};
use json::json::{ExpresionJson, JsonValue};
use logger::logger::Logger;
use redis_client::{
    driver::{redis_driver::RedisDriver, traits::FromRedis},
    tipos_datos::traits::DatoRedis,
};

use crate::{
    document_error::DocumentError,
    documents::{DocumentResult, documents_tracker::DocumentTracker},
};

/// Stores all documents in the DocumentTracker to Redis.
///
/// ### Arguments
/// * `document_tracker_clone` - A clone of the DocumentTracker wrapped in an Arc and
///   Mutex.
/// * `redis_driver` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `save_logger` - A reference to the Logger used for logging save operations.
/// ### This function iterates through all documents in the DocumentTracker and saves each
/// document's content to Redis. It logs success or failure for each document save operation.
/// If the document is successfully saved, it logs the document's name and ID.
/// If it fails to lock the DocumentTracker, it logs an error.
/// If it fails to save a document, it logs the error.
pub fn save_all_documents(
    document_tracker_clone: &Arc<Mutex<DocumentTracker>>,
    redis_driver: &mut RedisDriver,
    save_logger: &Logger,
) {
    if let Ok(mut tracker) = document_tracker_clone.lock() {
        let document_ids: Vec<usize> = tracker.get_all_documents().keys().copied().collect();
        for document_id in document_ids {
            if let Err(e) = save_document(redis_driver, &mut tracker, document_id) {
                save_logger.error(
                    format!("Failed to save document {document_id}: {e}").as_str(),
                    module_path!(),
                );
            } else if let Some(document) = tracker.get_document(&document_id) {
                save_logger.info(
                    format!(
                        "Document {}:{} saved successfully.",
                        document.name, document_id
                    )
                    .as_str(),
                    module_path!(),
                );
            }
        }
    } else {
        save_logger.error(
            "Failed to lock document tracker for saving documents.",
            module_path!(),
        );
    }
}

/// Saves a specific document to Redis.
/// ### Arguments
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `tracker` - A mutable reference to the DocumentTracker containing the document.
/// * `document_id` - The ID of the document to be saved.
/// ### Returns
/// * `DocumentResult<()>` - A result indicating success or failure of the save operation.
pub fn save_document(
    conn: &mut RedisDriver,
    tracker: &mut DocumentTracker,
    document_id: usize,
) -> DocumentResult<()> {
    let document = tracker.get_document(&document_id);
    if let Some(document) = document {
        let key = format!("documents:{document_id}");
        let command = vec!["SET".to_string(), key, document.content.to_string()];
        conn.safe_command(command)?;
    }
    Ok(())
}

/// Publishes the full document content for a given document ID.
/// ### Arguments
/// * `tracker` - A reference to the DocumentTracker wrapped in an Arc and Mutex.
/// * `sheet_tracker` - A reference to the SheetTracker wrapped in an Arc and Mutex.
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `event` - The event to be handled, which can be of various types such as DocumentCreationEvent, OperationsEvent, etc.
/// * `logger` - A reference to the Logger used for logging events.
pub fn full_document_handler(
    tracker: &Arc<Mutex<DocumentTracker>>,
    conn: &mut RedisDriver,
    logger: &Logger,
    full_document_event: FullDocumentEvent,
) -> Result<(), DocumentError> {
    let tracker = tracker.lock()?;
    let id: usize = full_document_event.document_id.parse()?;
    let content = tracker.get_document(&id);
    if let Some(document) = content {
        logger.info(
            format!(
                "Full document event received for document {}",
                full_document_event.document_id,
            )
            .as_str(),
            module_path!(),
        );
        let mut hashmap: HashMap<String, JsonValue> = HashMap::new();

        hashmap.insert("docId".to_string(), JsonValue::Number(id as f64));
        hashmap.insert(
            "text".to_string(),
            JsonValue::String(document.content.to_string()),
        );
        hashmap.insert(
            "type_request".to_string(),
            JsonValue::String("doc_text".to_string()),
        );

        let result = ExpresionJson::new_from_hashmap(hashmap);
        let command = vec![
            "PUBLISH".to_string(),
            "llm:request".to_string(),
            format!("\"{result}\""),
        ];
        conn.safe_command(command)?;
    }
    Ok(())
}

/// Handles the join event for a document.
/// ### Arguments
/// * `tracker` - A mutable reference to the DocumentTracker.
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
pub fn join_event_handler(
    tracker: &mut DocumentTracker,
    conn: &mut RedisDriver,
    logger: &Logger,
    incoming_join_event: JoinEvent,
) -> Result<(), DocumentError> {
    add_user_to_document(
        conn,
        &incoming_join_event.user_id,
        incoming_join_event.document_id,
    )?;
    tracker.add_user_to_document(
        incoming_join_event.document_id,
        incoming_join_event.user_id.clone(),
    )?;
    logger.info(
        format!(
            "User {} joined document {}",
            incoming_join_event.user_id, incoming_join_event.document_id
        )
        .as_str(),
        module_path!(),
    );
    publish_document_content(conn, tracker, incoming_join_event.document_id)?;
    Ok(())
}

/// Handles the list event for documents
/// ### Arguments
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `list_event` - The ListEvent containing the user ID and file type.
pub fn list_documents(conn: &mut RedisDriver, list_event: &ListEvent) -> Result<(), DocumentError> {
    let command = vec![
        "LRANGE".to_string(),
        "ms:documents".to_string(),
        "0".to_string(),
        "-1".to_string(),
    ];
    let response = conn.safe_command(command)?;
    let documents: Vec<String> = Vec::from_redis(response)?;
    let documents_str = if documents.is_empty() {
        "empty".to_string()
    } else {
        documents.join(",")
    };
    let command = vec![
        "PUBLISH".to_string(),
        format!("users:{}", list_event.user_id),
        format!("response\\:files\\;files\\:{}", documents_str),
    ];
    conn.safe_command(command)?;
    Ok(())
}

/// Handles the disconnect event for a document.
/// ### Arguments
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
/// * `incoming_disconnect_event` - The DisconnectEvent containing the user ID and document ID.
pub fn disconnect_event_handler(
    conn: &mut RedisDriver,
    logger: &Logger,
    incoming_disconnect_event: DisconnectEvent,
) -> Result<(), DocumentError> {
    remove_user_from_document(
        conn,
        &incoming_disconnect_event.user_id,
        incoming_disconnect_event.document_id,
    )?;
    logger.info(
        format!(
            "User {} disconnected from document {}",
            incoming_disconnect_event.user_id, incoming_disconnect_event.document_id
        )
        .as_str(),
        module_path!(),
    );
    publish_user_disconnect(
        conn,
        &incoming_disconnect_event.user_id,
        incoming_disconnect_event.document_id,
    )?;
    Ok(())
}

/// Handles the creation event for a document.
/// ### Arguments
/// * `tracker` - A mutable reference to the DocumentTracker.
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
/// * `creation_event` - The CreationEvent containing the user ID and document name.
pub fn creation_event_handler(
    tracker: &mut DocumentTracker,
    conn: &mut RedisDriver,
    logger: &Logger,
    creation_event: CreationEvent,
) -> Result<(), DocumentError> {
    let new_document_id = tracker.create_new_document(creation_event.name.as_str())?;
    store_document_name(conn, creation_event.name.as_str(), new_document_id)?;
    add_user_to_document(conn, creation_event.user_id.as_str(), new_document_id)?;
    logger.info(
        format!(
            "New document {} created by {}",
            creation_event.name, creation_event.user_id
        )
        .as_str(),
        module_path!(),
    );
    publish_creation(conn, creation_event, new_document_id)?;
    logger.info(
        format!("Document created with ID: {new_document_id}").as_str(),
        module_path!(),
    );
    Ok(())
}

/// Handles the operations event for a document.
/// ### Arguments
/// * `tracker` - A mutable reference to the DocumentTracker.
/// * `logger` - A reference to the Logger used for logging events.
/// * `incoming_operations_event` - The OperationsEvent containing a list of operations to be applied to the document.
pub fn operation_event_handler(
    tracker: &mut DocumentTracker,
    logger: &Logger,
    incoming_operations_event: OperationsEvent,
) -> Result<(), DocumentError> {
    let mut deletions = Vec::new();
    let mut insertions = Vec::new();

    for op in &incoming_operations_event.operations {
        match op {
            OperationEvent::Deletion(d) => deletions.push(d),
            OperationEvent::Insertion(i) => insertions.push(i),
        }
    }

    deletions.sort_by(|a, b| b.start_position.cmp(&a.start_position));
    insertions.sort_by_key(|i| i.position);
    for deletion in deletions {
        tracker.delete_from_document(
            deletion.id,
            deletion.start_position,
            deletion.end_position,
        )?;
        logger.info(
            format!("Deletion in document {} done", deletion.id).as_str(),
            module_path!(),
        );
    }

    for insertion in insertions {
        let entries = get_entries_from_content(tracker, insertion)?;
        tracker.insert_into_document(insertion.id, insertion.position, entries)?;
        logger.info(
            format!("Insertion in document {} done", insertion.id).as_str(),
            module_path!(),
        );
    }
    Ok(())
}

/// Retrieves documents from Redis and populates the DocumentTracker.
/// ### Arguments
/// * `document_tracker` - A reference to the DocumentTracker wrapped in an Arc and Mutex
/// * `redis_driver` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
pub fn retrieve_documents_from_redis(
    document_tracker: &Arc<Mutex<DocumentTracker>>,
    redis_driver: &mut RedisDriver,
    logger: &Logger,
) -> Result<(), DocumentError> {
    let command = vec![
        "LRANGE".to_string(),
        "ms:documents".to_string(),
        "0".to_string(),
        "-1".to_string(),
    ];
    if let Ok(response) = redis_driver.safe_command(command) {
        if let Ok(documents) = Vec::<String>::from_redis(response) {
            for entry in documents {
                if let Some((name, id_str)) = entry.rsplit_once(':') {
                    if let Ok(id) = id_str.parse::<usize>() {
                        let command = vec!["GET".to_string(), format!("documents:{}", id)];
                        if let Ok(content) = redis_driver.safe_command(command) {
                            if let DatoRedis::Null(_) = content {
                                logger.warn(
                                    format!("Document with ID {id} not found in Redis.").as_str(),
                                    module_path!(),
                                );
                                continue;
                            }
                            let content = String::from_redis(content)?;
                            if let Ok(mut tracker) = document_tracker.lock() {
                                let _ =
                                    tracker.add_existing_document(id, name.to_string(), content);
                                logger.info(
                                    format!("Loaded document {name} with ID: {id}").as_str(),
                                    module_path!(),
                                );
                            }
                        }
                    }
                }
            }
            logger.info("Loaded documents from Redis.", module_path!());
        }
    }
    Ok(())
}

/* PRIVATE API */

fn add_user_to_document(
    conn: &mut RedisDriver,
    user_id: &str,
    document_id: usize,
) -> DocumentResult<()> {
    let key = format!("documents:{document_id}:users");
    let command = vec!["SADD".to_string(), key, format!("\"{user_id}\"")];
    conn.safe_command(command)?;
    Ok(())
}

fn publish_document_content(
    conn: &mut RedisDriver,
    tracker: &mut DocumentTracker,
    document_id: usize,
) -> DocumentResult<()> {
    let document = tracker.get_document(&document_id);
    if let Some(document) = document {
        let key: String = format!("documents:{document_id}");
        let content = document.content.to_string();
        let users = document.get_users().join(",");
        let msg = format!("action\\:sync\\;content\\:{content}\\;users\\:{users}");
        let command = vec!["PUBLISH".to_string(), key, msg];
        conn.safe_command(command)?;
    }
    Ok(())
}

fn get_entries_from_content(
    document_tracker: &mut DocumentTracker,
    insert_event: &InsertionEvent,
) -> DocumentResult<Vec<CharEntry>> {
    let mut entries = Vec::new();
    for ch in insert_event.content.chars() {
        let timestamp = document_tracker.get_next_lamport_timestamp();
        let char_entry = CharEntry::new(ch, timestamp, insert_event.user_id.as_str());
        entries.push(char_entry);
    }
    Ok(entries)
}

fn publish_user_disconnect(
    conn: &mut RedisDriver,
    user_id: &str,
    document_id: usize,
) -> DocumentResult<()> {
    let command = vec![
        "PUBLISH".to_string(),
        format!("documents:{}", document_id),
        format!("action\\:disconnected\\;user\\:{user_id}"),
    ];
    conn.safe_command(command)?;
    Ok(())
}

fn publish_creation(
    conn: &mut RedisDriver,
    creation_event: CreationEvent,
    new_id: usize,
) -> DocumentResult<()> {
    let command = vec![
        "PUBLISH".to_string(),
        format!("users:{}", creation_event.user_id),
        format!("response\\:creation\\;id\\:{}", new_id),
    ];
    conn.safe_command(command)?;
    Ok(())
}

fn remove_user_from_document(
    conn: &mut RedisDriver,
    user_id: &str,
    document_id: usize,
) -> DocumentResult<()> {
    let command = vec![
        "SREM".to_string(),
        format!("documents:{document_id}"),
        format!("\"{user_id}\""),
    ];
    conn.safe_command(command)?;
    Ok(())
}

fn store_document_name(
    conn: &mut RedisDriver,
    document_name: &str,
    document_id: usize,
) -> DocumentResult<()> {
    let key = "ms:documents".to_string();
    let command = vec![
        "RPUSH".to_string(),
        key,
        format!("\"{document_name}:{document_id}\""),
    ];
    conn.safe_command(command)?;
    Ok(())
}
