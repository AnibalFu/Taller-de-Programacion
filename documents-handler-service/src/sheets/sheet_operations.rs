use std::{
    sync::{Arc, Mutex},
    vec,
};

use common::char_entry::CharEntry;
use events::events::{
    join_event::JoinEvent,
    list_event::ListEvent,
    sheet_creation_event::SheetCreationEvent,
    sheet_insert_event::SheetInsertEvent,
    sheet_operations_event::{SheetOperationEvent, SheetOperationsEvent},
};
use logger::logger::Logger;
use redis_client::driver::{redis_driver::RedisDriver, traits::FromRedis};

use crate::{
    document_error::DocumentError, documents::DocumentResult, sheets::sheet_tracker::SheetTracker,
};

/// Saves all sheets in the SheetTracker to Redis.
/// ### Arguments
/// * `sheet_tracker_clone` - A reference to the SheetTracker wrapped in an Arc and
///   Mutex.
/// * `redis_driver` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `save_logger` - A reference to the Logger used for logging events.
pub fn save_all_sheets(
    sheet_tracker_clone: &Arc<Mutex<SheetTracker>>,
    redis_driver: &mut RedisDriver,
    save_logger: &Logger,
) {
    if let Ok(mut sheet_tracker) = sheet_tracker_clone.lock() {
        let sheet_ids: Vec<usize> = sheet_tracker.get_all_sheets().keys().copied().collect();

        for sheet_id in sheet_ids {
            if let Err(e) = save_sheet(redis_driver, &mut sheet_tracker, sheet_id) {
                save_logger.error(
                    format!("Failed to save sheet {sheet_id}: {e}").as_str(),
                    module_path!(),
                );
            } else if let Ok(sheet) = sheet_tracker.get_sheet(sheet_id) {
                save_logger.info(
                    format!("Sheet {}:{} saved successfully.", sheet.name, sheet_id).as_str(),
                    module_path!(),
                );
            }
        }
    } else {
        save_logger.error(
            "Failed to lock sheet tracker for saving sheets.",
            module_path!(),
        );
    }
}

/// Handles the join event for a sheet.
/// ### Arguments
/// * `sheet_tracker` - A mutable reference to the SheetTracker.
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
/// * `incoming_join_event` - The JoinEvent containing the user ID and document ID.
pub fn join_sheet_event_handler(
    sheet_tracker: &mut SheetTracker,
    conn: &mut RedisDriver,
    logger: &Logger,
    incoming_join_event: JoinEvent,
) -> DocumentResult<()> {
    add_user_to_sheet(
        conn,
        &incoming_join_event.user_id,
        incoming_join_event.document_id,
    )?;
    sheet_tracker.add_user_to_sheet(
        incoming_join_event.document_id,
        incoming_join_event.user_id.clone(),
    )?;
    logger.info(
        format!(
            "User {} joined sheet {}",
            incoming_join_event.user_id, incoming_join_event.document_id
        )
        .as_str(),
        module_path!(),
    );
    publish_sheet_content(conn, sheet_tracker, incoming_join_event.document_id)?;
    Ok(())
}

/// Handles the operations event for a sheet.
/// ### Arguments
/// * `sheet_tracker` - A mutable reference to the SheetTracker.
/// * `logger` - A reference to the Logger used for logging events.
/// * `sheet_operations_event` - The SheetOperationsEvent containing a list of operations to be
///   applied to the sheet.
pub fn sheet_operations_handler(
    sheet_tracker: &mut SheetTracker,
    logger: &Logger,
    sheet_operations_event: SheetOperationsEvent,
) -> Result<(), DocumentError> {
    for operation in sheet_operations_event.operations {
        match operation {
            SheetOperationEvent::InsertIntoColumn(insertion_event) => {
                let entries = get_entries_from_sheet_content(&insertion_event)?;

                sheet_tracker.insert_into_sheet(
                    insertion_event.id,
                    insertion_event.row,
                    insertion_event.column,
                    insertion_event.position,
                    entries,
                )?;

                logger.info(
                    format!("Insertion in sheet {} done", insertion_event.id).as_str(),
                    module_path!(),
                );
            }
            SheetOperationEvent::DeleteIntoColumn(deletion_event) => {
                sheet_tracker.delete_from_sheet(
                    deletion_event.id,
                    deletion_event.row,
                    deletion_event.column,
                    deletion_event.start,
                    deletion_event.end,
                )?;

                logger.info(
                    format!("Deletion in sheet {} done", deletion_event.id).as_str(),
                    module_path!(),
                );
            }
        }
    }
    Ok(())
}

/// Handles the creation of a new sheet.
/// ### Arguments
/// * `sheet_tracker` - A mutable reference to the SheetTracker.
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
/// * `sheet_creation_event` - The SheetCreationEvent containing the name, width, height
///   and user ID of the new sheet.
pub fn sheet_creation_handler(
    sheet_tracker: &mut SheetTracker,
    conn: &mut RedisDriver,
    logger: &Logger,
    sheet_creation_event: SheetCreationEvent,
) -> Result<(), DocumentError> {
    let new_sheet_id = sheet_tracker.create_new_sheet(
        sheet_creation_event.name.as_str(),
        sheet_creation_event.width,
        sheet_creation_event.height,
    )?;
    logger.info(
        format!(
            "New sheet {} created by {} with ID: {}",
            sheet_creation_event.name, sheet_creation_event.user_id, new_sheet_id
        )
        .as_str(),
        module_path!(),
    );
    store_sheet_name(conn, sheet_creation_event.name.as_str(), new_sheet_id)?;
    add_user_to_sheet(conn, sheet_creation_event.user_id.as_str(), new_sheet_id)?;
    publish_sheet_creation(conn, sheet_creation_event, new_sheet_id)?;
    Ok(())
}

/// Handles the sheet event to list sheets or documents based on the event type.
/// and then publishes the result to the user's own channel.
/// ### Arguments
/// * `tracker` - A mutable reference to the DocumentTracker.
/// * `sheet_tracker` - A reference to the SheetTracker wrapped in an Arc and Mutex
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
pub fn list_sheets(conn: &mut RedisDriver, list_event: &ListEvent) -> Result<(), DocumentError> {
    let command = vec![
        "LRANGE".to_string(),
        "ms:sheets".to_string(),
        "0".to_string(),
        "-1".to_string(),
    ];
    let response = conn.safe_command(command)?;
    let sheets: Vec<String> = Vec::from_redis(response)?;
    let sheets_str = if sheets.is_empty() {
        "empty".to_string()
    } else {
        sheets.join(",")
    };
    let command = vec![
        "PUBLISH".to_string(),
        format!("users:{}", list_event.user_id),
        format!("response\\:files\\;files\\:{}", sheets_str),
    ];
    conn.safe_command(command)?;
    Ok(())
}

/// Retrieves sheets from Redis and populates the SheetTracker.
/// This function fetches all sheets stored in Redis, parses their names and IDs,
/// and adds them to the SheetTracker.
/// ### Arguments
/// * `tracker` - A mutable reference to the DocumentTracker.
/// * `sheet_tracker` - A reference to the SheetTracker wrapped in an Arc and Mutex
/// * `conn` - A mutable reference to the RedisDriver used to interact with Redis.
/// * `logger` - A reference to the Logger used for logging events.
pub fn retrieve_sheets_from_redis(
    sheet_tracker: &Arc<Mutex<SheetTracker>>,
    redis_driver: &mut RedisDriver,
    logger: &Logger,
) -> Result<(), DocumentError> {
    let command = vec![
        "LRANGE".to_string(),
        "ms:sheets".to_string(),
        "0".to_string(),
        "-1".to_string(),
    ];
    if let Ok(response) = redis_driver.safe_command(command) {
        if let Ok(sheets) = Vec::<String>::from_redis(response) {
            for entry in sheets {
                if let Some((name, id_str)) = entry.rsplit_once(':') {
                    if let Ok(id) = id_str.parse::<usize>() {
                        let command = vec!["GET".to_string(), format!("sheets:{}", id)];
                        if let Ok(content) = redis_driver.safe_command(command) {
                            let content = String::from_redis(content)?;
                            if let Ok(mut tracker) = sheet_tracker.lock() {
                                let _ = tracker.add_existing_sheet(id, name.to_string(), content);
                                logger.info(
                                    format!("Loaded sheet {name} with ID: {id}").as_str(),
                                    module_path!(),
                                );
                            }
                        }
                    }
                }
            }
            logger.info("Loaded sheets from Redis.", module_path!());
        }
    }
    Ok(())
}

/* PRIVATE API */

fn save_sheet(
    conn: &mut RedisDriver,
    tracker: &mut SheetTracker,
    sheet_id: usize,
) -> DocumentResult<()> {
    let sheet = tracker.get_sheet(sheet_id);
    if let Ok(sheet) = sheet {
        let key = format!("sheets:{sheet_id}");
        let command = vec!["SET".to_string(), key.clone(), sheet.to_raw_string()];
        conn.safe_command(command)?;
    }
    Ok(())
}

fn publish_sheet_content(
    conn: &mut RedisDriver,
    sheet_tracker: &mut SheetTracker,
    sheet_id: usize,
) -> DocumentResult<()> {
    let sheet = sheet_tracker.get_sheet(sheet_id)?;
    let key: String = format!("sheets:{sheet_id}");
    let content = sheet.to_raw_string();
    let users = sheet.get_users().join(",");
    let msg = format!(
        "action\\:sync\\;content\\:{}\\;users\\:{}\\;width\\:{}\\;height\\:{}",
        content, users, sheet.content.width, sheet.content.height
    );
    let command = vec!["PUBLISH".to_string(), key, msg];
    conn.safe_command(command)?;
    Ok(())
}

fn add_user_to_sheet(conn: &mut RedisDriver, user_id: &str, sheet_id: usize) -> DocumentResult<()> {
    let key = format!("sheets:{sheet_id}:users");
    let command = vec!["SADD".to_string(), key, format!("\"{user_id}\"")];
    conn.safe_command(command)?;
    Ok(())
}

fn get_entries_from_sheet_content(
    insert_event: &SheetInsertEvent,
) -> DocumentResult<Vec<CharEntry>> {
    let mut entries = Vec::new();
    for ch in insert_event.value.chars() {
        let timestamp = 0;
        let char_entry = CharEntry::new(ch, timestamp, insert_event.user_id.as_str());
        entries.push(char_entry);
    }
    Ok(entries)
}

fn store_sheet_name(
    conn: &mut RedisDriver,
    sheet_name: &str,
    sheet_id: usize,
) -> DocumentResult<()> {
    let command = vec![
        "RPUSH".to_string(),
        "ms:sheets".to_string(),
        format!("\"{}:{}\"", sheet_name, sheet_id),
    ];
    conn.safe_command(command)?;
    Ok(())
}

fn publish_sheet_creation(
    conn: &mut RedisDriver,
    creation_event: SheetCreationEvent,
    new_id: usize,
) -> DocumentResult<()> {
    let command = vec![
        "PUBLISH".to_string(),
        format!("users:{}", creation_event.user_id),
        format!(
            "response\\:sheet_creation\\;id\\:{}\\;name\\:{}\\;width\\:{}\\;height\\:{}",
            new_id, creation_event.name, creation_event.width, creation_event.height
        ),
    ];

    conn.safe_command(command)?;
    Ok(())
}
