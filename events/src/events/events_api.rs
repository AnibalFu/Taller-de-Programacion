use common::{
    CommonResult,
    common_error::{CommonError, CommonErrorKind},
    remove_quotes,
};
use json::json_parser::parser::obtener_json_raw;

use crate::events::{operation_event::OperationEvent, sheet_operations_event::SheetOperationEvent};

use super::event::Event;

// This module defines the events used in the documents handler service.
// Let's keep it in mind that redis messges from pubsub events are something like this:
// vec: Vec<String> = ["message", "documents:create", "new_document_id"]
// So we might want to check that the second element is the channel we are interested in.
const CHANNEL_INDEX: usize = 2;
const DOCUMENT_ID_INDEX: usize = 1;
const OPERATION_PARAMS_INDEX: usize = 3;
const OPERATION_INDEX: usize = 0;
const UTILS_CHANNEL: &str = "documents:utils";
const SHEETS_UTILS_CHANNEL: &str = "sheets:utils";

/****************

    API FUNCTIONS

*******************/

pub fn handle_microservice_pubsub_message(message: Vec<String>) -> CommonResult<Event> {
    if message[CHANNEL_INDEX].contains("documents") {
        documents_message_handler(message)
    } else if message[CHANNEL_INDEX].contains("sheets") {
        sheets_message_handler(message)
    } else {
        Err(CommonError::new(
            format!("Invalid channel in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        ))
    }
}

/// This function handles incoming messages from the Redis Pub/Sub channel.
/// It validates the message format and determines whether it is a creation event or an edition event.
/// # Arguments
/// * `message` - A vector of strings representing the message received from the Pub/Sub channel
///   these messages are expected to be in the format:
/// - For creation events: ["message", "sheets:utils", "action:create;sheet_name:new_sheet_name;user_id:123;width:5;height:5"]
/// - For edition events: ["message", "sheets:*", "action:edition;user_id:123;op:insert;column:0;row:0;value:Hello"]
/// - For deletion events: ["message", "sheets:*", "action:edition;user_id:123;op:delete;column:0;row:0;start:0;end:5"]
/// - For joining events: ["message", "sheets:*", "action:join;user_id:123"]
/// - For disconnecting events: ["message", "sheets:*", "action:disconnect;user_id:123"]
/// - For sync events: ["message", "sheets:*", "action:sync;content:Hello;users:user1,user2"]
fn sheets_message_handler(message: Vec<String>) -> CommonResult<Event> {
    validate_message(&message, 4)?;

    if message[CHANNEL_INDEX].eq(SHEETS_UTILS_CHANNEL) {
        return sheets_utils_event_handler(&message);
    }

    let (action, content) = message[OPERATION_PARAMS_INDEX]
        .split_once("\\;")
        .ok_or_else(|| {
            CommonError::new(
                format!("Invalid action in message: {}", message.join(" ")),
                CommonErrorKind::EventPubSub,
            )
        })?;

    let channel = &message[CHANNEL_INDEX];
    let channel = channel.split(":").collect::<Vec<&str>>();
    let document_id = channel[DOCUMENT_ID_INDEX];
    let action = action.split("\\:").collect::<Vec<&str>>()[1];
    match action {
        "join" => create_join_event(document_id, content, "sheets".to_string()),
        "disconnect" => create_disconnect_event(document_id, content),
        "edition" => sheet_edition_handler(document_id, content),
        "sync" => sheet_sync_event_handler(content),
        _ => Err(CommonError::new(
            format!("Invalid action in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )),
    }
}

/// This function handles incoming messages from the Redis Pub/Sub channel.
/// It validates the message format and determines whether it is a creation event or an edition event.
/// # Arguments
/// * `message` - A vector of strings representing the message received from the Pub/Sub channel.
///   these messages are expected to be in the format:
/// - For creation events: ["message", "documents:create", "document_name"]
/// - For edition events: ["message", "documents:*", "documents:document_id", "action:edition;user_id:123;op:insert;;content:Hello;position:0|"]
/// - For deletion events: ["message", "documents:*", "documents:document_id", "action:edition;user_id:123;op:delete;;end_position:5;start_position:0|"]
/// - For joining events: ["message", "documents:*", "documents:document_id", "action:join;user_id:123"]
/// - For disconnecting events: ["message", "documents:*", "documents:document_id", "action:disconnect;user_id:123"]
/// - For sync events: ["message", "documents:*", "documents:document_id", "action:sync;content:Hello;users:anibal,giu,tommy,camila"]
/// # Returns
/// A `CommonResult<IncomingEvent>` which is an instance of `IncomingEvent` if successful, or an error if the message
/// format is invalid or the operation is not recognized.
fn documents_message_handler(message: Vec<String>) -> Result<Event, CommonError> {
    validate_message(&message, 4)?;

    let json = obtener_json_raw(message[OPERATION_PARAMS_INDEX].clone());
    if let Ok(json) = json {
        let response_channel = json.get_value("response_channel").unwrap();
        let document_id = json.get_value("docId").unwrap();
        Ok(Event::new_full_document_event(
            remove_quotes(document_id.as_str()),
            remove_quotes(response_channel.as_str()),
        ))
    } else {
        if message[CHANNEL_INDEX].eq(UTILS_CHANNEL) {
            return utils_event_handler(&message);
        }

        let (action, content) = message[OPERATION_PARAMS_INDEX]
            .split_once("\\;")
            .ok_or_else(|| {
                CommonError::new(
                    format!("Invalid action in message: {}", message.join(" ")),
                    CommonErrorKind::EventPubSub,
                )
            })?;

        let channel = &message[CHANNEL_INDEX];
        let channel = channel.split(":").collect::<Vec<&str>>();
        let document_id = channel[DOCUMENT_ID_INDEX];
        let action = action.split("\\:").collect::<Vec<&str>>()[1];
        match action {
            "join" => create_join_event(document_id, content, "documents".to_string()),
            "disconnect" => create_disconnect_event(document_id, content),
            "edition" => edition_event_handler(document_id, content),
            _ => Err(CommonError::new(
                format!("Invalid action in message: {}", message.join(" ")),
                CommonErrorKind::EventPubSub,
            )),
        }
    }
}

pub fn handle_client_pubsub_message(message: Vec<String>) -> CommonResult<Event> {
    validate_message(&message, 3)?;

    let channel = &message[1];
    let ch_prefix = channel.split(":").collect::<Vec<&str>>()[0];
    if ch_prefix == "users" {
        users_event_handler(message)
    } else if ch_prefix == "documents" {
        handle_client_document_pubsub_message(message)
    } else if ch_prefix == "sheets" {
        handle_client_sheet_pubsub_message(message)
    } else {
        Err(CommonError::new(
            format!("Invalid channel prefix in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        ))
    }
}

fn handle_client_document_pubsub_message(message: Vec<String>) -> Result<Event, CommonError> {
    if message[1].eq(UTILS_CHANNEL) {
        return utils_event_handler(&message);
    }

    let (action, content) = message[2].split_once("\\;").ok_or_else(|| {
        CommonError::new(
            format!("Invalid action in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )
    })?;

    let channel = &message[1];
    let channel = channel.split(":").collect::<Vec<&str>>();
    let document_id = channel[DOCUMENT_ID_INDEX];
    let action = action.split("\\:").collect::<Vec<&str>>()[1];
    match action {
        "join" => create_join_event(document_id, content, "documents".to_string()),
        "disconnect" => create_disconnect_event(document_id, content),
        "edition" => edition_event_handler(document_id, content),
        "sync" => sync_event_handler(content),
        _ => Err(CommonError::new(
            format!("Invalid action in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )),
    }
}

fn handle_client_sheet_pubsub_message(message: Vec<String>) -> CommonResult<Event> {
    if message[1].eq(SHEETS_UTILS_CHANNEL) {
        return sheets_utils_event_handler(&message);
    }

    let (action, content) = message[2].split_once("\\;").ok_or_else(|| {
        CommonError::new(
            format!("Invalid action in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )
    })?;

    let channel = &message[1];
    let channel = channel.split(":").collect::<Vec<&str>>();
    let document_id = channel[DOCUMENT_ID_INDEX];
    let action = action.split("\\:").collect::<Vec<&str>>()[1];
    match action {
        "join" => create_join_event(document_id, content, "sheets".to_string()),
        "disconnect" => create_disconnect_event(document_id, content),
        "edition" => sheet_edition_handler(document_id, content),
        "sync" => sheet_sync_event_handler(content),
        _ => Err(CommonError::new(
            format!("Invalid action in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )),
    }
}

/****************

INTERNAL FUNCTIONS

*******************/
/// - For sync events: ["message", "documents:*", "documents:document_id", "action:sync;content:A1,A2,A3;B1,B2,B3;users:anibal,giu,tommy,camila,width:5;height:5"]
fn sheet_sync_event_handler(content: &str) -> CommonResult<Event> {
    let content = content.split("\\;").collect::<Vec<&str>>();
    let sync_content = content[0].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let users = content[1].split("\\:").collect::<Vec<&str>>()[1]
        .split(",")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let width = content[2].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid width in sync event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    let height = content[3].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid height in sync event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    Ok(Event::new_sheet_sync_event(
        sync_content,
        users,
        width,
        height,
    ))
}

fn sheets_utils_event_handler(message: &[String]) -> CommonResult<Event> {
    validate_message(message, 4)?;

    let tokens = message[3]
        .split("\\;")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let action = tokens[0].split("\\:").collect::<Vec<&str>>()[1];

    let event = match action {
        "create" => handle_sheet_creation_event(&tokens),
        "list" => handle_list_event(&tokens),
        _ => {
            return Err(CommonError::new(
                format!("Invalid action in message: {}", message.join(" ")),
                CommonErrorKind::EventPubSub,
            ));
        }
    };

    Ok(event)
}

fn handle_sheet_creation_event(tokens: &[String]) -> Event {
    let name = tokens[1].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let user_id = tokens[2].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let width = tokens[3].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .unwrap_or(5);
    let height = tokens[4].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .unwrap_or(5);

    Event::new_sheet_creation_event(user_id, name, width, height)
}

/// - For edition events: ["message", "sheets:*", "action:edition;user_id:123;op:insert;column:0;row:0;position:0;value:Hello"]
/// - For deletion events: ["message", "sheets:*", "action:edition;user_id:123;op:delete;column:0;row:0;start:0;end:5"]
fn sheet_edition_handler(document_id: &str, tokens: &str) -> CommonResult<Event> {
    let (user_id, operations) = tokens.split_once("\\;").map_or_else(
        || {
            Err(CommonError::new(
                format!("Invalid operation parameters in message: {tokens}"),
                CommonErrorKind::EventPubSub,
            ))
        },
        |(user_id, operations)| Ok((user_id, operations)),
    )?;

    let user_id = user_id.split("\\:").collect::<Vec<&str>>()[1].to_string();

    let parsed_operations = operations
        .split("\\|")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let mut decoded_operations = Vec::new();

    for operation in parsed_operations {
        let operation_params = &operation.split("\\;").collect::<Vec<&str>>();

        let operation_type = operation_params[OPERATION_INDEX]
            .split("\\:")
            .collect::<Vec<&str>>()[1];

        let edit_operation = match operation_type {
            "insert" => create_sheet_insert_event(&user_id, document_id, operation_params),
            "delete" => create_sheet_delete_event(&user_id, document_id, operation_params),
            _ => {
                return Err(CommonError::new(
                    format!("Invalid operation in message: {operations}"),
                    CommonErrorKind::EventPubSub,
                ));
            }
        }?;
        decoded_operations.push(edit_operation);
    }
    Ok(Event::new_sheet_operations_event(decoded_operations))
}

/// - For edition events: ["message", "sheets:*", "action:edition;user_id:123;op:insert;position:0;value:Hello;column:0;row:0"]
fn create_sheet_insert_event(
    user_id: &str,
    document_id: &str,
    operation_params: &[&str],
) -> CommonResult<SheetOperationEvent> {
    let position = operation_params[1].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid position in insert event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;
    
    let value = operation_params[2].split("\\:").collect::<Vec<&str>>()[1].to_string();

    let column = operation_params[3].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid column in insert event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;
    let row = operation_params[4].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid row in insert event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;
    Ok(SheetOperationEvent::new_insert_event(
        document_id.parse::<usize>()?,
        column,
        row,
        user_id.to_string(),
        value,
        position,
    ))
}

fn create_sheet_delete_event(
    user_id: &str,
    document_id: &str,
    operation_params: &[&str],
) -> CommonResult<SheetOperationEvent> {
    let start = operation_params[1].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid start position in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    let end = operation_params[2].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid end position in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    let column = operation_params[4].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid column in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;
    let row = operation_params[5].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid row in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    Ok(SheetOperationEvent::new_delete_event(
        document_id.parse::<usize>()?,
        user_id.to_string(),
        column,
        row,
        start,
        end,
    ))
}

fn sync_event_handler(content: &str) -> CommonResult<Event> {
    let content = content.split("\\;").collect::<Vec<&str>>();
    let sync_content = content[0].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let users = content[1].split("\\:").collect::<Vec<&str>>()[1]
        .split(",")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    Ok(Event::new_sync_event(sync_content, users))
}

fn users_event_handler(message: Vec<String>) -> CommonResult<Event> {
    validate_message(&message, 3)?;
    let channel = &message[1];
    let channel = channel.split(":").collect::<Vec<&str>>();
    let user_id = channel[1].to_string();
    let content = &message[2].split("\\;").collect::<Vec<&str>>();

    let response = content[0].split("\\:").collect::<Vec<&str>>()[1];
    match response {
        "sheet_creation" => {
            let sheet_id = content[1].split("\\:").collect::<Vec<&str>>()[1].to_string();
            let _name = content[2].split("\\:").collect::<Vec<&str>>()[1].to_string();

            let width = content[3].split("\\:").collect::<Vec<&str>>()[1]
                .parse::<usize>()
                .map_err(|_| {
                    CommonError::new(
                        "Invalid width in sheet creation event".to_string(),
                        CommonErrorKind::EventPubSub,
                    )
                })?;

            let height = content[4].split("\\:").collect::<Vec<&str>>()[1]
                .parse::<usize>()
                .map_err(|_| {
                    CommonError::new(
                        "Invalid height in sheet creation event".to_string(),
                        CommonErrorKind::EventPubSub,
                    )
                })?;

            Ok(Event::new_response_sheet_creation_event(
                user_id, sheet_id, width, height,
            ))
        }
        "creation" => {
            let document_id = content[1].split("\\:").collect::<Vec<&str>>()[1].to_string();
            Ok(Event::new_response_creation_event(user_id, document_id))
        }
        "files" => {
            let files = content[1].split("files\\:").collect::<Vec<&str>>()[1];
            if files.eq("empty") {
                Ok(Event::new_response_list_event(user_id, vec![]))
            } else {
                let files = files
                    .split(",")
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>();

                Ok(Event::new_response_list_event(user_id, files))
            }
        }
        _ => Err(CommonError::new(
            format!("Invalid response in message: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        )),
    }
}

// This function handles the creation and listing events for the utils channel.
// It expects the message to be in the format:
// ["message", "documents:utils", "action:create|list", "document_name:new_document_name;user_id:123"]

fn utils_event_handler(message: &[String]) -> CommonResult<Event> {
    let tokens = message[3]
        .split("\\;")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let action = tokens[0].split("\\:").collect::<Vec<&str>>()[1];
    let event = match action {
        "create" => handle_creation_event(&tokens),
        "list" => handle_list_event(&tokens),
        _ => {
            return Err(CommonError::new(
                format!("Invalid action in message: {}", message.join(" ")),
                CommonErrorKind::EventPubSub,
            ));
        }
    };

    Ok(event)
}

/// This function handles the list event for the utils channel.
/// It expects the message to be in the format:
/// ["message", "documents:utils", "action:list;file_type:docs;user_id:123"]
fn handle_list_event(tokens: &[String]) -> Event {
    let user_id = tokens[1].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let file_type = tokens[2].split("\\:").collect::<Vec<&str>>()[1].to_string();

    Event::new_list_event(user_id, file_type)
}

/// This function handles the creation event for the utils channel.
/// It expects the message to be in the format:
/// ["message", "documents:utils", "action:create;document_name:new_document_name;user_id:123"]
fn handle_creation_event(tokens: &[String]) -> Event {
    let document_name = tokens[1].split("\\:").collect::<Vec<&str>>()[1].to_string();
    let user_id = tokens[2].split("\\:").collect::<Vec<&str>>()[1].to_string();

    Event::new_creation_event(document_name, user_id)
}

// This function validates the message format.
// It checks that the message has at least 3 elements: ["message", "channel", "document_name" | "op:insert;..."].
fn validate_message(message: &[String], size: usize) -> CommonResult<()> {
    if message.len() < size {
        return Err(CommonError::new(
            format!("Invalid message format: {}", message.join(" ")),
            CommonErrorKind::EventPubSub,
        ));
    }
    Ok(())
}

/// handles the edition event for a document.
/// It expects the message to be in the format:
///  ["message", "documents:*", "documents:document_id", "op:insert;user_id:123;content:Hello;position:0"]
/// or ["message", "documents:*", "documents:document_id", "op:delete;user_id:123;end_position:5;start_position:0"].
fn edition_event_handler(document_id: &str, tokens: &str) -> CommonResult<Event> {
    let (user_id, operations) = tokens.split_once("\\;").map_or_else(
        || {
            Err(CommonError::new(
                format!("Invalid operation parameters in message: {tokens}"),
                CommonErrorKind::EventPubSub,
            ))
        },
        |(user_id, operations)| Ok((user_id, operations)),
    )?;

    let user_id = user_id.split("\\:").collect::<Vec<&str>>()[1].to_string();

    let parsed_operations = operations
        .split("\\|")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let mut decoded_operations = Vec::new();

    for operation in parsed_operations {
        let operation_params = &operation.split("\\;").collect::<Vec<&str>>();

        let operation_type = operation_params[OPERATION_INDEX]
            .split("\\:")
            .collect::<Vec<&str>>()[1];

        let edit_operation = match operation_type {
            "insert" => create_insert_event(&user_id, document_id, operation_params),
            "delete" => create_delete_event(&user_id, document_id, operation_params),
            _ => {
                return Err(CommonError::new(
                    format!("Invalid operation in message: {operations}"),
                    CommonErrorKind::EventPubSub,
                ));
            }
        }?;
        decoded_operations.push(edit_operation);
    }
    Ok(Event::new_operations_event(decoded_operations))
}

/// creates an insert event from the operation parameters.
/// It expects the operation parameters to be in the format: ["op:insert", "position:0", "content:Hello"].
/// # Arguments
/// * `document_id` - The ID of the document where the insert operation is performed.
/// * `operation_params` - A slice of strings representing the operation parameters.
/// # Returns
/// A `CommonResult<IncomingEvent>` which is an instance of `IncomingEvent::InsertIncoming` if successful, or an error if the parameters are invalid.
fn create_insert_event(
    user_id: &str,
    document_id: &str,
    operation_params: &[&str],
) -> CommonResult<OperationEvent> {
    let position = operation_params[1].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid position in insert event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;
    let content = operation_params[2].split("\\:").collect::<Vec<&str>>()[1].to_string();

    Ok(OperationEvent::new_insertion_event(
        document_id.parse::<usize>()?,
        user_id.to_string(),
        content,
        position,
    ))
}

/// creates a delete event from the operation parameters.
/// It expects the operation parameters to be in the format: ["op:delete", "end_position:5", "start_position:0"].
/// # Arguments
/// * `document_id` - The ID of the document where the delete operation is performed.
/// * `operation_params` - A slice of strings representing the operation parameters.
/// # Returns
/// A `CommonResult<IncomingEvent>` which is an instance of `IncomingEvent::DeletionIncoming` if successful, or an error if the parameters are invalid.
fn create_delete_event(
    user_id: &str,
    document_id: &str,
    operation_params: &[&str],
) -> CommonResult<OperationEvent> {
    let start_position = operation_params[1].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid start position in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    let end_position = operation_params[2].split("\\:").collect::<Vec<&str>>()[1]
        .parse::<usize>()
        .map_err(|_| {
            CommonError::new(
                "Invalid end position in delete event".to_string(),
                CommonErrorKind::EventPubSub,
            )
        })?;

    Ok(OperationEvent::new_deletion_event(
        document_id.parse::<usize>()?,
        user_id.to_string(),
        start_position,
        end_position,
    ))
}

/// This function creates a join event from the content of the message.
/// It expects the content to be in the format: "user_id:123".
/// # Arguments
/// * `document_id` - The ID of the document where the user is joining.
/// * `content` - A string representing the content of the message, which contains the user ID.
/// # Returns
/// A `CommonResult<IncomingEvent>` which is an instance of `IncomingEvent::JoinIncoming` if successful, or an error if the content is invalid.
fn create_join_event(document_id: &str, content: &str, file_type: String) -> CommonResult<Event> {
    let user_id = content.split("\\:").collect::<Vec<&str>>()[1].to_string();
    Ok(Event::new_join_event(
        user_id,
        document_id.parse::<usize>()?,
        file_type,
    ))
}

/// This function creates a disconnect event from the content of the message.
/// It expects the content to be in the format: "user_id:123".
/// # Arguments
/// * `document_id` - The ID of the document where the user is disconnecting.
/// * `content` - A string representing the content of the message, which contains the user ID.
/// # Returns
/// A `CommonResult<IncomingEvent>` which is an instance of `IncomingEvent::DisconnectIncoming` if successful, or an error if the content is invalid.
fn create_disconnect_event(document_id: &str, content: &str) -> Result<Event, CommonError> {
    let user_id = content.split("\\:").collect::<Vec<&str>>()[1].to_string();
    Ok(Event::new_disconnect_event(
        user_id,
        document_id.parse::<usize>()?,
    ))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utils_event_handler_for_creation() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:utils".to_string(),
            "action:create;document_name:new_document_name;user_id:123".to_string(),
        ];
        let event = utils_event_handler(&message).unwrap();
        if let Event::DocumentCreationEvent(creation_event) = event {
            assert_eq!(creation_event.name, "new_document_name");
            assert_eq!(creation_event.user_id, "123");
        }
    }

    #[test]
    fn test_utils_event_handler_for_list() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:utils".to_string(),
            "action:list;user_id:123;file_type:docs".to_string(),
        ];
        let event = utils_event_handler(&message).unwrap();
        if let Event::ListEvent(list_event) = event {
            assert_eq!(list_event.file_type, "docs");
            assert_eq!(list_event.user_id, "123");
        }
    }

    #[test]
    fn test_pubsub_message_edit_operation() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            "action:edition;user_id:123;op:insert;position:0;content:Hello".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::OperationsEvent(operations_event) = event {
            assert_eq!(operations_event.operations.len(), 1);
            if let OperationEvent::Insertion(insert_event) = &operations_event.operations[0] {
                assert_eq!(insert_event.id, 1);
                assert_eq!(insert_event.user_id, "123");
                assert_eq!(insert_event.content, "Hello");
                assert_eq!(insert_event.position, 0);
            } else {
                panic!("Expected insertIncoming event");
            }
        } else {
            panic!("Expected OperationsEvent");
        }
    }

    #[test]
    fn test_pubsub_message_delete_operation() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            "action:edition;user_id:123;op:delete;start_position:0;end_position:5".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::OperationsEvent(operations_event) = event {
            assert_eq!(operations_event.operations.len(), 1);
            if let OperationEvent::Deletion(delete_event) = &operations_event.operations[0] {
                assert_eq!(delete_event.id, 1);
                assert_eq!(delete_event.user_id, "123");
                assert_eq!(delete_event.start_position, 0);
                assert_eq!(delete_event.end_position, 5);
            } else {
                panic!("Expected deleteIncoming event");
            }
        } else {
            panic!("Expected OperationsEvent");
        }
    }

    #[test]
    fn test_multiple_pubsub_operations() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            "action:edition;user_id:123;op:insert;position:0;content:Hello|op:delete;start_position:0;end_position:5".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::OperationsEvent(operations_event) = event {
            assert_eq!(operations_event.operations.len(), 2);
            if let OperationEvent::Insertion(insert_event) = &operations_event.operations[0] {
                assert_eq!(insert_event.id, 1);
                assert_eq!(insert_event.user_id, "123");
                assert_eq!(insert_event.content, "Hello");
                assert_eq!(insert_event.position, 0);
            } else {
                panic!("Expected insertIncoming event");
            }
            if let OperationEvent::Deletion(delete_event) = &operations_event.operations[1] {
                assert_eq!(delete_event.id, 1);
                assert_eq!(delete_event.user_id, "123");
                assert_eq!(delete_event.end_position, 5);
                assert_eq!(delete_event.start_position, 0);
            } else {
                panic!("Expected deleteIncoming event");
            }
        } else {
            panic!("Expected OperationsEvent");
        }
    }

    #[test]
    fn test_join_event_creation() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            "action:join;user_id:123".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::JoinEvent(join_event) = event {
            assert_eq!(join_event.user_id, "123");
            assert_eq!(join_event.document_id, 1);
        } else {
            panic!("Expected JoinEvent");
        }
    }

    #[test]
    fn test_disconnect_event_creation() {
        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            "action:disconnect;user_id:123".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::DisconnectEvent(disconnect_event) = event {
            assert_eq!(disconnect_event.user_id, "123");
            assert_eq!(disconnect_event.document_id, 1);
        } else {
            panic!("Expected DisconnectEvent");
        }
    }

    #[test]
    fn test_response_creation_event() {
        let message = vec![
            "message".to_string(),
            "users:123".to_string(),
            "response:creation;id:1".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::ResponseCreationEvent(response_event) = event {
            assert_eq!(response_event.user_id, "123");
            assert_eq!(response_event.document_id, "1");
        } else {
            panic!("Expected ResponseCreationEvent");
        }
    }

    #[test]
    fn test_response_list_event() {
        let message = vec![
            "message".to_string(),
            "users:123".to_string(),
            "response:files;files:file1.txt,file2.txt".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::ResponseListEvent(response_event) = event {
            assert_eq!(response_event.user_id, "123");
            assert_eq!(response_event.files, vec!["file1.txt", "file2.txt"]);
        } else {
            panic!("Expected ResponseListEvent");
        }
    }

    #[test]
    fn test_join_event_using_handle_client_pubsub_message() {
        let message = vec![
            "message".to_string(),
            "documents:123".to_string(),
            "action:join;user_id:456".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::JoinEvent(join_event) = event {
            assert_eq!(join_event.user_id, "456");
            assert_eq!(join_event.document_id, 123);
            assert_eq!(join_event.file_type, "documents");
        } else {
            panic!("Expected JoinEvent");
        }
    }

    #[test]
    fn test_join_sheet_event_using_handle_client_pubsub_message() {
        let message = vec![
            "message".to_string(),
            "sheets:123".to_string(),
            "action:join;user_id:456".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::JoinEvent(join_event) = event {
            assert_eq!(join_event.user_id, "456");
            assert_eq!(join_event.document_id, 123);
            assert_eq!(join_event.file_type, "sheets");
        } else {
            panic!("Expected JoinEvent");
        }
    }

    #[test]
    fn test_disconnect_event_using_handle_client_pubsub_message() {
        let message = vec![
            "message".to_string(),
            "documents:123".to_string(),
            "action:disconnect;user_id:456".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::DisconnectEvent(disconnect_event) = event {
            assert_eq!(disconnect_event.user_id, "456");
            assert_eq!(disconnect_event.document_id, 123);
        } else {
            panic!("Expected DisconnectEvent");
        }
    }

    #[test]
    fn test_sync_event_using_handle_client_pubsub_message() {
        let message = vec![
            "message".to_string(),
            "documents:123".to_string(),
            "action:sync;content:Hello World;users:cami,anibal,giu,tommy".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::Sync(sync_event) = event {
            assert_eq!(sync_event.content, "Hello World");
            assert_eq!(
                sync_event.users,
                vec![
                    "cami".to_string(),
                    "anibal".to_string(),
                    "giu".to_string(),
                    "tommy".to_string()
                ]
            );
        } else {
            panic!("Expected SyncEvent");
        }
    }

    #[test]
    fn test_sheet_creation_event() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:utils".to_string(),
            "action:create;document_name:new_sheet;user_id:123;width:5;height:5".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::SheetCreationEvent(sheet_event) = event {
            assert_eq!(sheet_event.name, "new_sheet");
            assert_eq!(sheet_event.user_id, "123");
            assert_eq!(sheet_event.width, 5);
            assert_eq!(sheet_event.height, 5);
        } else {
            panic!("Expected SheetCreationEvent");
        }
    }

    #[test]
    fn test_list_sheets() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:utils".to_string(),
            "action:list;user_id:123;file_type:sheets".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::ListEvent(list_event) = event {
            assert_eq!(list_event.user_id, "123");
            assert_eq!(list_event.file_type, "sheets");
        } else {
            panic!("Expected ListEvent");
        }
    }

    #[test]
    fn test_sheet_edition_event() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:1".to_string(),
            "action:edition;user_id:123;op:insert;position:0;value:Hello;column:0;row:0"
                .to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::SheetOperationsEvent(sheet_operations_event) = event {
            assert_eq!(sheet_operations_event.operations.len(), 1);
            if let SheetOperationEvent::InsertIntoColumn(insert_event) =
                &sheet_operations_event.operations[0]
            {
                assert_eq!(insert_event.id, 1);
                assert_eq!(insert_event.user_id, "123");
                assert_eq!(insert_event.value, "Hello");
                assert_eq!(insert_event.column, 0);
                assert_eq!(insert_event.row, 0);
                assert_eq!(insert_event.position, 0);
            } else {
                panic!("Expected Insert event");
            }
        } else {
            panic!("Expected SheetOperationsEvent");
        }
    }

    #[test]
    fn test_sheet_deletion_event() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:1".to_string(),
            "action:edition;user_id:123;op:delete;start:0;end:5;content:12345;column:0;row:0"
                .to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::SheetOperationsEvent(sheet_operations_event) = event {
            assert_eq!(sheet_operations_event.operations.len(), 1);
            if let SheetOperationEvent::DeleteIntoColumn(delete_event) =
                &sheet_operations_event.operations[0]
            {
                assert_eq!(delete_event.id, 1);
                assert_eq!(delete_event.user_id, "123");
                assert_eq!(delete_event.column, 0);
                assert_eq!(delete_event.row, 0);
                assert_eq!(delete_event.start, 0);
                assert_eq!(delete_event.end, 5);
            } else {
                panic!("Expected Delete event");
            }
        }
    }

    #[test]
    fn test_multiple_sheet_edition_events() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:1".to_string(),
            "action:edition;user_id:123;op:insert;position:0;value:Hello;column:0;row:0|op:delete;start:0;end:5;content:12345;column:0;row:0".to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::SheetOperationsEvent(sheet_operations_event) = event {
            assert_eq!(sheet_operations_event.operations.len(), 2);
            if let SheetOperationEvent::InsertIntoColumn(insert_event) =
                &sheet_operations_event.operations[0]
            {
                assert_eq!(insert_event.id, 1);
                assert_eq!(insert_event.user_id, "123");
                assert_eq!(insert_event.value, "Hello");
                assert_eq!(insert_event.column, 0);
                assert_eq!(insert_event.row, 0);
                assert_eq!(insert_event.position, 0);
            } else {
                panic!("Expected Insert event");
            }
            if let SheetOperationEvent::DeleteIntoColumn(delete_event) =
                &sheet_operations_event.operations[1]
            {
                assert_eq!(delete_event.id, 1);
                assert_eq!(delete_event.user_id, "123");
                assert_eq!(delete_event.column, 0);
                assert_eq!(delete_event.row, 0);
                assert_eq!(delete_event.start, 0);
                assert_eq!(delete_event.end, 5);
            } else {
                panic!("Expected Delete event");
            }
        } else {
            panic!("Expected SheetOperationsEvent");
        }
    }

    #[test]
    fn test_response_sheet_creation_event() {
        let message = vec![
            "message".to_string(),
            "users:123".to_string(),
            "response:sheet_creation;sheet_id:1;name:test;width:5;height:5".to_string(),
        ];
        let event = handle_client_pubsub_message(message).unwrap();
        if let Event::ResponseSheetCreationEvent(response_event) = event {
            assert_eq!(response_event.user_id, "123");
            assert_eq!(response_event.sheet_id, "1");
            assert_eq!(response_event.width, 5);
            assert_eq!(response_event.height, 5);
        } else {
            panic!("Expected ResponseSheetCreationEvent");
        }
    }

    #[test]
    fn test_response_sheet_sync_event() {
        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:1".to_string(),
            "action:sync;content:Hello World;users:cami,anibal,giu,tommy;width:5;height:5"
                .to_string(),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::SheetSync(sync_event) = event {
            assert_eq!(sync_event.content, "Hello World");
            assert_eq!(
                sync_event.users,
                vec![
                    "cami".to_string(),
                    "anibal".to_string(),
                    "giu".to_string(),
                    "tommy".to_string()
                ]
            );
            assert_eq!(sync_event.width, 5);
            assert_eq!(sync_event.height, 5);
        } else {
            panic!("Expected SheetSyncEvent");
        }
    }
}
