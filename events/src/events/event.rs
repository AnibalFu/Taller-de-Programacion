use crate::events::{
    creation_event::CreationEvent,
    disconnect_event::DisconnectEvent,
    full_document_event::FullDocumentEvent,
    join_event::JoinEvent,
    list_event::ListEvent,
    operation_event::OperationEvent,
    operations_event::OperationsEvent,
    response_creation_event::ResponseCreationEvent,
    response_list_event::ResponseListEvent,
    response_sheet_creation_event::ResponseSheetCreationEvent,
    sheet_creation_event::SheetCreationEvent,
    sheet_operations_event::{SheetOperationEvent, SheetOperationsEvent},
    sheet_sync_event::SheetSyncEvent,
    sync_event::SyncEvent,
};

#[derive(Debug)]
pub enum Event {
    DocumentCreationEvent(CreationEvent),
    ListEvent(ListEvent),
    OperationsEvent(OperationsEvent),
    JoinEvent(JoinEvent),
    DisconnectEvent(DisconnectEvent),
    Sync(SyncEvent),
    ResponseListEvent(ResponseListEvent),
    ResponseCreationEvent(ResponseCreationEvent),
    SheetCreationEvent(SheetCreationEvent),
    SheetOperationsEvent(SheetOperationsEvent),
    ResponseSheetCreationEvent(ResponseSheetCreationEvent),
    SheetSync(SheetSyncEvent),
    FullDocumentEvent(FullDocumentEvent),
}

impl Event {
    pub fn new_creation_event(name: String, user_id: String) -> Self {
        Event::DocumentCreationEvent(CreationEvent { name, user_id })
    }

    pub fn new_operations_event(operations: Vec<OperationEvent>) -> Self {
        Event::OperationsEvent(OperationsEvent { operations })
    }

    pub fn new_join_event(user_id: String, document_id: usize, file_type: String) -> Self {
        Event::JoinEvent(JoinEvent {
            user_id,
            document_id,
            file_type,
        })
    }

    pub fn new_list_event(user_id: String, file_type: String) -> Self {
        Event::ListEvent(ListEvent { user_id, file_type })
    }

    pub fn new_disconnect_event(user_id: String, document_id: usize) -> Self {
        Event::DisconnectEvent(DisconnectEvent {
            user_id,
            document_id,
        })
    }

    pub fn new_sync_event(content: String, users: Vec<String>) -> Self {
        Event::Sync(SyncEvent { content, users })
    }

    pub fn new_response_list_event(user_id: String, files: Vec<String>) -> Self {
        Event::ResponseListEvent(ResponseListEvent { user_id, files })
    }

    pub fn new_response_creation_event(user_id: String, document_id: String) -> Self {
        Event::ResponseCreationEvent(ResponseCreationEvent {
            user_id,
            document_id,
        })
    }

    pub fn new_sheet_creation_event(
        user_id: String,
        name: String,
        width: usize,
        height: usize,
    ) -> Self {
        Event::SheetCreationEvent(SheetCreationEvent {
            user_id,
            name,
            width,
            height,
        })
    }

    pub fn new_sheet_operations_event(operations: Vec<SheetOperationEvent>) -> Self {
        Event::SheetOperationsEvent(SheetOperationsEvent { operations })
    }

    pub fn new_response_sheet_creation_event(
        user_id: String,
        sheet_id: String,
        width: usize,
        height: usize,
    ) -> Self {
        Event::ResponseSheetCreationEvent(ResponseSheetCreationEvent {
            user_id,
            sheet_id,
            width,
            height,
        })
    }

    pub fn new_sheet_sync_event(
        content: String,
        users: Vec<String>,
        width: usize,
        height: usize,
    ) -> Self {
        Event::SheetSync(SheetSyncEvent {
            content,
            users,
            width,
            height,
        })
    }

    pub fn new_full_document_event(document_id: String, response_channel: String) -> Self {
        Event::FullDocumentEvent(FullDocumentEvent {
            document_id,
            response_channel,
        })
    }
}
