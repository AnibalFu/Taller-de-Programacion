use crate::events::{sheet_delete_event::SheetDeletionEvent, sheet_insert_event::SheetInsertEvent};

#[derive(Debug)]
pub struct SheetOperationsEvent {
    pub operations: Vec<SheetOperationEvent>,
}

#[derive(Debug)]
pub enum SheetOperationEvent {
    InsertIntoColumn(SheetInsertEvent),
    DeleteIntoColumn(SheetDeletionEvent),
}

impl SheetOperationEvent {
    pub fn new_insert_event(
        id: usize,
        column: usize,
        row: usize,
        user_id: String,
        value: String,
        position: usize,
    ) -> Self {
        SheetOperationEvent::InsertIntoColumn(SheetInsertEvent {
            id,
            column,
            user_id,
            row,
            value,
            position,
        })
    }

    pub fn new_delete_event(
        id: usize,
        user_id: String,
        column: usize,
        row: usize,
        start: usize,
        end: usize,
    ) -> Self {
        SheetOperationEvent::DeleteIntoColumn(SheetDeletionEvent {
            id,
            user_id,
            column,
            row,
            start,
            end,
        })
    }
}
