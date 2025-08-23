use crate::events::{deletion_event::DeletionEvent, insertion_event::InsertionEvent};

#[derive(Debug)]
pub enum OperationEvent {
    Insertion(InsertionEvent),
    Deletion(DeletionEvent),
}

impl OperationEvent {
    pub fn new_insertion_event(
        id: usize,
        user_id: String,
        content: String,
        position: usize,
    ) -> Self {
        OperationEvent::Insertion(InsertionEvent {
            id,
            user_id,
            content,
            position,
        })
    }

    pub fn new_deletion_event(
        id: usize,
        user_id: String,
        start_position: usize,
        end_position: usize,
    ) -> Self {
        OperationEvent::Deletion(DeletionEvent {
            id,
            user_id,
            end_position,
            start_position,
        })
    }
}
