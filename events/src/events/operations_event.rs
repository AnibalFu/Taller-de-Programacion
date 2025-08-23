use crate::events::operation_event::OperationEvent;

#[derive(Debug)]
pub struct OperationsEvent {
    pub operations: Vec<OperationEvent>,
}
