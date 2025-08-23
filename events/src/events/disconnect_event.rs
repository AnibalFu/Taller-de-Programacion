#[derive(Debug)]
pub struct DisconnectEvent {
    pub user_id: String,
    pub document_id: usize,
}
