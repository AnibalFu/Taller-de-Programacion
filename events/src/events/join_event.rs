#[derive(Debug)]
pub struct JoinEvent {
    pub user_id: String,
    pub document_id: usize,
    pub file_type: String,
}
