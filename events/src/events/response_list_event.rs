#[derive(Debug, Clone)]
pub struct ResponseListEvent {
    pub user_id: String,
    pub files: Vec<String>,
}
