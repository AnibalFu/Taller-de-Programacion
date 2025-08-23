#[derive(Debug)]
pub struct SyncEvent {
    pub content: String,
    pub users: Vec<String>,
}
