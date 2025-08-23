#[derive(Debug)]
pub struct InsertionEvent {
    pub id: usize,
    pub user_id: String,
    pub content: String,
    pub position: usize,
}
