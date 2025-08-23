#[derive(Debug)]
pub struct DeletionEvent {
    pub id: usize,
    pub user_id: String,
    pub end_position: usize,
    pub start_position: usize,
}
