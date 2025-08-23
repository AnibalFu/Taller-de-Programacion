#[derive(Debug)]
pub struct SheetDeletionEvent {
    pub id: usize,
    pub user_id: String,
    pub column: usize,
    pub row: usize,
    pub start: usize,
    pub end: usize,
}
