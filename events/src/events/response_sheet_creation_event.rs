#[derive(Debug)]
pub struct ResponseSheetCreationEvent {
    pub user_id: String,
    pub sheet_id: String,
    pub width: usize,
    pub height: usize,
}
