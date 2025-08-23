#[derive(Debug)]
pub struct SheetCreationEvent {
    pub name: String,
    pub user_id: String,
    pub width: usize,
    pub height: usize,
}
