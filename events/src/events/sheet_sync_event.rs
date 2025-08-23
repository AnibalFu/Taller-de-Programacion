#[derive(Debug)]
pub struct SheetSyncEvent {
    // The content will be a string representation of the sheet, e.g., "A1,B1,C1;A2,B2,C2" for a 2x3 sheet
    pub content: String,
    // The users will be a list of user IDs currently editing the sheet
    pub users: Vec<String>,
    // The width and height of the sheet
    pub width: usize,
    // The height of the sheet
    pub height: usize,
}
