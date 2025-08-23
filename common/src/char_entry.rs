use crate::LamportTimestamp;

#[derive(Debug, Clone)]
pub struct CharEntry {
    /// The caracter itself
    pub ch: char,
    /// The Lamport timestamp associated with this character entry.
    pub timestamp: LamportTimestamp,
    /// The user ID of the user who created this character entry.
    pub user_id: String,
}

impl CharEntry {
    /// Creates a new `CharEntry` with the given character, timestamp, and user ID.
    ///
    /// # Arguments
    /// * `ch` - The character to be stored in the entry.
    /// * `timestamp` - The Lamport timestamp associated with this character entry.
    /// * `user_id` - The user ID of the user who created this character entry.
    pub fn new(ch: char, timestamp: LamportTimestamp, user_id: &str) -> Self {
        CharEntry {
            ch,
            timestamp,
            user_id: user_id.to_string(),
        }
    }
}
