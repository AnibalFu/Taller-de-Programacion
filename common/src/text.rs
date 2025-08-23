use std::cmp::min;

use crate::char_entry::CharEntry;

#[derive(Debug, Clone)]
pub struct Text {
    pub content: Vec<CharEntry>,
}

impl Default for Text {
    fn default() -> Self {
        Self::new()
    }
}

impl Text {
    pub fn new() -> Self {
        Text {
            content: Vec::new(),
        }
    }

    pub fn with_content(content: String) -> Self {
        let mut text = Text::new();
        for ch in content.chars() {
            text.content.push(CharEntry::new(ch, 0, "system")); // Using 0 timestamp and "system" user_id for initialization
        }
        text
    }

    pub fn insert_char(&mut self, position: usize, char_entry: CharEntry) {
        if position > self.content.len() {
            // If position is greater than current length, we will insert at the end
            self.content.push(char_entry);
        } else {
            self.content.insert(position, char_entry);
        }
    }

    pub fn insert_chars(&mut self, position: usize, entries: Vec<CharEntry>) {
        if position > self.content.len() {
            // We will insert at the end if position is greater than current length
            for entry in entries {
                self.content.push(entry);
            }
        } else {
            for (index, entry) in entries.into_iter().enumerate() {
                self.content.insert(position + index, entry);
            }
        }
    }

    pub fn delete_range(&mut self, start: usize, end: usize) {
        self.content.drain(start..=min(end, self.content.len() - 1));
    }

    pub fn get_content(&self) -> String {
        self.content.iter().map(|entry| entry.ch).collect()
    }

    pub fn len(&self) -> usize {
        self.content.len()
    }

    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_content())
    }
}
