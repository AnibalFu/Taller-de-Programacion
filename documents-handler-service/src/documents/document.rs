use common::text::Text;

use super::DocumentResult;

#[derive(Debug, Clone)]
// Struct that represents a document in the system.
pub struct Document {
    // Unique identifier for the document
    pub id: usize,
    // Name of the document
    pub name: String,
    // Content of the document
    pub content: Text,
    /// Users connected
    pub users: Vec<String>,
}

impl Document {
    // Creates a new document with the given id, name, and content
    pub fn new(id: usize, name: String) -> DocumentResult<Self> {
        Ok(Document {
            id,
            name,
            content: Text::new(),
            users: Vec::new(),
        })
    }

    pub fn new_with_content(id: usize, name: String, content: Text) -> DocumentResult<Self> {
        Ok(Document {
            id,
            name,
            content,
            users: Vec::new(),
        })
    }

    /// Inserts a new document with the specified name and returns a `DocumentResult` containing the new document.
    pub fn insert_content(
        &mut self,
        position: usize,
        entries: Vec<common::char_entry::CharEntry>,
    ) -> DocumentResult<()> {
        for (index, entry) in entries.into_iter().enumerate() {
            self.content.insert_char(position + index, entry);
        }
        Ok(())
    }

    /// Deletes a range of content from the document, specified by the start and end indices.
    pub fn delete_content(&mut self, start: usize, end: usize) -> DocumentResult<()> {
        self.content.delete_range(start, end);
        Ok(())
    }

    /// Returns the content of the document as a string.
    pub fn get_content(&self) -> String {
        self.content.to_string()
    }

    /// Returns the name of the document.
    pub fn get_name(&self) -> &str {
        &self.name
    }
    /// Returns current users connected to the document.
    pub fn get_users(&self) -> &Vec<String> {
        &self.users
    }
    /// Adds a user to the document's user list.
    pub fn add_user(&mut self, user: String) -> DocumentResult<()> {
        if !self.users.contains(&user) {
            self.users.push(user);
        }
        Ok(())
    }
    /// Removes a user from the document's user list.
    pub fn remove_user(&mut self, user: &str) -> DocumentResult<()> {
        if let Some(pos) = self.users.iter().position(|x| x == user) {
            self.users.remove(pos);
        }
        Ok(())
    }
}
