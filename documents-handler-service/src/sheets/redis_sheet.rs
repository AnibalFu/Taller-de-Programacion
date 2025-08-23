use common::{char_entry::CharEntry, sheet::Sheet};

use crate::{
    document_error::{DocumentError, DocumentErrorKind},
    documents::DocumentResult,
};

#[derive(Debug)]
pub struct RedisSheet {
    // Unique identifier for the sheet tracker
    pub id: usize,
    // Name of the sheet tracker
    pub name: String,
    // Content of the sheet tracker
    pub content: Sheet,
    // Users associated with the sheet tracker
    pub users: Vec<String>,
}

impl RedisSheet {
    pub fn new(id: usize, name: String, content: Sheet) -> DocumentResult<Self> {
        Ok(RedisSheet {
            id,
            name,
            content,
            users: Vec::new(),
        })
    }

    pub fn insert_content(
        &mut self,
        row: usize,
        column: usize,
        position: usize,
        entries: Vec<CharEntry>,
    ) -> DocumentResult<()> {
        let sheet = &mut self.content;

        if sheet.rows.len() < row || sheet.rows[row].len() < column {
            return Err(DocumentError::new(
                "Invalid row or column index".to_string(),
                DocumentErrorKind::InvalidRowOrColumnIndex,
            ));
        }

        let text = &mut sheet.rows[row][column];
        text.insert_chars(position, entries);

        Ok(())
    }

    pub fn delete_content(
        &mut self,
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    ) -> DocumentResult<()> {
        let sheet = &mut self.content;
        if sheet.rows.len() <= row || sheet.rows[row].len() <= column {
            return Err(DocumentError::new(
                "Invalid row or column index".to_string(),
                DocumentErrorKind::InvalidRowOrColumnIndex,
            ));
        }

        let text = &mut sheet.rows[row][column];
        text.delete_range(start, end);

        Ok(())
    }

    pub fn get_content(&self) -> Vec<Vec<String>> {
        self.content
            .rows
            .iter()
            .map(|row| row.iter().map(|text| text.to_string()).collect())
            .collect()
    }

    pub fn get_content_from_cell(&self, row: usize, column: usize) -> DocumentResult<String> {
        if row < self.content.rows.len() && column < self.content.rows[row].len() {
            Ok(self.content.rows[row][column].to_string())
        } else {
            Err(DocumentError::new(
                "Invalid row or column index".to_string(),
                DocumentErrorKind::InvalidRowOrColumnIndex,
            ))
        }
    }

    pub fn add_user(&mut self, user: String) -> DocumentResult<()> {
        if !self.users.contains(&user) {
            self.users.push(user);
        }
        Ok(())
    }

    pub fn remove_user(&mut self, user: &str) -> DocumentResult<()> {
        if let Some(pos) = self.users.iter().position(|x| x == user) {
            self.users.remove(pos);
        }
        Ok(())
    }

    pub fn get_users(&self) -> &Vec<String> {
        &self.users
    }

    pub fn to_raw_string(&self) -> String {
        self.content.to_raw_string()
    }
}
