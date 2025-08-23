use std::collections::HashMap;

use common::{char_entry::CharEntry, sheet::Sheet};

use crate::{
    document_error::DocumentError, documents::DocumentResult, sheets::redis_sheet::RedisSheet,
};

/// SheetTracker is a struct that tracks the state of sheets.
pub struct SheetTracker {
    /// Current index for sheets, used to generate unique IDs for new sheets
    current_index: usize,
    /// A map of sheet names to their corresponding RedisSheet instances
    sheets: HashMap<usize, RedisSheet>,
}

impl SheetTracker {
    /// Creates a new SheetTracker with an initial index of 0 and an empty sheets map.
    pub fn new() -> DocumentResult<Self> {
        Ok(SheetTracker {
            current_index: 0,
            sheets: HashMap::new(),
        })
    }

    pub fn create_new_sheet(
        &mut self,
        name: &str,
        width: usize,
        height: usize,
    ) -> DocumentResult<usize> {
        let new_index = self.current_index;
        let sheet = Sheet::new(width, height);
        let redis_sheet = RedisSheet::new(new_index, name.to_string(), sheet)?;

        self.sheets.insert(self.current_index, redis_sheet);

        self.current_index += 1;
        Ok(new_index)
    }

    pub fn insert_into_sheet(
        &mut self,
        sheet_id: usize,
        row: usize,
        column: usize,
        position: usize,
        entries: Vec<CharEntry>,
    ) -> DocumentResult<()> {
        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
            sheet.insert_content(row, column, position, entries)?;
            Ok(())
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn delete_from_sheet(
        &mut self,
        sheet_id: usize,
        row: usize,
        column: usize,
        start: usize,
        end: usize,
    ) -> DocumentResult<()> {
        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
            sheet.delete_content(row, column, start, end)?;
            Ok(())
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn get_sheet(&self, sheet_id: usize) -> DocumentResult<&RedisSheet> {
        self.sheets.get(&sheet_id).ok_or_else(|| {
            DocumentError::new(
                "Sheet not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            )
        })
    }

    pub fn get_content_from_cell(
        &self,
        sheet_id: usize,
        row: usize,
        column: usize,
    ) -> DocumentResult<String> {
        if let Some(sheet) = self.sheets.get(&sheet_id) {
            sheet.get_content_from_cell(row, column)
        } else {
            Err(DocumentError::new(
                "Sheet not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn add_user_to_sheet(&mut self, sheet_id: usize, user: String) -> DocumentResult<()> {
        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
            sheet.add_user(user)?;
            Ok(())
        } else {
            Err(DocumentError::new(
                "Sheet not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn remove_user_from_sheet(&mut self, sheet_id: usize, user: &str) -> DocumentResult<()> {
        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
            sheet.remove_user(user)?;
            Ok(())
        } else {
            Err(DocumentError::new(
                "Sheet not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn get_all_sheets(&self) -> &HashMap<usize, RedisSheet> {
        &self.sheets
    }

    pub fn add_existing_sheet(
        &mut self,
        sheet_id: usize,
        name: String,
        redis_sheet: String,
    ) -> DocumentResult<()> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.sheets.entry(sheet_id) {
            let sheet = Sheet::parse_raw_str_without_size(&redis_sheet);
            let redis_sheet = RedisSheet::new(sheet_id, name, sheet)?;
            e.insert(redis_sheet);
            Ok(())
        } else {
            Err(DocumentError::new(
                "Sheet ID already exists".to_string(),
                crate::document_error::DocumentErrorKind::AlreadyExists,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::char_entry::CharEntry;

    #[test]
    fn test_create_new_sheet() {
        let mut tracker = SheetTracker::new().unwrap();
        let sheet_id = tracker.create_new_sheet("Test Sheet", 3, 3).unwrap();
        assert_eq!(sheet_id, 0);
        assert!(tracker.sheets.contains_key(&sheet_id));
    }

    #[test]
    fn test_insert_into_sheet() {
        let mut tracker = SheetTracker::new().unwrap();
        let sheet_id = tracker.create_new_sheet("Test Sheet", 3, 3).unwrap();
        let entries = vec![CharEntry::new('A', 0, "test")];
        tracker
            .insert_into_sheet(sheet_id, 0, 0, 0, entries)
            .unwrap();
        let sheet = tracker.get_sheet(sheet_id).unwrap();
        assert_eq!(sheet.get_content_from_cell(0, 0).unwrap(), "A".to_string());
    }

    #[test]
    fn test_delete_from_sheet() {
        let mut tracker = SheetTracker::new().unwrap();
        let sheet_id = tracker.create_new_sheet("Test Sheet", 3, 3).unwrap();
        let entries = vec![CharEntry::new('A', 0, "test")];
        tracker
            .insert_into_sheet(sheet_id, 0, 0, 0, entries)
            .unwrap();
        tracker.delete_from_sheet(sheet_id, 0, 0, 0, 1).unwrap();
        let sheet = tracker.get_sheet(sheet_id).unwrap();
        assert_eq!(sheet.get_content_from_cell(0, 0).unwrap(), "".to_string());
    }

    #[test]
    fn test_add_and_remove_user() {
        let mut tracker = SheetTracker::new().unwrap();
        let sheet_id = tracker.create_new_sheet("Test Sheet", 3, 3).unwrap();
        tracker
            .add_user_to_sheet(sheet_id, "user1".to_string())
            .unwrap();
        let sheet = tracker.get_sheet(sheet_id).unwrap();
        assert!(sheet.get_users().contains(&"user1".to_string()));
        tracker.remove_user_from_sheet(sheet_id, "user1").unwrap();
        let sheet = tracker.get_sheet(sheet_id).unwrap();
        assert!(!sheet.get_users().contains(&"user1".to_string()));
    }
}
