use std::collections::HashMap;

use common::{char_entry::CharEntry, text::Text};

use crate::document_error::DocumentError;

use super::{DocumentResult, document::Document};

/// Struct that tracks existent documents in the Redis database.
pub struct DocumentTracker {
    /// Current index for documents, used to generate unique IDs for new documents.
    documents_current_index: usize,
    /// HashMap that stores documents with their IDs as keys.
    documents: HashMap<usize, Document>,
    /// Current Lamport timestamp for the document tracker.
    lamport_timestamp: usize,
}

impl DocumentTracker {
    /// Creates a new instance of `DocumentTracker`.
    /// This function initializes a connection to the Redis server and sets the current index for documents to zero.
    /// # Returns
    /// A `DocumentResult<Self>` which is an instance of `DocumentTracker` if successful, or an error if the connection fails.
    pub fn new() -> DocumentResult<Self> {
        Ok(DocumentTracker {
            documents_current_index: 0,
            documents: HashMap::new(),
            lamport_timestamp: 0,
        })
    }

    /// Creates a new document and returns its ID.
    pub fn create_new_document(&mut self, name: &str) -> DocumentResult<usize> {
        let next_id = self.documents_current_index;
        self.documents_current_index += 1;

        let new_document = Document::new(next_id, name.to_string())?;
        self.documents.insert(next_id, new_document);
        Ok(next_id)
    }

    pub fn get_next_lamport_timestamp(&mut self) -> usize {
        let next_timestamp = self.lamport_timestamp + 1;
        self.lamport_timestamp = next_timestamp;
        next_timestamp
    }

    pub fn insert_into_document(
        &mut self,
        id: usize,
        position: usize,
        entries: Vec<CharEntry>,
    ) -> DocumentResult<()> {
        let document = self.documents.get_mut(&id);

        if let Some(document) = document {
            document.insert_content(position, entries)
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn delete_from_document(
        &mut self,
        id: usize,
        start: usize,
        end: usize,
    ) -> DocumentResult<()> {
        let document = self.documents.get_mut(&id);
        if let Some(document) = document {
            document.delete_content(start, end)
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn get_document(&self, id: &usize) -> Option<&Document> {
        self.documents.get(id)
    }

    pub fn add_user_to_document(&mut self, id: usize, user: String) -> DocumentResult<()> {
        let document = self.documents.get_mut(&id);
        if let Some(document) = document {
            document.add_user(user)
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn remove_user_from_document(&mut self, id: usize, user: &str) -> DocumentResult<()> {
        let document = self.documents.get_mut(&id);
        if let Some(document) = document {
            document.remove_user(user)
        } else {
            Err(DocumentError::new(
                "Document not found".to_string(),
                crate::document_error::DocumentErrorKind::NotFound,
            ))
        }
    }

    pub fn get_all_documents(&self) -> &HashMap<usize, Document> {
        &self.documents
    }

    pub fn add_existing_document(
        &mut self,
        id: usize,
        name: String,
        document: String,
    ) -> DocumentResult<()> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.documents.entry(id) {
            let content = Text::with_content(document);
            let document = Document::new_with_content(id, name, content)?;
            e.insert(document);
            Ok(())
        } else {
            Err(DocumentError::new(
                "Document with this ID already exists".to_string(),
                crate::document_error::DocumentErrorKind::AlreadyExists,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use common::char_entry::CharEntry;
    use events::events::{event::Event, operation_event::OperationEvent};

    use crate::documents::documents_tracker::DocumentTracker;

    const TEST_DOCUMENT_NAME: &str = "Test Document";
    const TEST_USER_ID: &str = "test_user";

    #[test]
    fn test_creation_event_flow() {
        let document_tracker = DocumentTracker::new();
        if document_tracker.is_err() {
            panic!(
                "Failed to create DocumentTracker: {:?}",
                document_tracker.err()
            );
        }
        let mut document_tracker = document_tracker.unwrap();

        let _ = Event::new_creation_event(TEST_DOCUMENT_NAME.to_string(), TEST_USER_ID.to_string());
        let new_id = document_tracker.create_new_document(TEST_DOCUMENT_NAME);
        if new_id.is_err() {
            panic!("Failed to create new document: {:?}", new_id.err());
        }
        let new_id = new_id.unwrap();
        assert_eq!(new_id, 0, "New document ID should be 0");
    }

    #[test]
    fn test_insertion_event_flow() {
        let document_tracker = DocumentTracker::new();
        if document_tracker.is_err() {
            panic!(
                "Failed to create DocumentTracker: {:?}",
                document_tracker.err()
            );
        }
        let mut document_tracker = document_tracker.unwrap();

        let new_id = document_tracker.create_new_document(TEST_DOCUMENT_NAME);
        if new_id.is_err() {
            panic!("Failed to create new document: {:?}", new_id.err());
        }
        let new_id = new_id.unwrap();

        let insertion_event = OperationEvent::new_insertion_event(
            new_id,
            TEST_USER_ID.to_string(),
            "test".to_string(),
            0,
        );

        let incoming_operation_events = Event::new_operations_event(vec![insertion_event]);

        if let Event::OperationsEvent(operations_event) = incoming_operation_events {
            for operation in operations_event.operations {
                match operation {
                    OperationEvent::Insertion(insertion_event) => {
                        let mut entries = Vec::new();
                        for entry in insertion_event.content.chars() {
                            let char_entry = CharEntry::new(
                                entry,
                                document_tracker.get_next_lamport_timestamp(),
                                TEST_USER_ID,
                            );
                            entries.push(char_entry);
                        }
                        document_tracker
                            .insert_into_document(
                                insertion_event.id,
                                insertion_event.position,
                                entries,
                            )
                            .unwrap();
                    }
                    _ => panic!("Unexpected operation type"),
                }
            }
        } else {
            panic!("Expected OperationsEvent");
        }

        let document = document_tracker.documents.get(&new_id);
        assert_eq!(document.unwrap().content.to_string(), "test");
    }

    #[test]
    fn test_multiple_insertion_events() {
        let document_tracker = DocumentTracker::new();
        if document_tracker.is_err() {
            panic!(
                "Failed to create DocumentTracker: {:?}",
                document_tracker.err()
            );
        }
        let mut document_tracker = document_tracker.unwrap();

        let new_id = document_tracker.create_new_document(TEST_DOCUMENT_NAME);
        if new_id.is_err() {
            panic!("Failed to create new document: {:?}", new_id.err());
        }
        let new_id = new_id.unwrap();

        let insertion_event1 = OperationEvent::new_insertion_event(
            new_id,
            TEST_USER_ID.to_string(),
            "first".to_string(),
            0,
        );
        let insertion_event2 = OperationEvent::new_insertion_event(
            new_id,
            TEST_USER_ID.to_string(),
            "second".to_string(),
            3,
        );

        let incoming_operation_events =
            Event::new_operations_event(vec![insertion_event1, insertion_event2]);

        if let Event::OperationsEvent(operations_event) = incoming_operation_events {
            for operation in operations_event.operations {
                match operation {
                    OperationEvent::Insertion(insertion_event) => {
                        let mut entries = Vec::new();
                        for entry in insertion_event.content.chars() {
                            let char_entry = CharEntry::new(
                                entry,
                                document_tracker.get_next_lamport_timestamp(),
                                TEST_USER_ID,
                            );
                            entries.push(char_entry);
                        }
                        document_tracker
                            .insert_into_document(
                                insertion_event.id,
                                insertion_event.position,
                                entries,
                            )
                            .unwrap();
                    }
                    _ => panic!("Unexpected operation type"),
                }
            }
        } else {
            panic!("Expected OperationsEvent");
        }

        let document = document_tracker.documents.get(&new_id);
        assert_eq!(document.unwrap().content.to_string(), "firsecondst");
    }

    #[test]
    fn test_insertion_and_deletion() {
        let document_tracker = DocumentTracker::new();
        if document_tracker.is_err() {
            panic!(
                "Failed to create DocumentTracker: {:?}",
                document_tracker.err()
            );
        }
        let mut document_tracker = document_tracker.unwrap();

        let new_id = document_tracker.create_new_document(TEST_DOCUMENT_NAME);

        if new_id.is_err() {
            panic!("Failed to create new document: {:?}", new_id.err());
        }
        let new_id = new_id.unwrap();

        let insertion_event = OperationEvent::new_insertion_event(
            new_id,
            TEST_USER_ID.to_string(),
            "test content".to_string(),
            0,
        );
        let deletion_event =
            OperationEvent::new_deletion_event(new_id, TEST_USER_ID.to_string(), 0, 4);

        let incoming_operation_events =
            Event::new_operations_event(vec![insertion_event, deletion_event]);

        if let Event::OperationsEvent(operations_event) = incoming_operation_events {
            for operation in operations_event.operations {
                match operation {
                    OperationEvent::Insertion(insertion_event) => {
                        let mut entries = Vec::new();
                        for entry in insertion_event.content.chars() {
                            let char_entry = CharEntry::new(
                                entry,
                                document_tracker.get_next_lamport_timestamp(),
                                TEST_USER_ID,
                            );
                            entries.push(char_entry);
                        }
                        document_tracker
                            .insert_into_document(
                                insertion_event.id,
                                insertion_event.position,
                                entries,
                            )
                            .unwrap();
                    }
                    OperationEvent::Deletion(deletion_event) => {
                        document_tracker
                            .delete_from_document(
                                deletion_event.id,
                                deletion_event.start_position,
                                deletion_event.end_position,
                            )
                            .unwrap();
                    }
                }
            }
        } else {
            panic!("Expected OperationsEvent");
        }

        let document = document_tracker.documents.get(&new_id);
        assert_eq!(document.unwrap().content.to_string(), "content");
    }
}
