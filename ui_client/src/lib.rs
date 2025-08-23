pub mod excel_formulas;
pub mod excel_parser;
pub mod main_ui;
pub mod processes;
pub mod redis_command_process;
pub mod redis_ia_subscribe_process;
pub mod redis_subscribe_process;
pub mod rusty_docs_ui;
pub mod rusty_excel_ui;
pub mod ui_error;
pub mod user_data;

pub(crate) fn create_channel_name(document_id: &str) -> String {
    format!("documents:{document_id}")
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use common::{char_entry::CharEntry, lcs::atomic_ops, sheet::Sheet, text::Text};
    use events::{
        apply_operations,
        events::{
            event::Event, events_api::handle_microservice_pubsub_message,
            sheet_operations_event::SheetOperationEvent,
        },
    };

    #[test]
    fn test_simulation_edit() {
        let old_text = "asd";
        let mut text = Text::with_content(old_text.to_string());
        let new_text = "asd\n123";
        let operations = atomic_ops(old_text, new_text);

        let operations = operations.into_iter().collect::<Vec<_>>().join("\\|");

        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            format!("action:edition;user_id:test;{}", operations),
        ];

        let event = handle_microservice_pubsub_message(message).unwrap();

        if let Event::OperationsEvent(operations) = event {
            apply_operations(&mut text, operations.operations);
        } else {
            panic!("Expected OperationEvent");
        }

        assert_eq!(text.to_string(), "asd\n123");
        let old_text = new_text;
        let new_text = "asd\n13\n456";
        let operations = atomic_ops(old_text, new_text);
        let operations = operations.into_iter().collect::<Vec<_>>().join("\\|");

        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            format!("action:edition;user_id:test;{}", operations),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::OperationsEvent(operations) = event {
            apply_operations(&mut text, operations.operations);
        } else {
            panic!("Expected OperationEvent");
        }
        assert_eq!(text.to_string(), "asd\n13\n456");

        let old_text = new_text;
        let new_text = "asd\n13\n46\n789";
        let operations = atomic_ops(old_text, new_text);
        let operations = operations.into_iter().collect::<Vec<_>>().join("\\|");

        let message = vec![
            "message".to_string(),
            "documents:*".to_string(),
            "documents:1".to_string(),
            format!("action:edition;user_id:test;{}", operations),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        if let Event::OperationsEvent(operations) = event {
            apply_operations(&mut text, operations.operations);
        } else {
            panic!("Expected OperationEvent");
        }

        assert_eq!(text.to_string(), "asd\n13\n46\n789");
    }

    #[test]
    fn test_simulation_edit_sheets() {
        let sheet = Sheet::new(3, 3);
        let mut sheet = sheet;
        let entries = vec![
            CharEntry::new('a', 0, "test"),
            CharEntry::new('b', 0, "test"),
            CharEntry::new('c', 0, "test"),
        ];
        sheet.insert_into_column(0, 0, 0, entries);
        assert_eq!(sheet.rows[0][0].to_string(), "abc");
        let mut buffer_text: Vec<Vec<String>> = vec![
            vec!["".to_string(), "abc".to_string(), "".to_string()],
            vec!["".to_string(), "".to_string(), "".to_string()],
            vec!["".to_string(), "".to_string(), "".to_string()],
        ];
        let mut operations: Vec<String> = vec![];

        for row in 0..sheet.rows.len() {
            for column in 0..sheet.rows[row].len() {
                let text = &sheet.rows[row][column];
                let text_str = text.to_string();
                let buf_text = buffer_text.get_mut(row).unwrap().get_mut(column).unwrap();
                let result = atomic_ops(&text_str, buf_text);
                for res in result {
                    let operation = format!("{res};column:{column};row:{row}");
                    operations.push(operation);
                }
            }
        }

        assert_eq!(operations.len(), 2);
        assert_eq!(
            operations[0],
            "op:delete;start:0;end:2;content:abc;column:0;row:0"
        );
        assert_eq!(operations[1], "op:insert;pos:0;content:abc;column:1;row:0");

        let message = vec![
            "message".to_string(),
            "sheets:*".to_string(),
            "sheets:1".to_string(),
            format!("action:edition;user_id:test;{}", operations.join("|")),
        ];
        let event = handle_microservice_pubsub_message(message).unwrap();
        let mut delete_operations = Vec::new();
        let mut insert_operations = Vec::new();
        if let Event::SheetOperationsEvent(operations_event) = event {
            for op in operations_event.operations {
                match op {
                    SheetOperationEvent::InsertIntoColumn(sheet_insert_event) => {
                        insert_operations.push(sheet_insert_event);
                    }
                    SheetOperationEvent::DeleteIntoColumn(sheet_deletion_event) => {
                        delete_operations.push(sheet_deletion_event);
                    }
                }
            }
        }

        assert_eq!(insert_operations.len(), 1);
        assert_eq!(delete_operations.len(), 1);

        // Sort delete_operations by row, column, then start position
        delete_operations.sort_by(|a, b| {
            a.row
                .cmp(&b.row)
                .then(a.column.cmp(&b.column))
                .then(a.start.cmp(&b.start))
        });

        // Sort insert_operations by row, column, then position
        insert_operations.sort_by(|a, b| {
            a.row
                .cmp(&b.row)
                .then(a.column.cmp(&b.column))
                .then(a.position.cmp(&b.position))
        });

        for delete in delete_operations {
            let text = &mut sheet.rows[delete.row][delete.column];
            let start = min(delete.start, text.len());
            let end = min(delete.end, text.len());
            text.delete_range(start, end);
        }

        for insert in insert_operations {
            let text = &mut sheet.rows[insert.row][insert.column];
            let position = min(insert.position, text.len());
            let entries = insert
                .value
                .chars()
                .map(|c| CharEntry::new(c, 0, "test"))
                .collect();
            text.insert_chars(position, entries);
        }

        assert_eq!(sheet.rows[0][0].to_string(), "");
        assert_eq!(sheet.rows[0][1].to_string(), "abc");
    }
}
