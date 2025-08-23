use common::{char_entry::CharEntry, text::Text};

use crate::events::operation_event::OperationEvent;

pub mod events;

pub fn apply_operations(text: &mut Text, operations: Vec<OperationEvent>) {
    let mut offset: isize = 0;

    for op in &operations {
        match op {
            OperationEvent::Deletion(d) => {
                let adjusted_start = ((d.start_position as isize + offset).max(0) as usize).min(text.len());
                let adjusted_end = ((d.end_position as isize + offset).max(0) as usize).min(text.len());
        
                text.delete_range(adjusted_start, adjusted_end);
        
                // Update offset (deletions reduce text length)
                offset -= ((d.end_position - d.start_position) + 1) as isize;
            },
            OperationEvent::Insertion(i) => {
                let adjusted_position = ((i.position as isize + offset).max(0) as usize).min(text.len());

                let entries: Vec<CharEntry> = i
                    .content
                    .chars()
                    .map(|ch| CharEntry {
                        ch,
                        user_id: i.user_id.clone(),
                        timestamp: 0,
                    })
                    .collect();
        
                text.insert_chars(adjusted_position, entries);
        
                // Update offset (insertions increase text length)
                offset += i.content.chars().count() as isize;
            },
        }
    }
}
