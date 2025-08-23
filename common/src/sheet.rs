use crate::{char_entry::CharEntry, text::Text};

#[derive(Debug, Clone)]
pub struct Sheet {
    pub rows: Vec<Vec<Text>>,
    pub width: usize,
    pub height: usize,
}

impl Sheet {
    pub fn new(width: usize, height: usize) -> Self {
        let mut rows: Vec<Vec<Text>> = Vec::with_capacity(height);

        for _ in 0..height {
            let mut current_row = Vec::with_capacity(width);
            for _ in 0..width {
                current_row.push(Text::new());
            }
            rows.push(current_row);
        }

        Sheet {
            rows,
            width,
            height,
        }
    }

    /// Parses a raw string representation of a sheet into a `Sheet` object.
    /// The string should contain rows separated by newlines, and columns within each row separated by tabs.
    /// Each cell in the sheet is represented as a raw string, which will be converted
    /// into a `Text` object.
    /// /// # Arguments
    /// * `width` - The number of columns in the sheet.
    /// * `height` - The number of rows in the sheet.
    /// * `content` - The raw string content of the sheet, where each row is separated by a newline
    ///   and each column within a row is separated by a tab.
    /// # Returns
    /// A `Sheet` object containing the parsed rows and columns.
    /// The raw str is expected to be formatted as follows:
    /// `row1_col1,ro1_col2,row1_col3;row_2_col1,row2_col2,row2_col3;...`
    pub fn parse_raw_str(width: usize, height: usize, content: &str) -> Self {
        let mut rows: Vec<Vec<Text>> = Vec::with_capacity(height);
        let raw_rows: Vec<&str> = content.split('|').collect();

        for raw_row in raw_rows {
            let row = raw_row.split(',').collect::<Vec<&str>>();
            let mut current_row = Vec::with_capacity(width);
            for cell in row {
                let text = Text::with_content(cell.to_string());
                current_row.push(text);
            }
            rows.push(current_row);
        }

        Sheet {
            rows,
            width,
            height,
        }
    }

    pub fn parse_raw_str_without_size(content: &str) -> Self {
        let raw_rows: Vec<&str> = content.split('|').collect();
        let height = raw_rows.len();
        let width = if height > 0 {
            raw_rows[0].split(',').count()
        } else {
            0
        };

        let mut rows: Vec<Vec<Text>> = Vec::with_capacity(height);

        for raw_row in raw_rows {
            let row = raw_row.split(',').collect::<Vec<&str>>();
            let mut current_row = Vec::with_capacity(width);
            for cell in row {
                let text = Text::with_content(cell.to_string());
                current_row.push(text);
            }
            rows.push(current_row);
        }

        Sheet {
            rows,
            width,
            height,
        }
    }

    pub fn get_row(&self, index: usize) -> Option<&Vec<Text>> {
        self.rows.get(index)
    }

    pub fn get_row_mut(&mut self, index: usize) -> Option<&mut Vec<Text>> {
        self.rows.get_mut(index)
    }

    pub fn get_column(&self, index: usize) -> Option<Vec<&Text>> {
        if self.rows.is_empty() || index >= self.rows[0].len() {
            return None;
        }
        Some(self.rows.iter().filter_map(|row| row.get(index)).collect())
    }

    pub fn get_column_mut(&mut self, index: usize) -> Option<Vec<&mut Text>> {
        if self.rows.is_empty() || index >= self.rows[0].len() {
            return None;
        }
        Some(
            self.rows
                .iter_mut()
                .filter_map(|row| row.get_mut(index))
                .collect(),
        )
    }

    pub fn insert_into_column(
        &mut self,
        row: usize,
        column: usize,
        position: usize,
        entries: Vec<CharEntry>,
    ) {
        if row >= self.rows.len() {
            // Ensure the row exists
            self.rows.resize(row + 1, Vec::new());
        }
        if column >= self.rows[row].len() {
            // Ensure the column exists in the specified row
            self.rows[row].resize(column + 1, Text::new());
        }
        let text = &mut self.rows[row][column];
        text.insert_chars(position, entries);
    }

    pub fn delete_in_column(&mut self, row: usize, column: usize, start: usize, end: usize) {
        if row < self.rows.len() && column < self.rows[row].len() {
            let text = &mut self.rows[row][column];
            text.delete_range(start, end);
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn to_string_sheet(&self) -> Vec<Vec<String>> {
        self.rows
            .iter()
            .map(|row| row.iter().map(|text| text.to_string()).collect())
            .collect()
    }

    pub fn to_raw_string(&self) -> String {
        let mut raw_string = Vec::new();
        for row in &self.rows {
            let row_string: Vec<String> = row.iter().map(|text| text.to_string()).collect();
            let row_raw = row_string.join(",");
            raw_string.push(row_raw);
        }
        raw_string.join("\\|")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sheet_creation() {
        let sheet = Sheet::new(3, 3);
        assert_eq!(sheet.width, 3);
        assert_eq!(sheet.height, 3);
        assert_eq!(sheet.rows.len(), 3);
        for row in &sheet.rows {
            assert_eq!(row.len(), 3);
        }
    }

    #[test]
    fn test_sheet_parse_raw_str() {
        let raw_str = "cell1,cell2,cell3|cell4,cell5,cell6|cell7,cell8,cell9";
        let sheet = Sheet::parse_raw_str(3, 3, raw_str);
        assert_eq!(sheet.width, 3);
        assert_eq!(sheet.height, 3);
        assert_eq!(sheet.rows.len(), 3);
        assert_eq!(sheet.rows[0][0].to_string(), "cell1");
        assert_eq!(sheet.rows[1][1].to_string(), "cell5");
        assert_eq!(sheet.rows[2][2].to_string(), "cell9");
    }

    #[test]
    fn test_sheet_get_row() {
        let sheet = Sheet::new(3, 3);
        assert!(sheet.get_row(0).is_some());
        assert!(sheet.get_row(3).is_none()); // Out of bounds
    }

    #[test]
    fn test_sheet_get_column() {
        let sheet = Sheet::new(3, 3);
        assert!(sheet.get_column(0).is_some());
        assert!(sheet.get_column(3).is_none()); // Out of bounds
    }

    #[test]
    fn test_sheet_insert_into_column() {
        let mut sheet = Sheet::new(3, 3);
        let entries = vec![
            CharEntry::new('A', 0, "test"),
            CharEntry::new('B', 0, "test"),
            CharEntry::new('C', 0, "test"),
        ];
        sheet.insert_into_column(0, 0, 0, entries);
        assert_eq!(sheet.rows[0][0].to_string(), "ABC".to_string());
    }

    #[test]
    fn test_raw_string_conversion() {
        let mut sheet = Sheet::new(3, 3);
        let entries = vec![
            CharEntry::new('A', 0, "test"),
            CharEntry::new('B', 0, "test"),
            CharEntry::new('C', 0, "test"),
        ];
        sheet.insert_into_column(0, 0, 0, entries);
        let raw_string = sheet.to_raw_string();
        assert_eq!(raw_string, "ABC,,|,,|,,");
    }
}
