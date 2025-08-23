#[derive(Debug, Clone)]
pub enum FileSelected {
    Document(String, String),
    Sheet(String, String),
}

impl FileSelected {
    pub fn new_document(name: String, id: String) -> Self {
        FileSelected::Document(name, id)
    }
    pub fn new_excel(name: String, id: String) -> Self {
        FileSelected::Sheet(name, id)
    }
}

#[derive(Debug, Clone)]
pub struct UserData {
    pub username: String,
    pub password: String,
    pub logged_in: bool,
    pub file_selected: Option<FileSelected>,
}

impl Default for UserData {
    fn default() -> Self {
        Self::new()
    }
}

impl UserData {
    pub fn new() -> Self {
        UserData {
            username: String::new(),
            password: String::new(),
            logged_in: false,
            file_selected: None,
        }
    }

    pub fn set_file_selected(&mut self, file: FileSelected) {
        self.file_selected = Some(file);
    }

    pub fn clear_file_selected(&mut self) {
        self.file_selected = None;
    }
}
