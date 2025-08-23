use std::sync::{Arc, Mutex};

use egui::{Image, RichText, Spacing, Ui};
use events::events::event::Event;

use crate::{
    processes::Processes,
    rusty_docs_ui::RustyDocsUI,
    rusty_excel_ui::RustyExcelUI,
    ui_error::{UIError, UIErrorKind, UIResult},
    user_data::{FileSelected, UserData},
};

pub const FIXED_WIDTH: usize = 15;
pub const FIXED_HEIGHT: usize = 15;

pub struct MainUI {
    pub user_data: UserData,
    pub document_selected: bool,
    pub excel_document_selected: bool,
    pub rusty_docs_ui: RustyDocsUI,
    pub rusty_excel_ui: RustyExcelUI,
    pub show_modal_name: bool,
    pub show_current_files_modal: bool,
    pub document_name: String,
    pub excel_document_name: String,
    pub doc_channels: Vec<String>,
    pub sheet_channels: Vec<String>,
    pub command_sent: bool,
    pub show_excel_modal_name: bool,
    pub show_excel_current_files_modal: bool,
    pub processes: Arc<Mutex<Processes>>,
    pub logo_texture: Option<egui::TextureHandle>,
}

impl Default for MainUI {
    fn default() -> Self {
        let processes = Arc::new(Mutex::new(Processes::default()));
        Self {
            user_data: UserData::new(),
            document_selected: false,
            excel_document_selected: false,
            rusty_docs_ui: RustyDocsUI::new(processes.clone()),
            rusty_excel_ui: RustyExcelUI::new(processes.clone()),
            show_current_files_modal: false,
            show_modal_name: false,
            sheet_channels: Vec::new(),
            document_name: String::new(),
            excel_document_name: String::new(),
            doc_channels: Vec::new(),
            command_sent: false,
            show_excel_modal_name: false,
            show_excel_current_files_modal: false,
            processes,
            logo_texture: None,
        }
    }
}

impl eframe::App for MainUI {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        if self.logo_texture.is_none() {
            let image_bytes = include_bytes!("../../././assets/crab.png");
            let image = image::load_from_memory(image_bytes).unwrap().to_rgba8();
            let size = [image.width() as usize, image.height() as usize];
            let color_image = egui::ColorImage::from_rgba_unmultiplied(size, &image);
            let texture =
                ctx.load_texture("mi-imagen", color_image, egui::TextureOptions::default());
            self.logo_texture = Some(texture);
        }
        let has_logged_in = self.user_data.logged_in;

        if has_logged_in {
            match &self.user_data.file_selected {
                Some(FileSelected::Document(_, _)) => {
                    self.rusty_docs_ui.update(ctx, frame);
                }
                Some(FileSelected::Sheet(_, _)) => {
                    self.rusty_excel_ui.update(ctx, frame);
                }
                None => {
                    let _ = self.main_ui_components(ctx);
                }
            }
        } else {
            let _ = self.log_in(ctx);
        };
    }
}

impl MainUI {
    fn show_document_creation_modal(&mut self, ctx: &egui::Context) -> UIResult<()> {
        egui::Window::new("Nombre del Documento")
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Center the window
            .show(ctx, |ui| {
                ui.label("Ingrese el nombre del documento:");
                ui.add(egui::TextEdit::singleline(&mut self.document_name));
                ui.horizontal(|ui| -> UIResult<()> {
                    if ui.button("Aceptar").clicked() {
                        let document_name = self.document_name.clone();
                        let id = self.create_document(&document_name)?;

                        let _ = self.rusty_docs_ui.subscribe_to_document_events(
                            id.to_string(),
                            document_name.clone(),
                            self.user_data.clone(),
                        );

                        // Needed for race conditions
                        std::thread::sleep(std::time::Duration::from_millis(1000));

                        let _ = self.rusty_docs_ui.join_document();
                        self.user_data.file_selected =
                            Some(FileSelected::Document(document_name, id));
                        self.show_modal_name = false; // Cierra el modal
                    }

                    if ui.button("Cancelar").clicked() {
                        self.show_modal_name = false; // Cierra sin cambios
                    }
                    Ok(())
                });
            });
        Ok(())
    }

    fn show_excel_creation_modal(&mut self, ctx: &egui::Context) {
        egui::Window::new("Nombre de la hoja de cálculo")
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Center the window
            .show(ctx, |ui| {
                ui.label("Ingrese el nombre de la planilla:");
                ui.add(egui::TextEdit::singleline(&mut self.excel_document_name));
                ui.horizontal(|ui| -> UIResult<()> {
                    if ui.button("Aceptar").clicked() {
                        let document_name = self.excel_document_name.clone();
                        let id = self.create_sheet(&document_name)?;
                        let _ = self.rusty_excel_ui.subscribe_to_sheet_events(
                            &id,
                            &document_name,
                            &self.user_data,
                        );

                        // Needed for race conditions
                        std::thread::sleep(std::time::Duration::from_millis(500));

                        let _ = self.rusty_excel_ui.join_sheet();
                        self.user_data.file_selected = Some(FileSelected::Sheet(document_name, id));
                        self.show_excel_modal_name = false; // Cierra el modal
                    }

                    if ui.button("Cancelar").clicked() {
                        self.show_excel_modal_name = false; // Cierra sin cambios
                    }
                    Ok(())
                });
            });
    }

    fn display_documents_available_for_edition(&mut self, ctx: &egui::Context) -> UIResult<()> {
        egui::Window::new("Documentos")
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Centrado
            .show(ctx, |ui| -> UIResult<()> {
                ui.horizontal(|ui| -> UIResult<()> {
                    ui.label("Documentos disponibles para la edición:");
                    if ui.button("🔄").clicked() {
                        self.list_files("documents")?;
                    }
                    Ok(())
                });

                ui.separator();

                egui::ScrollArea::vertical()
                    .max_height(300.0)
                    .auto_shrink([false; 2])
                    .show(ui, |scroll_ui| -> UIResult<()> {
                        for channel in self.doc_channels.iter() {
                            let (file, id) = if let Some((file, id)) = channel.split_once(":") {
                                (file.to_string(), id.to_string())
                            } else {
                                continue;
                            };

                            if scroll_ui.button(file.to_string()).clicked() {
                                let _ = self.rusty_docs_ui.subscribe_to_document_events(
                                    id.to_string(),
                                    file.to_string(),
                                    self.user_data.clone(),
                                );

                                std::thread::sleep(std::time::Duration::from_millis(500));

                                let _ = self.rusty_docs_ui.join_document();

                                self.user_data.file_selected =
                                    Some(FileSelected::Document(file.to_string(), id.to_string()));
                                self.show_current_files_modal = false;
                            }
                        }
                        Ok(())
                    });

                ui.separator();

                ui.horizontal(|ui| {
                    if ui.button("Cerrar").clicked() {
                        self.show_current_files_modal = false;
                    }
                    self.command_sent = false;
                });
                Ok(())
            });
        Ok(())
    }

    fn display_sheets_available_for_edition(&mut self, ctx: &egui::Context) -> UIResult<()> {
        egui::Window::new("Hojas de cálculo")
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Centrado
            .show(ctx, |ui| -> UIResult<()> {
                ui.horizontal(|ui| {
                    ui.label("Hojas de cálculo disponibles para la edición:");
                    if ui.button("🔄").clicked() {
                        let _ = self.list_files("sheets");
                    }
                });

                ui.separator();

                egui::ScrollArea::vertical()
                    .max_height(300.0)
                    .auto_shrink([false; 2])
                    .show(ui, |scroll_ui| -> UIResult<()> {
                        for channel in self.sheet_channels.iter() {
                            let (file, id) = if let Some((file, id)) = channel.split_once(":") {
                                (file.to_string(), id.to_string())
                            } else {
                                continue;
                            };

                            if scroll_ui.button(file.to_string()).clicked() {
                                self.rusty_excel_ui.subscribe_to_sheet_events(
                                    &id,
                                    &file,
                                    &self.user_data,
                                )?;

                                std::thread::sleep(std::time::Duration::from_millis(500));

                                self.rusty_excel_ui.join_sheet()?;

                                self.user_data.file_selected =
                                    Some(FileSelected::Sheet(file.to_string(), id.to_string()));
                                self.show_excel_modal_name = false; // Cierra el modal
                            }
                        }
                        Ok(())
                    });

                ui.separator();

                ui.horizontal(|ui| {
                    if ui.button("Cerrar").clicked() {
                        self.show_excel_current_files_modal = false; // Cierra el modal
                    }
                    self.command_sent = false; // Reset command_sent
                });
                Ok(())
            });
        Ok(())
    }

    fn log_in(&mut self, ctx: &egui::Context) -> UIResult<()> {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                // Background text pattern
                ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                    let background_text = "Rusty Docs";

                    ui.label(
                        RichText::new(background_text)
                            .color(egui::Color32::from_rgba_premultiplied(128, 128, 128, 50))
                            .size(24.0),
                    );
                });
            });
        });

        egui::Window::new("Iniciar Sesión")
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Center the window
            .show(ctx, |ui| {
                ui.label("usuario:");
                ui.add(egui::TextEdit::singleline(&mut self.user_data.username));

                ui.label("contraseña:");
                ui.add(egui::TextEdit::singleline(&mut self.user_data.password).password(true));

                ui.horizontal(|ui| -> UIResult<()> {
                    if ui.button("Aceptar").clicked() {
                        // Process the username and password
                        if self.user_data.username.is_empty() || self.user_data.password.is_empty()
                        {
                            ui.label("Por favor, complete todos los campos.");
                        } else {
                            let host = std::env::var("REDIS_HOST")?;

                            let port = std::env::var("REDIS_PORT")?.parse::<u16>()?;

                            let user = self.user_data.username.clone();
                            let password = self.user_data.password.clone();

                            if let Ok(mut processes) = self.processes.lock() {
                                processes.start_commands_process(&user, &password, &host, port)?;
                                processes.start_self_updates_process(&user, &password, &host, port);
                                self.user_data.logged_in = true;
                            }
                        }
                    }
                    Ok(())
                });
            });
        Ok(())
    }

    fn main_ui_components(&mut self, ctx: &egui::Context) -> UIResult<()> {
        egui::TopBottomPanel::top("title")
            .resizable(true)
            .min_height(48.0)
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.add_space(60.0);
                    let img_size = egui::Vec2::splat(32.0);

                    if let Some(texture) = &self.logo_texture {
                        ui.add(Image::new(texture).max_size(img_size));
                        ui.add_space(8.0);
                    }

                    ui.heading(
                        RichText::new("Rusty Docs")
                            .color(egui::Rgba::from_rgb(111.0, 65.0, 25.0))
                            .size(48.0),
                    );

                    if let Some(texture) = &self.logo_texture {
                        ui.add_space(8.0);
                        ui.add(Image::new(texture).max_size(img_size));
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui: &mut Ui| -> UIResult<()> {
            ui.horizontal_top(|ui| -> UIResult<()> {
                ui.vertical_centered(|ui| {
                    ui.add_space(Spacing::default().item_spacing.y);

                    if self.show_modal_name {
                        let _ = self.show_document_creation_modal(ctx);
                    } else if self.show_excel_modal_name {
                        self.show_excel_creation_modal(ctx);
                    } else {
                        if ui.button("📄 Crear documento").clicked() {
                            self.show_modal_name = true;
                        }

                        ui.add_space(Spacing::default().item_spacing.y);

                        if ui.button("📊 Crear hoja de cálculo").clicked() {
                            self.show_excel_modal_name = true;
                        }

                        ui.add_space(Spacing::default().item_spacing.y);

                        if ui.button("📂 Abrir documentos").clicked() {
                            self.show_current_files_modal = true;
                            if self.doc_channels.is_empty() && !self.command_sent {
                                let _ = self.list_files("documents");
                                self.command_sent = true;
                            }
                        }

                        if ui.button("📊 Abrir hojas de cálculo").clicked() {
                            self.show_excel_current_files_modal = true;
                            if self.sheet_channels.is_empty() && !self.command_sent {
                                let _ = self.list_files("sheets");
                                self.command_sent = true
                            }
                        }

                        if self.show_current_files_modal {
                            let _ = self.display_documents_available_for_edition(ctx);
                        }

                        if self.show_excel_current_files_modal {
                            let _ = self.display_sheets_available_for_edition(ctx);
                        }
                    }
                });
                Ok(())
            });
            Ok(())
        });
        Ok(())
    }

    fn list_files(&mut self, file_type: &str) -> UIResult<()> {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(redis_command_process) = processes.commands_process.as_mut() {
                let command = format!(
                    "action\\:list\\;user_id\\:{}\\;file_type\\:{}",
                    self.user_data.username, file_type
                );
                let command = vec![
                    "PUBLISH".to_string(),
                    format!("{}:utils", file_type),
                    command,
                ];
                redis_command_process.send_command(command)?;
                if let Some(self_process) = processes.self_updates_process.as_mut() {
                    let content = self_process.message_receiver.recv()?;
                    if let Event::ResponseListEvent(list_event) = content {
                        if file_type == "sheets" {
                            self.sheet_channels = list_event.files.clone();
                        } else if file_type == "documents" {
                            // Assuming documents are handled in the same way
                            self.doc_channels = list_event.files.clone();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn create_document(&mut self, name: &str) -> UIResult<String> {
        match self.processes.lock() {
            Ok(mut processes) => match processes.commands_process.as_mut() {
                Some(redis_command_process) => {
                    let command = format!(
                        "action\\:create\\;document_name\\:{}\\;user_id\\:{}",
                        name, self.user_data.username
                    );
                    let command = vec![
                        "PUBLISH".to_string(),
                        "documents:utils".to_string(),
                        command,
                    ];
                    let _ = redis_command_process.send_command(command);

                    match processes.self_updates_process.as_mut() {
                        Some(self_process) => {
                            let content = self_process.message_receiver.recv()?;
                            if let Event::ResponseCreationEvent(creation_event) = content {
                                Ok(creation_event.document_id)
                            } else {
                                Err(UIError::new(
                                    format!("Expected ResponseCreationEvent, got: {content:?}"),
                                    UIErrorKind::Other,
                                ))
                            }
                        }
                        None => Err(UIError::new(
                            "Self updates process is not available.".to_string(),
                            UIErrorKind::ConnectionError,
                        )),
                    }
                }
                None => Err(UIError::new(
                    "Commands process is not available.".to_string(),
                    UIErrorKind::ConnectionError,
                )),
            },
            Err(e) => Err(UIError::new(
                format!("Failed to lock processes: {e}"),
                UIErrorKind::Other,
            )),
        }
    }

    fn create_sheet(&mut self, name: &str) -> UIResult<String> {
        match self.processes.lock() {
            Ok(mut processes) => match processes.commands_process.as_mut() {
                Some(redis_command_process) => {
                    let command = format!(
                        "action\\:create\\;sheet_name\\:{}\\;user_id\\:{}\\;width\\:{}\\;height\\:{}",
                        name, self.user_data.username, FIXED_WIDTH, FIXED_HEIGHT
                    );
                    let command = vec!["PUBLISH".to_string(), "sheets:utils".to_string(), command];
                    let _ = redis_command_process.send_command(command);

                    match processes.self_updates_process.as_mut() {
                        Some(self_process) => {
                            let content = self_process.message_receiver.recv()?;
                            if let Event::ResponseSheetCreationEvent(creation_event) = content {
                                Ok(creation_event.sheet_id)
                            } else {
                                Err(UIError::new(
                                    format!(
                                        "Expected ResponseSheetCreationEvent, got: {content:?}"
                                    ),
                                    UIErrorKind::Other,
                                ))
                            }
                        }
                        None => Err(UIError::new(
                            "Self updates process is not available.".to_string(),
                            UIErrorKind::ConnectionError,
                        )),
                    }
                }
                None => Err(UIError::new(
                    "Commands process is not available.".to_string(),
                    UIErrorKind::ConnectionError,
                )),
            },
            Err(e) => Err(UIError::new(
                format!("Failed to lock processes: {e}"),
                UIErrorKind::Other,
            )),
        }
    }
}
