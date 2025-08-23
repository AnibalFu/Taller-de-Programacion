use core::f32;
use json::libreria_json::obtener_campo_rec;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    vec,
};

use crate::{create_channel_name, processes::Processes, ui_error::UIResult, user_data::UserData};
use common::{char_entry::CharEntry, from_raw_string, lcs::atomic_ops, text::Text};
use egui::{Color32, Frame, InnerResponse, RichText, Stroke, TextureHandle, Ui};
use egui_commonmark::{CommonMarkCache, CommonMarkViewer};
use events::events::{event::Event, operation_event::OperationEvent};
use interpretefth::interpretar_texto;
use egui::text::CursorRange;

const USER: &str = "USER:";
const IA: &str = "IA:";
const IAERROR: &str = "IAERROR:";
pub struct RustyDocsUI {
    buffer_text: String,
    // This might be stored in redis
    document_name: String,
    document_id: String,
    processes: Arc<Mutex<Processes>>,
    user_data: Option<UserData>,
    users_connected: Vec<String>,
    old_text: String,
    backend_text: Text,
    cursor_byte: usize,
    is_dark: bool,
    show_ia_modal: bool,
    chat_input: String,
    chat_history: Vec<String>,
    cache: CommonMarkCache,
    emojis: HashMap<String, egui::TextureHandle>,
    asked_about_document: bool,
    request_id: String,
    cursor_range: Option<CursorRange>,
}

impl RustyDocsUI {
    pub fn new(processes: Arc<Mutex<Processes>>) -> Self {
        Self {
            buffer_text: String::new(),
            document_name: String::new(),
            document_id: String::new(),
            processes,
            user_data: None,
            users_connected: Vec::new(),
            old_text: String::new(),
            backend_text: Text::new(),
            cursor_byte: 0,
            is_dark: true,
            show_ia_modal: false,
            chat_input: String::new(),
            chat_history: Vec::new(),
            cache: CommonMarkCache::default(),
            emojis: HashMap::new(),
            asked_about_document: false,
            request_id: String::new(),
            cursor_range: None,
        }
    }

    pub fn subscribe_to_document_events(
        &mut self,
        id: String,
        name: String,
        user_data: UserData,
    ) -> UIResult<()> {
        let channel_id: String = create_channel_name(&id);
        self.document_id = id;
        self.document_name = name.clone();

        if let Ok(mut processes) = self.processes.lock() {
            let host = std::env::var("REDIS_HOST")?;

            let port = std::env::var("REDIS_PORT")?.parse::<u16>()?;

            let user = self
                .user_data
                .as_ref()
                .map_or(user_data.username.clone(), |ud| ud.username.clone());
            let password = self
                .user_data
                .as_ref()
                .map_or(user_data.password.clone(), |ud| ud.password.clone());

            processes.start_file_updates_process(&channel_id, &user, &password, &host, port);

            self.user_data = Some(user_data.clone());
            self.request_id = self.get_request_id();
            processes.start_ia_response_process(
                &user,
                &password,
                &host,
                port,
                self.request_id.clone(),
            );
        }
        Ok(())
    }

    pub fn join_document(&mut self) -> UIResult<()> {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(user_data) = &self.user_data {
                if let Some(commands_process) = processes.commands_process.as_mut() {
                    let command = vec![
                        "PUBLISH".to_string(),
                        format!("documents:{}", self.document_id),
                        format!("action\\:join\\;user_id\\:{}", user_data.username),
                    ];
                    commands_process.send_command(command)?;
                }

                if let Some(file_updates_process) = processes.file_updates_process.as_mut() {
                    let join_event = file_updates_process.message_receiver.recv()?; // Read join event
                    if let Event::JoinEvent(join_event) = join_event {
                        if !self.users_connected.contains(&join_event.user_id) {
                            self.users_connected.push(join_event.user_id.clone());
                        }
                    }

                    let content = file_updates_process.message_receiver.recv()?; // Wait for the Sync event after joining
                    if let Event::Sync(sync_event) = content {
                        self.backend_text = Text::with_content(sync_event.content.clone());
                        self.buffer_text = sync_event.content;
                        self.old_text = self.buffer_text.clone();
                        self.users_connected = sync_event.users.clone();
                    }
                }
            }
        }
        Ok(())
    }

    fn get_request_id(&self) -> String {
        let doc_id = self.document_id.clone();
        let username = match &self.user_data {
            Some(data) => data.username.clone(),
            None => "unknown_user".to_string(),
        };
        let request_id = format!("{doc_id}{username}");
        request_id
    }

    pub fn save(&mut self) {
        let differences = atomic_ops(&self.old_text, &self.buffer_text.to_string());
        if differences.is_empty() {
            return; // No changes to save
        }

        let operations = differences.join("\\|");
        self.old_text = self.buffer_text.to_string();

        if let Ok(mut processes) = self.processes.lock() {
            if let Some(commands_process) = processes.commands_process.as_mut() {
                if let Some(user_data) = &self.user_data {
                    let command = format!(
                        "action\\:edition\\;user_id\\:{}\\;{}",
                        user_data.username, operations
                    );

                    let command = vec![
                        "PUBLISH".to_string(),
                        format!("documents:{}", self.document_id),
                        command,
                    ];

                    let _ = commands_process.send_command(command);
                }
            }
        }
    }

    pub fn handle_events(&mut self) {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(file_updates_process) = processes.file_updates_process.as_mut() {
                if let Ok(content) = file_updates_process.message_receiver.try_recv() {
                    match content {
                        Event::OperationsEvent(operations_event) => {
                            let mut offset: isize = 0;
                            let text = &mut self.backend_text.clone();

                            for op in &operations_event.operations {
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

                            self.backend_text = text.clone();
                            self.buffer_text = from_raw_string(&text.get_content());
                            self.old_text = self.buffer_text.clone();
                        }
                        Event::JoinEvent(join_event) => {
                            if !self.users_connected.contains(&join_event.user_id) {
                                self.users_connected.push(join_event.user_id.clone());
                            }
                        }
                        Event::DisconnectEvent(disconnect_event) => {
                            if let Some(index) = self
                                .users_connected
                                .iter()
                                .position(|x| x == &disconnect_event.user_id)
                            {
                                self.users_connected.remove(index);
                            }
                        }
                        Event::Sync(sync_event) => {
                            self.backend_text = Text::with_content(sync_event.content.clone());
                            self.buffer_text = sync_event.content;
                            self.old_text = self.buffer_text.clone();
                            self.users_connected = sync_event.users.clone();
                        }
                        _ => {
                            // Handle other events if necessary
                        }
                    }
                }
            }
        }
    }

    pub fn handle_ia_response(&mut self) {
        let mut status: String = String::new();
        let mut response_text: String = String::new();

        if let Ok(mut processes) = self.processes.lock() {
            if let Some(ia_proc) = processes.ia_llm_process.as_mut() {
                if let Ok(respuesta) = ia_proc.message_receiver.try_recv() {
                    status = obtener_campo_rec(respuesta.to_string(), "status");
                    response_text = obtener_campo_rec(respuesta, "text");

                    if response_text.is_empty() {
                        self.chat_history
                            .push("IAERROR: No se pudo obtener la respuesta de la IA.".to_string());
                        return;
                    }
                    response_text = response_text
                        .replace("\\\"", "\"")
                        .trim_matches('"')
                        .to_string();
                } else {
                    return;
                }
            }
        }

        if status == "\"err\"" {
            self.chat_history.push(
                "IAERROR: Error al procesar la solicitud a la IA. Por favor, intente más tarde."
                    .to_string(),
            );
        } else if self.asked_about_document {
            self.buffer_text.clear();
            self.save();
            self.chat_history.push(format!("IA: {response_text}"));
            self.buffer_text = response_text.to_string();
            self.asked_about_document = false;
            self.cursor_byte = self.buffer_text.len();

        } else if !response_text.is_empty() {
            self.chat_history.push(format!("IA: {response_text}"));
            
            let char_index = if let Some(range) = self.cursor_range {
                range.primary.ccursor.index
            } else {
                self.buffer_text.len() + 1
            };

            let byte_index = self.buffer_text
                .char_indices()
                .nth(char_index)
                .map(|(byte_pos, _ch)| byte_pos)
                .unwrap_or(self.buffer_text.len());

            self.buffer_text.insert_str(byte_index, &response_text);

            self.cursor_byte = byte_index + response_text.len();
        }
        self.save();
    }

    pub fn set_id(&mut self, id: String) {
        self.document_id = id;
    }

    fn get_user_color(&self, user: &str) -> egui::Color32 {
        egui::Color32::from_rgb(
            user.len().wrapping_mul(37) as u8,
            user.len().wrapping_mul(73) as u8,
            user.len().wrapping_mul(139) as u8,
        )
    }

    fn show_users_connected(&mut self, ui: &mut Ui) {
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.horizontal(|ui| {
                for user in &self.users_connected {
                    let initial = if let Some(first_char) = user.chars().next() {
                        first_char.to_uppercase().to_string()
                    } else {
                        "U".to_string()
                    };

                    let color = self.get_user_color(user);

                    ui.add(egui::Label::new(
                        egui::RichText::new(initial.to_string())
                            .background_color(color)
                            .color(egui::Color32::WHITE),
                    ))
                    .on_hover_text(user.to_string())
                    .on_hover_cursor(egui::CursorIcon::PointingHand);
                }
            });
        });
    }

    fn check_dark_mode(&mut self, ctx: &egui::Context) {
        ctx.set_visuals(if self.is_dark {
            egui::Visuals::dark()
        } else {
            egui::Visuals::light()
        });
    }

    fn show_ia_chatbot(&mut self, ctx: &egui::Context) {
        self.save();

        egui::Window::new("Asistente IA")
        .open(&mut self.show_ia_modal)
        .resizable(true)
        .default_width(400.0)
        .show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.heading("Hola, en que puedo ayudarte?");
            });

            ui.separator();

           egui::ScrollArea::vertical()
            .max_height(300.0)
            .show(ui, |ui| {
                for mensaje in &self.chat_history {
                    if mensaje.starts_with(IA) {
                        ui.horizontal(|ui| {
                            show_dialog_cuadre(ui, mensaje.trim_end_matches('\n'), IA, egui::Color32::from_rgb(174, 182, 191));
                        });
                    } else if mensaje.starts_with(USER) {
                        ui.horizontal(|ui| {
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                                show_dialog_cuadre(ui, mensaje, USER, egui::Color32::from_rgb(174, 214, 241));
                            });
                        });
                    } else {
                        ui.horizontal(|ui| {
                            show_dialog_cuadre(ui, mensaje, IAERROR, egui::Color32::from_rgb(205, 97, 85));
                        });
                    }
                }
            });
            ui.separator();

            ui.horizontal(|ui| {
                let response = ui.text_edit_singleline(&mut self.chat_input);
                if (ui.button("Enviar").clicked() || response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) && !self.chat_input.trim().is_empty() {
                    let prompt = self.chat_input.trim().to_string();
                    self.chat_history.push(format!("USER: {prompt}"));                       
                    // Enviar el comando a Redis
                    if let Ok(mut processes) = self.processes.lock() {
                        if let Some(cmd_proc) = processes.commands_process.as_mut() {
                            let prompt = self.chat_input.trim();
                            let doc_id = self.document_id.clone();
                            let request_id = self.request_id.clone();
                            let type_request = if self.asked_about_document { "global" } else { "local" };
                            let cmd = format!(
                                "{{\"prompt\":\"{prompt}\", \"response_channel\":\"llm:{request_id}\", \"type_request\":\"{type_request}\", \"requestId\":\"{request_id}\", \"docId\":\"{doc_id}\"}}"
                            );

                            let cmd = vec![
                                "PUBLISH".to_string(),
                                "llm:request".to_string(),
                                cmd
                            ];
                            let _ = cmd_proc.send_command(cmd);
                        }
                    }
                        // Limpiar el campo de entrada
                    self.chat_input.clear();
                }
            });
        });
    }

    fn show_main_body_two_columns(&mut self, ctx: &egui::Context, ui_root: &mut Ui) {
        ui_root.columns(2, |cols| {
            self.show_main_editor(cols);
            self.show_main_preview(ctx, cols);
        })
    }

    fn get_contrast_color(&self) -> egui::Color32 {
        if self.is_dark {
            egui::Color32::WHITE
        } else {
            egui::Color32::BLACK
        }
    }

    fn show_main_editor(&mut self, cols: &mut [egui::Ui]) {
        let border_color = self.get_contrast_color();

        cols[0].vertical(|ui_left| {
            ui_left.heading("✏ Editor de texto");
            ui_left.separator();

            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .show(ui_left, |ui_scroll| {
                    egui::Frame::new()
                        .stroke(egui::Stroke::new(1.5, border_color))
                        .corner_radius(egui::CornerRadius::same(4))
                        .show(ui_scroll, |ui| {
                            let out = egui::TextEdit::multiline(&mut self.buffer_text)
                                .desired_width(f32::INFINITY)
                                .desired_rows(20)
                                .id_source("editor")
                                .show(ui);

                            if out.response.changed() {
                                self.save();
                            }
                            if let Some(range) = out.cursor_range {
                                self.cursor_range = Some(range);
                                self.cursor_byte = range.primary.ccursor.index;
                            }

                            out.response.context_menu(|ui| {
                                if ui.button("💻 Preguntar a la IA").clicked() {
                                    self.show_ia_modal = true;
                                    ui.close_menu();
                                }
                            });
                        });
                });
        });
    }

    fn show_main_preview(&mut self, ctx: &egui::Context, cols: &mut [egui::Ui]) {
        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .show(&mut cols[1], |ui_right| {
                ui_right.heading("🔍 Vista previa");
                ui_right.separator();

                let min_width = ui_right.available_width();

                egui::Frame::group(&ctx.style())
                    .corner_radius(egui::CornerRadius::same(4))
                    .inner_margin(egui::Margin::same(8))
                    .show(ui_right, |ui_group| {
                        ui_group.set_min_height(275.0);
                        ui_group.set_min_width(min_width);

                        CommonMarkViewer::new()
                            .syntax_theme_light("InspiredGitHub")
                            .syntax_theme_dark("base16-ocean.dark")
                            .show(ui_group, &mut self.cache, &self.buffer_text);
                    });
            });
    }

    fn show_document_header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            if let Some(button) = emoji_button(&self.emojis, "save_emoji", "Guardar", 18.0) {
                let save_response = ui.add(button);
                if save_response.clicked() {
                    self.save();
                }
            };

            if let Some(button) =
                emoji_button(&self.emojis, "dark_mode_emoji", "Cambiar tema", 18.0)
            {
                let dark_mode_response = ui.add(button);
                if dark_mode_response.clicked() {
                    self.is_dark = !self.is_dark;
                }
            };

            if let Some(button) = emoji_button(
                &self.emojis,
                "ia_emoji",
                "Preguntar a la IA sobre el documento",
                18.0,
            ) {
                let ia_response = ui.add(button);
                if ia_response.clicked() {
                    self.show_ia_modal = !self.show_ia_modal;
                    self.asked_about_document = true;
                }
            };

            self.show_users_connected(ui);
        });
    }

    fn load_emoji_from_assets(
        &mut self,
        path: &'static [u8],
        name: &str,
        ctx: &egui::Context,
    ) -> egui::TextureHandle {
        let emoji_image = image::load_from_memory(path).unwrap().to_rgba8();
        let size = [emoji_image.width() as usize, emoji_image.height() as usize];
        ctx.load_texture(
            name,
            egui::ColorImage::from_rgba_unmultiplied(size, &emoji_image),
            egui::TextureOptions::default(),
        )
    }

    fn show_toolbar_header(&mut self, ui_root: &mut Ui) {
        Frame::new()
            .fill(Color32::from_rgb(0, 120, 215))
            .inner_margin(egui::Margin::same(4.0 as i8))
            .stroke(Stroke::NONE)
            .show(ui_root, |ui_root| {
                ui_root.vertical(|ui_header| {
                    ui_header.label(
                        RichText::new(&self.document_name)
                            .heading()
                            .color(Color32::WHITE),
                    );
                    ui_header.add_space(8.0);
                    ui_header.separator();
                    self.show_document_header(ui_header);
                });
                ui_root.separator();
                ui_root.add_space(4.0);

                ui_root.horizontal(|ui_bar| {
                    if let Some(button) = emoji_button(&self.emojis, "save_emoji", "Exportar", 18.0) {
                        let save_response = ui_bar.add(button);
                        if save_response.clicked() {
                            if let Some(path) = rfd::FileDialog::new()
                                .set_file_name("documento.md")
                                .save_file()
                            {
                                let _ = std::fs::write(&path, &self.buffer_text);
                            }
                        }
                    };

                    if let Some(button) = emoji_button(&self.emojis, "image_emoji", "Imagen", 18.0) {
                        let image_response = ui_bar.add(button);
                        if image_response.clicked() {
                            self.buffer_text.insert_str(self.cursor_byte, "![Ejemplo](https://rustacean.net/assets/rustacean-flat-happy.png)\n");
                            self.cursor_byte = self.buffer_text.len();
                            self.save();
                        }
                    };

                    if let Some(button) = emoji_button(&self.emojis, "clear_emoji", "Limpiar", 18.0) {
                        let trash_response = ui_bar.add(button);
                        if trash_response.clicked() {
                            self.buffer_text.clear();
                            self.cursor_byte = 0;
                            self.save();
                        };
                    };

                    if let Some(button) = emoji_button(&self.emojis, "copy_emoji", "Copiar", 18.0) {
                        let habilitado = !self.buffer_text.is_empty();
                        let response = ui_bar.add_enabled(habilitado, button);
                        if response.clicked() {
                            ui_bar.ctx().copy_text(self.buffer_text.clone());
                        };
                    };

                    // Interprete forth
                    if let Some(button) = emoji_button(&self.emojis, "fth_emoji", "Forth", 18.0) {
                        let resto_response = ui_bar.add(button);
                        if resto_response.clicked() {
                            let texto = &self.buffer_text;
                            let lineas: Vec<String> = texto
                                .lines()
                                .map(|linea| linea.to_string())
                                .collect();
                            let (resultado, stack) = interpretar_texto(lineas);
                            self.save();
                            self.buffer_text.push_str("\n\n<-- **RESULTADO** -->");
                            self.buffer_text.push_str(&format!("\n\n{resultado}"));
                            self.buffer_text.push_str("\n\n<-- **STACK RESTANTE** -->");
                            self.buffer_text.push_str(&format!("\n\n{stack} <-- TOPE"));
                            self.save();
                        };
                    };
                    /* ComboBox de inserción rápida (usa cursor_byte) */
                    egui::ComboBox::from_label(RichText::new("Insertar…").color(Color32::WHITE),)
                        .selected_text("Seleccione")
                        .show_ui(ui_bar, |ui_combo| {
                            if ui_combo.button("Título H1").clicked() {
                                self.buffer_text.insert_str(self.cursor_byte, "# Título\n");
                                self.cursor_byte = self.buffer_text.len();
                                self.save();
                            }
                            if ui_combo.button("Lista viñetas").clicked() {
                                self.buffer_text
                                    .insert_str(self.cursor_byte, "- Item 1\n- Item 2\n");
                                self.cursor_byte = self.buffer_text.len();
                                self.save();
                            }
                            if ui_combo.button("Tabla 2×2").clicked() {
                                self.buffer_text.insert_str(
                                    self.cursor_byte,
                                    "| Cab 1 | Cab 2 |\n|-------|-------|\n|  A    |   B   |\n|  C    |   D   |\n",
                                );
                                self.cursor_byte = self.buffer_text.len();
                                self.save();
                            }
                            if ui_combo.button("Bloque código Rust").clicked() {
                                self.buffer_text.insert_str(
                                    self.cursor_byte,
                                    "rust\nfn main() {\n    println!(\"Hola\");\n}\n\n",
                                );
                                self.cursor_byte = self.buffer_text.len();
                                self.save();
                            }
                        });
                });

            });
    }

    fn insert_emoji(&mut self, key: &str, bytes: &'static [u8], ctx: &egui::Context) {
        let tex = self.load_emoji_from_assets(bytes, key, ctx);
        self.emojis.insert(key.to_string(), tex);
    }
}

impl eframe::App for RustyDocsUI {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.check_dark_mode(ctx);
        if self.emojis.is_empty() {
            self.insert_emoji(
                "note_emoji",
                include_bytes!("../../././assets/copy.png"),
                ctx,
            );
            self.insert_emoji(
                "save_emoji",
                include_bytes!("../../././assets/save.png"),
                ctx,
            );
            self.insert_emoji(
                "image_emoji",
                include_bytes!("../../././assets/image.png"),
                ctx,
            );
            self.insert_emoji(
                "copy_emoji",
                include_bytes!("../../././assets/copy.png"),
                ctx,
            );
            self.insert_emoji(
                "clear_emoji",
                include_bytes!("../../././assets/trash.png"),
                ctx,
            );
            self.insert_emoji(
                "dark_mode_emoji",
                include_bytes!("../../././assets/darkMode.png"),
                ctx,
            );
            self.insert_emoji("ia_emoji", include_bytes!("../../././assets/ia.png"), ctx);
            self.insert_emoji("fth_emoji", include_bytes!("../../././assets/f.png"), ctx);
        }

        egui::CentralPanel::default().show(ctx, |ui: &mut Ui| {
            self.show_toolbar_header(ui);

            ui.add_space(10.0);
            ui.separator();
            ui.add_space(5.0);
            self.show_main_body_two_columns(ctx, ui);

            if self.show_ia_modal {
                // subscrtibirme a canal llm:response
                self.show_ia_chatbot(ctx);
            };

            self.handle_events();
            self.handle_ia_response();
        });
    }
}

fn emoji_button<'a>(
    emojis: &'a HashMap<String, TextureHandle>,
    key: &str,
    label: &str,
    size: f32,
) -> Option<egui::Button<'a>> {
    emojis
        .get(key)
        .map(|emoji| egui::Button::image_and_text((emoji.id(), egui::Vec2::splat(size)), label))
}

fn agregar_saltos_de_linea(mensaje: &str) -> String {
    let mut mensaje_final = String::new();
    let mut contador_de_palabras = 0;
    for palabra in mensaje.split_ascii_whitespace() {
        if contador_de_palabras == 5 {
            mensaje_final.push('\n');
            contador_de_palabras = 0;
        } else {
            contador_de_palabras += 1;
        }
        mensaje_final.push_str(palabra);
        mensaje_final.push(' ');
    }
    mensaje_final.to_string()
}

fn show_dialog_cuadre(ui: &mut Ui, mensaje: &str, user: &str, color: Color32) -> InnerResponse<()> {
    let mensaje_final = agregar_saltos_de_linea(mensaje.trim_start_matches(user));
    egui::Frame::new()
        .fill(color)
        .corner_radius(egui::CornerRadius::same(10.0 as u8))
        .stroke(egui::Stroke::new(1.0, egui::Color32::GRAY))
        .inner_margin(egui::vec2(10.0, 6.0))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(mensaje_final).color(egui::Color32::BLACK));
        })
}
