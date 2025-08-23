use crate::{
    excel_parser::parse_command,
    main_ui::{FIXED_HEIGHT, FIXED_WIDTH},
    processes::Processes,
    ui_error::UIResult,
    user_data::UserData,
};
use common::{char_entry::CharEntry, lcs::atomic_ops, sheet::Sheet, text::Text};
use eframe::egui;
use egui::{Color32, RichText};
use events::events::{
    event::Event,
    sheet_operations_event::{SheetOperationEvent, SheetOperationsEvent},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    vec,
};

const CELL_WIDTH: f32 = 80.0; // ancho para cada celda
const CELL_HEIGHT: f32 = 30.0; // alto para cada celda
const ROW_NUMBER_SIDEBAR_WIDTH: f32 = 30.0; // Ancho para la barra lateral  (1, 2, 3...)
const CELL_SPACING_X: f32 = 1.0;
const CELL_SPACING_Y: f32 = 1.0;

pub struct RustyExcelUI {
    buffercells: Vec<Vec<String>>,
    sheet_name: String,
    sheet_id: String,
    processes: Arc<Mutex<Processes>>,
    user_data: Option<UserData>,
    users_connected: Vec<String>,
    backend_text: Option<Sheet>,
    is_dark: bool,
    cell_background_colors: Vec<Vec<Color32>>,
    selected_cell_for_color: Option<(usize, usize)>,
    sent_flag: bool,
}

impl RustyExcelUI {
    pub fn new(processes: Arc<Mutex<Processes>>) -> Self {
        Self {
            buffercells: Vec::new(),
            sheet_name: String::new(),
            sheet_id: String::new(),
            processes,
            user_data: None,
            users_connected: Vec::new(),
            backend_text: None,
            is_dark: true,
            cell_background_colors: vec![vec![Color32::BLACK; 15]; 15],
            selected_cell_for_color: None,
            sent_flag: false,
        }
    }

    pub fn subscribe_to_sheet_events(
        &mut self,
        id: &str,
        name: &str,
        user_data: &UserData,
    ) -> UIResult<()> {
        let channel_id = format!("sheets:{id}");
        self.sheet_id = id.to_string();
        self.sheet_name = name.to_string();

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
        }
        Ok(())
    }

    pub fn join_sheet(&mut self) -> UIResult<()> {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(user_data) = &self.user_data {
                if let Some(commands_process) = &mut processes.commands_process {
                    let command: Vec<String> = vec![
                        "PUBLISH".to_string(),
                        format!("sheets:{}", self.sheet_id),
                        format!("action\\:join\\;user_id\\:{}", user_data.username),
                    ];
                    commands_process.send_command(command)?;
                }

                if let Some(file_updates_process) = &mut processes.file_updates_process {
                    let join_event = file_updates_process.message_receiver.recv()?;
                    if let Event::JoinEvent(join_event) = join_event {
                        if !self.users_connected.contains(&join_event.user_id) {
                            self.users_connected.push(join_event.user_id);
                        }
                    }

                    let content = file_updates_process.message_receiver.recv()?;
                    if let Event::SheetSync(sync_event) = content {
                        self.backend_text = Some(Sheet::parse_raw_str(
                            sync_event.width,
                            sync_event.height,
                            &sync_event.content,
                        ));
                        self.buffercells = self.backend_text.as_ref().map_or_else(
                            || vec![vec!["".to_string()]],
                            |sheet| sheet.to_string_sheet(),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn handle_events(&mut self) {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(file_updates_process) = &mut processes.file_updates_process {
                while let Ok(event) = file_updates_process.message_receiver.try_recv() {
                    match event {
                        Event::JoinEvent(join_event) => {
                            if !self.users_connected.contains(&join_event.user_id) {
                                self.users_connected.push(join_event.user_id);
                            }
                        }
                        Event::DisconnectEvent(disconnect_event) => {
                            if let Some(pos) = self
                                .users_connected
                                .iter()
                                .position(|x| x == &disconnect_event.user_id)
                            {
                                self.users_connected.remove(pos);
                            }
                        }
                        Event::SheetOperationsEvent(sheet_operations_event) => {
                            let mut sheet = self.backend_text.as_ref().map_or_else(
                                || Sheet::new(FIXED_WIDTH, FIXED_HEIGHT),
                                |s| s.clone(),
                            );

                            update_sheet(&mut sheet, sheet_operations_event);
                            self.backend_text = Some(sheet);
                            self.buffercells = self.backend_text.as_ref().map_or_else(
                                || vec![vec!["".to_string()]],
                                |sheet| sheet.to_string_sheet(),
                            );
                        }
                        Event::SheetSync(sheet_sync_event) => {
                            self.backend_text = Some(Sheet::parse_raw_str(
                                sheet_sync_event.width,
                                sheet_sync_event.height,
                                &sheet_sync_event.content,
                            ));
                            self.buffercells = self.backend_text.as_ref().map_or_else(
                                || vec![vec!["".to_string()]],
                                |sheet| sheet.to_string_sheet(),
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn show_error_modal(&mut self, ctx: &egui::Context, message: &str) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.label(message);
            if ui.button("Forzar reinicio (ej: 5x5)").clicked() {
                self.buffercells = vec![vec!["".to_string(); 5]; 5];
            }
        });
    }

    fn show_save_header(&mut self, ui: &mut egui::Ui) -> UIResult<()> {
        ui.horizontal_wrapped(|ui_toolbar| -> UIResult<()> {
            //ui_toolbar.visuals_mut().button_frame = false;

            if ui_toolbar.button("💾").clicked() {
                let operations = self.compute_update();
                if operations.is_empty() {
                    return Ok(());
                }
                let operations_str = operations.join("\\|");
                if let Ok(mut processes) = self.processes.lock() {
                    if let Some(commands_process) = &mut processes.commands_process {
                        if let Some(user_data) = &self.user_data {
                            let command = format!(
                                "\"action\\:edition\\;user_id\\:{}\\;{}\"",
                                user_data.username, operations_str
                            );

                            let command = vec![
                                "PUBLISH".to_string(),
                                format!("sheets:{}", self.sheet_id),
                                command,
                            ];
                            commands_process.send_command(command)?;
                        }
                    }
                }
            };

            if ui_toolbar.button("🌗").clicked() {
                self.is_dark = !self.is_dark;
            };

            ui_toolbar.with_layout(
                egui::Layout::right_to_left(egui::Align::Center),
                |ui_right| {
                    for user in self.users_connected.iter().rev() {
                        let initial = if let Some(first_char) = user.chars().next() {
                            first_char.to_uppercase().to_string()
                        } else {
                            "U".to_string()
                        };
                        let color = egui::Color32::from_rgb(
                            user.len().wrapping_mul(37) as u8,
                            user.len().wrapping_mul(73) as u8,
                            user.len().wrapping_mul(139) as u8,
                        );
                        ui_right
                            .add(egui::Label::new(
                                egui::RichText::new(initial.to_string())
                                    .background_color(color)
                                    .color(egui::Color32::WHITE),
                            ))
                            .on_hover_text(user.to_string())
                            .on_hover_cursor(egui::CursorIcon::PointingHand);
                    }
                },
            );

            Ok(())
        });
        Ok(())
    }

    fn get_contrast_color(&self) -> Color32 {
        if self.is_dark {
            Color32::WHITE
        } else {
            Color32::BLACK
        }
    }

    fn show_header(&mut self, ui: &mut egui::Ui) -> UIResult<()> {
        let text_color = self.get_contrast_color();
        egui::Frame::new()
            .fill(Color32::from_rgb(11, 135, 3)) // color verde
            .outer_margin(egui::Margin::symmetric(0.0 as i8, 0.0 as i8))
            .inner_margin(egui::Margin::symmetric(4.0 as i8, 8.0 as i8))
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.label(
                    RichText::new(self.sheet_name.clone())
                        .heading()
                        .color(Color32::WHITE),
                );
                ui.separator();
                let _ = self.show_save_header(ui);
            });
        ui.horizontal(|ui_headers| {
            ui_headers.add_space(ROW_NUMBER_SIDEBAR_WIDTH + 10.0);
            let spacing = ui_headers.spacing_mut();
            spacing.item_spacing.x = CELL_SPACING_X;

            let cols_for_header = self.buffercells.first().map_or(0, |row| row.len());

            for i in 0..cols_for_header {
                let frame = egui::Frame::new();
                frame.show(ui_headers, |cell_content_ui| {
                    cell_content_ui
                        .set_max_size(egui::vec2(CELL_WIDTH, CELL_HEIGHT - CELL_SPACING_Y));
                    cell_content_ui.with_layout(
                        egui::Layout::centered_and_justified(egui::Direction::LeftToRight),
                        |cell_ui| {
                            cell_ui.label(
                                egui::RichText::new(format!("{}", (b'A' + i as u8) as char))
                                    .strong()
                                    .color(text_color),
                            );
                        },
                    );
                });
            }
        });
        Ok(())
    }

    fn get_rows_and_cols_to_draw(&mut self) -> (usize, usize) {
        if let Some(backend_text) = &self.backend_text {
            (backend_text.width, backend_text.height)
        } else {
            (FIXED_WIDTH, FIXED_HEIGHT)
        }
    }

    fn show_side_panel(&mut self, num_rows_to_draw: usize, row_numbers_ui: &mut egui::Ui) {
        let text_color = self.get_contrast_color();
        for i in 0..num_rows_to_draw {
            row_numbers_ui.allocate_ui_with_layout(
                egui::vec2(ROW_NUMBER_SIDEBAR_WIDTH, CELL_HEIGHT * 0.9),
                egui::Layout::centered_and_justified(egui::Direction::LeftToRight),
                |cell_content_ui| {
                    cell_content_ui.label(
                        egui::RichText::new(format!("{}", i + 1))
                            .strong()
                            .color(text_color),
                    );
                },
            );
            if i < num_rows_to_draw - 1 {
                row_numbers_ui.add_space(CELL_SPACING_Y);
            }
        }
    }

    fn show_grid(
        &mut self,
        horizontal_layout_ui: &mut egui::Ui,
        num_rows_to_draw: usize,
        num_cols_to_draw: usize,
    ) {
        egui::Grid::new("spreadsheet_grid")
            .striped(true)
            .spacing([CELL_SPACING_X, CELL_SPACING_Y])
            .show(horizontal_layout_ui, |grid_ui| {
                for r_idx in 0..num_rows_to_draw {
                    for c_idx in 0..num_cols_to_draw {
                        let cell_id = egui::Id::new(format!("{}{}", r_idx, b'A' + c_idx as u8));
                        let bg_color = self.cell_background_colors[r_idx][c_idx];

                        let cell = grid_ui.add_sized(
                            [CELL_WIDTH, CELL_HEIGHT],
                            egui::TextEdit::singleline(
                                self.buffercells
                                    .get_mut(r_idx)
                                    .and_then(|row| row.get_mut(c_idx))
                                    .map_or(&mut String::new(), |cell| cell),
                            )
                            .id(cell_id)
                            .background_color(bg_color),
                        );

                        cell.context_menu(|ui| {
                            if ui.button("Cambiar color de celda").clicked() {
                                self.selected_cell_for_color = Some((r_idx, c_idx));
                            }
                        });

                        let sheets_copy = self.buffercells.clone();
                        let enter_key_pressed = cell.ctx.input(|i| i.key_released(egui::Key::Enter));
                        if cell.lost_focus() || enter_key_pressed {
                            if let Some(val) = self.buffercells[r_idx].get_mut(c_idx) {
                                if val.starts_with('=') {
                                    let resultado = match parse_command(val.clone(), sheets_copy) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            self.show_error_modal(
                                                grid_ui.ctx(),
                                                &format!("Error al procesar la fórmula: {e}"),
                                            );
                                            continue;
                                        }
                                    };
                                    *val = resultado;
                                }
                            }
                            if enter_key_pressed && !self.sent_flag {
                                let operations = self.compute_update();
                                if operations.is_empty() {
                                    return;
                                }
                                let operations_str = operations.join("\\|");
                                if let Ok(mut processes) = self.processes.lock() {
                                    if let Some(commands_process) = &mut processes.commands_process
                                    {
                                        if let Some(user_data) = &self.user_data {
                                            let command = format!(
                                                "\"action\\:edition\\;user_id\\:{}\\;{}\"",
                                                user_data.username, operations_str
                                            );
                                            let command = vec![
                                                "PUBLISH".to_string(),
                                                format!("sheets:{}", self.sheet_id),
                                                command,
                                            ];
                                            if commands_process.send_command(command).is_ok() {
                                                // Successfully sent command
                                            } else {
                                                // Failed to send command
                                            }
                                        }
                                        self.sent_flag = true; // Set flag to indicate command sent
                                    }
                                }
                            }
                            if !enter_key_pressed && self.sent_flag{
                                self.sent_flag = false; // Reset flag after processing
                            }
                        }
                    }
                    grid_ui.end_row();
                }
            });
    }

    fn compute_update(&mut self) -> Vec<String> {
        let mut operations: Vec<String> = vec![];
        let backend_text = if let Some(backend_text) = &self.backend_text {
            backend_text
        } else {
            return operations;
        };

        for row in 0..self.buffercells.len() {
            for column in 0..self.buffercells[row].len() {
                let text = &backend_text.rows[row][column];
                let text_str = text.to_string();
                let buf_text = &self.buffercells[row][column];
                let result = atomic_ops(&text_str, buf_text);
                for res in result {
                    let operation = format!("{res}\\;column\\:{column}\\;row\\:{row}");
                    operations.push(operation);
                }
            }
        }

        operations
    }

    fn change_cells_colors(&mut self, new_color: Color32) {
        for fila in &mut self.cell_background_colors {
            for color in fila.iter_mut() {
                if *color == Color32::BLACK || *color == Color32::WHITE {
                    *color = new_color // gris oscuro
                }
            }
        }
    }

    fn check_dark_mode(&mut self, ctx: &egui::Context) {
        if self.is_dark {
            ctx.set_visuals(egui::Visuals::dark());
            self.change_cells_colors(Color32::BLACK); // gris oscuro
        } else {
            ctx.set_visuals(egui::Visuals::light());
            self.change_cells_colors(Color32::WHITE); // blanco
        }
    }

    fn show_color_picker(&mut self, ctx: &egui::Context, r_idx: usize, c_idx: usize) {
        if let Some(color) = self
            .cell_background_colors
            .get_mut(r_idx)
            .and_then(|row| row.get_mut(c_idx))
        {
            egui::Window::new("Selecciona un color")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    if egui::color_picker::color_edit_button_srgba(
                        ui,
                        color,
                        egui::color_picker::Alpha::Opaque,
                    )
                    .changed()
                    {
                        // se muta automáticamente
                    }

                    if ui.button("Cerrar").clicked() {
                        self.selected_cell_for_color = None;
                    }
                });
        }
    }
}
fn update_sheet(sheet: &mut Sheet, sheet_operations_event: SheetOperationsEvent) {
    let mut operations_bycell: HashMap<(usize, usize), Vec<SheetOperationEvent>> = HashMap::new();

    for operation in sheet_operations_event.operations {
        let key = match &operation {
            SheetOperationEvent::InsertIntoColumn(sheet_insert_event) => {
                (sheet_insert_event.row, sheet_insert_event.column)
            }
            SheetOperationEvent::DeleteIntoColumn(sheet_deletion_event) => {
                (sheet_deletion_event.row, sheet_deletion_event.column)
            }
        };
        operations_bycell.entry(key).or_default().push(operation);
    }

    for ((row, column), operations) in operations_bycell {
        let text = &mut sheet.rows[row][column];
        apply_operations(operations, text);
    }
}

fn apply_operations(operations: Vec<SheetOperationEvent>, text: &mut Text) {
    let mut delete_operations = Vec::new();
    let mut insert_operations = Vec::new();
    for op in operations {
        match op {
            SheetOperationEvent::InsertIntoColumn(sheet_insert_event) => {
                insert_operations.push(sheet_insert_event);
            }
            SheetOperationEvent::DeleteIntoColumn(sheet_deletion_event) => {
                delete_operations.push(sheet_deletion_event);
            }
        }
    }
    // Sort delete_operations by start position
    delete_operations.sort_by(|a, b| a.start.cmp(&b.start));
    // Sort insert_operations by position
    insert_operations.sort_by(|a, b| a.position.cmp(&b.position));
    // Apply delete operations
    for delete in delete_operations {
        let start = delete.start.min(text.len());
        let end = delete.end.min(text.len());
        text.delete_range(start, end);
    }
    // Apply insert operations
    for insert in insert_operations {
        let position = insert.position.min(text.len());
        let entries = insert
            .value
            .chars()
            .map(|c| CharEntry::new(c, 0, "test"))
            .collect();
        text.insert_chars(position, entries);
    }
}

impl eframe::App for RustyExcelUI {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.check_dark_mode(ctx);
        if self.buffercells.is_empty() || self.buffercells.first().is_none_or(|row| row.is_empty())
        {
            self.show_error_modal(
                ctx,
                "La hoja de cálculo no tiene datos o no se ha inicializado correctamente.",
            );
            return;
        }
        // -- ui grilla y barra lateral de números de fila --
        egui::CentralPanel::default().show(ctx, |ui| {
            let _ = self.show_header(ui);

            // --- logica para expandir el buffer si la ventana se agranda ---
            let (num_rows_to_draw, num_cols_to_draw) = self.get_rows_and_cols_to_draw();
            egui::ScrollArea::both()
                .id_salt("spreadsheet_scroll_area_combined")
                .auto_shrink([false; 2])
                .show(ui, |scroll_ui| {
                    scroll_ui.horizontal_top(|horizontal_layout_ui| {
                        horizontal_layout_ui.vertical(|row_numbers_ui| {
                            self.show_side_panel(num_rows_to_draw, row_numbers_ui);
                        });
                        horizontal_layout_ui.add_space(CELL_SPACING_X);
                        egui::Frame::new()
                            .fill(Color32::from_gray(40)) // gris oscuro
                            .inner_margin(egui::Margin::same(1.0 as i8))
                            .outer_margin(egui::Margin::same(0.0 as i8))
                            .show(horizontal_layout_ui, |grid_ui| {
                                self.show_grid(grid_ui, num_rows_to_draw, num_cols_to_draw);
                            });
                    });
                });
        });

        if let Some((r_idx, c_idx)) = self.selected_cell_for_color {
            self.show_color_picker(ctx, r_idx, c_idx);
        }

        self.handle_events();
    }
}
