use ui_client::main_ui::MainUI;

const APP_NAME: &str = "Rusty Docs";

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_active(true)
            .with_inner_size([480.0, 320.0])
            .with_title(APP_NAME)
            .with_resizable(true),
        ..Default::default()
    };

    eframe::run_native(
        APP_NAME,
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            Ok(Box::<MainUI>::default())
        }),
    )
}
