use documents_handler_service::{documents::DocumentResult, run};

fn main() -> DocumentResult<()> {
    let args: Vec<String> = std::env::args().collect();
    run(&args)
}
