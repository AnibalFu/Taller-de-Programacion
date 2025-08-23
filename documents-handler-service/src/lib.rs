use std::thread::spawn;

use documents::{
    DocumentResult, document_subscriptions_handler::DocumentSubscriptionsHandler,
    documents_tracker::DocumentTracker,
};
use events::events::{event::Event, list_event::ListEvent};
use logger::logger::Logger;
use redis_client::driver::redis_driver::RedisDriver;
use std::sync::{Arc, Mutex};

use crate::{
    configuration::Configuration,
    document_error::DocumentError,
    documents::document_operations::{
        creation_event_handler, disconnect_event_handler, full_document_handler,
        join_event_handler, list_documents, operation_event_handler, retrieve_documents_from_redis,
        save_all_documents,
    },
    save_timer::SaveTimer,
    sheets::{
        sheet_operations::{
            join_sheet_event_handler, list_sheets, retrieve_sheets_from_redis, save_all_sheets,
            sheet_creation_handler, sheet_operations_handler,
        },
        sheet_tracker::SheetTracker,
    },
};

const USER_DEFAULT: &str = "user";
const USER_DEFAULT_PASSWORD: &str = "default";

pub(crate) mod configuration;
pub mod document_error;
pub mod documents;
pub mod save_timer;
pub mod sheets;

pub fn run(args: &[String]) -> DocumentResult<()> {
    let configuration = Configuration::from_args(args)?;
    let subscriptions =
        DocumentSubscriptionsHandler::new(&configuration.redis_host, configuration.redis_port)?;
    let document_tracker = Arc::new(Mutex::new(DocumentTracker::new()?));
    let sheet_tracker = Arc::new(Mutex::new(SheetTracker::new()?));
    let mut redis_driver = RedisDriver::auth_connect(
        &configuration.redis_host,
        configuration.redis_port,
        USER_DEFAULT,
        USER_DEFAULT_PASSWORD,
    )?;
    let logger = Logger::new("microservice.log");

    let timer = SaveTimer::new(configuration.save_timer_ms); // Save every 10 seconds

    let document_tracker_clone = Arc::clone(&document_tracker);
    let sheet_tracker_clone = Arc::clone(&sheet_tracker);
    let save_logger = logger.clone();

    load_documents_and_sheets(
        &document_tracker,
        &sheet_tracker,
        &mut redis_driver,
        &logger,
    )?;

    start_backup_thread(
        configuration,
        timer,
        document_tracker_clone,
        sheet_tracker_clone,
        save_logger,
    );

    loop {
        match subscriptions.handle_incoming_message() {
            Ok(event) => {
                handle_incoming_message(
                    &document_tracker,
                    &sheet_tracker,
                    &mut redis_driver,
                    event,
                    &logger,
                )?;
            }
            Err(e) => {
                eprintln!("Error receiving message: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn start_backup_thread(
    configuration: Configuration,
    mut timer: SaveTimer,
    document_tracker_clone: Arc<Mutex<DocumentTracker>>,
    sheet_tracker_clone: Arc<Mutex<SheetTracker>>,
    save_logger: Logger,
) {
    spawn(move || {
        loop {
            if let Ok(mut redis_driver) = RedisDriver::auth_connect(
                &configuration.redis_host,
                configuration.redis_port,
                USER_DEFAULT,
                USER_DEFAULT_PASSWORD,
            ) {
                if timer.should_save() {
                    save_all_documents(&document_tracker_clone, &mut redis_driver, &save_logger);

                    save_all_sheets(&sheet_tracker_clone, &mut redis_driver, &save_logger);

                    timer.set_next_save();
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    });
}

fn handle_incoming_message(
    tracker: &Arc<Mutex<DocumentTracker>>,
    sheet_tracker: &Arc<Mutex<SheetTracker>>,
    conn: &mut RedisDriver,
    event: Event,
    logger: &Logger,
) -> DocumentResult<()> {
    match event {
        Event::DocumentCreationEvent(creation_event) => {
            let mut tracker = tracker.lock()?;
            creation_event_handler(&mut tracker, conn, logger, creation_event)?;
        }
        Event::OperationsEvent(incoming_operations_event) => {
            let mut tracker = tracker.lock()?;
            operation_event_handler(&mut tracker, logger, incoming_operations_event)?;
        }
        Event::JoinEvent(incoming_join_event) => {
            if incoming_join_event.file_type == "sheets" {
                let mut sheet_tracker = sheet_tracker.lock()?;
                join_sheet_event_handler(&mut sheet_tracker, conn, logger, incoming_join_event)?;
            } else {
                let mut tracker = tracker.lock()?;
                join_event_handler(&mut tracker, conn, logger, incoming_join_event)?;
            }
        }
        Event::DisconnectEvent(incoming_disconnect_event) => {
            disconnect_event_handler(conn, logger, incoming_disconnect_event)?;
        }
        Event::ListEvent(list_event) => list_event_handler(conn, list_event)?,
        Event::Sync(sync_event) => {
            logger.info(
                format!("Sync event received with content: {}", sync_event.content).as_str(),
                module_path!(),
            );
        }
        Event::SheetCreationEvent(sheet_creation_event) => {
            let mut sheet_tracker = sheet_tracker.lock()?;
            sheet_creation_handler(&mut sheet_tracker, conn, logger, sheet_creation_event)?;
        }
        Event::SheetOperationsEvent(sheet_operations_event) => {
            let mut sheet_tracker = sheet_tracker.lock()?;
            sheet_operations_handler(&mut sheet_tracker, logger, sheet_operations_event)?;
        }
        Event::SheetSync(sheet_sync_event) => {
            logger.info(
                format!(
                    "Sheet sync event received with content: {}",
                    sheet_sync_event.content
                )
                .as_str(),
                module_path!(),
            );
        }
        Event::FullDocumentEvent(full_document_event) => {
            full_document_handler(tracker, conn, logger, full_document_event)?;
        }
        _ => {
            logger.warn(
                format!("Unknown event type: {event:?}").as_str(),
                module_path!(),
            );
        }
    }
    Ok(())
}

fn list_event_handler(conn: &mut RedisDriver, list_event: ListEvent) -> Result<(), DocumentError> {
    match list_event.file_type.as_str() {
        "sheets" => {
            list_sheets(conn, &list_event)?;
        }
        "documents" => {
            list_documents(conn, &list_event)?;
        }
        _ => {
            eprint!("Unknown file type: {}", list_event.file_type);
        }
    };
    Ok(())
}

fn load_documents_and_sheets(
    document_tracker: &Arc<Mutex<DocumentTracker>>,
    sheet_tracker: &Arc<Mutex<SheetTracker>>,
    redis_driver: &mut RedisDriver,
    logger: &Logger,
) -> DocumentResult<()> {
    // Load documents
    retrieve_documents_from_redis(document_tracker, redis_driver, logger)?;

    // Load sheets
    retrieve_sheets_from_redis(sheet_tracker, redis_driver, logger)?;

    Ok(())
}
