use std::{
    sync::mpsc::{Receiver, Sender},
    thread::spawn,
};

use events::events::{event::Event, events_api::handle_microservice_pubsub_message};
use redis_client::driver::{redis_driver::RedisDriver, traits::FromRedis};

use crate::document_error::DocumentError;

use super::DocumentResult;
const DOCUMENTS_CHANNEL_PREFIX: &str = "documents:*";
const SHEETS_CHANNEL_PREFIX: &str = "sheets:*";

// This module handles the subscription to document creation events via Redis Pub/Sub.
// It provides a `DocumentSubscriptionsHandler` struct that manages the subscription and message handling.
// The `DocumentSubscriptionsHandler` struct contains a receiver that can be used to handle incoming document events.
pub struct DocumentSubscriptionsHandler {
    /// A receiver that listens for incoming document events.
    pub receiver: Receiver<Event>,
}

impl DocumentSubscriptionsHandler {
    /// Creates a new instance of `DocumentSubscriptionsHandler`.
    /// This function sets up a Redis subscription to the` and starts a thread to listen for incoming messages.
    /// # Returns
    /// A `DocumentResult<Self>` which is an instance of `DocumentSubscriptionsHandler` if successful, or an error if the connection fails.
    /// Contains a receiver that can be used to handle incoming document events.
    pub fn new(host: &str, port: u16) -> DocumentResult<Self> {
        // TODO: We might want to receive the host and port as parameters in the future.
        let (sender, receiver): (Sender<Event>, Receiver<Event>) = std::sync::mpsc::channel();

        let mut redis_subscription_connection =
            RedisDriver::auth_connect(host, port, "user", "default")?;
        spawn(move || -> DocumentResult<()> {
            let command = vec![
                "PSUBSCRIBE".to_string(),
                DOCUMENTS_CHANNEL_PREFIX.to_string(),
                SHEETS_CHANNEL_PREFIX.to_string(),
            ];
            redis_subscription_connection.safe_command(command)?;

            loop {
                if let Ok(data) = redis_subscription_connection.receive_response() {
                    let message: Vec<String> = Vec::from_redis(data)?;
                    let event = handle_microservice_pubsub_message(message);
                    match event {
                        Ok(event) => {
                            if sender.send(event).is_ok() {
                                // Successfully sent the event to the receiver
                            } else {
                                // If sending fails, we can log or handle it as needed
                                eprintln!("Failed to send event through channel.");
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error handling message: {e}");
                        }
                    }
                }
            }
            Ok(())
        });

        Ok(DocumentSubscriptionsHandler { receiver })
    }

    /// Handles incoming messages from the Redis subscription.
    /// Acts a blocking call that waits for messages to be received. Avoiding bussy waits
    /// ### Returns
    /// A `DocumentResult<DocumentIncomingEvent>` which contains the event if successful, or an error if the message could not be received.
    pub fn handle_incoming_message(&self) -> DocumentResult<Event> {
        self.receiver
            .recv()
            .map_err(|e| DocumentError::other(format!("Failed to receive message: {e}")))
    }
}
