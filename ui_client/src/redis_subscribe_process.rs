use std::{
    sync::mpsc::{Receiver, Sender},
    thread::spawn,
};

use events::events::{event::Event, events_api::handle_client_pubsub_message};
use redis_client::driver::{redis_driver::RedisDriver, traits::FromRedis};

use crate::ui_error::UIResult;

#[derive(Debug)]
pub struct RedisSubscribeProcess {
    pub message_receiver: Receiver<Event>,
}

impl RedisSubscribeProcess {
    pub fn new(
        channel_name: &str,
        user: String,
        password: String,
        host: String,
        port: u16,
    ) -> UIResult<Self> {
        let (message_sender, message_receiver): (Sender<Event>, Receiver<Event>) =
            std::sync::mpsc::channel();

        let channel_name = channel_name.to_string();
        let handle = spawn(move || -> UIResult<()> {
            let mut redis_driver =
                RedisDriver::auth_connect(&host, port, &user, &password)?;
            let command = vec!["SUBSCRIBE".to_string(), channel_name.clone()];
            redis_driver.safe_command(command)?;

            loop {
                if let Ok(data) = redis_driver.receive_response() {
                    let response: Vec<String> = Vec::from_redis(data)?;
                    let event = handle_client_pubsub_message(response.clone());
                    if let Ok(response) = event {
                        message_sender.send(response)?;
                    }
                }
            }
        });

        // Wait for thread to be created (this returns immediately after spawn)
        let _ = handle.thread().id();

        Ok( Self { message_receiver } )
    }
}
