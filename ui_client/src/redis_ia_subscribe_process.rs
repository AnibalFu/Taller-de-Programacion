use std::sync::mpsc::Receiver;

use common::from_raw_string;
use redis_client::driver::{redis_driver::RedisDriver, traits::FromRedis};

use crate::ui_error::UIResult;

#[derive(Debug)]
pub struct RedisSubscribeProcessIA {
    pub message_receiver: Receiver<String>,
}

impl RedisSubscribeProcessIA {
    pub fn new(
        channel_name: &str,
        user: String,
        password: String,
        host: String,
        port: u16,
    ) -> UIResult<Self> {
        let (tx, rx) = std::sync::mpsc::channel();

        let channel = channel_name.to_string();

        std::thread::spawn(move || -> UIResult<()> {
            let mut redis_driver =
                RedisDriver::auth_connect(&host, port, &user, &password)?;
            let command = vec!["SUBSCRIBE".to_string(), channel.clone()];
            redis_driver.safe_command(command)?;

            loop {
                if let Ok(data) = redis_driver.receive_response() {
                    let response: Vec<String> = Vec::from_redis(data).unwrap();
                    if response.len() >= 3 {
                        let payload = from_raw_string(&response[2]);
                        tx.send(payload)?;
                    }
                }
            }
        });

        Ok(Self {
            message_receiver: rx,
        })
    }
}
