use std::{
    sync::mpsc::{Receiver, Sender},
    thread::spawn,
};

use redis_client::{
    driver::{redis_driver::RedisDriver, traits::FromRedis},
    tipos_datos::traits::DatoRedis,
};

use crate::ui_error::UIResult;
#[derive(Debug)]
pub struct RedisCommandProcess {
    command_sender: Sender<Vec<String>>,
    data_receiver: Receiver<DatoRedis>,
}

impl RedisCommandProcess {
    pub fn new(user: &str, password: &str, host: &str, port: u16) -> UIResult<Self> {
        let (command_sender, command_receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) =
            std::sync::mpsc::channel();
        let (data_sender, data_receiver): (Sender<DatoRedis>, Receiver<DatoRedis>) =
            std::sync::mpsc::channel();

        let mut redis_driver = RedisDriver::auth_connect(host, port, user, password)?;

        spawn(move || {
            loop {
                if let Ok(command) = command_receiver.recv() {
                    let response = redis_driver.safe_command(command);
                    match response {
                        Ok(data) => {
                            if let Err(e) = data_sender.send(data) {
                                eprintln!("Failed to send data through channel: {e}");
                            }
                        }
                        Err(e) => {
                            eprintln!("Error executing command: {e}");
                        }
                    }
                }
            }
        });

        Ok(Self {
            command_sender,
            data_receiver,
        })
    }

    pub fn send_command(&self, command: Vec<String>) -> UIResult<()> {
        self.command_sender.send(command)?;
        Ok(())
    }

    pub fn receive_data<T: FromRedis>(&self) -> UIResult<T> {
        loop {
            if let Ok(data) = self.data_receiver.recv() {
                return Ok(T::from_redis(data)?);
            }
        }
    }
}
