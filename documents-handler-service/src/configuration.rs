use std::net::ToSocketAddrs;

use crate::{
    document_error::{DocumentError, DocumentErrorKind},
    documents::DocumentResult,
};

pub struct Configuration {
    pub redis_host: String,
    pub redis_port: u16,
    pub save_timer_ms: i64, // in milliseconds
}

impl Configuration {
    pub fn new(host: &str, port: u16, save_timer_ms: i64) -> DocumentResult<Self> {
        Ok(Self {
            redis_host: host.to_string(),
            redis_port: port,
            save_timer_ms,
        })
    }

    pub fn from_args(args: &[String]) -> DocumentResult<Self> {
        // format is redis_address=127.0.0.1:8088
        if args.len() < 2 {
            return Err(DocumentError::new(
                "Invalid amount of arguments".to_string(),
                DocumentErrorKind::InvalidAmountOfArguments,
            ));
        }

        let (ip, port) = if let Some((_parameter_name, value)) = args[1].split_once("=") {
            let addr = value
                .to_socket_addrs()
                .map_err(|_| {
                    DocumentError::new(
                        "Invalid redis address".to_string(),
                        DocumentErrorKind::InvalidArgs,
                    )
                })?
                .next()
                .ok_or_else(|| {
                    DocumentError::new(
                        "No address found".to_string(),
                        DocumentErrorKind::InvalidArgs,
                    )
                })?;

            let ip = addr.ip();
            let port = addr.port();
            (ip.to_string(), port)
        } else {
            return Err(DocumentError::new(
                "Expected parameter is redis_addr=127.0.0.1:8088".to_string(),
                DocumentErrorKind::InvalidArgs,
            ));
        };

        let save_timer_ms = if let Some((_parameter_name, value)) =
            args.get(2).and_then(|arg| arg.split_once("="))
        {
            value.parse::<i64>().map_err(|_| {
                DocumentError::new(
                    "Invalid save timer".to_string(),
                    DocumentErrorKind::InvalidArgs,
                )
            })?
        } else {
            30000 // default save timer
        };
        Self::new(&ip, port, save_timer_ms)
    }
}
