use std::net::{SocketAddr, ToSocketAddrs};

pub struct Configuration {
    pub redis_addr: SocketAddr,
    pub api_key: String,
}

impl Configuration {
    pub fn new(redis_addr: SocketAddr, api_key: &str) -> Self {
        Self {
            redis_addr,
            api_key: api_key.to_string(),
        }
    }

    pub fn from_args(args: &[String]) -> Self {
        // format is host=127.0.0.1 port=6379 api_key=your_api_key
        if args.len() < 4 {
            todo!()
        }

        let host = if let Some((_param_name, value)) = args[1].split_once('=') {
            value.to_string()
        } else {
            todo!()
        };

        let port = if let Some((_param_name, value)) = args[2].split_once('=') {
            value.parse::<u16>().expect("Invalid port number")
        } else {
            todo!()
        };

        let api_key = if let Some((_param_name, value)) = args[3].split_once('=') {
            value.to_string()
        } else {
            todo!()
        };

        let redis_addr = format!("{host}:{port}")
            .to_socket_addrs()
            .map(|mut addrs| addrs.next().expect("No valid address found"))
            .expect("Failed to parse redis address");

        Self::new(redis_addr, &api_key)
    }
}
