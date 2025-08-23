use std::{
    net::{TcpStream, ToSocketAddrs},
    ops::{Deref, DerefMut},
};

use super::RedisDriverResult;
use crate::{
    driver::redis_driver_error::{RedisDriverError, RedisDriverErrorKind},
    protocol::protocol_resp::{resp_client_command_read, resp_client_command_write},
    tipos_datos::traits::DatoRedis,
};
use crate::{protocol::protocol_resp::resp_api_command_write, tipos_datos::traits::TipoDatoRedis};

/// This struct will be used as an interface
/// between the user and the nodes
#[derive(Debug)]
pub struct RedisDriver {
    /// Contains the connection to the node
    connection: TcpStream,
    /// Contains the hostname of the node
    hostname: String,
    /// Contains the port of the node
    port: u16,
    /// Contains the user of the node
    user: Option<String>,
    /// Contains the password of the node
    password: Option<String>,
}

impl Deref for RedisDriver {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for RedisDriver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl RedisDriver {
    /// Connects and performs a handshake with the redis node
    /// returns the structure if the handshake is successful
    /// # Arguments
    /// * `hostname`: hostname of the redis node
    /// * `port`: port of the redis node
    /// # Returns
    /// * `RedisDriverResult<Self>`: Ok if the handshake was successful, Err if there was an error
    pub fn connect(hostname: &str, port: u16) -> RedisDriverResult<Self> {
        let stream = no_auth_handshake_connnect(format!("{hostname}:{port}").as_str())?;

        let result = RedisDriver {
            hostname: hostname.to_string(),
            port,
            connection: stream,
            user: None,
            password: None,
        };

        Ok(result)
    }

    /// Connects and performs a handshake with the redis node
    /// returns the structure if the handshake is successful
    /// # Arguments
    /// * `hostname`: hostname of the redis node
    /// * `port`: port of the redis node
    /// * `user`: user to authenticate with
    /// * `password`: password to authenticate with
    /// # Returns
    /// * `RedisDriverResult<Self>`: Ok if the handshake was successful, Err if there was an error
    pub fn auth_connect(
        hostname: &str,
        port: u16,
        user: &str,
        password: &str,
    ) -> RedisDriverResult<Self> {
        let stream = auth_handshake_connect(format!("{hostname}:{port}").as_str(), user, password)?;

        let result = RedisDriver {
            hostname: hostname.to_string(),
            port,
            connection: stream,
            user: Some(user.to_string()),
            password: Some(password.to_string()),
        };

        Ok(result)
    }

    /// Sends a command to the redis node, this is not a safe way to sennd commands
    /// as it does not handle errors or responses and if we receive a MOVED error
    /// it will not handle it, use `safe_command` instead
    /// It is safe to use for publish/subscribe commands
    /// and other commands that do not require a response.
    /// # Arguments
    /// * `command`: command to send to the redis node
    /// # Returns
    /// * `RedisDriverResult<()>`: Ok if the command was sent successfully, Err if there was an error
    fn command(&mut self, command: Vec<String>) -> RedisDriverResult<()> {
        Ok(resp_api_command_write(command, &mut self.connection)?)
    }

    /// Receives a response from the redis node, this is not a safe way to receive responses
    /// as it does not handle errors or responses and if we receive a MOVED error
    /// it will not handle it, use `safe_command` instead
    /// # Returns
    /// * `RedisDriverResult<DatoRedis>`: Ok with the response if the command was sent successfully, Err if there was an error
    /// # Errors
    /// * `RedisDriverError`: if there was an error reading the response
    pub fn receive_response(&mut self) -> RedisDriverResult<DatoRedis> {
        let response = resp_client_command_read(&mut self.connection)?;
        Ok(response)
    }

    /// Sends a command to the redis node and haStringndles MOVED errors
    /// by reconnecting to the new node and resending the command.
    /// # Arguments
    /// * `command`: command to send to the redis node
    /// # Returns
    /// * `RedisDriverResult<DatoRedis>`: Ok with the response if the command was sent successfully, Err if there was an error
    /// # Errors
    /// * `RedisDriverError`: if there was an error reading the response or if the MOVED error format is invalid    
    pub fn safe_command(&mut self, command: Vec<String>) -> RedisDriverResult<DatoRedis> {
        self.command(command.clone())?;
        let response = self.receive_response();
        match response {
            Ok(data) => match data {
                DatoRedis::SimpleError(e) => {
                    if e.tipo() == "MOVED" {
                        let mensaje = e.mensaje();
                        let addr = extract_addr_from_moved_err(mensaje)?;
                        let user = self.user.clone().unwrap();
                        let password = self.password.clone().unwrap();
                        let new_connection =
                            auth_handshake_connect(addr.as_str(), &user, &password)?;
                        self.connection = new_connection;
                        self.command(command)?;
                        self.receive_response()
                    } else {
                        Err(RedisDriverError::new(
                            format!("Simple error: {}", e.mensaje()),
                            RedisDriverErrorKind::ProtocolError,
                        ))
                    }
                }
                _ => Ok(data),
            },
            Err(e) => Err(RedisDriverError::new(
                format!("Error executing command: {e}"),
                RedisDriverErrorKind::ProtocolError,
            )),
        }
    }
    /// Receives a response from redis node and returns it as a redis driver result string
    pub fn receive_response_as_string(&mut self) -> RedisDriverResult<String> {
        self.connection.set_nonblocking(true)?; // Neeeded to avoid blocking states
        let mut buf = vec![0; 1];
        if let Ok(bytes_read) = self.connection.peek(&mut buf) {
            if bytes_read == 0 {
                return Err(RedisDriverError::new(
                    "Connection error".to_string(),
                    RedisDriverErrorKind::ConnectionError,
                ));
            }
            let response = resp_client_command_read(&mut self.connection)?;
            Ok(response.convertir_resp_a_string())
        } else {
            Err(RedisDriverError::new(
                "Nothing to read".to_string(),
                RedisDriverErrorKind::EmptyStreamError,
            ))
        }
    }
}

fn extract_addr_from_moved_err(mensaje: String) -> Result<String, RedisDriverError> {
    let (_slot, addr) = mensaje.split_once(" ").map(|s| (s.0, s.1)).ok_or_else(|| {
        RedisDriverError::new(
            "Invalid MOVED error format".to_string(),
            RedisDriverErrorKind::ProtocolError,
        )
    })?;
    Ok(addr.to_string())
}

fn no_auth_handshake_connnect(addr: &str) -> RedisDriverResult<TcpStream> {
    let mut stream = TcpStream::connect(addr)?;
    resp_client_command_write("HELLO 3".to_string(), &mut stream)?;

    let mut handshake_received = false;
    while !handshake_received {
        match resp_client_command_read(&mut stream) {
            Ok(_) => {
                handshake_received = true;
            }
            Err(_) => {
                return Err(RedisDriverError::new(
                    "Cannot connect to the redis server".to_string(),
                    RedisDriverErrorKind::DriverError,
                ));
            }
        }
    }

    Ok(stream)
}

fn auth_handshake_connect(addr: &str, user: &str, password: &str) -> RedisDriverResult<TcpStream> {
    let addr = addr.to_socket_addrs()?.next().ok_or_else(|| {
        RedisDriverError::new(
            "Invalid address format".to_string(),
            RedisDriverErrorKind::InvalidAddress,
        )
    })?;

    let mut stream = TcpStream::connect(addr)?;
    resp_client_command_write(format!("AUTH {user} {password}"), &mut stream)?;

    let mut handshake_received = false;
    while !handshake_received {
        match resp_client_command_read(&mut stream) {
            Ok(DatoRedis::SimpleError(simple_error)) => {
                return Err(RedisDriverError::new(
                    simple_error.mensaje(),
                    RedisDriverErrorKind::DriverError,
                ));
            }
            Ok(_) => {
                handshake_received = true;
            }
            Err(_) => {
                return Err(RedisDriverError::new(
                    "Cannot connect to the redis server".to_string(),
                    RedisDriverErrorKind::DriverError,
                ));
            }
        }
    }

    Ok(stream)
}

impl std::fmt::Display for RedisDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connected to {}:{}", self.hostname, self.port)
    }
}
