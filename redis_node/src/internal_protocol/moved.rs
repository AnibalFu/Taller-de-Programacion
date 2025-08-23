use crate::client_struct::client::Client;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct Moved {
    slot: u16,
    client: Arc<RwLock<Client>>,
}

impl Moved {
    pub fn new(slot: u16, client: Arc<RwLock<Client>>) -> Self {
        Moved { slot, client }
    }

    pub fn get_slot(&self) -> u16 {
        self.slot
    }

    pub fn get_client(&self) -> Arc<RwLock<Client>> {
        self.client.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_role::NodeRole;
    use logger::logger::Logger;
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, RwLock, mpsc::channel};

    fn dummy_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (client, server)
    }

    fn build_test_client() -> Arc<RwLock<Client>> {
        let (stream, _) = dummy_tcp_pair();
        let (_tx, _) = channel::<String>();

        let client = Client::new(
            "test_id".to_string(),
            stream,
            Logger::null(),
            Arc::new(RwLock::new(NodeRole::Master)),
        );

        Arc::new(RwLock::new(client))
    }

    #[test]
    fn test_moved_creation_and_access() {
        let client = build_test_client();
        let slot = 12345;

        let moved = Moved::new(slot, client.clone());

        assert_eq!(moved.get_slot(), slot);

        let client_guard = moved.get_client();
        assert!(Arc::ptr_eq(&client_guard, &client)); // verifica que es la misma instancia
    }
}
