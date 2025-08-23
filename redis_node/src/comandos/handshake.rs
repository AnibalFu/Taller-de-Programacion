use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use redis_client::tipos_datos::{map_reply::MapReply, traits::DatoRedis};

use crate::{client_struct::client::Client, comandos::utils::assert_correct_arguments_quantity};

pub fn hello(
    vec: &[String],
    client: &mut Arc<RwLock<Client>>,
    users: &HashMap<String, String>,
) -> Result<DatoRedis, DatoRedis> {
    if vec.len() == 1 {
        // Not supporting resp2 yet (we might have to check it but we're mostly doing resp3 stuff)
        Err(DatoRedis::new_simple_error(
            "NOPROTO".to_string(),
            "sorry this protocol version is not supported".to_string(),
        ))
    } else if vec.len() == 2 {
        if vec[1] != "3" {
            return Err(DatoRedis::new_simple_error(
                "NOPROTO".to_string(),
                "sorry this protocol version is not supported".to_string(),
            ));
        }
        let reply = create_hello_response(client)?;
        let mut guard = client.write().map_err(|_| {
            DatoRedis::new_simple_error("HANDSHAKE".to_string(), "client lock error".to_string())
        })?;
        guard.set_handshake(true);
        Ok(reply)
    } else if vec.len() == 5 {
        if vec[1] != "3" {
            return Err(DatoRedis::new_simple_error(
                "NOPROTO".to_string(),
                "sorry this protocol version is not supported".to_string(),
            ));
        }

        if vec[2].to_uppercase() != "AUTH" {
            return Err(DatoRedis::new_simple_error(
                "INVALIDCOMMAND".to_string(),
                "Wrong amount of arguments".to_string(),
            ));
        }
        let vec = &vec[2..];
        auth(vec, client, users)?;
        Ok(DatoRedis::new_simple_string("OK".to_string())?)
    } else {
        Err(DatoRedis::new_simple_error(
            "INVALIDCOMMAND".to_string(),
            "Wrong amount of arguments".to_string(),
        ))
    }
}

// Once we have the configuration file, we can use it to create the hello response
fn create_hello_response(client: &mut Arc<RwLock<Client>>) -> Result<DatoRedis, DatoRedis> {
    let mut reply = MapReply::new();
    reply.insert(
        DatoRedis::new_simple_string("server".to_string())?,
        DatoRedis::new_simple_string("redis".to_string())?,
    );
    reply.insert(
        DatoRedis::new_simple_string("version".to_string())?,
        DatoRedis::new_simple_string("0.0.1".to_string())?,
    );
    reply.insert(
        DatoRedis::new_simple_string("proto".to_string())?,
        DatoRedis::new_integer(3),
    );

    let guard = client.read().map_err(|_| {
        DatoRedis::new_simple_error("HANDSHAKE".to_string(), "client lock error".to_string())
    })?;
    let cli_id = obtener_id(&guard)?;
    reply.insert(
        DatoRedis::new_simple_string("id".to_string())?,
        DatoRedis::new_integer(cli_id),
    );
    reply.insert(
        DatoRedis::new_simple_string("mode".to_string())?,
        DatoRedis::new_simple_string("cluster".to_string())?,
    );

    let role = guard.get_type_of_node_connections().read().unwrap().clone();
    reply.insert(
        DatoRedis::new_simple_string("role".to_string())?,
        DatoRedis::new_simple_string(role.to_string())?,
    );
    reply.insert(
        DatoRedis::new_simple_string("modules".to_string())?,
        DatoRedis::new_array(),
    );
    Ok(DatoRedis::new_map_reply_with_content(reply))
}

fn obtener_id(guard: &RwLockReadGuard<Client>) -> Result<i64, DatoRedis> {
    let numeric_str: String = guard
        .client_id()
        .chars()
        .filter(|c| c.is_ascii_digit()) // Conservar solo dígitos
        .collect();

    numeric_str.parse::<i64>().map_err(|_| {
        DatoRedis::new_simple_error("HANDSHAKE".to_string(), "cli id not a number".to_string())
    })
}

pub fn auth(
    vec: &[String],
    client: &mut Arc<RwLock<Client>>,
    users_dict: &HashMap<String, String>,
) -> Result<DatoRedis, DatoRedis> {
    assert_correct_arguments_quantity("AUTH".to_string(), 3, vec.len())?;
    if users_dict.contains_key(&vec[1]) && users_dict[&vec[1]] == vec[2] {
        let mut guard = client.write().map_err(|_| {
            DatoRedis::new_simple_error("HANDSHAKE".to_string(), "client lock error".to_string())
        })?;
        guard.set_handshake(true);
        Ok(DatoRedis::new_bulk_string(
            "User authenticated".to_string(),
        )?)
    } else {
        Err(DatoRedis::new_simple_error(
            "AUTH".to_string(),
            format!("Authentication failed for user: {}", vec[1]),
        ))
    }
}

#[cfg(test)]
mod tests {
    const DEFAULT_USER: &str = "default";
    const DEFAULT_PASSWORD: &str = "password";
    use super::*;
    use crate::client_struct::client::Client;
    use crate::node_role::NodeRole;
    use logger::logger::Logger;
    use std::net::{TcpListener, TcpStream};

    fn dummy_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (client, server)
    }

    fn make_client() -> Arc<RwLock<Client>> {
        let (client_stream, _server_stream) = dummy_tcp_pair();
        let logger = Logger::null();

        let cliente = Client::new(
            "1234".to_string(),
            client_stream,
            logger,
            Arc::new(RwLock::new(NodeRole::Master)),
        );

        Arc::new(RwLock::new(cliente))
    }

    fn users_map() -> HashMap<String, String> {
        HashMap::from([
            (DEFAULT_USER.to_string(), DEFAULT_PASSWORD.to_string()),
            ("rust".to_string(), "secret".to_string()),
        ])
    }

    #[test]
    fn hello_len1_unsupported() {
        let mut cli = make_client();
        let res = hello(&["HELLO".into()], &mut cli, &users_map());
        assert!(res.is_err());
    }

    #[test]
    fn hello_proto_wrong_version() {
        let mut cli = make_client();
        let res = hello(&["HELLO".into(), "2".into()], &mut cli, &users_map());
        assert!(res.is_err());
    }

    #[test]
    fn hello_proto_v3_ok() {
        let mut cli = make_client();
        let res = hello(&["HELLO".into(), "3".into()], &mut cli, &users_map());
        assert!(res.is_ok());
        assert!(cli.read().unwrap().get_handshake());
    }

    #[test]
    fn hello_len5_wrong_version() {
        let mut cli = make_client();
        let v = vec!["HELLO", "2", "AUTH", DEFAULT_USER, DEFAULT_PASSWORD]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        assert!(hello(&v, &mut cli, &users_map()).is_err());
    }

    #[test]
    fn hello_len5_missing_auth_keyword() {
        let mut cli = make_client();
        let v = vec!["HELLO", "3", "FOO", DEFAULT_USER, DEFAULT_PASSWORD]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let res = hello(&v, &mut cli, &users_map());
        assert!(res.is_err());
    }

    #[test]
    fn hello_len5_bad_credentials() {
        let mut cli = make_client();
        let v = vec!["HELLO", "3", "AUTH", "bad", "creds"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        assert!(hello(&v, &mut cli, &users_map()).is_err());
    }

    #[test]
    fn hello_len5_good_credentials() {
        let mut cli = make_client();
        let v = vec!["HELLO", "3", "AUTH", DEFAULT_USER, DEFAULT_PASSWORD]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let res = hello(&v, &mut cli, &users_map());
        assert!(res.is_ok());
        assert!(cli.read().unwrap().get_handshake());
    }

    #[test]
    fn hello_other_lengths_error() {
        let mut cli = make_client();
        let v = vec!["HELLO", "3", "EXTRA"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        assert!(hello(&v, &mut cli, &users_map()).is_err());
    }

    // ---------- AUTH ----------

    #[test]
    fn auth_wrong_argc() {
        let mut cli = make_client();
        let v = vec!["AUTH".into(), "onlyuser".into()];
        assert!(auth(&v, &mut cli, &users_map()).is_err());
    }

    #[test]
    fn auth_bad_user_or_password() {
        let mut cli = make_client();
        let v = vec!["AUTH", "rust", "badpass"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        assert!(auth(&v, &mut cli, &users_map()).is_err());
    }

    #[test]
    fn auth_good_user_password() {
        let mut cli = make_client();
        let v = vec!["AUTH", "rust", "secret"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let res = auth(&v, &mut cli, &users_map());
        assert!(res.is_ok());
        assert!(cli.read().unwrap().get_handshake());
    }
}
