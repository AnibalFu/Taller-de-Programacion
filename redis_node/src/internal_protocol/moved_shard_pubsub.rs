use crate::client_struct::client::Client;
use redis_client::tipos_datos::traits::DatoRedis;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct MovedShardPubSub {
    resp: DatoRedis,
    client: Arc<RwLock<Client>>,
}

impl MovedShardPubSub {
    pub fn new(resp: DatoRedis, client: Arc<RwLock<Client>>) -> Self {
        MovedShardPubSub { resp, client }
    }

    pub fn get_resp(&self) -> DatoRedis {
        self.resp.clone()
    }

    pub fn get_client(&self) -> Arc<RwLock<Client>> {
        self.client.clone()
    }
}
