//! Este módulo contiene estructuras auxiliares para el proceso de
//! replica promotion
use crate::{
    cluster::voto::LastVote,
    internal_protocol::{fail_auth_req::FailOverAuthRequest, header::MessageHeader},
    node_id::NodeId,
};
use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
};

pub struct EstructurasFailover {
    pub marcador_fallas: HashMap<NodeId, HashSet<NodeId>>,
    pub sender_rep_offset: Option<Sender<FailOverAuthRequest>>,
    pub votos: HashMap<NodeId, LastVote>,
    pub sender_votos: Option<Sender<NodeId>>,
    pub sender_nuevo_master: Option<Sender<MessageHeader>>,
}

impl EstructurasFailover {
    pub fn new() -> Self {
        Self {
            marcador_fallas: HashMap::new(),
            sender_rep_offset: None,
            votos: HashMap::new(),
            sender_votos: None,
            sender_nuevo_master: None,
        }
    }
}
