//! Este módulo contiene una estructura que representa el último voto
//! de un master sobre la promoción de otro master
use crate::node_id::NodeId;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct LastVote {
    pub replica_id: NodeId,
    pub timestamp: Instant,
    pub current_epoch: u64,
}

impl LastVote {
    pub fn new(id: NodeId, timestamp: Instant, current_epoch: u64) -> Self {
        Self {
            replica_id: id,
            timestamp,
            current_epoch,
        }
    }

    /// Determina si puede votar a favor de una réplica
    ///
    /// # Parámetros
    /// - `master_epoch`: epoch del dueño del voto
    /// - `voto_anterior`: último voto sobre ese master, si lo hubo
    ///
    /// # Retorna
    /// - verdadero si el pedido es válido, falso en otro caso
    pub fn es_pedido_valido(
        &self,
        master_epoch: u64,
        voto_anterior: Option<&LastVote>,
        node_timeout: u64,
    ) -> bool {
        if let Some(voto) = voto_anterior {
            let ultimo_epoch = voto.current_epoch;
            let ultimo_timestamp = voto.timestamp;
            if ultimo_epoch >= self.current_epoch
                || (self.timestamp - ultimo_timestamp) < (Duration::from_millis(node_timeout) * 2)
            {
                return false;
            }
        }
        self.current_epoch >= master_epoch
    }
}
