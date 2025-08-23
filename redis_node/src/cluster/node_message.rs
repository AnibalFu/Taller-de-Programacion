//! Este módulo contiene la definición de los tipos de mensaje que envían
//! los nodos a otros nodos o a sí mismos

use crate::internal_protocol::internal_protocol_msg::ClusterMessage;
use crate::internal_protocol::moved::Moved;
use crate::internal_protocol::moved_shard_pubsub::MovedShardPubSub;
use crate::internal_protocol::redis_cmd::RedisCMD;

pub enum TipoMensajeNode {
    ClusterNode(ClusterMessage),
    InnerNode(InnerMensajeNode),
}

pub enum InnerMensajeNode {
    /// Payload para comandos Redis enviados internamente en el clúster.
    RedisCommand(RedisCMD),

    /// Payload para mensajes de publish (pub/sub).
    PubSub(RedisCMD),

    /// Payload para redireccion MOVED
    Moved(Moved),

    /// Payload para redireccion en Shard PubSub
    MovedShard(MovedShardPubSub),
}
