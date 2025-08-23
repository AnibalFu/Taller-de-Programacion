// comandos_persistencia.rs
pub const CMD_SAVE: &str = "SAVE";

// handshake.rs
pub const CMD_HELLO: &str = "HELLO";
pub const CMD_AUTH: &str = "AUTH";

// pub_sub.rs
pub const CMD_SUBSCRIBE: &str = "SUBSCRIBE";
pub const CMD_PUBLISH: &str = "PUBLISH";
pub const CMD_UNSUBSCRIBE: &str = "UNSUBSCRIBE";
pub const CMD_PUBSUB: &str = "PUBSUB";
pub const CMD_PUBSUB_NUMSUB: &str = "NUMSUB";
pub const CMD_PUBSUB_CHANNELS: &str = "CHANNELS";
pub const CMD_PSUBSCRIBE: &str = "PSUBSCRIBE";
pub const CMD_PUNSUBSCRIBE: &str = "PUNSUBSCRIBE";
pub const CMD_PUBSUB_NUMPAT: &str = "NUMPAT";
pub const CMD_SPUBLISH: &str = "SPUBLISH";
pub const CMD_SSUBSCRIBE: &str = "SSUBSCRIBE";
pub const CMD_SUNSUBSCRIBE: &str = "SUNSUBSCRIBE";
pub const CMD_PUBSUB_SHARDCHANNELS: &str = "SHARDCHANNELS";
pub const CMD_PUBSUB_SHARDNUMSUB: &str = "SHARDNUMSUB";

// comandos_normales.rs
// Comandos strings
pub const CMD_GET: &str = "GET";
pub const CMD_SET: &str = "SET";
pub const CMD_DEL: &str = "DEL";
pub const CMD_GETDEL: &str = "GETDEL";
pub const CMD_APPEND: &str = "APPEND";
pub const CMD_STRLEN: &str = "STRLEN";
pub const CMD_SUBSTR: &str = "SUBSTR";
pub const CMD_GETRANGE: &str = "GETRANGE";
pub const CMD_INCR: &str = "INCR";
pub const CMD_DECR: &str = "DECR";

// Comandos listas
pub const CMD_LINSERT: &str = "LINSERT";
pub const CMD_LPUSH: &str = "LPUSH";
pub const CMD_RPUSH: &str = "RPUSH";
pub const CMD_LLEN: &str = "LLEN";
pub const CMD_LPOP: &str = "LPOP";
pub const CMD_RPOP: &str = "RPOP";
pub const CMD_LRANGE: &str = "LRANGE";
pub const CMD_LSET: &str = "LSET";
pub const CMD_LREM: &str = "LREM";
pub const CMD_LTRIM: &str = "LTRIM";
pub const CMD_LINDEX: &str = "LINDEX";
pub const CMD_LMOVE: &str = "LMOVE";

// Comandos set
pub const CMD_SADD: &str = "SADD";
pub const CMD_SCARD: &str = "SCARD";
pub const CMD_SISMEMBER: &str = "SISMEMBER";
pub const CMD_SREM: &str = "SREM";
pub const CMD_SMEMBERS: &str = "SMEMBERS";

pub const OPERACION_EXITOSA: &str = "OK";
