//! Este módulo contiene la estructura que permite crear el nodo a partir de
//! su metadata
use crate::cluster::neighboring_node::NeighboringNodeInfo;
use crate::comandos::pub_sub_struct::PubSubBroker;
use crate::internal_protocol::node_flags::ClusterState;
use crate::node::Node;
use crate::node_id::NodeId;
use crate::node_role::NodeRole;
use crate::node_status::NodeStatus;
use crate::storage::Storage;
use logger::logger::Logger;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, RwLock};

pub struct NodeBuilder {
    id: Option<NodeId>,
    cli_addr: Option<SocketAddr>,
    node_addr: Option<SocketAddr>,
    cluster_addr: Option<SocketAddr>,
    public_addr: Option<SocketAddr>,
    role: Option<NodeRole>,
    status: Option<NodeStatus>,
    slot_range: Option<Range<u16>>,
    storage: Option<Arc<RwLock<Storage>>>,
    pub_sub: Option<PubSubBroker>,
    max_client_capacity: Option<usize>,
    act_client_active: Option<Arc<AtomicUsize>>,
    save_interval: Option<u64>,
    logger: Option<Logger>,

    config_epoch: Option<Arc<AtomicU64>>,
    current_epoch: Option<Arc<AtomicU64>>,
    replication_offset: Option<Arc<AtomicU64>>,
    master: Option<Option<NodeId>>,
    replicas: Option<Option<Vec<NodeId>>>,
    knows_nodes: Option<Arc<RwLock<HashMap<NodeId, NeighboringNodeInfo>>>>,
    node_timeout: Option<u64>,
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum BuildError {
    Missing(String),
}

impl Display for BuildError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::Missing(field) => write!(f, "Missing required field: {field}"),
        }
    }
}

impl NodeBuilder {
    pub fn new() -> Self {
        Self {
            id: None,
            cli_addr: None,
            node_addr: None,
            role: None,
            public_addr: None,
            status: None,
            slot_range: None,
            storage: None,
            pub_sub: None,
            max_client_capacity: None,
            act_client_active: None,
            save_interval: None,
            logger: None,

            config_epoch: None,
            current_epoch: None,
            replication_offset: None,
            master: None,
            replicas: None,
            knows_nodes: None,
            node_timeout: None,
            cluster_addr: None,
        }
    }

    pub fn id(mut self, id: NodeId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn cli_addr(mut self, addr: SocketAddr) -> Self {
        self.cli_addr = Some(addr);
        self
    }

    pub fn node_addr(mut self, addr: SocketAddr) -> Self {
        self.node_addr = Some(addr);
        self
    }

    pub fn role(mut self, role: NodeRole) -> Self {
        self.role = Some(role);
        self
    }

    pub fn status(mut self, status: NodeStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn slot_range(mut self, slot_range: Range<u16>) -> Self {
        self.slot_range = Some(slot_range);
        self
    }

    pub fn storage(mut self, storage: Arc<RwLock<Storage>>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn pub_sub(mut self, pub_sub: PubSubBroker) -> Self {
        self.pub_sub = Some(pub_sub);
        self
    }

    pub fn max_client_capacity(mut self, max: usize) -> Self {
        self.max_client_capacity = Some(max);
        self
    }

    pub fn act_client_active(mut self, act: Arc<AtomicUsize>) -> Self {
        self.act_client_active = Some(act);
        self
    }

    pub fn logger(mut self, logger: Logger) -> Self {
        self.logger = Some(logger);
        self
    }

    pub fn node_timeout(mut self, node_timeout: u64) -> Self {
        self.node_timeout = Some(node_timeout);
        self
    }

    pub fn save_interval(mut self, save_interval: u64) -> Self {
        self.save_interval = Some(save_interval);
        self
    }

    pub fn config_epoch(mut self, epoch: Arc<AtomicU64>) -> Self {
        self.config_epoch = Some(epoch);
        self
    }

    pub fn current_epoch(mut self, epoch: Arc<AtomicU64>) -> Self {
        self.current_epoch = Some(epoch);
        self
    }

    pub fn master(mut self, master: Option<NodeId>) -> Self {
        self.master = Some(master);
        self
    }

    pub fn replicas(mut self, replicas: Option<Vec<NodeId>>) -> Self {
        self.replicas = Some(replicas);
        self
    }

    pub fn knows_nodes(
        mut self,
        knows_nodes: Arc<RwLock<HashMap<NodeId, NeighboringNodeInfo>>>,
    ) -> Self {
        self.knows_nodes = Some(knows_nodes);
        self
    }

    pub fn cluster_addr(mut self, addr: SocketAddr) -> Self {
        self.cluster_addr = Some(addr);
        self
    }

    pub fn public_addr(mut self, addr: SocketAddr) -> Self {
        self.public_addr = Some(addr);
        self
    }

    pub fn build(self) -> Result<Node, BuildError> {
        Ok(Node {
            cluster_addr: self
                .cluster_addr
                .ok_or(BuildError::Missing("cluster_addr".to_string()))?,
            public_addr: self
                .public_addr
                .ok_or(BuildError::Missing("public_addr".to_string()))?,
            id: self.id.ok_or(BuildError::Missing("id".to_string()))?,
            cli_addr: self
                .cli_addr
                .ok_or(BuildError::Missing("cli_addr".to_string()))?,
            node_addr: self
                .node_addr
                .ok_or(BuildError::Missing("node_addr".to_string()))?,
            role: Arc::new(RwLock::new(self.role.unwrap_or(NodeRole::Master))),
            status: Arc::new(RwLock::new(self.status.unwrap_or(NodeStatus::Ok))),
            slot_range: self.slot_range.unwrap_or(0..0),
            storage: self
                .storage
                .unwrap_or_else(|| Arc::new(RwLock::new(Storage::in_memory(0..16384)))),
            pub_sub: self.pub_sub.unwrap_or_else(PubSubBroker::noop),
            max_client_capacity: self.max_client_capacity.unwrap_or_default(),
            act_client_active: self
                .act_client_active
                .unwrap_or_else(|| Arc::new(AtomicUsize::new(0))),
            save_interval: self.save_interval.unwrap_or_default(),
            logger: self.logger.unwrap_or_else(Logger::null),

            config_epoch: self
                .config_epoch
                .unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
            current_epoch: self
                .current_epoch
                .unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
            replication_offset: self
                .replication_offset
                .unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
            master: Arc::new(RwLock::new(self.master.unwrap_or(None))),
            replicas: Arc::new(RwLock::new(self.replicas.unwrap_or(None))),
            knows_nodes: self
                .knows_nodes
                .unwrap_or_else(|| Arc::new(RwLock::new(HashMap::new()))),
            cluster_state: Arc::new(RwLock::new(ClusterState::Ok)),
            node_timeout: self.node_timeout.unwrap_or(500),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, RwLock};

    fn dummy_logger() -> Logger {
        Logger::null()
    }

    fn dummy_storage() -> Arc<RwLock<Storage>> {
        Arc::new(RwLock::new(Storage::in_memory(0..100)))
    }

    fn dummy_pubsub() -> PubSubBroker {
        PubSubBroker::noop()
    }

    fn dummy_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn dummy_id() -> NodeId {
        NodeId::new()
    }

    #[test]
    fn test_build_node_success() {
        let id = dummy_id();
        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr(dummy_addr(6379))
            .node_addr(dummy_addr(7380))
            .slot_range(0..100)
            .storage(dummy_storage())
            .pub_sub(dummy_pubsub())
            .logger(dummy_logger())
            .cluster_addr(dummy_addr(17380))
            .public_addr(dummy_addr(17381))
            .build()
            .expect("Node should build successfully");

        assert_eq!(node.id, id);
        assert_eq!(node.cli_addr.port(), 6379);
        assert_eq!(node.node_addr.port(), 7380);
        assert_eq!(node.slot_range, 0..100);
    }

    #[test]
    fn test_build_node_missing_id() {
        let result = NodeBuilder::new()
            .cli_addr(dummy_addr(6379))
            .node_addr(dummy_addr(7380))
            .slot_range(0..100)
            .storage(dummy_storage())
            .pub_sub(dummy_pubsub())
            .logger(dummy_logger())
            .cluster_addr(dummy_addr(17380))
            .public_addr(dummy_addr(17381))
            .build();

        assert!(matches!(result, Err(BuildError::Missing(field)) if field == "id"));
    }

    #[test]
    fn test_build_node_defaults() {
        let id = dummy_id();
        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr(dummy_addr(6379))
            .node_addr(dummy_addr(7380))
            .slot_range(0..10)
            .storage(dummy_storage())
            .pub_sub(dummy_pubsub())
            .logger(dummy_logger())
            .cluster_addr(dummy_addr(17380))
            .public_addr(dummy_addr(17381))
            .build()
            .expect("build should succeed");

        assert_eq!(*node.role.read().unwrap(), NodeRole::Master);
        assert_eq!(*node.status.read().unwrap(), NodeStatus::Ok);
        assert_eq!(node.node_timeout, 500);
    }

    #[test]
    fn test_build_node_with_act_client_active() {
        let id = dummy_id();
        let act = Arc::new(AtomicUsize::new(7));
        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr(dummy_addr(6379))
            .node_addr(dummy_addr(7380))
            .slot_range(0..10)
            .storage(dummy_storage())
            .pub_sub(dummy_pubsub())
            .logger(dummy_logger())
            .act_client_active(act.clone())
            .cluster_addr(dummy_addr(17380))
            .public_addr(dummy_addr(17381))
            .build()
            .expect("build should succeed");
        assert_eq!(node.act_client_active.as_ref().load(Ordering::SeqCst), 7);
    }

    #[test]
    fn test_build_node_epochs_master_replicas_knows_nodes() {
        let id = dummy_id();
        let master_id = dummy_id();
        let replica1 = dummy_id();
        let replica2 = dummy_id();
        let config_epoch = Arc::new(AtomicU64::new(42));
        let current_epoch = Arc::new(AtomicU64::new(84));
        let knows_nodes: Arc<RwLock<HashMap<NodeId, NeighboringNodeInfo>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let node = NodeBuilder::new()
            .id(id.clone())
            .cli_addr(dummy_addr(6379))
            .node_addr(dummy_addr(7380))
            .slot_range(0..10)
            .role(NodeRole::Master)
            .status(NodeStatus::Ok)
            .max_client_capacity(84)
            .save_interval(500)
            .storage(dummy_storage())
            .pub_sub(dummy_pubsub())
            .logger(dummy_logger())
            .config_epoch(config_epoch.clone())
            .current_epoch(current_epoch.clone())
            .master(Some(master_id.clone()))
            .replicas(Some(vec![replica1.clone(), replica2.clone()]))
            .knows_nodes(knows_nodes.clone())
            .cluster_addr(dummy_addr(17380))
            .public_addr(dummy_addr(17381))
            .build()
            .expect("build should succeed");

        assert_eq!(node.master.read().unwrap().clone(), Some(master_id));
        assert_eq!(
            node.replicas.read().unwrap().clone(),
            Some(vec![replica1, replica2])
        );
    }
}
