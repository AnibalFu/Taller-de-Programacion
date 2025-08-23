use crate::internal_protocol::gossip::GossipEntry;
use crate::internal_protocol::internal_protocol_msg::ClusterMessage;
use crate::internal_protocol::node_flags::{ClusterState, NodeFlags};
use crate::node_id::NodeId;
use crate::node_role::NodeRole;
use std::net::SocketAddr;
use std::ops::Range;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct NeighboringNodeInfo {
    pub(crate) id: NodeId,
    pub(crate) cli_addr: SocketAddr,
    pub(crate) node_addr: SocketAddr,
    pub(crate) role: NodeRole,
    pub(crate) slot_range: Range<u16>,
    pub(crate) status: ClusterState,
    pub(crate) flags: NodeFlags,
    pub(crate) ping_sent_time: Instant,
    pub(crate) pong_received_time: Instant,
    pub(crate) master: Option<NodeId>,
}

impl NeighboringNodeInfo {
    pub fn from_cluster_msg(msg: &ClusterMessage) -> Self {
        let header = msg.header();

        NeighboringNodeInfo {
            id: header.node_id().clone(),
            cli_addr: header.client_node_addr(),
            node_addr: header.cluster_node_addr(),
            role: match header.master_id() {
                Some(_) => NodeRole::Replica,
                None => NodeRole::Master,
            },
            slot_range: header.slot_range(),
            status: header.status(),
            flags: header.flags(),
            ping_sent_time: Instant::now(),
            pong_received_time: Instant::now(),
            master: header.master_id(),
        }
    }

    pub fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    pub fn get_cli_addr(&self) -> SocketAddr {
        self.cli_addr
    }

    pub fn get_node_addr(&self) -> SocketAddr {
        self.node_addr
    }

    pub fn get_slot_range(&self) -> Range<u16> {
        self.slot_range.clone()
    }

    pub fn to_gossip_entry(&self) -> GossipEntry {
        GossipEntry::new(self.id.clone(), self.node_addr, self.flags.clone())
    }

    pub fn get_role(&self) -> NodeRole {
        self.role.clone()
    }

    pub fn get_status(&self) -> &ClusterState {
        &self.status
    }

    pub fn update_role_and_flags(&mut self, flags: NodeFlags) {
        self.role = match flags.is_master() {
            true => NodeRole::Master,
            false => NodeRole::Replica,
        };
        self.flags = flags;
    }

    pub fn set_ping_sent_time(&mut self, time: Instant) {
        self.ping_sent_time = time;
    }

    pub fn set_pong_received_time(&mut self, time: Instant) {
        self.pong_received_time = time;
    }

    pub fn get_ping_sent_time(&self) -> Duration {
        self.ping_sent_time.elapsed()
    }

    pub fn get_pong_received_time(&self) -> Duration {
        self.pong_received_time.elapsed()
    }

    pub fn is_suspected_failed(&self, timeout: Duration) -> bool {
        Instant::now().duration_since(self.pong_received_time) > timeout
            && self.pong_received_time.elapsed() > self.ping_sent_time.elapsed()
    }

    pub fn set_fail(&mut self, value: bool) {
        self.flags.set_fail(value);
    }

    pub fn set_pfail(&mut self, value: bool) {
        self.flags.set_pfail(value);
    }

    pub fn get_flags(&self) -> &NodeFlags {
        &self.flags
    }

    pub fn master_id(&self) -> Option<NodeId> {
        self.master.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    fn dummy_node_id() -> NodeId {
        NodeId::new()
    }

    fn dummy_socket_addr() -> SocketAddr {
        "127.0.0.1:6379".parse().unwrap()
    }

    #[test]
    fn test_update_role_and_flags_sets_role_correctly() {
        let mut node_info = NeighboringNodeInfo {
            id: dummy_node_id(),
            cli_addr: dummy_socket_addr(),
            node_addr: dummy_socket_addr(),
            role: NodeRole::Replica,
            slot_range: 0..1000,
            status: ClusterState::Ok,
            flags: NodeFlags::new(false, false, false, false),
            ping_sent_time: Instant::now(),
            pong_received_time: Instant::now(),
            master: None,
        };

        // Cambiar flags a is_master = true
        let new_flags = NodeFlags::new(true, false, false, false);
        node_info.update_role_and_flags(new_flags.clone());

        assert_eq!(node_info.get_role(), NodeRole::Master);
        assert_eq!(node_info.get_flags(), &new_flags);

        // Cambiar flags a is_master = false
        let new_flags = NodeFlags::new(false, true, false, false);
        node_info.update_role_and_flags(new_flags.clone());

        assert_eq!(node_info.get_role(), NodeRole::Replica);
        assert_eq!(node_info.get_flags(), &new_flags);
        assert_eq!(node_info.get_cli_addr(), dummy_socket_addr());
        assert_eq!(node_info.get_node_addr(), dummy_socket_addr())
    }

    #[test]
    fn test_is_suspected_failed_logic() {
        let now = Instant::now();
        let mut node_info = NeighboringNodeInfo {
            id: dummy_node_id(),
            cli_addr: dummy_socket_addr(),
            node_addr: dummy_socket_addr(),
            role: NodeRole::Master,
            slot_range: 0..1000,
            status: ClusterState::Ok,
            flags: NodeFlags::new(true, false, false, false),
            ping_sent_time: now - Duration::from_secs(10),
            pong_received_time: now - Duration::from_secs(5),
            master: None,
        };

        let timeout = Duration::from_secs(3);

        // Ahora pong_received_time hace 5s, ping_sent_time hace 10s => pong más reciente que ping, debería ser false
        assert!(!node_info.is_suspected_failed(timeout));

        // Cambiar pong_received_time a hace 20s para simular timeout
        node_info.pong_received_time = now - Duration::from_secs(20);

        // Ahora pong_received_time más viejo que timeout y pong_received_time.elapsed() > ping_sent_time.elapsed()
        assert!(node_info.is_suspected_failed(timeout));
        node_info.set_fail(true);
        assert_eq!(
            node_info.get_flags(),
            &NodeFlags::new(true, false, true, false)
        );
        assert!(node_info.master_id().is_none())
    }
}
