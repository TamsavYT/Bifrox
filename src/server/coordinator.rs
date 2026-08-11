use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout: Duration,
    pub assigned_partitions: HashMap<String, Vec<u32>>,
    pub last_heartbeat: Instant,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub state: GroupState,
    pub generation_id: u32,
    pub leader: Option<String>,
    pub members: HashMap<String, MemberState>,
}

impl ConsumerGroup {
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: GroupState::Empty,
            generation_id: 0,
            leader: None,
            members: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct GroupCoordinator {
    groups: Mutex<HashMap<String, ConsumerGroup>>,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub state_str: String,
    pub members: Vec<crate::protocol::wire::DescribedGroupMember>,
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupCoordinator {
    fn prune_expired_members(group: &mut ConsumerGroup) {
        let now = Instant::now();
        let expired: Vec<String> = group
            .members
            .iter()
            .filter(|(_, member)| now.duration_since(member.last_heartbeat) > member.session_timeout)
            .map(|(member_id, _)| member_id.clone())
            .collect();

        for member_id in expired {
            group.members.remove(&member_id);
            if group.leader.as_deref() == Some(&member_id) {
                group.leader = group.members.keys().next().cloned();
            }
        }

        if group.members.is_empty() {
            group.state = GroupState::Empty;
            group.leader = None;
        }
    }

    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
        }
    }

    pub fn join_group(&self, group_id: &str, member_id: &str, _protocols: Vec<String>) -> Result<String, String> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.entry(group_id.to_string()).or_insert_with(|| ConsumerGroup::new(group_id.to_string()));
        Self::prune_expired_members(group);
        
        let m_id = if member_id.is_empty() {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().to_string()
        } else {
            member_id.to_string()
        };

        let mut rebalance_needed = false;
        if !group.members.contains_key(&m_id) {
            group.members.insert(m_id.clone(), MemberState {
                member_id: m_id.clone(),
                client_id: m_id.clone(), // Simplify for now
                session_timeout: Duration::from_secs(10),
                assigned_partitions: HashMap::new(),
                last_heartbeat: Instant::now(),
            });
            rebalance_needed = true;
        } else if let Some(member) = group.members.get_mut(&m_id) {
            member.last_heartbeat = Instant::now();
        }

        if rebalance_needed || group.state == GroupState::Empty {
            group.state = GroupState::PreparingRebalance;
            group.generation_id = group.generation_id.saturating_add(1);
        }
        if group.leader.is_none() {
            group.leader = Some(m_id.clone());
        }
        
        Ok(m_id)
    }

    pub fn sync_group(&self, group_id: &str, generation_id: u32, member_id: &str, assignments: Vec<crate::protocol::wire::MemberAssignment>) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            Self::prune_expired_members(group);
            if generation_id != group.generation_id {
                return Err(format!(
                    "Generation mismatch: expected {}, got {}",
                    group.generation_id, generation_id
                ));
            }
            if group.leader.as_deref() == Some(member_id) {
                for assign in assignments {
                    if let Some(member) = group.members.get_mut(&assign.member_id) {
                        member.assigned_partitions.insert(assign.topic, assign.partitions);
                    }
                }
                group.state = GroupState::Stable;
                return Ok(());
            }
            return Err("Only group leader can sync assignments".to_string());
        }
        Err("Group not found".to_string())
    }

    pub fn heartbeat(&self, group_id: &str, generation_id: u32, member_id: &str) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            Self::prune_expired_members(group);
            if generation_id != group.generation_id {
                return Err(format!(
                    "Generation mismatch: expected {}, got {}",
                    group.generation_id, generation_id
                ));
            }
            if let Some(member) = group.members.get_mut(member_id) {
                member.last_heartbeat = Instant::now();
                return Ok(());
            }
        }
        Err("Group or member not found".to_string())
    }

    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            group.members.remove(member_id);
            if group.members.is_empty() {
                group.state = GroupState::Empty;
                group.leader = None;
            } else {
                group.state = GroupState::PreparingRebalance;
                if group.leader.as_deref() == Some(member_id) {
                    group.leader = group.members.keys().next().cloned();
                }
            }
            return Ok(());
        }
        Err("Group not found".to_string())
    }

    pub fn list_groups(&self) -> Vec<String> {
        let mut groups = self.groups.lock().unwrap();
        for group in groups.values_mut() {
            Self::prune_expired_members(group);
        }
        groups.keys().cloned().collect()
    }

    pub fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.get_mut(group_id)?;
        Self::prune_expired_members(group);
        let state_str = format!("{:?}", group.state);
        let mut members = Vec::new();
        for (m_id, member) in &group.members {
            let mut assigned_partitions = Vec::new();
            for (topic, parts) in &member.assigned_partitions {
                for p in parts {
                    assigned_partitions.push((topic.clone(), *p));
                }
            }
            members.push(crate::protocol::wire::DescribedGroupMember {
                member_id: m_id.clone(),
                assigned_partitions,
            });
        }
        Some(GroupDescription {
            group_id: group_id.to_string(),
            state_str,
            members,
        })
    }
}
