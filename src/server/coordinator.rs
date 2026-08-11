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
    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
        }
    }

    pub fn join_group(&self, group_id: &str, member_id: &str, _protocols: Vec<String>) -> Result<String, String> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.entry(group_id.to_string()).or_insert_with(|| ConsumerGroup::new(group_id.to_string()));
        
        let m_id = if member_id.is_empty() {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().to_string()
        } else {
            member_id.to_string()
        };

        if !group.members.contains_key(&m_id) {
            group.members.insert(m_id.clone(), MemberState {
                member_id: m_id.clone(),
                client_id: m_id.clone(), // Simplify for now
                session_timeout: Duration::from_secs(10),
                assigned_partitions: HashMap::new(),
                last_heartbeat: Instant::now(),
            });
        }
        
        group.state = GroupState::PreparingRebalance;
        if group.leader.is_none() {
            group.leader = Some(m_id.clone());
        }
        
        Ok(m_id)
    }

    pub fn sync_group(&self, group_id: &str, _generation_id: u32, member_id: &str, assignments: Vec<crate::protocol::wire::MemberAssignment>) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            if group.leader.as_deref() == Some(member_id) {
                for assign in assignments {
                    if let Some(member) = group.members.get_mut(&assign.member_id) {
                        member.assigned_partitions.insert(assign.topic, assign.partitions);
                    }
                }
                group.state = GroupState::Stable;
                group.generation_id += 1;
            }
            return Ok(());
        }
        Err("Group not found".to_string())
    }

    pub fn heartbeat(&self, group_id: &str, _generation_id: u32, member_id: &str) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
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
        let groups = self.groups.lock().unwrap();
        groups.keys().cloned().collect()
    }

    pub fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let groups = self.groups.lock().unwrap();
        let group = groups.get(group_id)?;
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
