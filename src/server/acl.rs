use parking_lot::RwLock;
use std::collections::HashSet;

/// Kafka Resource Types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ResourceType {
    Unknown = 0,
    Any = 1,
    Topic = 2,
    Group = 3,
    Cluster = 4,
    TransactionalId = 5,
    User = 6,
}

impl From<u8> for ResourceType {
    fn from(v: u8) -> Self {
        match v {
            1 => ResourceType::Any,
            2 => ResourceType::Topic,
            3 => ResourceType::Group,
            4 => ResourceType::Cluster,
            5 => ResourceType::TransactionalId,
            6 => ResourceType::User,
            _ => ResourceType::Unknown,
        }
    }
}

/// Kafka Resource Pattern Types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ResourcePatternType {
    Unknown = 0,
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4,
}

impl From<u8> for ResourcePatternType {
    fn from(v: u8) -> Self {
        match v {
            1 => ResourcePatternType::Any,
            2 => ResourcePatternType::Match,
            3 => ResourcePatternType::Literal,
            4 => ResourcePatternType::Prefixed,
            _ => ResourcePatternType::Unknown,
        }
    }
}

/// Kafka ACL Operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AclOperation {
    Unknown = 0,
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
}

impl From<u8> for AclOperation {
    fn from(v: u8) -> Self {
        match v {
            1 => AclOperation::Any,
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            _ => AclOperation::Unknown,
        }
    }
}

/// Kafka ACL Permission Types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AclPermissionType {
    Unknown = 0,
    Any = 1,
    Deny = 2,
    Allow = 3,
}

impl From<u8> for AclPermissionType {
    fn from(v: u8) -> Self {
        match v {
            1 => AclPermissionType::Any,
            2 => AclPermissionType::Deny,
            3 => AclPermissionType::Allow,
            _ => AclPermissionType::Unknown,
        }
    }
}

/// ACL Binding representing a single access control rule
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclBinding {
    pub resource_type: u8,
    pub resource_name: String,
    pub pattern_type: u8,
    pub principal: String,
    pub host: String,
    pub operation: u8,
    pub permission_type: u8,
}

/// Persistent in-memory ACL Manager enforcing Kafka Access Control Lists
#[derive(Debug, Default)]
pub struct AclManager {
    bindings: RwLock<HashSet<AclBinding>>,
}

impl AclManager {
    pub fn new() -> Self {
        Self {
            bindings: RwLock::new(HashSet::new()),
        }
    }

    pub fn add_acl(&self, binding: AclBinding) -> bool {
        let mut guard = self.bindings.write();
        guard.insert(binding)
    }

    pub fn remove_acl(&self, binding: &AclBinding) -> bool {
        let mut guard = self.bindings.write();
        guard.remove(binding)
    }

    pub fn list_acls(&self, filter: &AclBinding) -> Vec<AclBinding> {
        let guard = self.bindings.read();
        guard
            .iter()
            .filter(|b| {
                let match_res_type = filter.resource_type == 1
                    || filter.resource_type == 0
                    || filter.resource_type == b.resource_type;
                let match_res_name = filter.resource_name.is_empty()
                    || filter.resource_name == "*"
                    || filter.resource_name == b.resource_name;
                let match_principal = filter.principal.is_empty()
                    || filter.principal == "*"
                    || filter.principal == b.principal;
                let match_host =
                    filter.host.is_empty() || filter.host == "*" || filter.host == b.host;
                let match_op = filter.operation == 1
                    || filter.operation == 0
                    || filter.operation == b.operation;
                let match_perm = filter.permission_type == 1
                    || filter.permission_type == 0
                    || filter.permission_type == b.permission_type;
                match_res_type
                    && match_res_name
                    && match_principal
                    && match_host
                    && match_op
                    && match_perm
            })
            .cloned()
            .collect()
    }

    /// Kafka ACL Authorization Check Algorithm
    #[allow(clippy::too_many_arguments)]
    pub fn authorize(
        &self,
        principal: &str,
        host: &str,
        operation: u8,
        resource_type: u8,
        resource_name: &str,
        super_users: &[String],
        acls_enabled: bool,
    ) -> bool {
        if !acls_enabled {
            return true;
        }

        // Super users are exempt from ACL restrictions
        if super_users.iter().any(|u| u == principal || u == "*") {
            return true;
        }

        let guard = self.bindings.read();
        if guard.is_empty() {
            // Kafka ACL Security Standard: When ACL enforcement is enabled (`acls_enabled = true`),
            // default to DENY for User:ANONYMOUS unless superuser or explicit allow rules exist.
            return super_users.iter().any(|u| u == principal);
        }

        // 1. Check for matching Deny rules
        for b in guard.iter() {
            if b.permission_type == (AclPermissionType::Deny as u8)
                && self.matches_rule(b, principal, host, operation, resource_type, resource_name)
            {
                return false;
            }
        }

        // 2. Check for matching Allow rules
        for b in guard.iter() {
            if b.permission_type == (AclPermissionType::Allow as u8)
                && self.matches_rule(b, principal, host, operation, resource_type, resource_name)
            {
                return true;
            }
        }

        // Default Deny if ACLs enabled and no matching Allow rule found
        false
    }

    fn matches_rule(
        &self,
        rule: &AclBinding,
        principal: &str,
        host: &str,
        operation: u8,
        resource_type: u8,
        resource_name: &str,
    ) -> bool {
        // Resource Type match
        if rule.resource_type != (ResourceType::Any as u8) && rule.resource_type != resource_type {
            return false;
        }

        // Operation match
        if rule.operation != (AclOperation::Any as u8)
            && rule.operation != (AclOperation::All as u8)
            && rule.operation != operation
        {
            return false;
        }

        // Principal match
        if rule.principal != "*" && rule.principal != principal {
            return false;
        }

        // Host match
        if rule.host != "*" && rule.host != host {
            return false;
        }

        // Resource Name match (PatternType)
        let pattern_type = rule.pattern_type;
        if pattern_type == (ResourcePatternType::Prefixed as u8) {
            if !resource_name.starts_with(&rule.resource_name) && rule.resource_name != "*" {
                return false;
            }
        } else {
            // Literal or default
            if rule.resource_name != "*" && rule.resource_name != resource_name {
                return false;
            }
        }

        true
    }
}
