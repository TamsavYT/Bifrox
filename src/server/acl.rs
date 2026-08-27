use dashmap::DashMap;

/// Resource Types
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

/// Resource Pattern Types
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

/// ACL Operations
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

/// ACL Permission Types
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

/// ACL Binding representing a single access control rule.
///
/// `host` supports three forms, checked in `matches_rule`: `"*"` (any host), an exact IP
/// literal, or a CIDR range (e.g. `"10.0.0.0/8"`) — the same CIDR matcher used by the
/// Prometheus metrics endpoint's IP allowlist (`crate::server::listener::cidr_contains`).
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

/// Persistent in-memory ACL Manager enforcing Access Control Lists.
///
/// Bindings are split into two `DashMap`s rather than one global `RwLock<HashSet<_>>`:
/// - `literal`, keyed by `(resource_type, resource_name)`, holds `Literal`-pattern
///   bindings — the common case — giving `authorize()` an O(1) bucket lookup instead of a
///   full scan of every ACL on the broker.
/// - `patterned` holds `Prefixed`/`Match`/`Any` bindings (which can't be exact-name-keyed,
///   since e.g. a `Prefixed("orders-")` rule must be considered for a lookup on
///   `"orders-2024"`), bucketed by `resource_type` alone so a lookup only scans the rules
///   that could possibly apply to the resource type being checked, not every ACL for every
///   resource type on the broker.
///
/// `DashMap` is itself internally sharded, so — independent of the bucketing above —
/// concurrent `authorize()` calls checking different resources no longer contend on a
/// single lock the way the old single global `RwLock` did; only calls that happen to hash
/// to the same shard (and touch the same bucket) ever block each other.
#[derive(Debug, Default)]
pub struct AclManager {
    literal: DashMap<(u8, String), Vec<AclBinding>>,
    patterned: DashMap<u8, Vec<AclBinding>>,
}

impl AclManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Only a `Literal`-pattern binding with a concrete (non-`"*"`) resource name can be
    /// exact-keyed — `resource_name == "*"` is this codebase's wildcard-any-name sentinel
    /// even under a nominal `Literal` pattern type (see `matches_rule`'s literal-pattern
    /// arm and every CLI/admin call site that defaults `resource_name` to `"*"`), so a
    /// literal-looking `"*"` binding must still land in `patterned` — keying it under the
    /// literal string `"*"` would make it invisible to a lookup for any other name.
    fn bucket_key(binding: &AclBinding) -> Option<(u8, String)> {
        if binding.pattern_type == ResourcePatternType::Literal as u8
            && binding.resource_name != "*"
        {
            Some((binding.resource_type, binding.resource_name.clone()))
        } else {
            None
        }
    }

    pub fn add_acl(&self, binding: AclBinding) -> bool {
        if let Some(key) = Self::bucket_key(&binding) {
            let mut bucket = self.literal.entry(key).or_default();
            if bucket.contains(&binding) {
                return false;
            }
            bucket.push(binding);
        } else {
            let mut bucket = self.patterned.entry(binding.resource_type).or_default();
            if bucket.contains(&binding) {
                return false;
            }
            bucket.push(binding);
        }
        true
    }

    pub fn remove_acl(&self, binding: &AclBinding) -> bool {
        if let Some(key) = Self::bucket_key(binding) {
            if let Some(mut bucket) = self.literal.get_mut(&key) {
                let before = bucket.len();
                bucket.retain(|b| b != binding);
                return bucket.len() != before;
            }
            false
        } else if let Some(mut bucket) = self.patterned.get_mut(&binding.resource_type) {
            let before = bucket.len();
            bucket.retain(|b| b != binding);
            bucket.len() != before
        } else {
            false
        }
    }

    pub fn contains(&self, binding: &AclBinding) -> bool {
        if let Some(key) = Self::bucket_key(binding) {
            self.literal
                .get(&key)
                .is_some_and(|bucket| bucket.contains(binding))
        } else {
            self.patterned
                .get(&binding.resource_type)
                .is_some_and(|bucket| bucket.contains(binding))
        }
    }

    pub fn is_empty(&self) -> bool {
        self.literal.iter().all(|b| b.is_empty()) && self.patterned.iter().all(|b| b.is_empty())
    }

    fn for_each_binding(&self, mut f: impl FnMut(&AclBinding)) {
        for bucket in self.literal.iter() {
            for b in bucket.value() {
                f(b);
            }
        }
        for bucket in self.patterned.iter() {
            for b in bucket.value() {
                f(b);
            }
        }
    }

    pub fn list_acls(&self, filter: &AclBinding) -> Vec<AclBinding> {
        let mut out = Vec::new();
        self.for_each_binding(|b| {
            let match_res_type = filter.resource_type == 1
                || filter.resource_type == 0
                || filter.resource_type == b.resource_type;
            let match_res_name = filter.resource_name.is_empty()
                || filter.resource_name == "*"
                || filter.resource_name == b.resource_name;
            let match_principal = filter.principal.is_empty()
                || filter.principal == "*"
                || filter.principal == b.principal;
            let match_host = filter.host.is_empty() || filter.host == "*" || filter.host == b.host;
            let match_op =
                filter.operation == 1 || filter.operation == 0 || filter.operation == b.operation;
            let match_perm = filter.permission_type == 1
                || filter.permission_type == 0
                || filter.permission_type == b.permission_type;
            if match_res_type
                && match_res_name
                && match_principal
                && match_host
                && match_op
                && match_perm
            {
                out.push(b.clone());
            }
        });
        out
    }

    /// ACL Authorization Check Algorithm
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

        if self.is_empty() {
            // When ACL enforcement is enabled (`acls_enabled = true`),
            // default to DENY for User:ANONYMOUS unless superuser or explicit allow rules exist.
            return super_users.iter().any(|u| u == principal);
        }

        // Only the exact-name literal bucket plus this resource type's patterned bucket
        // can possibly match — no need to touch ACLs for other resource types/names at all.
        let literal_key = (resource_type, resource_name.to_string());
        let candidates: Vec<AclBinding> = self
            .literal
            .get(&literal_key)
            .map(|b| b.value().clone())
            .unwrap_or_default()
            .into_iter()
            .chain(
                self.patterned
                    .get(&resource_type)
                    .map(|b| b.value().clone())
                    .unwrap_or_default(),
            )
            .collect();

        // 1. Check for matching Deny rules
        for b in &candidates {
            if b.permission_type == (AclPermissionType::Deny as u8)
                && self.matches_rule(b, principal, host, operation, resource_type, resource_name)
            {
                return false;
            }
        }

        // 2. Check for matching Allow rules
        for b in &candidates {
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

        // Host match: "*" (any), an exact literal, or a CIDR range.
        if rule.host != "*" && rule.host != host {
            let host_ip: Option<std::net::IpAddr> = host.parse().ok();
            let cidr_matches = rule.host.contains('/')
                && host_ip.is_some_and(|ip| crate::server::listener::cidr_contains(&rule.host, ip));
            if !cidr_matches {
                return false;
            }
        }

        // Resource Name match (PatternType)
        match ResourcePatternType::from(rule.pattern_type) {
            ResourcePatternType::Prefixed => {
                if rule.resource_name != "*" && !resource_name.starts_with(&rule.resource_name) {
                    return false;
                }
            }
            ResourcePatternType::Match | ResourcePatternType::Any => {
                // MATCH pattern type: `rule.resource_name` may contain `*` as a
                // wildcard matching any run of characters (anywhere in the pattern, not
                // just as a suffix) — e.g. `"orders-*-eu"` matches `"orders-42-eu"`. A
                // bare `"*"` (or an `Any` pattern type) matches everything, same as today.
                if rule.resource_name != "*" && !glob_match(&rule.resource_name, resource_name) {
                    return false;
                }
            }
            _ => {
                // Literal or unrecognized pattern type
                if rule.resource_name != "*" && rule.resource_name != resource_name {
                    return false;
                }
            }
        }

        true
    }
}

/// Simple `*`-wildcard glob match (no other special characters): `*` matches any run of
/// zero or more characters, anywhere in `pattern`; every other character must match
/// literally. Iterative two-pointer algorithm (the standard glob-matching approach), so it
/// runs in O(pattern.len() * text.len()) worst case with no recursion.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '*') {
            star_idx = Some(pi);
            match_idx = ti;
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some(si) = star_idx {
            pi = si + 1;
            match_idx += 1;
            ti = match_idx;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: ResourcePatternType,
        principal: &str,
        host: &str,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> AclBinding {
        AclBinding {
            resource_type: resource_type as u8,
            resource_name: resource_name.to_string(),
            pattern_type: pattern_type as u8,
            principal: principal.to_string(),
            host: host.to_string(),
            operation: operation as u8,
            permission_type: permission_type as u8,
        }
    }

    #[test]
    fn glob_match_supports_wildcard_anywhere() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("orders-*", "orders-2024"));
        assert!(glob_match("*-eu", "orders-eu"));
        assert!(glob_match("orders-*-eu", "orders-42-eu"));
        assert!(!glob_match("orders-*-eu", "orders-42-us"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
    }

    #[test]
    fn literal_deny_beats_literal_allow() {
        let mgr = AclManager::new();
        mgr.add_acl(binding(
            ResourceType::Topic,
            "orders",
            ResourcePatternType::Literal,
            "*",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        ));
        mgr.add_acl(binding(
            ResourceType::Topic,
            "orders",
            ResourcePatternType::Literal,
            "User:bob",
            "*",
            AclOperation::Read,
            AclPermissionType::Deny,
        ));

        assert!(mgr.authorize(
            "User:alice",
            "10.0.0.1",
            AclOperation::Read as u8,
            ResourceType::Topic as u8,
            "orders",
            &[],
            true,
        ));
        assert!(!mgr.authorize(
            "User:bob",
            "10.0.0.1",
            AclOperation::Read as u8,
            ResourceType::Topic as u8,
            "orders",
            &[],
            true,
        ));
    }

    #[test]
    fn prefixed_and_match_patterns_apply_across_matching_names() {
        let mgr = AclManager::new();
        mgr.add_acl(binding(
            ResourceType::Topic,
            "orders-",
            ResourcePatternType::Prefixed,
            "*",
            "*",
            AclOperation::Write,
            AclPermissionType::Allow,
        ));
        mgr.add_acl(binding(
            ResourceType::Topic,
            "*-eu",
            ResourcePatternType::Match,
            "*",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        ));

        assert!(mgr.authorize(
            "User:x",
            "1.2.3.4",
            AclOperation::Write as u8,
            ResourceType::Topic as u8,
            "orders-2024",
            &[],
            true,
        ));
        assert!(mgr.authorize(
            "User:x",
            "1.2.3.4",
            AclOperation::Read as u8,
            ResourceType::Topic as u8,
            "payments-eu",
            &[],
            true,
        ));
        assert!(!mgr.authorize(
            "User:x",
            "1.2.3.4",
            AclOperation::Read as u8,
            ResourceType::Topic as u8,
            "payments-us",
            &[],
            true,
        ));
    }

    #[test]
    fn cidr_host_restricts_by_ip_range() {
        let mgr = AclManager::new();
        mgr.add_acl(binding(
            ResourceType::Cluster,
            "*",
            ResourcePatternType::Literal,
            "*",
            "10.0.0.0/8",
            AclOperation::ClusterAction,
            AclPermissionType::Allow,
        ));

        assert!(mgr.authorize(
            "User:x",
            "10.1.2.3",
            AclOperation::ClusterAction as u8,
            ResourceType::Cluster as u8,
            "bifrox-cluster",
            &[],
            true,
        ));
        assert!(!mgr.authorize(
            "User:x",
            "192.168.1.1",
            AclOperation::ClusterAction as u8,
            ResourceType::Cluster as u8,
            "bifrox-cluster",
            &[],
            true,
        ));
    }

    #[test]
    fn remove_and_list_acls_round_trip_across_both_buckets() {
        let mgr = AclManager::new();
        let literal = binding(
            ResourceType::Group,
            "my-group",
            ResourcePatternType::Literal,
            "User:a",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        );
        let prefixed = binding(
            ResourceType::Group,
            "svc-",
            ResourcePatternType::Prefixed,
            "User:a",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        );
        assert!(mgr.add_acl(literal.clone()));
        assert!(mgr.add_acl(prefixed.clone()));
        assert!(
            !mgr.add_acl(literal.clone()),
            "duplicate insert should report false"
        );

        let all = mgr.list_acls(&binding(
            ResourceType::Any,
            "",
            ResourcePatternType::Any,
            "",
            "",
            AclOperation::Any,
            AclPermissionType::Any,
        ));
        assert_eq!(all.len(), 2);

        assert!(mgr.remove_acl(&literal));
        assert!(!mgr.contains(&literal));
        assert!(mgr.contains(&prefixed));
        assert!(!mgr.is_empty());

        assert!(mgr.remove_acl(&prefixed));
        assert!(mgr.is_empty());
    }
}
