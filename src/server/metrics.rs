use std::sync::atomic::{AtomicU64, Ordering};

/// Prometheus Metrics Collector for Hermes Broker Engine
#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub produce_bytes_total: AtomicU64,
    pub fetch_bytes_total: AtomicU64,
    pub produce_records_total: AtomicU64,
    pub active_connections: AtomicU64,
    pub quota_throttled_clients_total: AtomicU64,
    pub acl_denied_requests_total: AtomicU64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_produce(&self, bytes: u64, records_count: u64) {
        self.produce_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.produce_records_total
            .fetch_add(records_count, Ordering::Relaxed);
    }

    pub fn record_fetch(&self, bytes: u64) {
        self.fetch_bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_connection_open(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_close(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_quota_throttle(&self) {
        self.quota_throttled_clients_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_acl_deny(&self) {
        self.acl_denied_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Renders standard Prometheus exposition format string
    pub fn render_prometheus(&self, active_topics: usize, active_brokers: usize) -> String {
        let produce_bytes = self.produce_bytes_total.load(Ordering::Relaxed);
        let fetch_bytes = self.fetch_bytes_total.load(Ordering::Relaxed);
        let produce_recs = self.produce_records_total.load(Ordering::Relaxed);
        let active_conns = self.active_connections.load(Ordering::Relaxed);
        let throttled = self.quota_throttled_clients_total.load(Ordering::Relaxed);
        let acl_denied = self.acl_denied_requests_total.load(Ordering::Relaxed);

        format!(
            "# HELP hermes_produce_bytes_total Total bytes produced to Hermes brokers.\n\
             # TYPE hermes_produce_bytes_total counter\n\
             hermes_produce_bytes_total {}\n\n\
             # HELP hermes_fetch_bytes_total Total bytes fetched from Hermes brokers.\n\
             # TYPE hermes_fetch_bytes_total counter\n\
             hermes_fetch_bytes_total {}\n\n\
             # HELP hermes_produce_records_total Total record count produced.\n\
             # TYPE hermes_produce_records_total counter\n\
             hermes_produce_records_total {}\n\n\
             # HELP hermes_active_connections Current active TCP client connections.\n\
             # TYPE hermes_active_connections gauge\n\
             hermes_active_connections {}\n\n\
             # HELP hermes_topics_count Current active topic count in storage catalog.\n\
             # TYPE hermes_topics_count gauge\n\
             hermes_topics_count {}\n\n\
             # HELP hermes_active_brokers_count Current active registered broker count.\n\
             # TYPE hermes_active_brokers_count gauge\n\
             hermes_active_brokers_count {}\n\n\
             # HELP hermes_quota_throttled_clients_total Count of requests throttled by byte-rate quotas.\n\
             # TYPE hermes_quota_throttled_clients_total counter\n\
             hermes_quota_throttled_clients_total {}\n\n\
             # HELP hermes_acl_denied_requests_total Count of requests denied by ACL authorization rules.\n\
             # TYPE hermes_acl_denied_requests_total counter\n\
             hermes_acl_denied_requests_total {}\n",
            produce_bytes,
            fetch_bytes,
            produce_recs,
            active_conns,
            active_topics,
            active_brokers,
            throttled,
            acl_denied,
        )
    }
}
