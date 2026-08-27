use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bound (ms) of each latency histogram bucket (Prometheus "le" convention: a
/// sample increments every bucket whose bound is >= its value). The last bucket is
/// implicitly `+Inf` via `total_count`/`total_sum`.
const LATENCY_BUCKETS_MS: [u64; 11] = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

/// Fixed-bucket latency histogram, Prometheus-compatible (`_bucket`, `_sum`, `_count`).
/// Each bucket and the running sum/count are independent atomics rather than one lock —
/// recording a sample is a handful of relaxed increments, safe to call from any number of
/// concurrent request-handling tasks without contention on a shared histogram-wide lock.
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_MS.len()],
    sum_ms: AtomicU64,
    count: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: Default::default(),
            sum_ms: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    /// Increments exactly the *smallest* bucket whose bound is >= `elapsed` (a sample
    /// larger than every bound falls only into the implicit `+Inf` bucket, i.e. none of
    /// the fixed ones). `render` turns these per-bucket counts into Prometheus's expected
    /// cumulative form (each `le="X"` line = count of samples <= X) at render time —
    /// storing them cumulatively here instead would double-count every sample into every
    /// bucket it falls under on every single `record` call, which is only O(1) vs.
    /// O(buckets) here for no correctness benefit.
    pub fn record(&self, elapsed: Duration) {
        let ms = elapsed.as_millis() as u64;
        if let Some(i) = LATENCY_BUCKETS_MS.iter().position(|&bound| ms <= bound) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders this histogram's `_bucket`/`_sum`/`_count` lines under Prometheus metric
    /// name `name`, with `extra_labels` (already formatted as `key="value",...` or empty)
    /// merged into every line's label set.
    fn render(&self, name: &str, extra_labels: &str, out: &mut String) {
        let label_prefix = if extra_labels.is_empty() {
            String::new()
        } else {
            format!("{},", extra_labels)
        };
        let mut cumulative = 0u64;
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            out.push_str(&format!(
                "{}_bucket{{{}le=\"{}\"}} {}\n",
                name, label_prefix, bound, cumulative
            ));
        }
        let total = self.count.load(Ordering::Relaxed);
        out.push_str(&format!(
            "{}_bucket{{{}le=\"+Inf\"}} {}\n",
            name, label_prefix, total
        ));
        out.push_str(&format!(
            "{}_sum{{{}}} {}\n",
            name,
            extra_labels,
            self.sum_ms.load(Ordering::Relaxed)
        ));
        out.push_str(&format!("{}_count{{{}}} {}\n", name, extra_labels, total));
    }
}

/// Per-topic produce/fetch counters (per-topic metric dimensions).
#[derive(Debug, Default)]
struct TopicMetrics {
    produce_bytes_total: AtomicU64,
    produce_records_total: AtomicU64,
    fetch_bytes_total: AtomicU64,
}

/// Prometheus Metrics Collector for Bifrox Broker Engine
#[derive(Debug, Default)]
pub struct MetricsCollector {
    pub produce_bytes_total: AtomicU64,
    pub fetch_bytes_total: AtomicU64,
    pub produce_records_total: AtomicU64,
    pub active_connections: AtomicU64,
    pub quota_throttled_clients_total: AtomicU64,
    pub acl_denied_requests_total: AtomicU64,
    /// Per-topic breakdown of the same produce/fetch counters above. A broker-wide total
    /// tells an operator *that* something is busy; this tells them *which topic*.
    per_topic: DashMap<String, TopicMetrics>,
    /// Request-latency distributions for the two hottest request types — previously
    /// nothing tracked request latency at all, only aggregate byte/record counts, so a
    /// broker could look "fine" on throughput while individual requests were creeping up
    /// in latency (e.g. from GC-like retention/compaction pauses) with no way to see it.
    pub produce_latency_ms: LatencyHistogram,
    pub fetch_latency_ms: LatencyHistogram,
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

    /// Same accounting as `record_produce`, additionally broken down by topic.
    pub fn record_produce_topic(&self, topic: &str, bytes: u64, records_count: u64) {
        self.record_produce(bytes, records_count);
        let entry = self.per_topic.entry(topic.to_string()).or_default();
        entry
            .produce_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        entry
            .produce_records_total
            .fetch_add(records_count, Ordering::Relaxed);
    }

    /// Same accounting as `record_fetch`, additionally broken down by topic.
    pub fn record_fetch_topic(&self, topic: &str, bytes: u64) {
        self.record_fetch(bytes);
        let entry = self.per_topic.entry(topic.to_string()).or_default();
        entry.fetch_bytes_total.fetch_add(bytes, Ordering::Relaxed);
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

        let mut out = format!(
            "# HELP bifrox_produce_bytes_total Total bytes produced to Bifrox brokers.\n\
             # TYPE bifrox_produce_bytes_total counter\n\
             bifrox_produce_bytes_total {}\n\n\
             # HELP bifrox_fetch_bytes_total Total bytes fetched from Bifrox brokers.\n\
             # TYPE bifrox_fetch_bytes_total counter\n\
             bifrox_fetch_bytes_total {}\n\n\
             # HELP bifrox_produce_records_total Total record count produced.\n\
             # TYPE bifrox_produce_records_total counter\n\
             bifrox_produce_records_total {}\n\n\
             # HELP bifrox_active_connections Current active TCP client connections.\n\
             # TYPE bifrox_active_connections gauge\n\
             bifrox_active_connections {}\n\n\
             # HELP bifrox_topics_count Current active topic count in storage catalog.\n\
             # TYPE bifrox_topics_count gauge\n\
             bifrox_topics_count {}\n\n\
             # HELP bifrox_active_brokers_count Current active registered broker count.\n\
             # TYPE bifrox_active_brokers_count gauge\n\
             bifrox_active_brokers_count {}\n\n\
             # HELP bifrox_quota_throttled_clients_total Count of requests throttled by byte-rate quotas.\n\
             # TYPE bifrox_quota_throttled_clients_total counter\n\
             bifrox_quota_throttled_clients_total {}\n\n\
             # HELP bifrox_acl_denied_requests_total Count of requests denied by ACL authorization rules.\n\
             # TYPE bifrox_acl_denied_requests_total counter\n\
             bifrox_acl_denied_requests_total {}\n\n",
            produce_bytes,
            fetch_bytes,
            produce_recs,
            active_conns,
            active_topics,
            active_brokers,
            throttled,
            acl_denied,
        );

        out.push_str(
            "# HELP bifrox_topic_produce_bytes_total Bytes produced, broken down by topic.\n\
             # TYPE bifrox_topic_produce_bytes_total counter\n",
        );
        for entry in self.per_topic.iter() {
            out.push_str(&format!(
                "bifrox_topic_produce_bytes_total{{topic=\"{}\"}} {}\n",
                entry.key(),
                entry.value().produce_bytes_total.load(Ordering::Relaxed)
            ));
        }
        out.push_str(
            "\n# HELP bifrox_topic_produce_records_total Records produced, broken down by topic.\n\
             # TYPE bifrox_topic_produce_records_total counter\n",
        );
        for entry in self.per_topic.iter() {
            out.push_str(&format!(
                "bifrox_topic_produce_records_total{{topic=\"{}\"}} {}\n",
                entry.key(),
                entry.value().produce_records_total.load(Ordering::Relaxed)
            ));
        }
        out.push_str(
            "\n# HELP bifrox_topic_fetch_bytes_total Bytes fetched, broken down by topic.\n\
             # TYPE bifrox_topic_fetch_bytes_total counter\n",
        );
        for entry in self.per_topic.iter() {
            out.push_str(&format!(
                "bifrox_topic_fetch_bytes_total{{topic=\"{}\"}} {}\n",
                entry.key(),
                entry.value().fetch_bytes_total.load(Ordering::Relaxed)
            ));
        }

        out.push_str(
            "\n# HELP bifrox_produce_latency_ms Produce request latency in milliseconds.\n\
             # TYPE bifrox_produce_latency_ms histogram\n",
        );
        self.produce_latency_ms
            .render("bifrox_produce_latency_ms", "", &mut out);
        out.push_str(
            "\n# HELP bifrox_fetch_latency_ms Fetch request latency in milliseconds.\n\
             # TYPE bifrox_fetch_latency_ms histogram\n",
        );
        self.fetch_latency_ms
            .render("bifrox_fetch_latency_ms", "", &mut out);

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_topic_counters_stay_independent_and_sum_into_the_global_total() {
        let m = MetricsCollector::new();
        m.record_produce_topic("orders", 100, 2);
        m.record_produce_topic("payments", 50, 1);
        m.record_produce_topic("orders", 25, 1);

        assert_eq!(m.produce_bytes_total.load(Ordering::Relaxed), 175);
        assert_eq!(m.produce_records_total.load(Ordering::Relaxed), 4);

        let rendered = m.render_prometheus(0, 0);
        assert!(rendered.contains("bifrox_topic_produce_bytes_total{topic=\"orders\"} 125"));
        assert!(rendered.contains("bifrox_topic_produce_bytes_total{topic=\"payments\"} 50"));
    }

    #[test]
    fn latency_histogram_buckets_are_cumulative_and_le_inf_matches_count() {
        let h = LatencyHistogram::default();
        h.record(Duration::from_millis(3));
        h.record(Duration::from_millis(30));
        h.record(Duration::from_millis(3000));

        let mut out = String::new();
        h.render("test_latency_ms", "", &mut out);

        assert!(out.contains("test_latency_ms_bucket{le=\"1\"} 0"));
        assert!(out.contains("test_latency_ms_bucket{le=\"5\"} 1"));
        assert!(out.contains("test_latency_ms_bucket{le=\"50\"} 2"));
        assert!(out.contains("test_latency_ms_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("test_latency_ms_count{} 3"));
        assert!(out.contains(&format!("test_latency_ms_sum{{}} {}", 3 + 30 + 3000)));
    }
}
