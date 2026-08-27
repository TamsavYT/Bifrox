use dashmap::DashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Token-bucket rate limiter tracking bytes consumed by a single client key.
///
/// The bucket refills continuously at `rate` bytes/sec, up to a burst capacity of
/// `rate` bytes (i.e. clients may burst up to one second's worth of quota before
/// being throttled). This mirrors `client.quota.callback` byte-rate model,
/// simplified to a single default quota (no per-user overrides).
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    /// Consumes `amount` tokens, refilling first based on elapsed time.
    /// Returns the delay the caller should wait before its response is released,
    /// Throttling rather than rejecting: the request is still processed,
    /// but its response is delayed to force the client to slow down.
    fn consume(&mut self, amount: f64, rate: f64, capacity: f64) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * rate).min(capacity);
        self.tokens -= amount;

        if self.tokens < 0.0 {
            Duration::from_secs_f64((-self.tokens / rate).min(60.0))
        } else {
            Duration::ZERO
        }
    }
}

/// Per-logical-client byte-rate quota enforcement for produce and fetch traffic.
///
/// The caller provides the logical quota key, which may represent an authenticated
/// principal, a connection-scoped client_id, or a fallback source-IP identity.
/// When either `produce_rate` or `fetch_rate` is `None`, the corresponding quota is
/// disabled and requests are never delayed — this preserves existing unthrottled
/// behavior by default.
#[derive(Debug)]
pub struct QuotaManager {
    produce_rate: Option<u64>,
    fetch_rate: Option<u64>,
    produce_buckets: DashMap<String, Mutex<TokenBucket>>,
    fetch_buckets: DashMap<String, Mutex<TokenBucket>>,
}

impl QuotaManager {
    pub fn new(produce_rate: Option<u64>, fetch_rate: Option<u64>) -> Self {
        Self {
            produce_rate,
            fetch_rate,
            produce_buckets: DashMap::new(),
            fetch_buckets: DashMap::new(),
        }
    }

    /// Accounts `bytes` of produced data for `client_key` and sleeps for as long as
    /// necessary to keep this client within its configured produce byte-rate quota.
    /// No-op when no produce quota is configured.
    pub async fn throttle_produce(&self, client_key: &str, bytes: u64) {
        Self::throttle(&self.produce_buckets, self.produce_rate, client_key, bytes).await;
    }

    /// Accounts `bytes` of fetched data for `client_key` and sleeps for as long as
    /// necessary to keep this client within its configured fetch byte-rate quota.
    /// No-op when no fetch quota is configured.
    pub async fn throttle_fetch(&self, client_key: &str, bytes: u64) {
        Self::throttle(&self.fetch_buckets, self.fetch_rate, client_key, bytes).await;
    }

    async fn throttle(
        buckets: &DashMap<String, Mutex<TokenBucket>>,
        rate: Option<u64>,
        client_key: &str,
        bytes: u64,
    ) {
        let rate = match rate {
            Some(r) if r > 0 => r as f64,
            _ => return,
        };
        if bytes == 0 {
            return;
        }

        let capacity = rate; // Allow bursting up to 1 second's worth of quota.
        let delay = {
            let entry = buckets
                .entry(client_key.to_string())
                .or_insert_with(|| Mutex::new(TokenBucket::new(capacity)));
            let mut bucket = entry.lock().unwrap();
            bucket.consume(bytes as f64, rate, capacity)
        };

        if delay > Duration::ZERO {
            tracing::debug!(
                "Quota: throttling client '{}' for {:?} (bytes={})",
                client_key,
                delay,
                bytes
            );
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_quota_never_throttles() {
        let qm = QuotaManager::new(None, None);
        let start = Instant::now();
        qm.throttle_produce("client-a", 10_000_000).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn exceeding_quota_delays_response() {
        // 100 bytes/sec quota, consume 300 bytes in one shot (3x burst) — must delay.
        let qm = QuotaManager::new(Some(100), None);
        let start = Instant::now();
        qm.throttle_produce("client-b", 300).await;
        // First call has full burst capacity, so violation should be ~2 sec (300-100 over rate 100/s).
        assert!(start.elapsed() >= Duration::from_millis(1900));
    }

    #[tokio::test]
    async fn separate_clients_have_independent_buckets() {
        let qm = QuotaManager::new(Some(100), None);
        qm.throttle_produce("client-c", 100).await; // exhausts client-c's burst
        let start = Instant::now();
        qm.throttle_produce("client-d", 10).await; // different client, should not be throttled
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}
