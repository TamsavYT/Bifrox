static NEXT_TEST_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
use bifrox::{config::SecurityProtocol, EngineConfig, Server, StorageEngine, TestClient};
struct Broker {
    path: std::path::PathBuf,
    engine: StorageEngine,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Broker {
    async fn start(protocol: SecurityProtocol, acls: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "bifrox_auth_{}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let engine = StorageEngine::new(EngineConfig {
            data_dir: path.clone(),
            bind_addr: "127.0.0.1:0".into(),
            security_protocol: protocol,
            acls_enabled: acls,
            preallocate_segments: false,
            ..Default::default()
        })
        .unwrap();
        engine.create_topic("private", 1).await.unwrap();
        engine
            .get_or_create_partition("private", 0)
            .unwrap()
            .produce(b"secret-test-record")
            .unwrap();
        let server = Server::new(engine.clone());
        let (listener, addr) = server.bind().unwrap();
        let task = tokio::spawn(async move { server.run_with_listener(listener).await });
        Self {
            path,
            engine,
            addr,
            task,
        }
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
#[tokio::test]
async fn sasl_gate_precedes_zero_copy_and_topic_creation() {
    let broker = Broker::start(SecurityProtocol::SaslPlaintext, false).await;
    let mut client = TestClient::connect(broker.addr).await.unwrap();
    assert!(client.fetch("private", 0, 0, 65536).await.is_err());
    assert!(client
        .produce_single("unauth-created", "key", None, 1, b"payload")
        .await
        .is_err());
    assert!(!broker.engine.topic_is_registered("unauth-created"));
    assert!(!broker.path.join("unauth-created-0").exists());
}
#[tokio::test]
async fn denied_acl_precedes_topic_creation() {
    let broker = Broker::start(SecurityProtocol::Plaintext, true).await;
    let mut client = TestClient::connect(broker.addr).await.unwrap();
    assert!(client
        .produce_single("denied-created", "key", None, 1, b"payload")
        .await
        .is_err());
    assert!(!broker.engine.topic_is_registered("denied-created"));
    assert!(!broker.path.join("denied-created-0").exists());
}
#[tokio::test]
async fn client_mutations_cannot_touch_internal_topics() {
    let broker = Broker::start(SecurityProtocol::Plaintext, false).await;
    let mut client = TestClient::connect(broker.addr).await.unwrap();
    for topic in [
        "__cluster_metadata",
        "__transaction_state",
        "__consumer_offsets",
        "__share_state",
    ] {
        let pm = broker.engine.get_or_create_partition(topic, 0).unwrap();
        let before = pm.latest_offset();
        assert!(client.create_topic(topic, 1).await.is_err());
        assert!(client.delete_topic(topic).await.is_err());
        assert!(client
            .produce_single(topic, "key", None, 1, b"malicious")
            .await
            .is_err());
        assert!(client.alter_configs(topic, &[]).await.is_err());
        assert!(broker.path.join(format!("{topic}-0")).exists());
        assert_eq!(pm.latest_offset(), before);
    }
}
