use bifrox::replication::{
    grpc::send_grpc_replication_fetch, transport::PeerTransport, ReplicationFetchRequest,
};
use bifrox::{config::SecurityProtocol, EngineConfig, Server, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Fixture {
    dir: std::path::PathBuf,
    server: EngineConfig,
    client: EngineConfig,
}
impl Fixture {
    fn new() -> Self {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let dir = std::env::temp_dir().join(format!(
            "peer_tls_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = params.self_signed(&key).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
        for id in 1..=3 {
            let leaf_key = KeyPair::generate().unwrap();
            let leaf = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap()
                .signed_by(&leaf_key, &ca, &key)
                .unwrap();
            std::fs::write(dir.join(format!("{id}.pem")), leaf.pem()).unwrap();
            std::fs::write(dir.join(format!("{id}.key")), leaf_key.serialize_pem()).unwrap();
        }
        let config = |id, peer| EngineConfig {
            node_id: id,
            data_dir: dir.join(format!("data-{id}")),
            bind_addr: "127.0.0.1:0".into(),
            security_protocol: SecurityProtocol::Ssl,
            ssl_ca_path: Some(dir.join("ca.pem")),
            ssl_cert_path: Some(dir.join(format!("{id}.pem"))),
            ssl_key_path: Some(dir.join(format!("{id}.key"))),
            replication_peer_certificates: std::collections::HashMap::from([(
                peer,
                dir.join(format!("{peer}.pem")),
            )]),
            preallocate_segments: false,
            ..Default::default()
        };
        let server = config(1, 2);
        let client = config(2, 1);
        Self {
            dir,
            server,
            client,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn request(id: u32) -> ReplicationFetchRequest {
    ReplicationFetchRequest {
        follower_node_id: id,
        topic: "private".into(),
        partition: 0,
        fetch_offset: 0,
        max_bytes: 65536,
    }
}
#[tokio::test]
async fn tls_fetch_requires_pinned_identity_and_replica_membership() {
    let f = Fixture::new();
    let engine = StorageEngine::new(f.server.clone()).unwrap();
    let pm = engine.get_or_create_partition("private", 0).unwrap();
    pm.produce(b"secret").unwrap();
    pm.update_leadership(1, 0, vec![1, 2], vec![1, 2]);
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();
    let task = tokio::spawn(async move { server.run_with_listener(listener).await });
    let transport = PeerTransport::from_config(&f.client).unwrap();
    assert!(
        !send_grpc_replication_fetch(&addr.to_string(), &request(2), &transport)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        send_grpc_replication_fetch(&addr.to_string(), &request(3), &transport)
            .await
            .is_err()
    );
    pm.update_leadership(1, 0, vec![1], vec![1]);
    assert!(
        send_grpc_replication_fetch(&addr.to_string(), &request(2), &transport)
            .await
            .is_err()
    );
    let mut unknown = f.client.clone();
    unknown.node_id = 3;
    unknown.ssl_cert_path = Some(f.dir.join("3.pem"));
    unknown.ssl_key_path = Some(f.dir.join("3.key"));
    assert!(send_grpc_replication_fetch(
        &addr.to_string(),
        &request(3),
        &PeerTransport::from_config(&unknown).unwrap()
    )
    .await
    .is_err());
    let mut wrong_server = f.client.clone();
    wrong_server.replication_peer_certificates =
        std::collections::HashMap::from([(3, f.dir.join("3.pem"))]);
    assert!(PeerTransport::from_config(&wrong_server)
        .unwrap()
        .connect(&addr.to_string())
        .await
        .is_err());
    assert!(bifrox::replication::send_leader_heartbeat(
        &addr.to_string(),
        &f.server.cluster_id,
        3,
        1,
        "127.0.0.1:19999",
        &transport
    )
    .await
    .is_err());
    assert!(bifrox::replication::send_leader_heartbeat(
        &addr.to_string(),
        &f.server.cluster_id,
        2,
        1,
        "127.0.0.1:19999",
        &transport
    )
    .await
    .is_ok());
    // A trusted replica certificate does not grant controller command authority.
    use bytes::BufMut;
    let mut payload = Vec::new();
    bifrox::protocol::wire::write_pascal_string(&mut payload, &f.server.cluster_id);
    payload.put_u32(2);
    payload.put_u64(0);
    payload.push(0);
    bifrox::protocol::wire::write_pascal_string(&mut payload, "private");
    payload.put_u32(0);
    let request = bifrox::replication::encode_frame(
        bifrox::replication::FrameType::TransactionCommand,
        &payload,
    );
    let mut stream = transport.connect(&addr.to_string()).await.unwrap();
    // The earlier heartbeat made node 2 the controller; a stale term must still fail.
    engine.replication().set_epoch(1);
    let reply = bifrox::replication::envelope::exchange_frame(
        &mut stream,
        &request,
        bifrox::replication::FrameType::TransactionResponse,
    )
    .await
    .unwrap();
    assert_eq!(reply.first(), Some(&1));
    payload[2 + f.server.cluster_id.len()..2 + f.server.cluster_id.len() + 4]
        .copy_from_slice(&3u32.to_be_bytes());
    let spoof = bifrox::replication::encode_frame(
        bifrox::replication::FrameType::TransactionCommand,
        &payload,
    );
    let mut stream = transport.connect(&addr.to_string()).await.unwrap();
    assert!(bifrox::replication::envelope::exchange_frame(
        &mut stream,
        &spoof,
        bifrox::replication::FrameType::TransactionResponse
    )
    .await
    .is_err());
    task.abort();
}
#[tokio::test]
async fn plaintext_and_sasl_connections_cannot_read_replication_data() {
    for protocol in [SecurityProtocol::Plaintext, SecurityProtocol::SaslPlaintext] {
        let f = Fixture::new();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: f.dir.join("plain"),
            bind_addr: "127.0.0.1:0".into(),
            security_protocol: protocol,
            ..Default::default()
        })
        .unwrap();
        engine
            .get_or_create_partition("private", 0)
            .unwrap()
            .produce(b"secret")
            .unwrap();
        let server = Server::new(engine);
        let (listener, addr) = server.bind().unwrap();
        let task = tokio::spawn(async move { server.run_with_listener(listener).await });
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        socket.write_all(&request(2).encode_frame()).await.unwrap();
        let mut buf = [0; 1];
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), socket.read(&mut buf))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        task.abort();
    }
}
#[test]
fn insecure_replication_cannot_override_client_security() {
    for protocol in [
        SecurityProtocol::Ssl,
        SecurityProtocol::SaslSsl,
        SecurityProtocol::SaslPlaintext,
    ] {
        assert!(PeerTransport::from_config(&EngineConfig {
            allow_insecure_replication: true,
            security_protocol: protocol,
            ..Default::default()
        })
        .is_err());
    }
    assert!(PeerTransport::from_config(&EngineConfig {
        allow_insecure_replication: true,
        acls_enabled: true,
        ..Default::default()
    })
    .is_err());
    assert!(PeerTransport::from_config(&EngineConfig {
        allow_insecure_replication: true,
        auth_token: Some("secret".into()),
        ..Default::default()
    })
    .is_err());
}
