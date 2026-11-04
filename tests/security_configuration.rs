use bifrox::{config::SecurityProtocol, EngineConfig};
use std::path::PathBuf;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "bifrox_config_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn listener_scheme_selects_security_without_property_order_dependence() {
    let t = Temp::new();
    let path = t.0.join("server.properties");
    for (scheme, expected) in [
        ("SSL", SecurityProtocol::Ssl),
        ("SASL_SSL", SecurityProtocol::SaslSsl),
        ("SASL_PLAINTEXT", SecurityProtocol::SaslPlaintext),
        ("PLAINTEXT", SecurityProtocol::Plaintext),
    ] {
        for text in [
            format!("listeners={scheme}://127.0.0.1:9092\n"),
            format!("security.protocol={scheme}\nlisteners={scheme}://127.0.0.1:9092\n"),
            format!("listeners={scheme}://127.0.0.1:9092\nsecurity.protocol={scheme}\n"),
        ] {
            std::fs::write(&path, text).unwrap();
            let config = EngineConfig::from_properties_file(&path).unwrap();
            assert_eq!(config.security_protocol, expected);
            assert_eq!(config.bind_addr, "127.0.0.1:9092");
        }
    }
}
#[test]
fn invalid_and_conflicting_settings_fail_closed() {
    let t = Temp::new();
    let path = t.0.join("server.properties");
    for text in [
        "security.protocol=SASL_SSLL\n",
        "listeners=INVALID://127.0.0.1:9092\n",
        "listeners=SSL://127.0.0.1:9092\nsecurity.protocol=PLAINTEXT\n",
        "security.protocol=PLAINTEXT\nlisteners=SSL://127.0.0.1:9092\n",
    ] {
        std::fs::write(&path, text).unwrap();
        assert!(EngineConfig::from_properties_file(&path).is_err());
    }
}
#[test]
fn incomplete_tls_material_does_not_generate_a_replacement_identity() {
    let cfg = EngineConfig {
        ssl_cert_path: Some("missing-cert.pem".into()),
        ..Default::default()
    };
    assert!(bifrox::server::listener::build_tls_acceptor(&cfg).is_err());
}
