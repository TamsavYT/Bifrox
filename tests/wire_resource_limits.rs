use bifrox::protocol::WireRequest;

fn frame(command: u8, body: Vec<u8>) -> Vec<u8> {
    let mut bytes = vec![command];
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend(body);
    bytes
}

#[test]
fn hostile_counts_are_rejected_before_allocation() {
    // Each prefix ends immediately before an attacker-controlled collection count.
    for (command, prefix) in [(0x10, 4), (0x11, 8), (0x19, 12), (0x2c, 2), (0x2d, 2)] {
        let mut body = vec![0; prefix];
        body.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            WireRequest::decode_framed(&frame(command, body)).is_err(),
            "command {command}"
        );
    }
    let mut body = vec![0; 6]; // topic, empty upserts
    body.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(WireRequest::decode_framed(&frame(0x2d, body)).is_err());
    let mut metadata = vec![0x0c, 0, 0]; // topic config, empty topic name
    metadata.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(bifrox::MetadataRecord::decode(&metadata).is_err());
}

#[test]
fn nested_collections_share_a_decode_budget() {
    // Valid-sized frame whose nested record counts collectively exceed the budget.
    let mut body = vec![0; 8];
    body.extend_from_slice(&2u32.to_be_bytes());
    for _ in 0..2 {
        body.extend_from_slice(&[0, 0, 0, 0]); // member and topic names
        body.extend_from_slice(&50_000u32.to_be_bytes());
        body.resize(body.len() + 50_000 * 4, 0);
    }
    assert!(WireRequest::decode_framed(&frame(0x11, body)).is_err());
}
