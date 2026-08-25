use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandCode {
    ProduceBatch = 0x01,
    Fetch = 0x02,
    CommitOffset = 0x03,
    FetchOffset = 0x04,
    Seek = 0x05,
    LatestOffset = 0x06,
    BeginTx = 0x07,
    CommitTx = 0x08,
    AbortTx = 0x09,
    FetchByTimestamp = 0x0A,
    /// P1: Read-committed fetch — hides uncommitted and aborted records
    FetchCommitted = 0x0B,
    Ping = 0x0C,
    ListTopics = 0x0D,
    DescribeCluster = 0x0E,
    DeleteTopic = 0x0F,
    JoinGroup = 0x10,
    SyncGroup = 0x11,
    Heartbeat = 0x12,
    LeaveGroup = 0x13,
    CreateTopic = 0x14,
    DescribeTopic = 0x15,
    ListGroups = 0x16,
    DescribeGroup = 0x17,
    InitProducerId = 0x18,
    AddPartitionsToTxn = 0x19,
    EndTxn = 0x1A,
    OffsetCommit = 0x1B,
    OffsetFetch = 0x1C,
    SaslHandshake = 0x1D,
    SaslAuthenticate = 0x1E,
    DescribeAcls = 0x1F,
    CreateAcls = 0x20,
    DeleteAcls = 0x21,
    RegisterBroker = 0x22,
    UnregisterBroker = 0x23,
    SetClientId = 0x24,
    UpsertScramUser = 0x25,
    DeleteScramUser = 0x26,
    ShareFetch = 0x27,
    ShareAcknowledge = 0x28,
    ShareGroupHeartbeat = 0x29,
    ShareGroupDescribe = 0x2A,
    DescribeConfigs = 0x2B,
    AlterConfigs = 0x2C,
    IncrementalAlterConfigs = 0x2D,
    /// Returns the protocol versions and command codes this broker supports, so a client
    /// can discover them before sending a request rather than probing and handling
    /// rejection. Deliberately answerable under the legacy framing, since a client that
    /// does not yet know the envelope exists is exactly who needs to ask.
    NegotiateProtocol = 0x2E,
}

impl TryFrom<u8> for CommandCode {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(CommandCode::ProduceBatch),
            0x02 => Ok(CommandCode::Fetch),
            0x03 => Ok(CommandCode::CommitOffset),
            0x04 => Ok(CommandCode::FetchOffset),
            0x05 => Ok(CommandCode::Seek),
            0x06 => Ok(CommandCode::LatestOffset),
            0x07 => Ok(CommandCode::BeginTx),
            0x08 => Ok(CommandCode::CommitTx),
            0x09 => Ok(CommandCode::AbortTx),
            0x0A => Ok(CommandCode::FetchByTimestamp),
            0x0B => Ok(CommandCode::FetchCommitted),
            0x0C => Ok(CommandCode::Ping),
            0x0D => Ok(CommandCode::ListTopics),
            0x0E => Ok(CommandCode::DescribeCluster),
            0x0F => Ok(CommandCode::DeleteTopic),
            0x10 => Ok(CommandCode::JoinGroup),
            0x11 => Ok(CommandCode::SyncGroup),
            0x12 => Ok(CommandCode::Heartbeat),
            0x13 => Ok(CommandCode::LeaveGroup),
            0x14 => Ok(CommandCode::CreateTopic),
            0x15 => Ok(CommandCode::DescribeTopic),
            0x16 => Ok(CommandCode::ListGroups),
            0x17 => Ok(CommandCode::DescribeGroup),
            0x18 => Ok(CommandCode::InitProducerId),
            0x19 => Ok(CommandCode::AddPartitionsToTxn),
            0x1A => Ok(CommandCode::EndTxn),
            0x1B => Ok(CommandCode::OffsetCommit),
            0x1C => Ok(CommandCode::OffsetFetch),
            0x1D => Ok(CommandCode::SaslHandshake),
            0x1E => Ok(CommandCode::SaslAuthenticate),
            0x1F => Ok(CommandCode::DescribeAcls),
            0x20 => Ok(CommandCode::CreateAcls),
            0x21 => Ok(CommandCode::DeleteAcls),
            0x22 => Ok(CommandCode::RegisterBroker),
            0x23 => Ok(CommandCode::UnregisterBroker),
            0x24 => Ok(CommandCode::SetClientId),
            0x25 => Ok(CommandCode::UpsertScramUser),
            0x26 => Ok(CommandCode::DeleteScramUser),
            0x27 => Ok(CommandCode::ShareFetch),
            0x28 => Ok(CommandCode::ShareAcknowledge),
            0x29 => Ok(CommandCode::ShareGroupHeartbeat),
            0x2A => Ok(CommandCode::ShareGroupDescribe),
            0x2B => Ok(CommandCode::DescribeConfigs),
            0x2C => Ok(CommandCode::AlterConfigs),
            0x2D => Ok(CommandCode::IncrementalAlterConfigs),
            0x2E => Ok(CommandCode::NegotiateProtocol),
            _ => Err(WireError::UnknownCommand(value)),
        }
    }
}

pub const MAX_REQUEST_PAYLOAD_BYTES: usize = 64 * 1024 * 1024; // 64MB cap (SEC-01)

/// Introduces a versioned request envelope. Chosen from the range no `CommandCode` uses
/// (codes run 0x01..=0x2E) and no inter-node magic uses (0xAA/0xAC/0xAE/0xAF/0xBB), so a
/// broker can tell the two framings apart from the very first byte.
///
/// This is what makes the versioned framing addable without a flag day: a client that
/// knows nothing about it keeps sending a bare command code and is still understood, while
/// a newer client opts in per request.
pub const VERSIONED_ENVELOPE_MAGIC: u8 = 0xF1;

/// Oldest envelope version this broker accepts.
pub const PROTOCOL_VERSION_MIN: u16 = 1;
/// Newest envelope version this broker understands. Bump when the envelope itself gains a
/// field; individual requests carry their own shape inside the payload.
pub const PROTOCOL_VERSION_MAX: u16 = 1;

/// Every command code this build accepts, ascending.
///
/// Reported by `NegotiateProtocol` so a client can tell in advance whether a command
/// exists here, rather than sending it and interpreting the resulting error — which is
/// indistinguishable from the command failing for an ordinary reason.
pub fn supported_command_codes() -> Vec<u8> {
    (0x01u8..=0xFF)
        .filter(|code| CommandCode::try_from(*code).is_ok())
        .collect()
}

/// Tagged-field identifiers carried in a versioned request envelope.
///
/// A tag is how an optional per-request field is added without changing any existing
/// layout: brokers that know a tag act on it, brokers that don't skip it. Values are
/// permanent once assigned.
pub mod tags {
    /// Read isolation for a fetch. Payload is one byte — see [`super::IsolationLevel`].
    pub const ISOLATION_LEVEL: u8 = 0x01;

    /// Marks a request that a broker is relaying on a client's behalf. The payload is
    /// empty; the tag's presence is the signal.
    ///
    /// The receiving broker serves such a request locally instead of forwarding it onward.
    /// Without it, forwarding can ping-pong: the controller creates a topic and forwards to
    /// the newly assigned leader, which has not yet received that assignment through the
    /// metadata log, sees an unknown topic, concludes it is not the leader, and forwards
    /// straight back.
    pub const FORWARDED: u8 = 0x02;

    /// Requested `session.timeout.ms` for the member joining via `JoinGroup`. Payload is a
    /// big-endian `u32` of milliseconds.
    ///
    /// The coordinator does not trust this outright — it clamps it into a sane range (see
    /// `server::coordinator::{MIN_SESSION_TIMEOUT, MAX_SESSION_TIMEOUT}`) before using it
    /// as the member's eviction threshold, so a client cannot ask for a timeout so short it
    /// gets evicted on ordinary jitter, or so long it defeats failure detection. An absent
    /// tag — a legacy client, or any request built without it — keeps the coordinator's
    /// historical fixed default.
    pub const SESSION_TIMEOUT_MS: u8 = 0x03;

    /// Identifies the consumer group member a request on the consuming path (currently
    /// `Fetch`) is made on behalf of, so the coordinator can record when that member last
    /// made progress (issue #54: a member that keeps heartbeating but has stopped
    /// consuming is otherwise indistinguishable from a healthy one). Payload is two
    /// pascal strings back to back: `[group_id][member_id]`.
    pub const GROUP_MEMBER: u8 = 0x04;

    /// How long a fetch may wait for data before answering empty, in milliseconds. Payload
    /// is a big-endian `u32`. Kafka's `fetch.max.wait.ms`.
    ///
    /// Without it a fetch against an idle partition returns empty immediately and the
    /// consumer asks again, burning a round trip per poll tick for nothing. With it the
    /// broker holds the request until data arrives, which cuts request volume *and* lowers
    /// delivery latency — a polling consumer waits on average half its poll interval for a
    /// record that already landed, a parked one is woken as soon as it commits.
    ///
    /// An absent tag means zero: answer immediately, exactly as before.
    pub const MAX_WAIT_MS: u8 = 0x05;

    /// How many bytes must accumulate before a waiting fetch is answered. Payload is a
    /// big-endian `u32`. Kafka's `fetch.min.bytes`.
    ///
    /// Only meaningful alongside [`MAX_WAIT_MS`], which bounds the wait. An absent tag
    /// means 1: any data at all completes the fetch.
    pub const MIN_BYTES: u8 = 0x06;
}

/// Read isolation requested by a fetch.
///
/// Committed-only reads previously required calling a *different command*
/// (`FetchCommitted`) rather than setting a flag on the ordinary fetch path, so a client
/// had to know in advance which command to use and could not express isolation as the
/// per-request property it actually is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Serves everything up to the high watermark, including records from transactions
    /// that were later aborted. The historical behavior of `Fetch`, and still the default
    /// so an unset tag means exactly what it always did.
    #[default]
    ReadUncommitted,
    /// Bounds the read at the last stable offset and filters aborted ranges and control
    /// markers.
    ReadCommitted,
}

impl IsolationLevel {
    pub fn to_byte(self) -> u8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }

    /// Unknown values decode as `ReadUncommitted` rather than erroring: an unrecognised
    /// isolation from a newer client must not fail the fetch outright, and the permissive
    /// value is also the historical default.
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => IsolationLevel::ReadCommitted,
            _ => IsolationLevel::ReadUncommitted,
        }
    }
}

/// Recognised tagged fields from a request envelope.
///
/// Deliberately a small struct of *known* tags rather than a list of raw ones: unknown
/// tags are still skipped during decode, so this gains a field only when a tag is actually
/// implemented. Was `Copy` until [`tags::GROUP_MEMBER`] added a `String`-bearing field;
/// every other field is still cheap, and the common case — no group-member tag on the
/// request — clones for free since `Option::None` allocates nothing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestTags {
    pub isolation_level: Option<IsolationLevel>,
    /// True when a broker relayed this request on a client's behalf — see
    /// [`tags::FORWARDED`]. Such a request is served where it lands, never forwarded on.
    pub forwarded: bool,
    /// `session.timeout.ms` the joining member asked for — see
    /// [`tags::SESSION_TIMEOUT_MS`]. `None` means no tag was sent at all, which the
    /// coordinator treats differently from an in-range value: it keeps its historical
    /// default rather than clamping "nothing" into something.
    pub session_timeout_ms: Option<u32>,
    /// `(group_id, member_id)` a consuming-path request is made on behalf of — see
    /// [`tags::GROUP_MEMBER`].
    pub group_member: Option<(String, String)>,
    /// `fetch.max.wait.ms` — see [`tags::MAX_WAIT_MS`]. `None` means answer immediately.
    pub max_wait_ms: Option<u32>,
    /// `fetch.min.bytes` — see [`tags::MIN_BYTES`]. `None` means 1.
    pub min_bytes: Option<u32>,
}

/// How a request arrived, which determines how its response must be framed.
///
/// The wire format had no version field at all: a broker and client built from different
/// revisions could not detect a mismatch and would silently misinterpret each other's
/// bytes, and every layout change was a hard break requiring every broker and client to be
/// upgraded together. This is the mechanism that ends that — see `VERSIONED_ENVELOPE_MAGIC`.
///
/// No longer `Copy` now that `RequestTags` can carry a `String`-bearing tag — callers that
/// need to use a `framing` value more than once now hold it by reference (`&RequestFraming`)
/// rather than relying on an implicit copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFraming {
    /// Bare `[cmd][len][payload]`, from a client that predates the envelope. Responses go
    /// back unwrapped, exactly as before.
    Legacy,
    /// Carries a protocol version, a correlation id, and any recognised tagged fields.
    Versioned {
        api_version: u16,
        /// Echoed in the response so a client can match replies to requests — the
        /// prerequisite for having more than one request in flight per connection.
        correlation_id: u32,
        tags: RequestTags,
    },
}

impl RequestFraming {
    pub fn correlation_id(&self) -> Option<u32> {
        match self {
            RequestFraming::Legacy => None,
            RequestFraming::Versioned { correlation_id, .. } => Some(*correlation_id),
        }
    }

    /// Recognised tagged fields, or the defaults for a legacy request — which had no way
    /// to express them at all.
    pub fn tags(&self) -> RequestTags {
        match self {
            RequestFraming::Legacy => RequestTags::default(),
            RequestFraming::Versioned { tags, .. } => tags.clone(),
        }
    }

    /// Isolation the request asked for, defaulting to the historical behavior when unset.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.tags().isolation_level.unwrap_or_default()
    }

    /// True when a broker relayed this request rather than a client sending it directly.
    pub fn is_forwarded(&self) -> bool {
        self.tags().forwarded
    }

    /// `session.timeout.ms` requested via [`tags::SESSION_TIMEOUT_MS`], or `None` if the
    /// request carried no such tag (a legacy request always reads `None`). Unlike
    /// `isolation_level`/`is_forwarded`, this deliberately does not default the value here
    /// — clamping an absent request into a default is the coordinator's job, not the wire
    /// layer's, since only the coordinator knows what that default and clamp range are.
    pub fn session_timeout_ms(&self) -> Option<u32> {
        self.tags().session_timeout_ms
    }

    /// How long this fetch may wait for data — see [`tags::MAX_WAIT_MS`]. Zero when
    /// untagged, meaning answer immediately, which is what every fetch did before long
    /// polling existed.
    pub fn max_wait_ms(&self) -> u32 {
        self.tags().max_wait_ms.unwrap_or(0)
    }

    /// How many bytes must accumulate before a waiting fetch is answered — see
    /// [`tags::MIN_BYTES`]. Defaults to 1: any data at all is enough.
    pub fn min_bytes(&self) -> u32 {
        self.tags().min_bytes.unwrap_or(1).max(1)
    }

    /// `(group_id, member_id)` this request is attributed to via
    /// [`tags::GROUP_MEMBER`], or `None` if untagged.
    pub fn group_member(&self) -> Option<(String, String)> {
        self.tags().group_member
    }
}

/// Rewraps a request for relay to another broker, marking it as forwarded.
///
/// Any envelope already on `raw_request` is replaced rather than nested: a second envelope
/// would leave the receiver parsing the outer one and then finding `0xF1` where a command
/// code belongs, which decodes as an unknown command. The inner request is preserved
/// byte-for-byte, and recognised tags from the original envelope are carried across — so a
/// forwarded fetch keeps the isolation level the client asked for, a forwarded join keeps
/// the session timeout it requested, and a forwarded consuming-path request keeps the
/// member it was attributed to.
pub fn wrap_forwarded_request(raw_request: &[u8]) -> Result<Vec<u8>, WireError> {
    let (inner, tags) = strip_envelope(raw_request)?;

    let mut out = Vec::with_capacity(inner.len() + 16);
    out.put_u8(VERSIONED_ENVELOPE_MAGIC);
    out.put_u16(PROTOCOL_VERSION_MAX);
    // The relaying broker owns the correlation id on this hop; the client's own id belongs
    // to its connection with us, and we relay the response body back to it unchanged.
    out.put_u32(0);

    let mut tag_section = Vec::new();
    let mut tag_count = 0u8;
    tag_section.put_u8(tags::FORWARDED);
    tag_section.put_u16(0);
    tag_count += 1;
    if let Some(isolation) = tags.isolation_level {
        tag_section.put_u8(tags::ISOLATION_LEVEL);
        tag_section.put_u16(1);
        tag_section.put_u8(isolation.to_byte());
        tag_count += 1;
    }
    if let Some(session_timeout_ms) = tags.session_timeout_ms {
        tag_section.put_u8(tags::SESSION_TIMEOUT_MS);
        tag_section.put_u16(4);
        tag_section.put_u32(session_timeout_ms);
        tag_count += 1;
    }
    if let Some(max_wait_ms) = tags.max_wait_ms {
        tag_section.put_u8(tags::MAX_WAIT_MS);
        tag_section.put_u16(4);
        tag_section.put_u32(max_wait_ms);
        tag_count += 1;
    }
    if let Some(min_bytes) = tags.min_bytes {
        tag_section.put_u8(tags::MIN_BYTES);
        tag_section.put_u16(4);
        tag_section.put_u32(min_bytes);
        tag_count += 1;
    }
    if let Some((group_id, member_id)) = &tags.group_member {
        let mut payload = Vec::new();
        write_pascal_string(&mut payload, group_id);
        write_pascal_string(&mut payload, member_id);
        tag_section.put_u8(tags::GROUP_MEMBER);
        tag_section.put_u16(payload.len() as u16);
        tag_section.extend_from_slice(&payload);
        tag_count += 1;
    }
    out.put_u8(tag_count);
    out.extend_from_slice(&tag_section);
    out.extend_from_slice(inner);
    Ok(out)
}

/// Re-frames a response received from another broker for the client that asked for it.
///
/// The two hops can be framed differently: a broker always relays using the versioned
/// envelope (to carry the forwarded marker), so the leader answers framed — but the client
/// may have sent a bare legacy request and would read the envelope prefix as the response
/// status. This strips whatever framing the leader used and applies the client's.
pub fn relay_response(leader_response: &[u8], client_framing: &RequestFraming) -> Vec<u8> {
    let inner = if leader_response.first() == Some(&VERSIONED_ENVELOPE_MAGIC)
        && leader_response.len() >= 5
    {
        &leader_response[5..] // magic + echoed correlation id
    } else {
        leader_response
    };

    match client_framing {
        RequestFraming::Legacy => inner.to_vec(),
        RequestFraming::Versioned { correlation_id, .. } => {
            let mut out = Vec::with_capacity(inner.len() + 5);
            out.put_u8(VERSIONED_ENVELOPE_MAGIC);
            out.put_u32(*correlation_id);
            out.extend_from_slice(inner);
            out
        }
    }
}

/// Splits a request into its inner `[cmd][len][payload]` and the recognised tags of any
/// envelope wrapping it. A legacy request is returned unchanged with default tags.
pub fn strip_envelope(src: &[u8]) -> Result<(&[u8], RequestTags), WireError> {
    if src.first() != Some(&VERSIONED_ENVELOPE_MAGIC) {
        return Ok((src, RequestTags::default()));
    }
    let mut cursor = src;
    if cursor.len() < 8 {
        return Err(WireError::Incomplete {
            needed: 8,
            available: cursor.len(),
        });
    }
    cursor.get_u8(); // magic
    cursor.get_u16(); // api_version
    cursor.get_u32(); // correlation_id
    let tagged_count = cursor.get_u8() as usize;

    let mut tags_out = RequestTags::default();
    for _ in 0..tagged_count {
        if cursor.len() < 3 {
            return Err(WireError::Incomplete {
                needed: 3,
                available: cursor.len(),
            });
        }
        let tag = cursor.get_u8();
        let len = cursor.get_u16() as usize;
        if cursor.len() < len {
            return Err(WireError::Incomplete {
                needed: len,
                available: cursor.len(),
            });
        }
        match tag {
            tags::ISOLATION_LEVEL if len >= 1 => {
                tags_out.isolation_level = Some(IsolationLevel::from_byte(cursor[0]));
            }
            tags::FORWARDED => tags_out.forwarded = true,
            tags::SESSION_TIMEOUT_MS if len >= 4 => {
                tags_out.session_timeout_ms =
                    Some(u32::from_be_bytes(cursor[0..4].try_into().unwrap()));
            }
            tags::GROUP_MEMBER => {
                let mut value = &cursor[..len];
                if let Ok(group_id) = read_pascal_string(&mut value) {
                    if let Ok(member_id) = read_pascal_string(&mut value) {
                        tags_out.group_member = Some((group_id, member_id));
                    }
                }
            }
            _ => {}
        }
        cursor = &cursor[len..];
    }
    let consumed = src.len() - cursor.len();
    Ok((&src[consumed..], tags_out))
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("Unknown wire command code: 0x{0:02X}")]
    UnknownCommand(u8),
    #[error("Unsupported protocol version {requested}; this broker supports {min}..={max}")]
    UnsupportedVersion { requested: u16, min: u16, max: u16 },
    #[error("Insufficient wire buffer bytes: needed {needed}, available {available}")]
    Incomplete { needed: usize, available: usize },
    #[error("Protocol error: {0}")]
    InvalidProtocol(String),
}

/// Request payloads received from clients over TCP
#[derive(Debug, Clone)]
pub enum RequestPayload {
    ProduceBatch {
        topic: String,
        key: String,
        transaction_id: String,
        num_partitions: u32,
        /// One encoded [`crate::protocol::RecordBatch`], built and compressed by the
        /// producer. The broker never decodes the records inside it — the producer id,
        /// epoch and base sequence it needs for idempotence all live in the batch's
        /// plaintext header, which is why they are not repeated in this envelope.
        batch: Bytes,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    },
    CommitOffset {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    FetchOffset {
        group_id: String,
        topic: String,
        partition: u32,
    },
    Seek {
        topic: String,
        partition: u32,
        offset: u64,
    },
    LatestOffset {
        topic: String,
        partition: u32,
    },
    BeginTx {
        transaction_id: String,
        producer_id: u64,
    },
    CommitTx {
        transaction_id: String,
    },
    AbortTx {
        transaction_id: String,
    },
    FetchByTimestamp {
        topic: String,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    },
    /// P1: Same wire shape as Fetch, but triggers read-committed LSO filtering
    FetchCommitted {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    },
    Ping,
    /// Asks the broker which protocol versions and commands it supports.
    NegotiateProtocol,
    ListTopics,
    DescribeCluster,
    DeleteTopic {
        topic: String,
    },
    JoinGroup {
        group_id: String,
        member_id: String,
        protocols: Vec<String>,
        /// A consumer's stable identity across restarts (`group.instance.id`), if it
        /// declared one. Trails the protocol list, so a client that never sends it
        /// produces exactly the bytes it always did — see [`read_trailing_pascal_string`].
        group_instance_id: Option<String>,
    },
    SyncGroup {
        group_id: String,
        generation_id: u32,
        member_id: String,
        assignments: Vec<MemberAssignment>,
    },
    Heartbeat {
        group_id: String,
        generation_id: u32,
        member_id: String,
    },
    LeaveGroup {
        group_id: String,
        member_id: String,
        /// Identifies a static member by its `group.instance.id` instead of by the member
        /// id it happens to hold this generation. A restarting static consumer does not
        /// know its current member id; this is how it — or an operator — retires the
        /// instance for real rather than leaving a slot that only expires on timeout.
        group_instance_id: Option<String>,
    },
    CreateTopic {
        topic: String,
        partitions: u32,
    },
    DescribeTopic {
        topic: String,
    },
    ListGroups,
    DescribeGroup {
        group_id: String,
    },
    InitProducerId {
        transactional_id: String,
    },
    AddPartitionsToTxn {
        transactional_id: String,
        producer_id: u64,
        producer_epoch: i16,
        topics: Vec<(String, Vec<u32>)>,
    },
    EndTxn {
        transactional_id: String,
        producer_id: u64,
        producer_epoch: i16,
        committed: bool,
    },
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
        metadata: String,
    },
    OffsetFetch {
        group_id: String,
        topic: String,
        partition: u32,
    },
    SaslHandshake {
        mechanism: String,
    },
    SaslAuthenticate {
        auth_bytes: Vec<u8>,
    },
    DescribeAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    CreateAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    DeleteAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    RegisterBroker {
        node_id: u32,
        endpoint: String,
    },
    UnregisterBroker {
        node_id: u32,
    },
    SetClientId {
        client_id: String,
    },
    UpsertScramUser {
        username: String,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    DeleteScramUser {
        username: String,
    },
    ShareFetch {
        group_id: String,
        member_id: String,
        topic: String,
        partition: u32,
        max_records: u32,
        max_bytes: u32,
        lock_timeout_ms: u32,
        acknowledgements: Vec<AckBatch>,
    },
    ShareAcknowledge {
        group_id: String,
        member_id: String,
        topic: String,
        partition: u32,
        acknowledgements: Vec<AckBatch>,
    },
    ShareGroupHeartbeat {
        group_id: String,
        member_id: String,
    },
    ShareGroupDescribe {
        group_id: String,
    },
    DescribeConfigs {
        topic: String,
    },
    /// Full-replace semantics (Kafka `AlterConfigs`): the given configs entirely replace
    /// the topic's stored config map.
    AlterConfigs {
        topic: String,
        configs: Vec<(String, String)>,
    },
    /// Merge semantics (Kafka `IncrementalAlterConfigs`): `upserts` are set/overwritten,
    /// `deletes` are removed, everything else in the topic's current config map is left
    /// untouched.
    IncrementalAlterConfigs {
        topic: String,
        upserts: Vec<(String, String)>,
        deletes: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AcknowledgeType {
    Accept = 1,
    Release = 2,
    Reject = 3,
    Renew = 4,
}

impl TryFrom<u8> for AcknowledgeType {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(AcknowledgeType::Accept),
            2 => Ok(AcknowledgeType::Release),
            3 => Ok(AcknowledgeType::Reject),
            4 => Ok(AcknowledgeType::Renew),
            _ => Err(WireError::InvalidProtocol(format!(
                "Unknown acknowledge type: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckBatch {
    pub first_offset: u64,
    pub last_offset: u64,
    pub ack_type: AcknowledgeType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRecordBatch {
    pub first_offset: u64,
    pub last_offset: u64,
    pub delivery_count: u16,
    pub records: Vec<crate::protocol::RecordFrame>,
}

#[derive(Debug, Clone)]
pub struct MemberAssignment {
    pub member_id: String,
    pub topic: String,
    pub partitions: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct WireRequest {
    pub cmd: CommandCode,
    pub payload: RequestPayload,
}

impl WireRequest {
    /// Decode wire request from buffer: `[Cmd: 1b] | [Payload Len: 4b] | [Payload Bytes]`
    ///
    /// Accepts the versioned envelope too, discarding its metadata. Callers that need to
    /// echo a correlation id — i.e. anything writing a response — should use
    /// `decode_framed` instead.
    pub fn decode(src: &[u8]) -> Result<(Self, usize), WireError> {
        Self::decode_framed(src).map(|(req, _, used)| (req, used))
    }

    /// Decode a request, reporting how it was framed.
    ///
    /// Two framings are accepted from the same socket:
    ///
    /// - **Legacy**: `[cmd: 1b][payload_len: 4b][payload]` — what every existing client
    ///   sends. Left working exactly as-is.
    /// - **Versioned**: `[0xF1][api_version: 2b][correlation_id: 4b][tagged_count: 1b]
    ///   (repeated [tag: 1b][len: 2b][bytes])[cmd: 1b][payload_len: 4b][payload]`
    ///
    /// The magic byte is outside the command-code range, so the two are distinguishable
    /// from the first byte and no negotiation round trip is required before sending a
    /// request. The tagged-field section is the forward-compatibility hook: a newer client
    /// can add fields an older broker will skip rather than misparse.
    pub fn decode_framed(mut src: &[u8]) -> Result<(Self, RequestFraming, usize), WireError> {
        let original_len = src.len();
        if src.is_empty() {
            return Err(WireError::Incomplete {
                needed: 1,
                available: 0,
            });
        }

        let framing = if src[0] == VERSIONED_ENVELOPE_MAGIC {
            // [magic][api_version: 2][correlation_id: 4][tagged_count: 1]
            if src.len() < 8 {
                return Err(WireError::Incomplete {
                    needed: 8,
                    available: src.len(),
                });
            }
            src.get_u8(); // magic
            let api_version = src.get_u16();
            let correlation_id = src.get_u32();

            if !(PROTOCOL_VERSION_MIN..=PROTOCOL_VERSION_MAX).contains(&api_version) {
                // Refused explicitly rather than parsed on a guess. Silently attempting a
                // version we do not implement is how a mismatch turns into misread bytes
                // instead of a clear error.
                return Err(WireError::UnsupportedVersion {
                    requested: api_version,
                    min: PROTOCOL_VERSION_MIN,
                    max: PROTOCOL_VERSION_MAX,
                });
            }

            let tagged_count = src.get_u8() as usize;
            let mut tags = RequestTags::default();
            for _ in 0..tagged_count {
                if src.len() < 3 {
                    return Err(WireError::Incomplete {
                        needed: 3,
                        available: src.len(),
                    });
                }
                let tag = src.get_u8();
                let len = src.get_u16() as usize;
                if src.len() < len {
                    return Err(WireError::Incomplete {
                        needed: len,
                        available: src.len(),
                    });
                }
                let value = &src[..len];
                match tag {
                    tags::ISOLATION_LEVEL if len >= 1 => {
                        tags.isolation_level = Some(IsolationLevel::from_byte(value[0]));
                    }
                    tags::FORWARDED => tags.forwarded = true,
                    tags::SESSION_TIMEOUT_MS if len >= 4 => {
                        tags.session_timeout_ms =
                            Some(u32::from_be_bytes(value[0..4].try_into().unwrap()));
                    }
                    tags::MAX_WAIT_MS if len >= 4 => {
                        tags.max_wait_ms =
                            Some(u32::from_be_bytes(value[0..4].try_into().unwrap()));
                    }
                    tags::MIN_BYTES if len >= 4 => {
                        tags.min_bytes = Some(u32::from_be_bytes(value[0..4].try_into().unwrap()));
                    }
                    tags::GROUP_MEMBER => {
                        let mut v = value;
                        if let Ok(group_id) = read_pascal_string(&mut v) {
                            if let Ok(member_id) = read_pascal_string(&mut v) {
                                tags.group_member = Some((group_id, member_id));
                            }
                        }
                    }
                    // Unknown tags are skipped, not rejected — that is the whole point of
                    // the section, and why a future field can be added without breaking
                    // this build. A known tag with an unusable length is skipped for the
                    // same reason rather than failing the whole request.
                    _ => {}
                }
                src = &src[len..];
            }

            RequestFraming::Versioned {
                api_version,
                correlation_id,
                tags,
            }
        } else {
            RequestFraming::Legacy
        };

        if src.len() < 5 {
            return Err(WireError::Incomplete {
                needed: 5,
                available: src.len(),
            });
        }

        let raw_cmd = src.get_u8();
        let cmd = CommandCode::try_from(raw_cmd)?;
        let payload_len = src.get_u32() as usize;

        if payload_len > MAX_REQUEST_PAYLOAD_BYTES {
            return Err(WireError::InvalidProtocol(format!(
                "Payload length {} exceeds maximum allowed limit of 64MB",
                payload_len
            )));
        }

        if src.len() < payload_len {
            return Err(WireError::Incomplete {
                needed: payload_len,
                available: src.len(),
            });
        }

        let mut payload_buf = &src[..payload_len];
        let req_payload = match cmd {
            CommandCode::ProduceBatch => {
                let topic = read_pascal_string(&mut payload_buf)?;
                let key = read_pascal_string(&mut payload_buf)?;
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                // num_partitions (4) + batch_len (4)
                if payload_buf.len() < 8 {
                    return Err(WireError::Incomplete {
                        needed: 8,
                        available: payload_buf.len(),
                    });
                }
                let num_partitions = payload_buf.get_u32();
                let batch_len = payload_buf.get_u32() as usize;
                if payload_buf.len() < batch_len {
                    return Err(WireError::Incomplete {
                        needed: batch_len,
                        available: payload_buf.len(),
                    });
                }
                let batch = Bytes::copy_from_slice(&payload_buf[..batch_len]);

                RequestPayload::ProduceBatch {
                    topic,
                    key,
                    transaction_id,
                    num_partitions,
                    batch,
                }
            }
            CommandCode::Fetch => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::Fetch {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                }
            }
            CommandCode::CommitOffset => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 12 {
                    return Err(WireError::Incomplete {
                        needed: 12,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                RequestPayload::CommitOffset {
                    group_id,
                    topic,
                    partition,
                    offset,
                }
            }
            CommandCode::FetchOffset => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::FetchOffset {
                    group_id,
                    topic,
                    partition,
                }
            }
            CommandCode::Seek => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 12 {
                    return Err(WireError::Incomplete {
                        needed: 12,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                RequestPayload::Seek {
                    topic,
                    partition,
                    offset,
                }
            }
            CommandCode::LatestOffset => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::LatestOffset { topic, partition }
            }
            CommandCode::BeginTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 8 {
                    return Err(WireError::Incomplete {
                        needed: 8,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                RequestPayload::BeginTx {
                    transaction_id,
                    producer_id,
                }
            }
            CommandCode::CommitTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::CommitTx { transaction_id }
            }
            CommandCode::AbortTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::AbortTx { transaction_id }
            }
            CommandCode::FetchByTimestamp => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let target_timestamp = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::FetchByTimestamp {
                    topic,
                    partition,
                    target_timestamp,
                    max_bytes,
                }
            }
            CommandCode::FetchCommitted => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::FetchCommitted {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                }
            }
            CommandCode::Ping => RequestPayload::Ping,
            CommandCode::NegotiateProtocol => RequestPayload::NegotiateProtocol,
            CommandCode::ListTopics => RequestPayload::ListTopics,
            CommandCode::DescribeCluster => RequestPayload::DescribeCluster,
            CommandCode::DeleteTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DeleteTopic { topic }
            }
            CommandCode::JoinGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let proto_count = payload_buf.get_u32() as usize;
                let mut protocols = Vec::with_capacity(proto_count);
                for _ in 0..proto_count {
                    protocols.push(read_pascal_string(&mut payload_buf)?);
                }
                let group_instance_id = read_trailing_pascal_string(&mut payload_buf)?;
                RequestPayload::JoinGroup {
                    group_id,
                    member_id,
                    protocols,
                    group_instance_id,
                }
            }
            CommandCode::SyncGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let generation_id = payload_buf.get_u32();
                let member_id = read_pascal_string(&mut payload_buf)?;

                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let assign_count = payload_buf.get_u32() as usize;
                let mut assignments = Vec::with_capacity(assign_count);
                for _ in 0..assign_count {
                    let a_member_id = read_pascal_string(&mut payload_buf)?;
                    let a_topic = read_pascal_string(&mut payload_buf)?;
                    if payload_buf.len() < 4 {
                        return Err(WireError::Incomplete {
                            needed: 4,
                            available: payload_buf.len(),
                        });
                    }
                    let p_count = payload_buf.get_u32() as usize;
                    if payload_buf.len() < p_count * 4 {
                        return Err(WireError::Incomplete {
                            needed: p_count * 4,
                            available: payload_buf.len(),
                        });
                    }
                    let mut partitions = Vec::with_capacity(p_count);
                    for _ in 0..p_count {
                        partitions.push(payload_buf.get_u32());
                    }
                    assignments.push(MemberAssignment {
                        member_id: a_member_id,
                        topic: a_topic,
                        partitions,
                    });
                }
                RequestPayload::SyncGroup {
                    group_id,
                    generation_id,
                    member_id,
                    assignments,
                }
            }
            CommandCode::Heartbeat => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let generation_id = payload_buf.get_u32();
                let member_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::Heartbeat {
                    group_id,
                    generation_id,
                    member_id,
                }
            }
            CommandCode::LeaveGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                let group_instance_id = read_trailing_pascal_string(&mut payload_buf)?;
                RequestPayload::LeaveGroup {
                    group_id,
                    member_id,
                    group_instance_id,
                }
            }
            CommandCode::CreateTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partitions = payload_buf.get_u32();
                RequestPayload::CreateTopic { topic, partitions }
            }
            CommandCode::DescribeTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DescribeTopic { topic }
            }
            CommandCode::ListGroups => RequestPayload::ListGroups,
            CommandCode::DescribeGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DescribeGroup { group_id }
            }
            CommandCode::InitProducerId => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::InitProducerId { transactional_id }
            }
            CommandCode::AddPartitionsToTxn => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 10 {
                    return Err(WireError::Incomplete {
                        needed: 10,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                let producer_epoch = payload_buf.get_i16();

                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let topic_count = payload_buf.get_u32() as usize;
                let mut topics = Vec::with_capacity(topic_count);

                for _ in 0..topic_count {
                    let t_name = read_pascal_string(&mut payload_buf)?;
                    if payload_buf.len() < 4 {
                        return Err(WireError::Incomplete {
                            needed: 4,
                            available: payload_buf.len(),
                        });
                    }
                    let p_count = payload_buf.get_u32() as usize;
                    if payload_buf.len() < p_count * 4 {
                        return Err(WireError::Incomplete {
                            needed: p_count * 4,
                            available: payload_buf.len(),
                        });
                    }
                    let mut parts = Vec::with_capacity(p_count);
                    for _ in 0..p_count {
                        parts.push(payload_buf.get_u32());
                    }
                    topics.push((t_name, parts));
                }
                RequestPayload::AddPartitionsToTxn {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    topics,
                }
            }
            CommandCode::EndTxn => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 11 {
                    return Err(WireError::Incomplete {
                        needed: 11,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                let producer_epoch = payload_buf.get_i16();
                let committed = payload_buf.get_u8() != 0;
                RequestPayload::EndTxn {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    committed,
                }
            }
            CommandCode::OffsetCommit => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 12 {
                    return Err(WireError::Incomplete {
                        needed: 12,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                let metadata = if !payload_buf.is_empty() {
                    read_pascal_string(&mut payload_buf)?
                } else {
                    String::new()
                };
                RequestPayload::OffsetCommit {
                    group_id,
                    topic,
                    partition,
                    offset,
                    metadata,
                }
            }
            CommandCode::OffsetFetch => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::OffsetFetch {
                    group_id,
                    topic,
                    partition,
                }
            }
            CommandCode::SaslHandshake => {
                let mechanism = read_pascal_string(&mut payload_buf)?;
                RequestPayload::SaslHandshake { mechanism }
            }
            CommandCode::SaslAuthenticate => {
                let auth_bytes = payload_buf.to_vec();
                RequestPayload::SaslAuthenticate { auth_bytes }
            }
            CommandCode::DescribeAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::DescribeAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::CreateAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::CreateAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::DeleteAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::DeleteAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::RegisterBroker => {
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let node_id = payload_buf.get_u32();
                let endpoint = read_pascal_string(&mut payload_buf)?;
                RequestPayload::RegisterBroker { node_id, endpoint }
            }
            CommandCode::UnregisterBroker => {
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let node_id = payload_buf.get_u32();
                RequestPayload::UnregisterBroker { node_id }
            }
            CommandCode::SetClientId => {
                let client_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::SetClientId { client_id }
            }
            CommandCode::UpsertScramUser => {
                let username = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let iterations = payload_buf.get_u32();
                let salt = Self::read_len_prefixed_bytes(&mut payload_buf)?;
                let stored_key = Self::read_len_prefixed_bytes(&mut payload_buf)?;
                let server_key = Self::read_len_prefixed_bytes(&mut payload_buf)?;
                RequestPayload::UpsertScramUser {
                    username,
                    iterations,
                    salt,
                    stored_key,
                    server_key,
                }
            }
            CommandCode::DeleteScramUser => {
                let username = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DeleteScramUser { username }
            }
            CommandCode::ShareFetch => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let max_records = payload_buf.get_u32();
                let max_bytes = payload_buf.get_u32();
                let lock_timeout_ms = payload_buf.get_u32();

                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let ack_count = payload_buf.get_u32() as usize;
                if payload_buf.len() < ack_count * 17 {
                    return Err(WireError::Incomplete {
                        needed: ack_count * 17,
                        available: payload_buf.len(),
                    });
                }
                let mut acknowledgements = Vec::with_capacity(ack_count);
                for _ in 0..ack_count {
                    let first_offset = payload_buf.get_u64();
                    let last_offset = payload_buf.get_u64();
                    let ack_type_raw = payload_buf.get_u8();
                    let ack_type = AcknowledgeType::try_from(ack_type_raw)?;
                    acknowledgements.push(AckBatch {
                        first_offset,
                        last_offset,
                        ack_type,
                    });
                }

                RequestPayload::ShareFetch {
                    group_id,
                    member_id,
                    topic,
                    partition,
                    max_records,
                    max_bytes,
                    lock_timeout_ms,
                    acknowledgements,
                }
            }
            CommandCode::ShareAcknowledge => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 8 {
                    return Err(WireError::Incomplete {
                        needed: 8,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let ack_count = payload_buf.get_u32() as usize;
                if payload_buf.len() < ack_count * 17 {
                    return Err(WireError::Incomplete {
                        needed: ack_count * 17,
                        available: payload_buf.len(),
                    });
                }
                let mut acknowledgements = Vec::with_capacity(ack_count);
                for _ in 0..ack_count {
                    let first_offset = payload_buf.get_u64();
                    let last_offset = payload_buf.get_u64();
                    let ack_type_raw = payload_buf.get_u8();
                    let ack_type = AcknowledgeType::try_from(ack_type_raw)?;
                    acknowledgements.push(AckBatch {
                        first_offset,
                        last_offset,
                        ack_type,
                    });
                }

                RequestPayload::ShareAcknowledge {
                    group_id,
                    member_id,
                    topic,
                    partition,
                    acknowledgements,
                }
            }
            CommandCode::ShareGroupHeartbeat => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::ShareGroupHeartbeat {
                    group_id,
                    member_id,
                }
            }
            CommandCode::ShareGroupDescribe => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::ShareGroupDescribe { group_id }
            }
            CommandCode::DescribeConfigs => {
                let topic = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DescribeConfigs { topic }
            }
            CommandCode::AlterConfigs => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let count = payload_buf.get_u32() as usize;
                let mut configs = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = read_pascal_string(&mut payload_buf)?;
                    let value = read_pascal_string(&mut payload_buf)?;
                    configs.push((key, value));
                }
                RequestPayload::AlterConfigs { topic, configs }
            }
            CommandCode::IncrementalAlterConfigs => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let upsert_count = payload_buf.get_u32() as usize;
                let mut upserts = Vec::with_capacity(upsert_count);
                for _ in 0..upsert_count {
                    let key = read_pascal_string(&mut payload_buf)?;
                    let value = read_pascal_string(&mut payload_buf)?;
                    upserts.push((key, value));
                }
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let delete_count = payload_buf.get_u32() as usize;
                let mut deletes = Vec::with_capacity(delete_count);
                for _ in 0..delete_count {
                    deletes.push(read_pascal_string(&mut payload_buf)?);
                }
                RequestPayload::IncrementalAlterConfigs {
                    topic,
                    upserts,
                    deletes,
                }
            }
        };

        // Measured against the original buffer rather than assumed to be `5 + payload_len`,
        // so the envelope's own bytes (magic, version, correlation id, tagged fields) are
        // included and the connection loop advances past the whole request.
        let total_consumed = original_len - (src.len() - payload_len);
        Ok((
            WireRequest {
                cmd,
                payload: req_payload,
            },
            framing,
            total_consumed,
        ))
    }

    fn read_len_prefixed_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, WireError> {
        if buf.len() < 2 {
            return Err(WireError::Incomplete {
                needed: 2,
                available: buf.len(),
            });
        }
        let len = buf.get_u16() as usize;
        if buf.len() < len {
            return Err(WireError::Incomplete {
                needed: len,
                available: buf.len(),
            });
        }
        let bytes = buf[..len].to_vec();
        *buf = &buf[len..];
        Ok(bytes)
    }
}

/// Reads an optional pascal string appended after a request's historical fields.
///
/// This is how a request payload gains a field without a flag day: the frame already
/// delimits the payload exactly, so an older client simply leaves nothing here and gets
/// `None`, while a newer one appends the field. An empty string reads as `None` too, so a
/// client may unconditionally write the field and let "unset" be the empty value rather
/// than having to vary its encoding.
///
/// Only valid for a field that trails everything else — anything written after it could no
/// longer be told apart from its absence.
fn read_trailing_pascal_string(buf: &mut &[u8]) -> Result<Option<String>, WireError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let value = read_pascal_string(buf)?;
    Ok(if value.is_empty() { None } else { Some(value) })
}

/// Helper function to read pascal-style strings: `[Len: 2b] | [UTF-8 bytes]`
fn read_pascal_string(buf: &mut &[u8]) -> Result<String, WireError> {
    if buf.len() < 2 {
        return Err(WireError::Incomplete {
            needed: 2,
            available: buf.len(),
        });
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(WireError::Incomplete {
            needed: len,
            available: buf.len(),
        });
    }
    let str_bytes = &buf[..len];
    *buf = &buf[len..];
    String::from_utf8(str_bytes.to_vec())
        .map_err(|e| WireError::InvalidProtocol(format!("Invalid UTF-8 string: {}", e)))
}

/// Helper function to write pascal-style strings: `[Len: 2b] | [UTF-8 bytes]`
pub fn write_pascal_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    // H9: Guard against silent u16 wrap-around.  Strings in this protocol (topic
    // names, cluster IDs, addresses) are always short; exceeding 65535 bytes is a
    // programming error, not a runtime condition.
    assert!(
        bytes.len() <= u16::MAX as usize,
        "write_pascal_string: string too long ({} bytes, max {})",
        bytes.len(),
        u16::MAX
    );
    buf.put_u16(bytes.len() as u16);
    buf.put_slice(bytes);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub assigned_partitions: Vec<(String, u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedPartition {
    pub partition_id: u32,
    pub high_watermark: u64,
    pub leader_id: u32,
    pub replicas: Vec<u32>,
}

/// Encodes DescribeTopic binary payload: `[Topic: pascal] | [NumPartitions: 4b] | { [PartitionID: 4b] | [HighWatermark: 8b] | [LeaderID: 4b] | [ReplicasLen: 4b] | [Replicas...] }...`
pub fn encode_describe_topic_response(topic: &str, partitions: &[DescribedPartition]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_pascal_string(&mut buf, topic);
    buf.put_u32(partitions.len() as u32);
    for p in partitions {
        buf.put_u32(p.partition_id);
        buf.put_u64(p.high_watermark);
        buf.put_u32(p.leader_id);
        buf.put_u32(p.replicas.len() as u32);
        for &r in &p.replicas {
            buf.put_u32(r);
        }
    }
    buf
}

/// Encodes DescribeGroup binary payload: `[GroupState: pascal] | [MemberCount: 4b] | { [MemberID: pascal] | [NumAssignments: 4b] | { [Topic: pascal] | [Partition: 4b] }... }...`
pub fn encode_describe_group_response(state: &str, members: &[DescribedGroupMember]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_pascal_string(&mut buf, state);
    buf.put_u32(members.len() as u32);
    for member in members {
        write_pascal_string(&mut buf, &member.member_id);
        buf.put_u32(member.assigned_partitions.len() as u32);
        for (topic, partition) in &member.assigned_partitions {
            write_pascal_string(&mut buf, topic);
            buf.put_u32(*partition);
        }
    }
    buf
}

/// Encodes OffsetFetch binary payload: `[Offset: 8b] | [Metadata: pascal]`
pub fn encode_offset_fetch_response(offset: u64, metadata: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10 + metadata.len());
    buf.put_u64(offset);
    write_pascal_string(&mut buf, metadata);
    buf
}

/// Encodes DescribeConfigs binary payload: `[Count: 4b] { [Key: pascal] [Value: pascal] }...`
pub fn encode_describe_configs_response(configs: &[(String, String)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(configs.len() as u32);
    for (key, value) in configs {
        write_pascal_string(&mut buf, key);
        write_pascal_string(&mut buf, value);
    }
    buf
}

/// Encodes ShareFetch binary payload: `[NumBatches: 4b] | { [FirstOffset: 8b] | [LastOffset: 8b] | [DeliveryCount: 2b] | [RecordsLen: 4b] | [RecordFrames...] }...`
pub fn encode_share_fetch_response(batches: &[AcquiredRecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(batches.len() as u32);
    for batch in batches {
        buf.put_u64(batch.first_offset);
        buf.put_u64(batch.last_offset);
        buf.put_u16(batch.delivery_count);
        buf.put_u32(batch.records.len() as u32);
        for frame in &batch.records {
            frame.encode_into(&mut buf);
        }
    }
    buf
}

/// Encodes ShareAcknowledge binary payload: `[ErrorCode: 2b] | [ErrorMessage: pascal]`
pub fn encode_share_acknowledge_response(error_code: i16, error_msg: Option<&str>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_i16(error_code);
    write_pascal_string(&mut buf, error_msg.unwrap_or(""));
    buf
}

/// Encodes ShareGroupDescribe binary payload: `[State: pascal] | [MemberCount: 4b] | { [MemberID: pascal] }... | [InFlightCount: 8b] | [StartOffset: 8b]`
pub fn encode_share_group_describe_response(
    state: &str,
    active_members: &[String],
    inflight_count: usize,
    start_offset: u64,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_pascal_string(&mut buf, state);
    buf.put_u32(active_members.len() as u32);
    for member in active_members {
        write_pascal_string(&mut buf, member);
    }
    buf.put_u64(inflight_count as u64);
    buf.put_u64(start_offset);
    buf
}

/// Binary response returned to clients over TCP: `[Status Code: 1b] | [Payload Len: 4b] | [Payload]`
#[derive(Debug)]
pub struct WireResponse {
    pub status: u8, // 0 = OK, 1 = Error
    pub payload: Vec<u8>,
}

impl WireResponse {
    pub fn ok(payload: Vec<u8>) -> Self {
        Self { status: 0, payload }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            status: 1,
            payload: msg.as_bytes().to_vec(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + self.payload.len());
        buf.put_u8(self.status);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        buf
    }

    /// Same encoding as `encode()`, but writes into a caller-supplied buffer instead of
    /// allocating a fresh `Vec` — lets a connection loop reuse one scratch buffer across
    /// every request/response round trip instead of allocating one per response.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.status);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);
    }

    /// Encodes the response in the framing its request arrived in.
    ///
    /// A legacy request gets a bare response, byte-for-byte as before. A versioned request
    /// gets `[0xF1][correlation_id: 4b]` first, so the client can match this reply to the
    /// request that produced it — which is what allows more than one request in flight per
    /// connection instead of forcing a strict request/response lockstep.
    /// Allocating form of `encode_framed_into`, for the paths that build a response
    /// buffer rather than writing into the connection's scratch buffer.
    pub fn encode_framed(&self, framing: &RequestFraming) -> Vec<u8> {
        let mut buf = Vec::with_capacity(10 + self.payload.len());
        self.encode_framed_into(framing, &mut buf);
        buf
    }

    pub fn encode_framed_into(&self, framing: &RequestFraming, buf: &mut impl BufMut) {
        if let RequestFraming::Versioned { correlation_id, .. } = framing {
            buf.put_u8(VERSIONED_ENVELOPE_MAGIC);
            buf.put_u32(*correlation_id);
        }
        self.encode_into(buf);
    }
}

#[cfg(test)]
mod envelope_tests {
    use super::*;

    fn legacy_ping() -> Vec<u8> {
        let mut b = Vec::new();
        b.put_u8(CommandCode::Ping as u8);
        b.put_u32(0);
        b
    }

    /// A client that knows nothing about the envelope must keep working byte-for-byte.
    /// The whole point of introducing the framing behind a magic byte is that it needs no
    /// flag day.
    #[test]
    fn legacy_framing_still_decodes_untouched() {
        let bytes = legacy_ping();
        let (req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(req.cmd, CommandCode::Ping);
        assert_eq!(framing, RequestFraming::Legacy);
        assert_eq!(used, bytes.len(), "must consume exactly the request");
        assert_eq!(framing.correlation_id(), None);
    }

    /// A legacy response must stay unwrapped, so an old client sees exactly what it did
    /// before.
    #[test]
    fn legacy_response_is_not_wrapped() {
        let resp = WireResponse::ok(vec![1, 2, 3]);
        assert_eq!(
            resp.encode_framed(&RequestFraming::Legacy),
            resp.encode(),
            "legacy responses must be byte-identical to the pre-envelope encoding"
        );
    }

    /// The envelope carries a version and a correlation id, and the request inside it
    /// decodes the same as if it had been sent bare.
    #[test]
    fn versioned_envelope_round_trips_and_reports_correlation() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(0xDEAD_BEEF);
        bytes.put_u8(0); // no tagged fields
        bytes.extend_from_slice(&legacy_ping());

        let (req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(req.cmd, CommandCode::Ping);
        assert_eq!(
            framing,
            RequestFraming::Versioned {
                api_version: PROTOCOL_VERSION_MAX,
                correlation_id: 0xDEAD_BEEF,
                tags: RequestTags::default(),
            }
        );
        assert_eq!(
            used,
            bytes.len(),
            "the envelope's own bytes must be counted as consumed"
        );

        // The response echoes the correlation id so a client can match it to this request.
        let encoded = WireResponse::ok(vec![]).encode_framed(&framing);
        assert_eq!(encoded[0], VERSIONED_ENVELOPE_MAGIC);
        assert_eq!(
            u32::from_be_bytes(encoded[1..5].try_into().unwrap()),
            0xDEAD_BEEF
        );
    }

    /// Unknown tagged fields are skipped rather than rejected. This is the forward-
    /// compatibility hook: a newer client can add a field and an older broker still
    /// understands the request instead of misreading it.
    #[test]
    fn unknown_tagged_fields_are_skipped_not_rejected() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(7);
        bytes.put_u8(2); // two tagged fields this build has never heard of
        bytes.put_u8(0x40);
        bytes.put_u16(3);
        bytes.extend_from_slice(&[9, 9, 9]);
        bytes.put_u8(0x41);
        bytes.put_u16(1);
        bytes.push(1);
        bytes.extend_from_slice(&legacy_ping());

        let (req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(req.cmd, CommandCode::Ping);
        assert_eq!(framing.correlation_id(), Some(7));
        assert_eq!(used, bytes.len());
    }

    /// A version this broker does not implement is refused explicitly. Guessing at it is
    /// how a mismatch becomes silently misread bytes instead of a clear error.
    #[test]
    fn unsupported_version_is_rejected_explicitly() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX + 1);
        bytes.put_u32(1);
        bytes.put_u8(0);
        bytes.extend_from_slice(&legacy_ping());

        match WireRequest::decode_framed(&bytes) {
            Err(WireError::UnsupportedVersion {
                requested,
                min,
                max,
            }) => {
                assert_eq!(requested, PROTOCOL_VERSION_MAX + 1);
                assert_eq!(min, PROTOCOL_VERSION_MIN);
                assert_eq!(max, PROTOCOL_VERSION_MAX);
            }
            other => panic!(
                "expected UnsupportedVersion, got {:?}",
                other.map(|(r, _, _)| r.cmd)
            ),
        }
    }

    /// A truncated envelope must report "need more data" so the connection loop waits for
    /// the rest, rather than being treated as a fatal protocol error that drops the
    /// connection.
    #[test]
    fn truncated_envelope_asks_for_more_data() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        assert!(matches!(
            WireRequest::decode_framed(&bytes),
            Err(WireError::Incomplete { .. })
        ));
    }

    /// A recognised tag is surfaced to handlers, while unknown tags around it are still
    /// skipped — both behaviors have to hold at once for the section to be useful.
    #[test]
    fn isolation_tag_is_parsed_alongside_unknown_tags() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(11);
        bytes.put_u8(3);
        bytes.put_u8(0x7E); // unknown, before
        bytes.put_u16(2);
        bytes.extend_from_slice(&[1, 2]);
        bytes.put_u8(tags::ISOLATION_LEVEL);
        bytes.put_u16(1);
        bytes.put_u8(IsolationLevel::ReadCommitted.to_byte());
        bytes.put_u8(0x7F); // unknown, after
        bytes.put_u16(1);
        bytes.put_u8(9);
        bytes.extend_from_slice(&legacy_ping());

        let (_req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(framing.isolation_level(), IsolationLevel::ReadCommitted);
        assert_eq!(used, bytes.len());
    }

    /// No tag means read-uncommitted — the behavior `Fetch` has always had, so legacy
    /// clients are unaffected by isolation existing.
    #[test]
    fn absent_isolation_tag_defaults_to_read_uncommitted() {
        let (_req, legacy, _) = WireRequest::decode_framed(&legacy_ping()).unwrap();
        assert_eq!(legacy.isolation_level(), IsolationLevel::ReadUncommitted);
        assert_eq!(legacy.tags().isolation_level, None);

        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(1);
        bytes.put_u8(0);
        bytes.extend_from_slice(&legacy_ping());
        let (_req, versioned, _) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(versioned.isolation_level(), IsolationLevel::ReadUncommitted);
    }

    /// A recognised `SESSION_TIMEOUT_MS` tag is surfaced alongside unknown tags around it —
    /// same shape as `isolation_tag_is_parsed_alongside_unknown_tags`.
    #[test]
    fn session_timeout_tag_is_parsed_alongside_unknown_tags() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(11);
        bytes.put_u8(3);
        bytes.put_u8(0x7E); // unknown, before
        bytes.put_u16(2);
        bytes.extend_from_slice(&[1, 2]);
        bytes.put_u8(tags::SESSION_TIMEOUT_MS);
        bytes.put_u16(4);
        bytes.put_u32(45_000);
        bytes.put_u8(0x7F); // unknown, after
        bytes.put_u16(1);
        bytes.put_u8(9);
        bytes.extend_from_slice(&legacy_ping());

        let (_req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(framing.session_timeout_ms(), Some(45_000));
        assert_eq!(used, bytes.len());
    }

    /// No tag means the wire layer reports nothing — it is the coordinator's job to turn
    /// an absent request into its own default, not the wire layer's (see
    /// `RequestFraming::session_timeout_ms`).
    #[test]
    fn absent_session_timeout_tag_reads_as_none() {
        let (_req, legacy, _) = WireRequest::decode_framed(&legacy_ping()).unwrap();
        assert_eq!(legacy.session_timeout_ms(), None);

        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(1);
        bytes.put_u8(0);
        bytes.extend_from_slice(&legacy_ping());
        let (_req, versioned, _) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(versioned.session_timeout_ms(), None);
    }

    /// `wrap_forwarded_request` must carry the `SESSION_TIMEOUT_MS` tag across too, not
    /// just `ISOLATION_LEVEL` — otherwise a forwarded join would silently lose the session
    /// timeout it asked for.
    #[test]
    fn wrap_forwarded_request_carries_session_timeout_tag() {
        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(1);
        bytes.put_u8(1);
        bytes.put_u8(tags::SESSION_TIMEOUT_MS);
        bytes.put_u16(4);
        bytes.put_u32(6_000);
        bytes.extend_from_slice(&legacy_ping());

        let wrapped = wrap_forwarded_request(&bytes).unwrap();
        let (_req, framing, _used) = WireRequest::decode_framed(&wrapped).unwrap();
        assert!(framing.is_forwarded());
        assert_eq!(framing.session_timeout_ms(), Some(6_000));
    }

    /// The `GROUP_MEMBER` tag's payload is two pascal strings back to back, and both must
    /// round-trip through decode.
    #[test]
    fn group_member_tag_is_parsed() {
        let mut member_payload = Vec::new();
        write_pascal_string(&mut member_payload, "my-group");
        write_pascal_string(&mut member_payload, "member-7");

        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(1);
        bytes.put_u8(1);
        bytes.put_u8(tags::GROUP_MEMBER);
        bytes.put_u16(member_payload.len() as u16);
        bytes.extend_from_slice(&member_payload);
        bytes.extend_from_slice(&legacy_ping());

        let (_req, framing, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(
            framing.group_member(),
            Some(("my-group".to_string(), "member-7".to_string()))
        );
        assert_eq!(used, bytes.len());
    }

    #[test]
    fn absent_group_member_tag_reads_as_none() {
        let (_req, legacy, _) = WireRequest::decode_framed(&legacy_ping()).unwrap();
        assert_eq!(legacy.group_member(), None);
    }

    /// `wrap_forwarded_request` must carry the `GROUP_MEMBER` tag across too — otherwise a
    /// forwarded consuming-path request would silently lose the member it was attributed
    /// to.
    #[test]
    fn wrap_forwarded_request_carries_group_member_tag() {
        let mut member_payload = Vec::new();
        write_pascal_string(&mut member_payload, "g");
        write_pascal_string(&mut member_payload, "m");

        let mut bytes = Vec::new();
        bytes.put_u8(VERSIONED_ENVELOPE_MAGIC);
        bytes.put_u16(PROTOCOL_VERSION_MAX);
        bytes.put_u32(1);
        bytes.put_u8(1);
        bytes.put_u8(tags::GROUP_MEMBER);
        bytes.put_u16(member_payload.len() as u16);
        bytes.extend_from_slice(&member_payload);
        bytes.extend_from_slice(&legacy_ping());

        let wrapped = wrap_forwarded_request(&bytes).unwrap();
        let (_req, framing, _used) = WireRequest::decode_framed(&wrapped).unwrap();
        assert!(framing.is_forwarded());
        assert_eq!(
            framing.group_member(),
            Some(("g".to_string(), "m".to_string()))
        );
    }

    /// The magic byte must not collide with any command code, or a legacy request would be
    /// mistaken for an envelope.
    #[test]
    fn envelope_magic_is_not_a_command_code() {
        assert!(CommandCode::try_from(VERSIONED_ENVELOPE_MAGIC).is_err());
    }

    fn join_group_request(protocols: &[&str], trailing: Option<&str>) -> Vec<u8> {
        let mut inner = Vec::new();
        write_pascal_string(&mut inner, "g");
        write_pascal_string(&mut inner, "m");
        inner.put_u32(protocols.len() as u32);
        for p in protocols {
            write_pascal_string(&mut inner, p);
        }
        if let Some(trailing) = trailing {
            write_pascal_string(&mut inner, trailing);
        }
        let mut bytes = Vec::new();
        bytes.put_u8(CommandCode::JoinGroup as u8);
        bytes.put_u32(inner.len() as u32);
        bytes.extend_from_slice(&inner);
        bytes
    }

    /// The field a static member needs is appended after the request's existing ones, so a
    /// client that predates it sends the identical bytes and must still decode — as a
    /// dynamic member, which is what it is.
    #[test]
    fn join_group_without_instance_id_is_dynamic() {
        let bytes = join_group_request(&["range"], None);
        let (req, _, used) = WireRequest::decode_framed(&bytes).unwrap();
        assert_eq!(used, bytes.len(), "must consume exactly the request");
        match req.payload {
            RequestPayload::JoinGroup {
                group_instance_id,
                protocols,
                ..
            } => {
                assert_eq!(protocols, vec!["range".to_string()]);
                assert_eq!(group_instance_id, None);
            }
            other => panic!("expected JoinGroup, got {:?}", other),
        }
    }

    #[test]
    fn join_group_carries_instance_id_when_present() {
        let bytes = join_group_request(&["range", "roundrobin"], Some("worker-3"));
        let (req, _, _) = WireRequest::decode_framed(&bytes).unwrap();
        match req.payload {
            RequestPayload::JoinGroup {
                group_instance_id,
                protocols,
                ..
            } => {
                assert_eq!(
                    protocols,
                    vec!["range".to_string(), "roundrobin".to_string()],
                    "the instance id must not be mistaken for another protocol"
                );
                assert_eq!(group_instance_id, Some("worker-3".to_string()));
            }
            other => panic!("expected JoinGroup, got {:?}", other),
        }
    }

    /// A client may write the field unconditionally and leave it empty when unset, rather
    /// than having to send structurally different bytes for the two cases.
    #[test]
    fn empty_instance_id_reads_as_unset() {
        let bytes = join_group_request(&["range"], Some(""));
        let (req, _, _) = WireRequest::decode_framed(&bytes).unwrap();
        match req.payload {
            RequestPayload::JoinGroup {
                group_instance_id, ..
            } => assert_eq!(group_instance_id, None),
            other => panic!("expected JoinGroup, got {:?}", other),
        }
    }
}
