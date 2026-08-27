//! The versioned, length-delimited envelope every inter-node frame travels in (issue #48).
//!
//! # What was wrong
//!
//! Inter-node framing was five hand-rolled formats — `0xAA` replication push, `0xAC`
//! heartbeat, `0xAE`/`0xAF` vote request/response, `0xBB` pull fetch — each parsed by
//! reading fields in order until the decoder ran out of things it knew about. None of them
//! carried a version, and only the pull fetch carried a length. Two consequences followed,
//! and both had already bitten:
//!
//! 1. **Every layout change was a lockstep upgrade.** A field could not be added without
//!    every broker in the cluster agreeing on the new layout simultaneously. Two such
//!    changes had already accumulated on `0xAA` alone — the leader's high watermark, and
//!    the payload becoming record batches rather than frames — each landing with a comment
//!    admitting there was no negotiation on the path.
//!
//! 2. **A partial read poisoned the *next* request.** Peer connections are pooled
//!    (`get_or_connect_peer`), and responses were bare field sequences read with
//!    successive `read_exact` calls. A reader that stopped before the last field left those
//!    bytes in the socket, where the next response's reader consumed them as its own status
//!    byte. The failure is silent and permanent: a leftover byte reads as a nonzero status,
//!    i.e. a rejection, so a healthy peer looks permanently offline.
//!
//! # What this is
//!
//! ```text
//! [magic: 0xB0] [version: 2b] [frame_type: 1b] [payload_len: 4b] [payload: payload_len]
//! ```
//!
//! The **length** is what makes future changes safe, more than the version number is. A
//! version tells a receiver how to read a frame it understands; a length tells it how far
//! to skip past one it does not. With both, a decoder can always consume exactly one frame
//! and stay in sync — which is precisely what the old formats could not do, and the reason
//! the previous attempt at this (the parked `inter-node-versioning` branch) concluded that
//! adopting a version was a one-time flag day rather than a rolling upgrade.
//!
//! Bifrox has no deployed clusters, so that flag day costs nothing today and is worth
//! spending exactly once, here, to make it the last one.
//!
//! # Adding a field later
//!
//! Append it to the payload's [extension section](write_extensions) as a new `(tag, len,
//! bytes)` entry — never by widening a frame's fixed fields. A receiver that does not know
//! the tag skips `len` bytes and carries on, so old and new brokers interoperate without a
//! version bump. This is the same tolerant, additive pattern the client protocol uses for
//! its own tagged fields (`protocol::wire`'s `RequestTags`).
//!
//! Bump [`INTER_NODE_PROTOCOL_VERSION`] only for a change the extension section cannot
//! express — a fixed field changing meaning or disappearing.

use bytes::{Buf, BufMut};

/// Leading byte of every versioned inter-node frame, request and response alike.
///
/// Distinct from every other magic in use: the log's `0xC0` record batch
/// (`protocol::batch`), the client protocol's `0xF1` versioned envelope
/// (`protocol::wire`), `0xCE`/`0xCF` (share-group / consumer-offset snapshots), and the
/// 4-byte `0xCAFEBABE` auth preamble. It also differs from the pre-versioning inter-node
/// magics it replaces (`0xAA`, `0xAC`, `0xAE`, `0xAF`, `0xBB`), so a broker still speaking
/// those is rejected as unknown rather than misparsed.
pub const INTER_NODE_MAGIC: u8 = 0xB0;

/// Version of the inter-node protocol this build speaks.
///
/// Bumped only for a change the extension section cannot express — see the module docs.
/// A receiver rejects a frame whose version it does not know rather than guessing at the
/// layout, which is the whole point of carrying it.
pub const INTER_NODE_PROTOCOL_VERSION: u16 = 1;

/// `magic(1) + version(2) + frame_type(1) + payload_len(4)`.
pub const ENVELOPE_HEADER_SIZE: usize = 8;

/// Largest payload a peer may declare, matching the cap the client protocol and the
/// replication-fetch response already use. Checked against the *declared* length before a
/// single byte is buffered, so a hostile or corrupt peer cannot induce a large allocation
/// just by claiming a large frame.
pub const MAX_INTER_NODE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// What an inter-node frame carries. Requests and their responses are distinct types, so a
/// response can never be mistaken for a request on a pooled connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Leader pushes replicated batch bytes to a follower.
    ReplicationPush = 1,
    /// Follower's answer to [`FrameType::ReplicationPush`].
    ReplicationPushAck = 2,
    /// Leader announces itself and its term to a peer.
    Heartbeat = 3,
    /// Peer's answer to [`FrameType::Heartbeat`], carrying its own identity and roles.
    HeartbeatAck = 4,
    /// Raft candidate solicits a vote.
    VoteRequest = 5,
    /// Peer's answer to [`FrameType::VoteRequest`].
    VoteResponse = 6,
    /// Follower pulls batch bytes from its leader (follower fetch).
    ReplicationFetch = 7,
    /// Leader's answer to [`FrameType::ReplicationFetch`].
    ReplicationFetchResponse = 8,
}

impl FrameType {
    pub fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => FrameType::ReplicationPush,
            2 => FrameType::ReplicationPushAck,
            3 => FrameType::Heartbeat,
            4 => FrameType::HeartbeatAck,
            5 => FrameType::VoteRequest,
            6 => FrameType::VoteResponse,
            7 => FrameType::ReplicationFetch,
            8 => FrameType::ReplicationFetchResponse,
            _ => return None,
        })
    }
}

/// Why a buffer could not be read as one inter-node frame.
///
/// [`EnvelopeError::Incomplete`] is the only recoverable one and is deliberately distinct:
/// it means "this is a well-formed frame that has not all arrived yet", which a connection
/// loop answers by reading more, never by closing. Every other variant means the peer is
/// speaking something this build cannot parse, and the connection cannot be resynchronized
/// by reading further.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Fewer bytes than the frame declares. Carries how many are needed in total, so a
    /// caller reading from a socket can size its next read instead of guessing.
    Incomplete { needed: usize },
    /// Leading byte is not [`INTER_NODE_MAGIC`].
    NotAnInterNodeFrame { found: u8 },
    /// A version this build does not know how to parse.
    UnsupportedVersion { version: u16 },
    /// A frame type this build does not know.
    UnknownFrameType { frame_type: u8 },
    /// Declared payload exceeds [`MAX_INTER_NODE_PAYLOAD_BYTES`].
    PayloadTooLarge { declared: usize },
    /// The payload ended mid-field, or its extension section was malformed.
    MalformedPayload(&'static str),
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::Incomplete { needed } => {
                write!(f, "incomplete inter-node frame: need {} bytes", needed)
            }
            EnvelopeError::NotAnInterNodeFrame { found } => write!(
                f,
                "not an inter-node frame: expected magic 0x{:02X}, got 0x{:02X}",
                INTER_NODE_MAGIC, found
            ),
            EnvelopeError::UnsupportedVersion { version } => write!(
                f,
                "unsupported inter-node protocol version {} (this build speaks {})",
                version, INTER_NODE_PROTOCOL_VERSION
            ),
            EnvelopeError::UnknownFrameType { frame_type } => {
                write!(f, "unknown inter-node frame type {}", frame_type)
            }
            EnvelopeError::PayloadTooLarge { declared } => write!(
                f,
                "inter-node payload of {} bytes exceeds the {} byte maximum",
                declared, MAX_INTER_NODE_PAYLOAD_BYTES
            ),
            EnvelopeError::MalformedPayload(what) => {
                write!(f, "malformed inter-node payload: {}", what)
            }
        }
    }
}

impl std::error::Error for EnvelopeError {}

impl From<EnvelopeError> for std::io::Error {
    fn from(err: EnvelopeError) -> Self {
        let kind = match err {
            EnvelopeError::Incomplete { .. } => std::io::ErrorKind::UnexpectedEof,
            _ => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, err.to_string())
    }
}

/// One decoded frame: what it is, what version the sender spoke, and its payload.
///
/// `version` is retained rather than discarded after validation so a handler can branch on
/// it — the reason to carry a version at all is to let a receiver serve a peer that is
/// behind it, not merely to reject one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterNodeFrame<'a> {
    pub frame_type: FrameType,
    pub version: u16,
    pub payload: &'a [u8],
    /// Total bytes this frame occupied, header included — what a connection loop advances
    /// its cursor by.
    pub total_len: usize,
}

/// Wraps `payload` in an envelope announcing this build's version.
pub fn encode_frame(frame_type: FrameType, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(ENVELOPE_HEADER_SIZE + payload.len());
    encode_frame_into(&mut buf, frame_type, payload);
    buf
}

/// Allocation-free form of [`encode_frame`], for a caller that already owns a buffer.
pub fn encode_frame_into(buf: &mut Vec<u8>, frame_type: FrameType, payload: &[u8]) {
    buf.reserve(ENVELOPE_HEADER_SIZE + payload.len());
    buf.put_u8(INTER_NODE_MAGIC);
    buf.put_u16(INTER_NODE_PROTOCOL_VERSION);
    buf.put_u8(frame_type as u8);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
}

/// Reads the single frame at the start of `src`.
///
/// Defensive throughout: the declared length is bounds-checked against
/// [`MAX_INTER_NODE_PAYLOAD_BYTES`] before it is used for anything, and the payload slice
/// is taken from what actually arrived rather than from what the header claims — a
/// truncated or hostile frame yields an `Err`, never a panic or an oversized allocation.
pub fn decode_frame(src: &[u8]) -> Result<InterNodeFrame<'_>, EnvelopeError> {
    let header = decode_header(src)?;
    let total_len = ENVELOPE_HEADER_SIZE + header.payload_len;
    if src.len() < total_len {
        return Err(EnvelopeError::Incomplete { needed: total_len });
    }
    Ok(InterNodeFrame {
        frame_type: header.frame_type,
        version: header.version,
        payload: &src[ENVELOPE_HEADER_SIZE..total_len],
        total_len,
    })
}

/// One frame's fixed header, before its payload has necessarily arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub frame_type: FrameType,
    pub version: u16,
    pub payload_len: usize,
}

/// Reads only the fixed header.
///
/// Separate from [`decode_frame`] for the reader that pulls frames off a socket: it must
/// learn how many payload bytes to ask for *before* it can have a whole frame to decode,
/// and it must be able to reject a bad version, an unknown type, or an absurd length
/// without first buffering the body those fields describe.
pub fn decode_header(src: &[u8]) -> Result<FrameHeader, EnvelopeError> {
    if src.len() < ENVELOPE_HEADER_SIZE {
        return Err(EnvelopeError::Incomplete {
            needed: ENVELOPE_HEADER_SIZE,
        });
    }
    let mut header = &src[..ENVELOPE_HEADER_SIZE];
    let magic = header.get_u8();
    if magic != INTER_NODE_MAGIC {
        return Err(EnvelopeError::NotAnInterNodeFrame { found: magic });
    }
    let version = header.get_u16();
    let frame_type_byte = header.get_u8();
    let payload_len = header.get_u32() as usize;

    // Size first, then meaning: a frame too large to accept is rejected before anything is
    // allocated for it, and before its type or version is trusted enough to act on.
    if payload_len > MAX_INTER_NODE_PAYLOAD_BYTES {
        return Err(EnvelopeError::PayloadTooLarge {
            declared: payload_len,
        });
    }
    if version > INTER_NODE_PROTOCOL_VERSION {
        return Err(EnvelopeError::UnsupportedVersion { version });
    }
    let Some(frame_type) = FrameType::from_byte(frame_type_byte) else {
        return Err(EnvelopeError::UnknownFrameType {
            frame_type: frame_type_byte,
        });
    };
    Ok(FrameHeader {
        frame_type,
        version,
        payload_len,
    })
}

/// Reads exactly one frame from `stream`, header first and then precisely the payload it
/// declares.
///
/// This is the half of the design that fixes the *silent* failure, as opposed to the
/// lockstep-upgrade one. Responses used to be bare field sequences pulled off the socket
/// with a chain of `read_exact` calls, one per field. Peer connections are pooled, so a
/// reader that stopped before the final field — because it did not know the field existed,
/// or gave up early — left those bytes in the socket for the *next* exchange to consume as
/// its own status byte. A nonzero leftover reads as a rejection, so a perfectly healthy
/// peer appears permanently offline, and nothing anywhere reports why.
///
/// Reading a declared length makes that unrepresentable: either the whole frame arrives and
/// the socket is left positioned exactly at the next one, or this errors and the caller
/// discards the connection.
pub async fn read_frame<S>(stream: &mut S) -> std::io::Result<(FrameHeader, Vec<u8>)>
where
    S: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut header_buf = [0u8; ENVELOPE_HEADER_SIZE];
    stream.read_exact(&mut header_buf).await?;
    let header = decode_header(&header_buf)?;
    let mut payload = vec![0u8; header.payload_len];
    if header.payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((header, payload))
}

/// Writes one enveloped request and reads the enveloped reply, checking it is the reply
/// type expected.
///
/// The type check is not ceremony: on a pooled connection, a reply arriving out of step
/// with its request is exactly the symptom the old field-by-field readers produced, and
/// silently parsing it as the expected type is how that went unnoticed. Here it is an
/// error, and the caller drops the connection rather than carrying the confusion forward.
pub async fn exchange_frame<S>(
    stream: &mut S,
    request: &[u8],
    expected: FrameType,
) -> std::io::Result<Vec<u8>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    stream.write_all(request).await?;
    let (header, payload) = read_frame(stream).await?;
    if header.frame_type != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "expected a {:?} reply, got {:?}",
                expected, header.frame_type
            ),
        ));
    }
    Ok(payload)
}

/// Writes a payload's trailing extension section.
///
/// ```text
/// [ext_count: 2b] ( [tag: 2b] [len: 4b] [bytes: len] )*
/// ```
///
/// Every frame payload ends with one, even when empty, so there is always somewhere for
/// the next field to go. Adding one here rather than widening a frame's fixed fields is
/// what lets a broker that has never heard of the field keep parsing: it skips `len` bytes
/// and moves on.
pub fn write_extensions(buf: &mut Vec<u8>, extensions: &[(u16, Vec<u8>)]) {
    buf.put_u16(extensions.len() as u16);
    for (tag, bytes) in extensions {
        buf.put_u16(*tag);
        buf.put_u32(bytes.len() as u32);
        buf.put_slice(bytes);
    }
}

/// Convenience for the common case of a payload carrying no extensions yet.
pub fn write_no_extensions(buf: &mut Vec<u8>) {
    buf.put_u16(0);
}

/// Reads the extension section at `cursor`, advancing past it.
///
/// Returns every entry, known tag or not — recognising them is the caller's job, and an
/// unrecognised one is not an error. This is what makes the section additive: a frame
/// carrying a field from a newer build parses cleanly here and the unknown entry is simply
/// never looked at.
pub fn read_extensions(cursor: &mut &[u8]) -> Result<Vec<(u16, Vec<u8>)>, EnvelopeError> {
    if cursor.remaining() < 2 {
        return Err(EnvelopeError::MalformedPayload(
            "payload ended before its extension section",
        ));
    }
    let count = cursor.get_u16() as usize;
    let mut extensions = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        if cursor.remaining() < 6 {
            return Err(EnvelopeError::MalformedPayload(
                "extension entry header truncated",
            ));
        }
        let tag = cursor.get_u16();
        let len = cursor.get_u32() as usize;
        if cursor.remaining() < len {
            return Err(EnvelopeError::MalformedPayload(
                "extension entry shorter than its declared length",
            ));
        }
        extensions.push((tag, cursor[..len].to_vec()));
        cursor.advance(len);
    }
    Ok(extensions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips_with_its_type_version_and_payload() {
        let encoded = encode_frame(FrameType::Heartbeat, b"payload bytes");
        let frame = decode_frame(&encoded).unwrap();
        assert_eq!(frame.frame_type, FrameType::Heartbeat);
        assert_eq!(frame.version, INTER_NODE_PROTOCOL_VERSION);
        assert_eq!(frame.payload, b"payload bytes");
        assert_eq!(frame.total_len, encoded.len());
    }

    /// The property the whole design rests on: a receiver can consume exactly one frame
    /// and land on the start of the next, with no knowledge of what the payload meant.
    /// Without it, every layout change is a lockstep cluster upgrade.
    #[test]
    fn frames_stay_delimited_when_several_share_a_buffer() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&encode_frame(FrameType::Heartbeat, b"first"));
        stream.extend_from_slice(&encode_frame(FrameType::VoteRequest, b""));
        stream.extend_from_slice(&encode_frame(FrameType::ReplicationPush, b"third frame"));

        let mut cursor = 0usize;
        let mut seen = Vec::new();
        while cursor < stream.len() {
            let frame = decode_frame(&stream[cursor..]).unwrap();
            seen.push((frame.frame_type, frame.payload.to_vec()));
            cursor += frame.total_len;
        }
        assert_eq!(cursor, stream.len());
        assert_eq!(
            seen,
            vec![
                (FrameType::Heartbeat, b"first".to_vec()),
                (FrameType::VoteRequest, Vec::new()),
                (FrameType::ReplicationPush, b"third frame".to_vec()),
            ]
        );
    }

    /// A frame that has not all arrived must be reported as such and nothing else — a
    /// connection loop distinguishes "read more" from "close the connection" purely by
    /// this, and getting it wrong drops healthy peers under normal TCP segmentation.
    #[test]
    fn a_partially_arrived_frame_reports_how_much_more_it_needs() {
        let encoded = encode_frame(FrameType::ReplicationPush, &vec![7u8; 300]);
        for prefix in [
            0usize,
            1,
            ENVELOPE_HEADER_SIZE - 1,
            ENVELOPE_HEADER_SIZE,
            100,
        ] {
            match decode_frame(&encoded[..prefix]) {
                Err(EnvelopeError::Incomplete { needed }) => {
                    assert!(
                        needed > prefix,
                        "needed {} must exceed what was supplied ({})",
                        needed,
                        prefix
                    );
                    assert!(needed <= encoded.len());
                }
                other => panic!("expected Incomplete at prefix {}, got {:?}", prefix, other),
            }
        }
        // One byte short of complete is still incomplete, not malformed.
        assert!(matches!(
            decode_frame(&encoded[..encoded.len() - 1]),
            Err(EnvelopeError::Incomplete { .. })
        ));
    }

    /// An oversized declared length must be refused on the header alone. Sizing a buffer
    /// from a number a peer chose, before checking it, is how a single 4-byte field turns
    /// into a remote OOM.
    #[test]
    fn an_oversized_declared_payload_is_refused_without_being_buffered() {
        let mut frame = encode_frame(FrameType::ReplicationPush, b"");
        frame[4..8].copy_from_slice(&(MAX_INTER_NODE_PAYLOAD_BYTES as u32 + 1).to_be_bytes());
        assert_eq!(
            decode_frame(&frame),
            Err(EnvelopeError::PayloadTooLarge {
                declared: MAX_INTER_NODE_PAYLOAD_BYTES + 1
            })
        );
        // ...and it is refused on the 8-byte header alone, without the body arriving.
        assert_eq!(
            decode_frame(&frame[..ENVELOPE_HEADER_SIZE]),
            Err(EnvelopeError::PayloadTooLarge {
                declared: MAX_INTER_NODE_PAYLOAD_BYTES + 1
            })
        );
    }

    #[test]
    fn a_frame_from_a_newer_protocol_version_is_rejected_not_guessed_at() {
        let mut frame = encode_frame(FrameType::Heartbeat, b"x");
        frame[1..3].copy_from_slice(&(INTER_NODE_PROTOCOL_VERSION + 1).to_be_bytes());
        assert_eq!(
            decode_frame(&frame),
            Err(EnvelopeError::UnsupportedVersion {
                version: INTER_NODE_PROTOCOL_VERSION + 1
            })
        );
    }

    /// Every pre-versioning magic must be refused rather than misparsed. These bytes used
    /// to *be* the protocol, so a broker on an older build reaching a new one has to fail
    /// loudly instead of having its fields read as an envelope header.
    #[test]
    fn pre_versioning_magic_bytes_are_refused_as_foreign() {
        for legacy in [0xAAu8, 0xAC, 0xAE, 0xAF, 0xBB] {
            let mut buf = vec![legacy];
            buf.extend_from_slice(&[0u8; 32]);
            assert_eq!(
                decode_frame(&buf),
                Err(EnvelopeError::NotAnInterNodeFrame { found: legacy }),
                "legacy magic 0x{:02X} must not decode as an envelope",
                legacy
            );
        }
    }

    #[test]
    fn an_unknown_frame_type_is_reported_rather_than_dispatched() {
        let mut frame = encode_frame(FrameType::Heartbeat, b"x");
        frame[3] = 200;
        assert_eq!(
            decode_frame(&frame),
            Err(EnvelopeError::UnknownFrameType { frame_type: 200 })
        );
    }

    /// The additive path this whole change exists to enable: a payload carrying a tag the
    /// reader has never heard of must still parse, with the unknown entry skipped by its
    /// declared length and everything after it intact.
    #[test]
    fn an_unknown_extension_tag_is_skipped_without_disturbing_what_follows() {
        let mut payload = Vec::new();
        payload.put_u32(0xDEAD_BEEF); // a fixed field, to prove the section starts after it
        write_extensions(
            &mut payload,
            &[
                (1, b"known".to_vec()),
                (999, vec![0xFF; 40]), // from a build this reader knows nothing about
                (2, b"also known".to_vec()),
            ],
        );
        payload.extend_from_slice(b"TRAILER");

        let mut cursor = &payload[..];
        assert_eq!(cursor.get_u32(), 0xDEAD_BEEF);
        let extensions = read_extensions(&mut cursor).unwrap();
        assert_eq!(extensions.len(), 3);
        assert_eq!(extensions[0], (1, b"known".to_vec()));
        assert_eq!(extensions[2], (2, b"also known".to_vec()));
        assert_eq!(
            cursor, b"TRAILER",
            "the unknown entry must be skipped by exactly its declared length"
        );
    }

    #[test]
    fn an_extension_entry_longer_than_the_payload_is_rejected() {
        let mut payload = Vec::new();
        payload.put_u16(1);
        payload.put_u16(7); // tag
        payload.put_u32(1000); // declared length, far beyond what follows
        payload.extend_from_slice(b"short");
        let mut cursor = &payload[..];
        assert!(matches!(
            read_extensions(&mut cursor),
            Err(EnvelopeError::MalformedPayload(_))
        ));
    }

    #[test]
    fn an_empty_extension_section_round_trips() {
        let mut payload = Vec::new();
        write_no_extensions(&mut payload);
        let mut cursor = &payload[..];
        assert!(read_extensions(&mut cursor).unwrap().is_empty());
        assert!(cursor.is_empty());
    }
}
