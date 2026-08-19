use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use std::num::NonZeroU32;

pub(crate) const DEFAULT_SCRAM_SHA256_ITERATIONS: u32 = 4096;

/// Hash family backing a SCRAM mechanism.
///
/// A credential is derived for one specific mechanism and is not transferable to another:
/// the salted password, stored key and server key all depend on the hash, so a SHA-256
/// credential cannot verify a SHA-512 exchange.
///
/// Credentials are currently stored keyed by username alone, so a user holds exactly one
/// credential and re-creating it under a different mechanism *replaces* the previous one.
/// Holding a SHA-256 and a SHA-512 credential for the same user simultaneously would need
/// the credential store keyed by `(username, mechanism)`; that is not implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ScramMechanism {
    #[default]
    Sha256,
    Sha512,
}

impl ScramMechanism {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
            ScramMechanism::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// Wire/storage discriminant. `0` is SHA-256 so credential records written before
    /// mechanisms existed decode as SHA-256, which is what they were.
    pub(crate) fn to_byte(self) -> u8 {
        match self {
            ScramMechanism::Sha256 => 0,
            ScramMechanism::Sha512 => 1,
        }
    }

    pub(crate) fn from_byte(b: u8) -> Self {
        match b {
            1 => ScramMechanism::Sha512,
            _ => ScramMechanism::Sha256,
        }
    }

    /// Length in bytes of this mechanism's digest — also the salted-password length.
    pub(crate) fn output_len(self) -> usize {
        match self {
            ScramMechanism::Sha256 => ring::digest::SHA256_OUTPUT_LEN,
            ScramMechanism::Sha512 => ring::digest::SHA512_OUTPUT_LEN,
        }
    }

    fn digest_algorithm(self) -> &'static ring::digest::Algorithm {
        match self {
            ScramMechanism::Sha256 => &ring::digest::SHA256,
            ScramMechanism::Sha512 => &ring::digest::SHA512,
        }
    }

    fn hmac_algorithm(self) -> ring::hmac::Algorithm {
        match self {
            ScramMechanism::Sha256 => ring::hmac::HMAC_SHA256,
            ScramMechanism::Sha512 => ring::hmac::HMAC_SHA512,
        }
    }

    fn pbkdf2_algorithm(self) -> ring::pbkdf2::Algorithm {
        match self {
            ScramMechanism::Sha256 => ring::pbkdf2::PBKDF2_HMAC_SHA256,
            ScramMechanism::Sha512 => ring::pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }
}

impl std::str::FromStr for ScramMechanism {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "SCRAM-SHA-256" | "SHA-256" | "SHA256" => Ok(ScramMechanism::Sha256),
            "SCRAM-SHA-512" | "SHA-512" | "SHA512" => Ok(ScramMechanism::Sha512),
            _ => Err(format!("Unknown SCRAM mechanism: '{}'", s)),
        }
    }
}

impl std::fmt::Display for ScramMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScramCredential {
    pub username: String,
    pub mechanism: ScramMechanism,
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl ScramCredential {
    pub(crate) fn new(
        username: String,
        mechanism: ScramMechanism,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) -> Self {
        Self {
            username,
            mechanism,
            iterations,
            salt,
            stored_key,
            server_key,
        }
    }

    pub(crate) fn generate(
        username: &str,
        password: &str,
        mechanism: ScramMechanism,
        iterations: u32,
    ) -> Result<Self, ring::error::Unspecified> {
        let salt = generate_salt(16)?;
        Ok(Self::from_password(
            username, password, mechanism, iterations, salt,
        ))
    }

    pub(crate) fn from_password(
        username: &str,
        password: &str,
        mechanism: ScramMechanism,
        iterations: u32,
        salt: Vec<u8>,
    ) -> Self {
        let salted_password =
            derive_scram_salted_password_with(password, &salt, iterations, mechanism);
        let client_key = scram_hmac(mechanism, &salted_password, b"Client Key");
        let stored_key = scram_hash(mechanism, &client_key);
        let server_key = scram_hmac(mechanism, &salted_password, b"Server Key");
        Self {
            username: username.to_string(),
            mechanism,
            iterations,
            salt,
            stored_key,
            server_key,
        }
    }

    pub(crate) fn verify_password(&self, password: &str) -> bool {
        let candidate = Self::from_password(
            &self.username,
            password,
            self.mechanism,
            self.iterations,
            self.salt.clone(),
        );
        constant_time_eq(&candidate.stored_key, &self.stored_key)
            && constant_time_eq(&candidate.server_key, &self.server_key)
    }

    pub(crate) fn verify_client_proof(&self, auth_message: &str, proof: &[u8]) -> bool {
        let client_signature =
            scram_hmac(self.mechanism, &self.stored_key, auth_message.as_bytes());
        if client_signature.len() != proof.len() {
            return false;
        }

        let recovered_client_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(proof_byte, sig_byte)| proof_byte ^ sig_byte)
            .collect();
        let recovered_stored_key = scram_hash(self.mechanism, &recovered_client_key);
        constant_time_eq(&recovered_stored_key, &self.stored_key)
    }

    pub(crate) fn build_server_final(&self, auth_message: &str) -> String {
        let server_signature =
            scram_hmac(self.mechanism, &self.server_key, auth_message.as_bytes());
        format!("v={}", BASE64_STANDARD.encode(server_signature))
    }
}

pub(crate) fn generate_salt(len: usize) -> Result<Vec<u8>, ring::error::Unspecified> {
    let rng = ring::rand::SystemRandom::new();
    let mut salt = vec![0u8; len];
    ring::rand::SecureRandom::fill(&rng, &mut salt)?;
    Ok(salt)
}

/// SHA-256 salted-password derivation, kept for callers that are inherently SHA-256.
pub(crate) fn derive_scram_salted_password(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> [u8; ring::digest::SHA256_OUTPUT_LEN] {
    let mut out = [0u8; ring::digest::SHA256_OUTPUT_LEN];
    ring::pbkdf2::derive(
        ring::pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations)
            .unwrap_or(NonZeroU32::new(DEFAULT_SCRAM_SHA256_ITERATIONS).unwrap()),
        salt,
        password.as_bytes(),
        &mut out,
    );
    out
}

/// Mechanism-parameterized salted-password derivation (`Hi()` in RFC 5802). The output is
/// the mechanism's digest length, so SHA-512 produces a 64-byte salted password rather
/// than being truncated to 32.
pub(crate) fn derive_scram_salted_password_with(
    password: &str,
    salt: &[u8],
    iterations: u32,
    mechanism: ScramMechanism,
) -> Vec<u8> {
    let mut out = vec![0u8; mechanism.output_len()];
    ring::pbkdf2::derive(
        mechanism.pbkdf2_algorithm(),
        NonZeroU32::new(iterations)
            .unwrap_or(NonZeroU32::new(DEFAULT_SCRAM_SHA256_ITERATIONS).unwrap()),
        salt,
        password.as_bytes(),
        &mut out,
    );
    out
}

/// HMAC under the given mechanism's hash.
pub(crate) fn scram_hmac(mechanism: ScramMechanism, key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(mechanism.hmac_algorithm(), key);
    ring::hmac::sign(&key, data).as_ref().to_vec()
}

/// Digest under the given mechanism's hash.
pub(crate) fn scram_hash(mechanism: ScramMechanism, data: &[u8]) -> Vec<u8> {
    ring::digest::digest(mechanism.digest_algorithm(), data)
        .as_ref()
        .to_vec()
}

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    scram_hmac(ScramMechanism::Sha256, key, data)
}

pub(crate) fn sha256(data: &[u8]) -> Vec<u8> {
    scram_hash(ScramMechanism::Sha256, data)
}

/// Constant-time byte comparison for secret material.
///
/// Uses `subtle::ConstantTimeEq`, which is purpose-built for this and carries an explicit
/// guarantee that the comparison stays branch-free. The previous implementation was a
/// hand-rolled `fold` over the bytes: correct as written, but nothing in the source marked
/// it timing-sensitive, so the optimizer was free to rewrite it into a short-circuit loop.
/// If it ever did, the time taken to reject a proof would leak how many leading bytes were
/// right — turning offline guessing into an online byte-at-a-time attack against the stored
/// credential.
///
/// (`ring::constant_time::verify_slices_are_equal` is *not* the alternative it appears to
/// be: it is deprecated and documented as an internal helper with no side-channel promise.)
///
/// A length mismatch returns `false` immediately. That is not a leak worth avoiding here:
/// these values are fixed-width digests, so their length is already public.
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if left.len() != right.len() {
        return false;
    }
    left.ct_eq(right).into()
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Computes the `tls-server-end-point` channel-binding value for a certificate (RFC 5929).
///
/// The binding is a hash of the server's DER certificate. SHA-256 is used regardless of the
/// SCRAM mechanism's own hash: RFC 5929 ties the choice to the certificate's *signature*
/// algorithm, with SHA-256 substituted whenever that is MD5 or SHA-1, and every certificate
/// worth binding to today is signed with SHA-256 or stronger.
///
/// Binding this value into the authentication exchange is what stops a proof captured on
/// one TLS connection being replayed on another: a proof computed against this server's
/// certificate cannot authenticate against a different endpoint, so an attacker who
/// terminates TLS in the middle cannot forward the client's credentials onward.
pub fn tls_server_end_point(cert_der: &[u8]) -> Vec<u8> {
    scram_hash(ScramMechanism::Sha256, cert_der)
}

/// Builds the `c=` value a client sends: base64 of the GS2 header followed by the binding
/// data. `None` binding produces the plain `n,,` header used by the non-PLUS mechanisms.
pub fn encode_channel_binding(binding: Option<&[u8]>) -> String {
    let mut raw = Vec::new();
    match binding {
        Some(data) => {
            raw.extend_from_slice(b"p=tls-server-end-point,,");
            raw.extend_from_slice(data);
        }
        // "n," means the client does not support channel binding; the trailing comma is
        // the empty authzid field.
        None => raw.extend_from_slice(b"n,,"),
    }
    BASE64_STANDARD.encode(raw)
}

/// Verifies a client's `c=` value against what this server expects.
///
/// `expected_binding` is `Some` only when the mechanism negotiated was a `-PLUS` variant.
/// A mismatch means the client either bound to a different endpoint — the man-in-the-middle
/// case this exists to catch — or claimed a binding the server cannot confirm.
pub fn verify_channel_binding(c_value: &str, expected_binding: Option<&[u8]>) -> bool {
    let Ok(decoded) = BASE64_STANDARD.decode(c_value) else {
        return false;
    };
    match expected_binding {
        Some(binding) => {
            let prefix = b"p=tls-server-end-point,,";
            if !decoded.starts_with(prefix) {
                return false;
            }
            constant_time_eq(&decoded[prefix.len()..], binding)
        }
        // Without a negotiated binding the header must be one of the non-binding forms.
        // "y,," means the client supports binding but believes the server does not — it is
        // accepted here because rejecting it would break clients that probe, and the
        // downgrade it signals is only meaningful when the server actually offers -PLUS.
        None => decoded == b"n,," || decoded == b"y,,",
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Each mechanism must derive keys under its own hash. A SHA-512 credential produces
    /// 64-byte keys where SHA-256 produces 32, and the two are not interchangeable.
    #[test]
    fn mechanisms_derive_distinct_key_material() {
        let salt = vec![7u8; 16];
        let sha256 =
            ScramCredential::from_password("u", "pw", ScramMechanism::Sha256, 4096, salt.clone());
        let sha512 = ScramCredential::from_password("u", "pw", ScramMechanism::Sha512, 4096, salt);

        assert_eq!(sha256.stored_key.len(), 32);
        assert_eq!(sha512.stored_key.len(), 64);
        assert_eq!(sha512.server_key.len(), 64);
        assert_ne!(
            sha256.stored_key, sha512.stored_key,
            "the same password under different mechanisms must not share key material"
        );
    }

    /// Verification must succeed under the credential's own mechanism and fail under the
    /// other — a SHA-256 credential cannot validate a SHA-512 exchange.
    #[test]
    fn credential_verifies_only_under_its_own_mechanism() {
        for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            let cred =
                ScramCredential::generate("alice", "correct horse", mechanism, 4096).unwrap();
            assert_eq!(cred.mechanism, mechanism);
            assert!(cred.verify_password("correct horse"));
            assert!(!cred.verify_password("wrong horse"));

            // Same salt and password, other mechanism — must not verify against this one.
            let other = match mechanism {
                ScramMechanism::Sha256 => ScramMechanism::Sha512,
                ScramMechanism::Sha512 => ScramMechanism::Sha256,
            };
            let cross = ScramCredential::from_password(
                "alice",
                "correct horse",
                other,
                4096,
                cred.salt.clone(),
            );
            assert_ne!(
                cross.stored_key, cred.stored_key,
                "cross-mechanism key material must differ"
            );
        }
    }

    /// A full client-proof round trip under each mechanism.
    #[test]
    fn client_proof_round_trips_under_each_mechanism() {
        for mechanism in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            let password = "s3cr3t";
            let salt = generate_salt(16).unwrap();
            let cred =
                ScramCredential::from_password("bob", password, mechanism, 4096, salt.clone());
            let auth_message = "n=bob,r=abc,s=xyz,i=4096,c=biws,r=abc";

            // Client side: reconstruct the proof from the password.
            let salted = derive_scram_salted_password_with(password, &salt, 4096, mechanism);
            let client_key = scram_hmac(mechanism, &salted, b"Client Key");
            let stored_key = scram_hash(mechanism, &client_key);
            let client_signature = scram_hmac(mechanism, &stored_key, auth_message.as_bytes());
            let proof: Vec<u8> = client_key
                .iter()
                .zip(client_signature.iter())
                .map(|(k, s)| k ^ s)
                .collect();

            assert!(
                cred.verify_client_proof(auth_message, &proof),
                "{} proof must verify",
                mechanism
            );
            let mut tampered = proof.clone();
            tampered[0] ^= 0xFF;
            assert!(
                !cred.verify_client_proof(auth_message, &tampered),
                "{} must reject a tampered proof",
                mechanism
            );
        }
    }

    /// A binding proves the exchange belongs to one specific TLS endpoint. A proof bound to
    /// one certificate must not verify against another — that is the whole point: an
    /// attacker terminating TLS in the middle cannot forward captured credentials onward.
    #[test]
    fn channel_binding_is_tied_to_the_certificate() {
        let cert_a = b"-----cert-a-----";
        let cert_b = b"-----cert-b-----";
        let bind_a = tls_server_end_point(cert_a);
        let bind_b = tls_server_end_point(cert_b);
        assert_ne!(
            bind_a, bind_b,
            "different certificates must bind differently"
        );
        assert_eq!(
            bind_a,
            tls_server_end_point(cert_a),
            "binding must be stable"
        );

        let c_value = encode_channel_binding(Some(&bind_a));
        assert!(verify_channel_binding(&c_value, Some(&bind_a)));
        assert!(
            !verify_channel_binding(&c_value, Some(&bind_b)),
            "a binding for one endpoint must not verify against another"
        );
    }

    /// The `c=` value used to be checked only for presence, so a client could claim any
    /// binding and be believed. Each of these was accepted before and must not be now.
    #[test]
    fn channel_binding_rejects_mismatched_and_malformed_values() {
        let binding = tls_server_end_point(b"server-cert");

        // Claiming no binding when one was negotiated.
        assert!(!verify_channel_binding(
            &encode_channel_binding(None),
            Some(&binding)
        ));
        // Claiming a binding when none was negotiated.
        assert!(!verify_channel_binding(
            &encode_channel_binding(Some(&binding)),
            None
        ));
        // Right header, wrong binding bytes.
        let mut wrong = b"p=tls-server-end-point,,".to_vec();
        wrong.extend_from_slice(&tls_server_end_point(b"other-cert"));
        assert!(!verify_channel_binding(
            &BASE64_STANDARD.encode(wrong),
            Some(&binding)
        ));
        // Not valid base64 at all.
        assert!(!verify_channel_binding("not!base64", Some(&binding)));
        assert!(!verify_channel_binding("not!base64", None));
    }

    /// Without a negotiated binding the header must still be one of the non-binding forms
    /// rather than anything the client feels like sending. `y,,` is accepted because it is
    /// how a binding-capable client reports that the server did not offer -PLUS.
    #[test]
    fn non_plus_mechanisms_accept_only_non_binding_headers() {
        assert!(verify_channel_binding(&BASE64_STANDARD.encode("n,,"), None));
        assert!(verify_channel_binding(&BASE64_STANDARD.encode("y,,"), None));
        assert!(!verify_channel_binding(
            &BASE64_STANDARD.encode("p=tls-server-end-point,,"),
            None
        ));
        // "biws" is base64("n,,") — the value the client has always sent.
        assert!(verify_channel_binding("biws", None));
    }

    /// The storage discriminant must round-trip, and an absent byte must mean SHA-256 so
    /// credential records written before mechanisms existed replay as what they were.
    #[test]
    fn mechanism_byte_defaults_to_sha256_for_legacy_records() {
        assert_eq!(ScramMechanism::from_byte(0), ScramMechanism::Sha256);
        assert_eq!(ScramMechanism::from_byte(1), ScramMechanism::Sha512);
        assert_eq!(ScramMechanism::from_byte(200), ScramMechanism::Sha256);
        assert_eq!(ScramMechanism::default(), ScramMechanism::Sha256);
        for m in [ScramMechanism::Sha256, ScramMechanism::Sha512] {
            assert_eq!(ScramMechanism::from_byte(m.to_byte()), m);
            assert_eq!(m.as_str().parse::<ScramMechanism>().unwrap(), m);
        }
    }
}
