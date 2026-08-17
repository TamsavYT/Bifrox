use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use std::num::NonZeroU32;

pub(crate) const DEFAULT_SCRAM_SHA256_ITERATIONS: u32 = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScramCredential {
    pub username: String,
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl ScramCredential {
    pub(crate) fn new(
        username: String,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) -> Self {
        Self {
            username,
            iterations,
            salt,
            stored_key,
            server_key,
        }
    }

    pub(crate) fn generate(
        username: &str,
        password: &str,
        iterations: u32,
    ) -> Result<Self, ring::error::Unspecified> {
        let salt = generate_salt(16)?;
        Ok(Self::from_password(username, password, iterations, salt))
    }

    pub(crate) fn from_password(
        username: &str,
        password: &str,
        iterations: u32,
        salt: Vec<u8>,
    ) -> Self {
        let salted_password = derive_scram_salted_password(password, &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        Self {
            username: username.to_string(),
            iterations,
            salt,
            stored_key,
            server_key,
        }
    }

    pub(crate) fn verify_password(&self, password: &str) -> bool {
        let candidate =
            Self::from_password(&self.username, password, self.iterations, self.salt.clone());
        constant_time_eq(&candidate.stored_key, &self.stored_key)
            && constant_time_eq(&candidate.server_key, &self.server_key)
    }

    pub(crate) fn verify_client_proof(&self, auth_message: &str, proof: &[u8]) -> bool {
        let client_signature = hmac_sha256(&self.stored_key, auth_message.as_bytes());
        if client_signature.len() != proof.len() {
            return false;
        }

        let recovered_client_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(proof_byte, sig_byte)| proof_byte ^ sig_byte)
            .collect();
        let recovered_stored_key = sha256(&recovered_client_key);
        constant_time_eq(&recovered_stored_key, &self.stored_key)
    }

    pub(crate) fn build_server_final(&self, auth_message: &str) -> String {
        let server_signature = hmac_sha256(&self.server_key, auth_message.as_bytes());
        format!("v={}", BASE64_STANDARD.encode(server_signature))
    }
}

pub(crate) fn generate_salt(len: usize) -> Result<Vec<u8>, ring::error::Unspecified> {
    let rng = ring::rand::SystemRandom::new();
    let mut salt = vec![0u8; len];
    ring::rand::SecureRandom::fill(&rng, &mut salt)?;
    Ok(salt)
}

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

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data).as_ref().to_vec()
}

pub(crate) fn sha256(data: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .to_vec()
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
