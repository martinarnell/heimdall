//! API key format, generation, and hashing.
//!
//! Format: `hk_live_<22 base62 chars>` — 16 random bytes (128 bits) encoded
//! base62 with leading-zero padding to keep length stable. 128 bits is
//! brute-force-infeasible forever; the extra entropy of 192 bits would only
//! pad an already-safe number. `hk_test_…` is reserved for future use.
//!
//! Storage: HMAC-SHA-256(pepper, full_key) — 32 bytes. Pepper is a single
//! server-wide secret in `HEIMDALL_KEY_PEPPER` (hex). See `plan.md` for why
//! we use HMAC-with-pepper rather than bcrypt or per-row salt.

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

pub const KEY_PREFIX_LIVE: &str = "hk_live_";
pub const KEY_PREFIX_TEST: &str = "hk_test_";

/// Number of random bytes in the key body. 16 bytes = 128 bits of entropy.
pub const KEY_RANDOM_BYTES: usize = 16;
/// base62 of a u128 is ≤ 22 chars; we pad to exactly 22 for stable length.
pub const KEY_BODY_LEN: usize = 22;
/// Total key length including the 8-char `hk_live_` prefix.
pub const KEY_TOTAL_LEN: usize = 8 + KEY_BODY_LEN;

/// Output of HMAC-SHA-256 — 32 bytes.
pub type KeyHash = [u8; 32];

/// A freshly generated API key (full secret, only shown once).
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// The full key string (e.g. `hk_live_8K3pQwM2nRfX7vYbT4hAjL9`).
    /// Must be shown to the user EXACTLY ONCE then forgotten.
    pub full: String,
    /// HMAC-SHA-256(pepper, full) — what gets stored in the DB.
    pub hash: KeyHash,
    /// First 12 chars (`hk_live_8K3p`) — safe to display.
    pub prefix: String,
    /// Last 4 chars (`hAjL`) — safe to display.
    pub last4: String,
}

/// Generate a new live-mode API key using the OS CSPRNG.
pub fn generate_key(pepper: &[u8]) -> ApiKey {
    let mut bytes = [0u8; KEY_RANDOM_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let n = u128::from_be_bytes(bytes);

    let mut body = base62::encode(n);
    while body.len() < KEY_BODY_LEN {
        body.insert(0, '0');
    }
    debug_assert_eq!(body.len(), KEY_BODY_LEN);

    let full = format!("{KEY_PREFIX_LIVE}{body}");
    let hash = hash_key(pepper, &full);
    let prefix = full[..12].to_string();
    let last4 = full[full.len() - 4..].to_string();

    ApiKey { full, hash, prefix, last4 }
}

/// HMAC-SHA-256(pepper, key). Deterministic. The unique-index lookup key.
pub fn hash_key(pepper: &[u8], key: &str) -> KeyHash {
    let mut mac = Hmac::<Sha256>::new_from_slice(pepper)
        .expect("HMAC-SHA-256 accepts any key length");
    mac.update(key.as_bytes());
    let out = mac.finalize().into_bytes();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&out);
    hash
}

/// Cheap pre-flight on a candidate string before we bother hashing it.
/// Rejects obvious garbage so we don't waste an HMAC on every malformed input.
pub fn looks_like_key(s: &str) -> bool {
    if s.len() != KEY_TOTAL_LEN {
        return false;
    }
    if !(s.starts_with(KEY_PREFIX_LIVE) || s.starts_with(KEY_PREFIX_TEST)) {
        return false;
    }
    // base62 alphabet: 0-9 a-z A-Z. Prefix is also alphanumeric + underscore.
    s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_has_correct_shape() {
        let pepper = b"test-pepper";
        let a = generate_key(pepper);
        assert!(a.full.starts_with(KEY_PREFIX_LIVE));
        assert_eq!(a.full.len(), KEY_TOTAL_LEN);
        assert_eq!(a.prefix.len(), 12);
        assert_eq!(a.last4.len(), 4);
        assert_eq!(&a.full[..12], &a.prefix);
        assert_eq!(&a.full[a.full.len() - 4..], &a.last4);
    }

    #[test]
    fn keys_are_unique() {
        let pepper = b"test-pepper";
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let k = generate_key(pepper);
            assert!(seen.insert(k.full.clone()), "duplicate key generated");
            assert!(seen.insert(hex::encode(k.hash)), "duplicate hash");
        }
    }

    #[test]
    fn hash_is_deterministic() {
        let pepper = b"test-pepper";
        let k = generate_key(pepper);
        assert_eq!(hash_key(pepper, &k.full), k.hash);
    }

    #[test]
    fn hash_changes_with_pepper() {
        let k = generate_key(b"pepper-a");
        let recomputed = hash_key(b"pepper-b", &k.full);
        assert_ne!(k.hash, recomputed);
    }

    #[test]
    fn looks_like_key_rejects_garbage() {
        assert!(!looks_like_key(""));
        assert!(!looks_like_key("hk_live_"));
        assert!(!looks_like_key("Bearer hk_live_aaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!looks_like_key("sk_live_aaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!looks_like_key("hk_live_aaaaa")); // too short
    }

    #[test]
    fn looks_like_key_accepts_real_keys() {
        let k = generate_key(b"x");
        assert!(looks_like_key(&k.full), "rejected: {}", k.full);
    }
}
