//! Anonymous (no API key) per-IP rate limiter.
//!
//! Backed by `governor`'s keyed in-memory limiter. We bucket IPs by /32 (IPv4)
//! or /64 (IPv6) so a single offender can't burn unique IPs from a /64 block.
//!
//! Default: 60 requests/minute. Configurable via `tier::ANON_RATE_PER_MINUTE`.

use std::net::IpAddr;
use std::num::NonZeroU32;

use governor::{Quota, RateLimiter, clock::DefaultClock};
use governor::state::keyed::DashMapStateStore;

type IpLimiter = RateLimiter<u128, DashMapStateStore<u128>, DefaultClock>;

pub struct AnonLimiter {
    inner: IpLimiter,
}

impl AnonLimiter {
    pub fn new(per_minute: u32) -> Self {
        let n = NonZeroU32::new(per_minute.max(1)).unwrap();
        let quota = Quota::per_minute(n).allow_burst(n);
        Self { inner: RateLimiter::dashmap(quota) }
    }

    /// Returns `Ok(())` if the request is allowed, `Err(())` if rate-limited.
    pub fn check(&self, ip: IpAddr) -> Result<(), ()> {
        let key = bucket_key(ip);
        self.inner.check_key(&key).map_err(|_| ())
    }
}

/// Map IPv4 → /32 (full address), IPv6 → /64 prefix. Returned as u128 for a
/// single keyspace.
fn bucket_key(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u32::from(v4) as u128,
        IpAddr::V6(v6) => {
            // Take the high 64 bits (prefix); zero the low 64.
            let bits = u128::from(v6);
            bits & 0xFFFF_FFFF_FFFF_FFFF_0000_0000_0000_0000u128
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn limiter_blocks_after_burst() {
        let lim = AnonLimiter::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_err());
    }

    #[test]
    fn separate_ips_have_separate_buckets() {
        let lim = AnonLimiter::new(1);
        let a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(lim.check(a).is_ok());
        assert!(lim.check(b).is_ok());
        assert!(lim.check(a).is_err());
        assert!(lim.check(b).is_err());
    }
}
