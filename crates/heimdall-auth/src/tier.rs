//! Tier definitions and defaults.
//!
//! Tiers are stored as strings in the DB (`'free'` / `'pro'` / `'enterprise'`)
//! and looked up here for default RPS + monthly quota. Per-key overrides on
//! `api_keys.rate_limit_rps` and `api_keys.monthly_quota` win over tier defaults.

use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Free,
    Pro,
    Enterprise,
}

#[derive(Debug, Clone, Copy)]
pub struct TierLimits {
    pub rate_limit_rps: u32,
    pub monthly_quota: u64,
}

impl Tier {
    pub fn defaults(self) -> TierLimits {
        match self {
            Tier::Free => TierLimits {
                rate_limit_rps: 10,
                monthly_quota: 100_000,
            },
            Tier::Pro => TierLimits {
                rate_limit_rps: 100,
                monthly_quota: 1_000_000,
            },
            Tier::Enterprise => TierLimits {
                rate_limit_rps: 500,
                monthly_quota: u64::MAX,
            },
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Free => "free",
            Tier::Pro => "pro",
            Tier::Enterprise => "enterprise",
        }
    }
}

impl FromStr for Tier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "free" => Ok(Tier::Free),
            "pro" => Ok(Tier::Pro),
            "enterprise" => Ok(Tier::Enterprise),
            other => Err(format!("unknown tier '{other}'")),
        }
    }
}

/// Anonymous (no API key) per-IP limit.
pub const ANON_RATE_PER_MINUTE: u32 = 60;
