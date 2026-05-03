//! `heimdall admin …` — bootstrap user + key management before the website
//! lands. Operates directly on the same SQLite file that the API server uses.
//!
//! All commands honor `HEIMDALL_AUTH_DB` (default `/var/lib/heimdall/auth.db`)
//! and `HEIMDALL_KEY_PEPPER` (must be set; we'll never silently mint a key
//! against an unknown pepper).

use std::path::PathBuf;

use clap::Subcommand;
use heimdall_auth::{
    key::generate_key,
    store::{KeyStore, NewKey},
    tier::Tier,
};

use crate::auth::DEFAULT_DB_PATH;

#[derive(Subcommand)]
pub enum AdminCmd {
    /// Generate a fresh 32-byte hex pepper for HEIMDALL_KEY_PEPPER.
    /// Run this ONCE per deployment, then store the value in /etc/heimdall/auth.env
    /// and your password manager. Rotating it later invalidates every key.
    PepperGenerate,

    /// User management
    #[command(subcommand)]
    User(UserCmd),

    /// API key management
    #[command(subcommand)]
    Key(KeyCmd),
}

#[derive(Subcommand)]
pub enum UserCmd {
    /// Create a user (or no-op if email already exists).
    Create {
        #[arg(long)]
        email: String,
        #[arg(long, default_value = "free")]
        tier: String,
    },
    /// List all users.
    List,
    /// Update a user's tier.
    SetTier {
        #[arg(long)]
        email: String,
        #[arg(long)]
        tier: String,
    },
}

#[derive(Subcommand)]
pub enum KeyCmd {
    /// Create a new API key for an existing user.
    /// Prints the full key to stdout ONCE; the user must save it immediately.
    Create {
        #[arg(long)]
        email: String,
        #[arg(long)]
        name: Option<String>,
    },
    /// List all keys (optionally filter to one user).
    List {
        #[arg(long)]
        email: Option<String>,
    },
    /// Revoke a key by its prefix (e.g. `hk_live_8K3p`).
    Revoke {
        #[arg(long)]
        prefix: String,
    },
    /// Override per-key RPS (omit `--rps` to clear and fall back to tier default).
    SetRateLimit {
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        rps: Option<u32>,
    },
    /// Override per-key monthly quota (omit `--monthly` to clear).
    SetQuota {
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        monthly: Option<u64>,
    },
}

pub fn run(cmd: AdminCmd) -> anyhow::Result<()> {
    match cmd {
        AdminCmd::PepperGenerate => {
            use rand::RngCore;
            let mut bytes = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut bytes);
            println!("{}", hex::encode(bytes));
            eprintln!();
            eprintln!("# Save the line above to /etc/heimdall/auth.env as:");
            eprintln!("#   HEIMDALL_KEY_PEPPER=<value>");
            eprintln!("# Permissions must be 0600 root:root.");
            eprintln!("# Also save it to your password manager — there is no recovery.");
            return Ok(());
        }
        _ => {}
    }

    let pepper = require_pepper()?;
    let store = open_store()?;

    match cmd {
        AdminCmd::PepperGenerate => unreachable!(),

        AdminCmd::User(UserCmd::Create { email, tier }) => {
            let _: Tier = tier.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            let id = store.upsert_user(&email, &tier)?;
            println!("user {id}: {email} ({tier})");
        }
        AdminCmd::User(UserCmd::List) => {
            let users = store.list_users()?;
            if users.is_empty() {
                println!("(no users)");
            } else {
                println!("{:>4}  {:<32}  {:<10}  {:<22}  blocked", "id", "email", "tier", "stripe_customer");
                for u in users {
                    println!(
                        "{:>4}  {:<32}  {:<10}  {:<22}  {}",
                        u.id,
                        u.email,
                        u.tier,
                        u.stripe_customer_id.as_deref().unwrap_or("-"),
                        if u.blocked_at.is_some() { "yes" } else { "no" }
                    );
                }
            }
        }
        AdminCmd::User(UserCmd::SetTier { email, tier }) => {
            let _: Tier = tier.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            let user = store
                .get_user_by_email(&email)?
                .ok_or_else(|| anyhow::anyhow!("no user with email {email}"))?;
            store.set_user_tier(user.id, &tier)?;
            println!("user {} tier -> {tier}", user.id);
        }

        AdminCmd::Key(KeyCmd::Create { email, name }) => {
            let user = store
                .get_user_by_email(&email)?
                .ok_or_else(|| anyhow::anyhow!("no user with email {email}; create one first"))?;
            let key = generate_key(&pepper);
            store.insert_key(NewKey {
                user_id: user.id,
                key_hash: key.hash,
                key_prefix: key.prefix.clone(),
                key_last4: key.last4.clone(),
                name,
            })?;
            println!();
            println!("API key for {} ({}):", user.email, user.tier);
            println!();
            println!("    {}", key.full);
            println!();
            println!("This is the ONLY time the full key is shown. Save it now.");
        }
        AdminCmd::Key(KeyCmd::List { email }) => {
            let mut keys = if let Some(e) = email {
                let user = store
                    .get_user_by_email(&e)?
                    .ok_or_else(|| anyhow::anyhow!("no user with email {e}"))?;
                store.list_keys_for_user(user.id)?
            } else {
                store.load_active_keys()?
            };
            keys.sort_by_key(|k| k.id);
            if keys.is_empty() {
                println!("(no keys)");
                return Ok(());
            }
            println!("{:>4}  {:<16}  {:<10}  {:<8}  {:<8}  {}", "id", "prefix...last4", "tier", "rps", "quota", "name");
            for k in keys {
                let display = format!("{}…{}", k.key_prefix, k.key_last4);
                let rps = k.rate_limit_rps_override.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
                let q = k.monthly_quota_override.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
                println!(
                    "{:>4}  {:<16}  {:<10}  {:<8}  {:<8}  {}",
                    k.id,
                    display,
                    k.user_tier,
                    rps,
                    q,
                    k.name.as_deref().unwrap_or("-"),
                );
            }
        }
        AdminCmd::Key(KeyCmd::Revoke { prefix }) => {
            let n = store.revoke_key_by_prefix(&prefix)?;
            if n == 0 {
                anyhow::bail!("no active key matched prefix '{prefix}'");
            }
            println!("revoked {n} key(s) matching prefix '{prefix}'");
        }
        AdminCmd::Key(KeyCmd::SetRateLimit { prefix, rps }) => {
            let key = find_key_by_prefix(&store, &prefix)?;
            store.set_rate_limit(key.id, rps)?;
            println!(
                "key {} rate_limit_rps -> {}",
                key.id,
                rps.map(|v| v.to_string()).unwrap_or_else(|| "tier default".into())
            );
        }
        AdminCmd::Key(KeyCmd::SetQuota { prefix, monthly }) => {
            let key = find_key_by_prefix(&store, &prefix)?;
            store.set_monthly_quota(key.id, monthly)?;
            println!(
                "key {} monthly_quota -> {}",
                key.id,
                monthly.map(|v| v.to_string()).unwrap_or_else(|| "tier default".into())
            );
        }
    }
    Ok(())
}

fn require_pepper() -> anyhow::Result<Vec<u8>> {
    let s = std::env::var("HEIMDALL_KEY_PEPPER")
        .map_err(|_| anyhow::anyhow!(
            "HEIMDALL_KEY_PEPPER is not set. Run `heimdall admin pepper-generate` first."))?;
    let bytes = hex::decode(s.trim())
        .map_err(|e| anyhow::anyhow!("HEIMDALL_KEY_PEPPER must be hex: {e}"))?;
    if bytes.len() < 16 {
        anyhow::bail!("HEIMDALL_KEY_PEPPER too short ({} bytes); need ≥ 16", bytes.len());
    }
    Ok(bytes)
}

fn open_store() -> anyhow::Result<KeyStore> {
    let path: PathBuf = std::env::var("HEIMDALL_AUTH_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_DB_PATH));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(KeyStore::open(&path)?)
}

fn find_key_by_prefix(
    store: &KeyStore,
    prefix: &str,
) -> anyhow::Result<heimdall_auth::store::ApiKeyRow> {
    let all = store.load_active_keys()?;
    let matches: Vec<_> = all.into_iter().filter(|k| k.key_prefix == prefix).collect();
    match matches.len() {
        0 => anyhow::bail!("no active key with prefix '{prefix}'"),
        1 => Ok(matches.into_iter().next().unwrap()),
        n => anyhow::bail!("{n} keys match prefix '{prefix}' — be more specific"),
    }
}
