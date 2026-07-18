//! ssh-facts collector: ssh into each target host and gather basic facts
//! (kernel, cpus, memory, disk, docker containers).
//!
//! Implementation shells out to system `ssh` for simplicity in P1. P2 may
//! switch to `russh` for pure-Rust sessions and key management.
//!
//! Gathered facts become `fleet.host` entities (upserted) plus `Fact`
//! observations on those entities with source.kind=Collector.

use crate::CollectorConfig;
use anyhow::{anyhow, Result};
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::source::{Source, SourceKind, Transport};
use cmdb_core::Store;
use chrono::Utc;
use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;pub async fn run(store: Arc<dyn Store>, cfg: CollectorConfig) -> Result<()> {
    if cfg.targets.is_empty() {
        return Err(anyhow!("ssh-facts: --targets is required (comma-separated hosts)"));
    }
    let user = cfg.ssh_user.as_deref().unwrap_or("helios");

    loop {
        for host in &cfg.targets {
            if let Err(e) = collect_one(&store, &cfg, host, user).await {
                tracing::warn!(host = %host, error = %e, "ssh-facts: collect failed");
            }
        }
        if cfg.interval_seconds == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_seconds)).await;
    }
    Ok(())
}

async fn collect_one(
    store: &Arc<dyn Store>,
    cfg: &CollectorConfig,
    host: &str,
    user: &str,
) -> Result<()> {
    tracing::info!(host = %host, "ssh-facts: collecting");

    let facts = ssh_collect(host, user, cfg.ssh_port)?;
    if facts.hostname.is_empty() {
        return Ok(());
    }

    // Upsert the host entity first.
    let attrs = json!({
        "hostname": facts.hostname,
        "os": facts.os,
        "kernel": facts.kernel,
        "arch": facts.arch,
        "cpus": facts.cpus,
        "mem_total_kb": facts.mem_total_kb,
    });
    let input = EntityInput::new(&cfg.namespace, "fleet.host", &facts.hostname)
        .with_attrs(attrs)
        .with_tags([host.to_string()]);
    let source = make_source(cfg, host);
    let entity = store.put_entity(input, source.clone()).await?;

    // Record time-varying observations as Facts with ttl.
    for (key, value) in facts.into_observations() {
        let _ = store
            .add_fact(FactInput {
                namespace: cfg.namespace.clone(),
                entity: EntityRef::by_id(entity.id),
                key,
                value,
                source: Source {
                    confidence: 0.9,
                    ttl_seconds: Some(900),
                    ..source.clone()
                },
            })
            .await?;
    }

    Ok(())
}

#[derive(Debug, Default)]
struct HostFacts {
    hostname: String,
    os: String,
    kernel: String,
    arch: String,
    cpus: u64,
    mem_total_kb: u64,
    load_1: Option<f64>,
    disk_used_pct: Option<f64>,
    docker_containers: Vec<String>,
}

impl HostFacts {
    fn into_observations(self) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        if let Some(load) = self.load_1 {
            out.push(("load_1".to_string(), json!(load)));
        }
        if let Some(disk) = self.disk_used_pct {
            out.push(("disk_used_pct".to_string(), json!(disk)));
        }
        if !self.docker_containers.is_empty() {
            out.push((
                "docker_containers".to_string(),
                json!(self.docker_containers),
            ));
        }
        out
    }
}

fn ssh_collect(host: &str, user: &str, port: u16) -> Result<HostFacts> {
    let target = format!("{user}@{host}");
    // The remote login shell may be fish, csh, or anything else — we don't
    // want our script to be interpreted by it. Instead invoke `sh -s`
    // explicitly and feed the script via stdin. sh exists on every Linux
    // and gives us a consistent POSIX shell regardless of the user's login
    // shell. (Helios runs fish as default; older script via `ssh host
    // '<script>'` interpreted the script with fish and broke on (..).)
    let script = r####"
echo "###HOSTNAME###"; hostname
echo "###UNAME###"; uname -srm
echo "###NPROC###"; nproc 2>/dev/null || echo 0
echo "###MEMTOTAL###"; awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo 0
echo "###LOAD###"; cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo 0
echo "###DISK###"; df -P / 2>/dev/null | awk 'NR==2 {gsub("%",""); print $5}' || echo 0
echo "###DOCKER###"; (command -v docker >/dev/null && docker ps --format '{{.Names}}' 2>/dev/null) || true
"####;

    let mut child = Command::new("ssh")
        .args([
            "-p", &port.to_string(),
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=accept-new",
            &target,
            "sh", "-s",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(script.as_bytes());
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ssh failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_facts(&stdout))
}

fn parse_facts(stdout: &str) -> HostFacts {
    let mut f = HostFacts::default();
    let mut section = "";
    let mut docker_names = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("###").and_then(|s| s.strip_suffix("###")) {
            section = name;
            continue;
        }
        match section {
            "HOSTNAME" => f.hostname = line.to_string(),
            "UNAME" => {
                let mut parts = line.split_whitespace();
                f.os = parts.next().unwrap_or("").to_string();
                f.kernel = parts.next().unwrap_or("").to_string();
                f.arch = parts.next().unwrap_or("").to_string();
            }
            "NPROC" => f.cpus = line.parse().unwrap_or(0),
            "MEMTOTAL" => f.mem_total_kb = line.parse().unwrap_or(0),
            "LOAD" => f.load_1 = line.parse().ok(),
            "DISK" => f.disk_used_pct = line.parse().ok(),
            "DOCKER" => docker_names.push(line.to_string()),
            _ => {}
        }
    }
    f.docker_containers = docker_names.into_iter().filter(|s| !s.is_empty()).collect();
    f
}

fn make_source(cfg: &CollectorConfig, host: &str) -> Source {
    Source {
        kind: SourceKind::Collector,
        identity: format!("collector.ssh-facts.{}", host),
        transport: Transport::Cli,
        nats_subject: None,
        observed_at: Utc::now(),
        confidence: 0.9,
        ttl_seconds: None,
        sig: None,
        evidence_ref: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_facts_from_fixture() {
        let fixture = r#"
###HOSTNAME###
miraku-home
###UNAME###
Linux 6.6.71 x86_64
###NPROC###
8
###MEMTOTAL###
32795040
###LOAD###
0.42
###DISK###
73
###DOCKER###
cmdb-pg
nats
"#;
        let f = parse_facts(fixture);
        assert_eq!(f.hostname, "miraku-home");
        assert_eq!(f.os, "Linux");
        assert_eq!(f.kernel, "6.6.71");
        assert_eq!(f.arch, "x86_64");
        assert_eq!(f.cpus, 8);
        assert_eq!(f.mem_total_kb, 32795040);
        assert_eq!(f.load_1, Some(0.42));
        assert_eq!(f.disk_used_pct, Some(73.0));
        assert_eq!(f.docker_containers, vec!["cmdb-pg".to_string(), "nats".to_string()]);
    }
}
