//! Provenance: every Fact carries a `Source` describing who/what observed it,
//! when, with what confidence, and how long it stays fresh.
//!
//! Trust comes from the transport layer (NATS ACLs, mTLS, MCP auth), not from
//! envelope signatures — ana v0.1 has none. The `sig` slot is reserved so
//! adding ed25519 in ana v0.2 is non-breaking.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Collector,
    Agent,
    Human,
    Inferred,
}

impl SourceKind {
    pub fn default_confidence(self) -> f32 {
        match self {
            SourceKind::Human => 1.0,
            SourceKind::Collector => 0.9,
            SourceKind::Agent => 0.7,
            SourceKind::Inferred => 0.5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Nats,
    Mcp,
    Http,
    Cli,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub algo: String,
    pub value: String,
    pub key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "ref")]
pub enum EvidenceRef {
    Ulid(String),
    NatsMsg { subject: String, seq: u64 },
    FilePath(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub kind: SourceKind,
    pub identity: String,
    pub transport: Transport,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub nats_subject: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub confidence: f32,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ttl_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sig: Option<Signature>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub evidence_ref: Option<EvidenceRef>,
}

impl Source {
    pub fn new_cli(identity: impl Into<String>) -> Self {
        Self {
            kind: SourceKind::Human,
            identity: identity.into(),
            transport: Transport::Cli,
            nats_subject: None,
            observed_at: Utc::now(),
            confidence: SourceKind::Human.default_confidence(),
            ttl_seconds: None,
            sig: None,
            evidence_ref: None,
        }
    }

    pub fn new_agent(identity: impl Into<String>) -> Self {
        Self {
            kind: SourceKind::Agent,
            identity: identity.into(),
            transport: Transport::Nats,
            nats_subject: None,
            observed_at: Utc::now(),
            confidence: SourceKind::Agent.default_confidence(),
            ttl_seconds: None,
            sig: None,
            evidence_ref: None,
        }
    }

    pub fn with_confidence(mut self, c: f32) -> Self {
        self.confidence = c.clamp(0.0, 1.0);
        self
    }

    pub fn with_ttl_seconds(mut self, s: i64) -> Self {
        self.ttl_seconds = Some(s);
        self
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        match self.ttl_seconds {
            Some(ttl) if ttl > 0 => now.signed_duration_since(self.observed_at).num_seconds() > ttl,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_serde_roundtrip() {
        let s = Source::new_cli("user:helios").with_confidence(0.9).with_ttl_seconds(3600);
        let json = serde_json::to_string(&s).unwrap();
        let back: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(back.identity, "user:helios");
        assert_eq!(back.confidence, 0.9);
        assert_eq!(back.ttl_seconds, Some(3600));
    }

    #[test]
    fn ttl_expiry() {
        let mut s = Source::new_cli("x");
        s.observed_at = Utc::now() - chrono::Duration::seconds(7200);
        s.ttl_seconds = Some(3600);
        assert!(s.is_expired(Utc::now()));

        let s2 = Source::new_cli("y");
        assert!(!s2.is_expired(Utc::now()));
    }
}
