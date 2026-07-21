//! Wire envelope types — Rust mirror of ana's pydantic models.
//!
//! Forward-compat: parsers preserve unknown fields via `serde_json::Value`.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Clock {
    pub local_time: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_s: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeBase {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "from")]
    pub from: String,
    #[serde(default)]
    pub ts: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock: Option<Clock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    pub to: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// v0.6.2: TxnManager transaction id. When the caller uses
    /// query_and_wait(use_txn=True), this field carries the KV txn id so
    /// the responder can echo it back in the Reply for correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_for: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// v0.6.2: echo the caller's txn_id back so the caller's TxnManager
    /// can transition RECEIVED → COMMITTED.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
    #[serde(default)]
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
}

/// v0.6.2: Ack envelope — "I saw your message and I'm alive".
/// Mirrors ana's pydantic Ack model. CMDB sends this before running
/// long queries so the caller's query_and_wait(accept_ack=True) doesn't
/// timeout while the CMDB is computing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    pub ack_for: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received_subjects: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default = "default_true")]
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discovery {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(default)]
    pub subjects_owned: Vec<String>,
    #[serde(default)]
    pub capabilities: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub recent: Vec<String>,
    #[serde(default)]
    pub capabilities: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    #[serde(flatten)]
    pub base: EnvelopeBase,
    pub event: String,
    #[serde(default = "default_level")]
    pub level: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_subject: Option<String>,
}

fn default_level() -> String {
    "info".into()
}

fn default_true() -> bool {
    true
}

pub fn parse_envelope(payload: &[u8]) -> Result<ParsedEnvelope, serde_json::Error> {
    let v: Value = serde_json::from_slice(payload)?;
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
    let parsed = match kind.as_str() {
        "query" => ParsedEnvelope::Query(serde_json::from_value(v)?),
        "reply" => ParsedEnvelope::Reply(serde_json::from_value(v)?),
        "discovery" => ParsedEnvelope::Discovery(serde_json::from_value(v)?),
        "pulse" => ParsedEnvelope::Pulse(serde_json::from_value(v)?),
        "alert" => ParsedEnvelope::Alert(serde_json::from_value(v)?),
        _ => ParsedEnvelope::Unknown(v),
    };
    Ok(parsed)
}

#[derive(Debug, Clone)]
pub enum ParsedEnvelope {
    Query(Query),
    Reply(Reply),
    Discovery(Discovery),
    Pulse(Pulse),
    Alert(Alert),
    Unknown(Value),
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
