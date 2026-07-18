//! Pluggable text embedders for semantic search.
//!
//! Default strategy (`from_env`):
//!   1. If `CMDB_OLLAMA_URL` (or `OLLAMA_API_URL`) is set → OllamaEmbedder
//!   2. Else if `OPENAI_API_KEY` is set → OpenAiEmbedder
//!   3. Else → NoopEmbedder (returns zeros; search falls back to substring)
//!
//! Default Ollama model: `nomic-embed-text` (768 dims).
//! Override with `CMDB_EMBED_MODEL`.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
pub const DEFAULT_DIM: usize = 768;
pub const DEFAULT_OLLAMA_MODEL: &str = "nomic-embed-text";
pub const DEFAULT_OPENAI_MODEL: &str = "text-embedding-3-small";

#[async_trait]
pub trait Embedder: Send + Sync {
    fn name(&self) -> &str;
    fn dim(&self) -> usize;
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;
}

pub fn from_env() -> Box<dyn Embedder> {
    let ollama_url = std::env::var("CMDB_OLLAMA_URL")
        .or_else(|_| std::env::var("OLLAMA_API_URL"))
        .ok();
    if let Some(url) = ollama_url {
        let model =
            std::env::var("CMDB_EMBED_MODEL").unwrap_or_else(|_| DEFAULT_OLLAMA_MODEL.into());
        tracing::info!(%url, %model, "embedder: ollama");
        return Box::new(OllamaEmbedder::new(url, model));
    }
    if std::env::var("OPENAI_API_KEY").is_ok() {
        let model =
            std::env::var("CMDB_EMBED_MODEL").unwrap_or_else(|_| DEFAULT_OPENAI_MODEL.into());
        tracing::info!(%model, "embedder: openai");
        return Box::new(OpenAiEmbedder::new(model));
    }
    tracing::info!("embedder: noop (set CMDB_OLLAMA_URL or OPENAI_API_KEY for semantic search)");
    Box::new(NoopEmbedder::new(DEFAULT_DIM))
}

pub fn text_for_entity(e: &cmdb_core::Entity) -> String {
    let mut parts: Vec<String> = vec![e.entity_type.clone(), e.name.clone()];
    if let serde_json::Value::Object(m) = &e.attrs {
        for (k, v) in m {
            parts.push(k.clone());
            match v {
                serde_json::Value::String(s) => parts.push(s.clone()),
                other => parts.push(other.to_string().trim_matches('"').to_string()),
            }
        }
    }
    for tag in &e.tags {
        parts.push(tag.clone());
    }
    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Ollama
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OllamaEmbedder {
    url: String,
    model: String,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct OllamaReq<'a> {
    model: &'a str,
    prompt: &'a str,
}

#[derive(Deserialize)]
struct OllamaResp {
    embedding: Vec<f32>,
}

impl OllamaEmbedder {
    pub fn new(url: String, model: String) -> Self {
        Self {
            url,
            model,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Embedder for OllamaEmbedder {
    fn name(&self) -> &str {
        "ollama"
    }
    fn dim(&self) -> usize {
        // The actual dim comes from the model; we let it return whatever it
        // returns and trust the migration to match. For nomic-embed-text this
        // is 768.
        DEFAULT_DIM
    }
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let req = OllamaReq {
            model: &self.model,
            prompt: text,
        };
        let resp: OllamaResp = self
            .client
            .post(format!("{}/api/embeddings", self.url.trim_end_matches('/')))
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.embedding)
    }
}

// ---------------------------------------------------------------------------
// OpenAI
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OpenAiEmbedder {
    model: String,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct OpenAiReq<'a> {
    model: &'a str,
    input: &'a str,
}

#[derive(Deserialize)]
struct OpenAiResp {
    data: Vec<OpenAiDatum>,
}

#[derive(Deserialize)]
struct OpenAiDatum {
    embedding: Vec<f32>,
}

impl OpenAiEmbedder {
    pub fn new(model: String) -> Self {
        Self {
            model,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Embedder for OpenAiEmbedder {
    fn name(&self) -> &str {
        "openai"
    }
    fn dim(&self) -> usize {
        match self.model.as_str() {
            "text-embedding-3-small" => 1536,
            "text-embedding-3-large" => 3072,
            "text-embedding-ada-002" => 1536,
            _ => DEFAULT_DIM,
        }
    }
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let req = OpenAiReq {
            model: &self.model,
            input: text,
        };
        let key = std::env::var("OPENAI_API_KEY")?;
        let resp: OpenAiResp = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .bearer_auth(key)
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        resp.data
            .into_iter()
            .next()
            .map(|d| d.embedding)
            .ok_or_else(|| anyhow::anyhow!("openai returned no embeddings"))
    }
}

// ---------------------------------------------------------------------------
// Noop
// ---------------------------------------------------------------------------

pub struct NoopEmbedder {
    dim: usize,
}

impl NoopEmbedder {
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

#[async_trait]
impl Embedder for NoopEmbedder {
    fn name(&self) -> &str {
        "noop"
    }
    fn dim(&self) -> usize {
        self.dim
    }
    async fn embed(&self, _text: &str) -> Result<Vec<f32>> {
        Ok(vec![0.0; self.dim])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_for_entity_includes_relevant_fields() {
        let e = cmdb_core::Entity {
            id: cmdb_core::EntityId::new(),
            namespace: "ns".into(),
            entity_type: "fleet.host".into(),
            name: "miraku-home".into(),
            attrs: serde_json::json!({"os": "nixos", "cpus": 8}),
            tags: ["nixos".to_string(), "primary".to_string()].into_iter().collect(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            version: 1,
        };
        let s = text_for_entity(&e);
        assert!(s.contains("fleet.host"));
        assert!(s.contains("miraku-home"));
        assert!(s.contains("nixos"));
        assert!(s.contains("primary"));
        assert!(s.contains("cpus"));
        assert!(s.contains("8"));
    }

    #[tokio::test]
    async fn noop_returns_zeros() {
        let e = NoopEmbedder::new(4);
        let v = e.embed("hi").await.unwrap();
        assert_eq!(v, vec![0.0; 4]);
        assert_eq!(e.dim(), 4);
    }
}
