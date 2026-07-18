//! Scoped API tokens for helios-cmdb.
//!
//! Token format: `cmdb_<id_ulid>.<secret_base64>` — id portion is stored
//! in plaintext (used for DB lookup), secret is hashed via SHA-256.
//!
//! Tokens carry:
//!   - identity: becomes source.identity on writes
//!   - namespace_scope: which namespaces the token can touch (empty = all)
//!   - op_scope: 'read' | 'write' | 'admin' (empty = all)
//!
//! Usage:
//!   let mgr = TokenManager::new(pool);
//!   let token = mgr.create(CreateToken { ... }).await?;  // returns full secret ONCE
//!   let principal = mgr.verify(&raw_token_string).await?;  // Principal

use axum::extract::FromRequestParts;
use axum::http::Request;
use axum::http::{request::Parts, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, middleware::Next, response::Response as AxumResponse};
use base64::Engine;
use chrono::{DateTime, Utc};
use cmdb_core::id::EntityId;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;

pub const PREFIX: &str = "cmdb_";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: String,
    pub identity: String,
    pub namespace_scope: Vec<String>,
    pub op_scope: Vec<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateToken {
    pub identity: String,
    pub namespace_scope: Vec<String>,
    pub op_scope: Vec<String>,
    pub description: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Result of token creation: the Token metadata + the raw secret string
/// (only seen at creation time).
#[derive(Debug, Clone)]
pub struct CreatedToken {
    pub token: Token,
    pub raw: String,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub identity: String,
    pub namespace_scope: BTreeSet<String>,
    pub op_scope: BTreeSet<String>,
    pub token_id: String,
}

impl Principal {
    pub fn can_read(&self, namespace: &str) -> bool {
        self.has_op("read") && self.in_namespace(namespace)
    }
    pub fn can_write(&self, namespace: &str) -> bool {
        self.has_op("write") && self.in_namespace(namespace)
    }
    pub fn is_admin(&self) -> bool {
        self.has_op("admin")
    }
    fn has_op(&self, op: &str) -> bool {
        self.op_scope.is_empty() || self.op_scope.contains(op)
    }
    fn in_namespace(&self, ns: &str) -> bool {
        self.namespace_scope.is_empty() || self.namespace_scope.contains(ns)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    Missing,
    #[error("malformed Authorization header: expected 'Bearer cmdb_...'")]
    Malformed,
    #[error("token not found")]
    NotFound,
    #[error("token revoked")]
    Revoked,
    #[error("token expired")]
    Expired,
    #[error("token secret mismatch")]
    BadSecret,
    #[error("database error: {0}")]
    Db(String),
}

impl From<sqlx::Error> for AuthError {
    fn from(e: sqlx::Error) -> Self {
        AuthError::Db(e.to_string())
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let code = match &self {
            AuthError::Missing | AuthError::Malformed => StatusCode::UNAUTHORIZED,
            AuthError::NotFound | AuthError::Revoked | AuthError::Expired | AuthError::BadSecret => {
                StatusCode::UNAUTHORIZED
            }
            AuthError::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, format!("{{\"error\":\"{}\"}}", self)).into_response()
    }
}

#[derive(Clone)]
pub struct TokenManager {
    pool: PgPool,
}

impl TokenManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, input: CreateToken) -> Result<CreatedToken, AuthError> {
        let id = EntityId::new();
        let secret = generate_secret(32);
        let hash = hash_secret(&secret);
        let raw = format!("{}{}.{}", PREFIX, id.as_ulid(), secret);

        sqlx::query(
            r#"INSERT INTO api_tokens
                 (id, secret_hash, identity, namespace_scope, op_scope, description, created_at, expires_at)
               VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)"#,
        )
        .bind(id.as_uuid())
        .bind(&hash)
        .bind(&input.identity)
        .bind(&input.namespace_scope)
        .bind(&input.op_scope)
        .bind(&input.description)
        .bind(input.expires_at)
        .execute(&self.pool)
        .await?;

        let token = Token {
            id: id.as_ulid().to_string(),
            identity: input.identity,
            namespace_scope: input.namespace_scope,
            op_scope: input.op_scope,
            description: input.description,
            created_at: Utc::now(),
            expires_at: input.expires_at,
            revoked_at: None,
            last_used_at: None,
        };
        Ok(CreatedToken { token, raw })
    }

    pub async fn verify(&self, raw: &str) -> Result<Principal, AuthError> {
        let rest = raw.strip_prefix(PREFIX).ok_or(AuthError::Malformed)?;
        let (id_str, secret) = rest.split_once('.').ok_or(AuthError::Malformed)?;
        let id: EntityId = id_str.parse().map_err(|_| AuthError::Malformed)?;

        let row = sqlx::query(
            r#"SELECT secret_hash, identity, namespace_scope, op_scope, expires_at, revoked_at
               FROM api_tokens WHERE id = $1"#,
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AuthError::NotFound)?;

        let row: TokenRow = map_token_row(&row);

        if let Some(rev) = row.revoked_at {
            tracing::warn!(token_id = %id_str, revoked_at = ?rev, "token revoked");
            return Err(AuthError::Revoked);
        }
        if let Some(exp) = row.expires_at {
            if exp < Utc::now() {
                return Err(AuthError::Expired);
            }
        }

        let expected_hash = hash_secret(secret);
        if expected_hash != row.secret_hash {
            return Err(AuthError::BadSecret);
        }

        let _ = sqlx::query("UPDATE api_tokens SET last_used_at = NOW() WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await;

        Ok(Principal {
            identity: row.identity,
            namespace_scope: row.namespace_scope.into_iter().collect(),
            op_scope: row.op_scope.into_iter().collect(),
            token_id: id_str.to_string(),
        })
    }

    pub async fn list(&self) -> Result<Vec<Token>, AuthError> {
        let rows = sqlx::query(
            r#"SELECT id, identity, namespace_scope, op_scope, description,
                      created_at, expires_at, revoked_at, last_used_at
               FROM api_tokens ORDER BY created_at DESC"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| Token {
            id: EntityId::from_uuid(r.get("id")).as_ulid().to_string(),
            identity: r.get("identity"),
            namespace_scope: r.get("namespace_scope"),
            op_scope: r.get("op_scope"),
            description: r.get("description"),
            created_at: r.get("created_at"),
            expires_at: r.get("expires_at"),
            revoked_at: r.get("revoked_at"),
            last_used_at: r.get("last_used_at"),
        }).collect())
    }

    pub async fn revoke(&self, id_str: &str) -> Result<(), AuthError> {
        let id: EntityId = id_str.parse().map_err(|_| AuthError::Malformed)?;
        let res = sqlx::query(
            "UPDATE api_tokens SET revoked_at = NOW() WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(AuthError::NotFound);
        }
        Ok(())
    }
}

struct TokenRow {
    secret_hash: String,
    identity: String,
    namespace_scope: Vec<String>,
    op_scope: Vec<String>,
    expires_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
}

fn map_token_row(row: &sqlx::postgres::PgRow) -> TokenRow {
    use sqlx::Row;
    TokenRow {
        secret_hash: row.get("secret_hash"),
        identity: row.get("identity"),
        namespace_scope: row.get("namespace_scope"),
        op_scope: row.get("op_scope"),
        expires_at: row.get("expires_at"),
        revoked_at: row.get("revoked_at"),
    }
}

fn generate_secret(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// Axum extractor: pulls Principal out of request extensions (set by the
/// auth middleware). Returns 401 if not present.
#[derive(Clone, Debug)]
pub struct Auth(pub Principal);

impl<S: Send + Sync> FromRequestParts<S> for Auth {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Principal>()
            .cloned()
            .map(Auth)
            .ok_or(AuthError::Missing)
    }
}

/// Middleware: read Bearer token, verify via TokenManager, insert Principal
/// into request extensions. Mounted only on routes that require auth.
pub async fn require_token(
    State(mgr): State<TokenManager>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Result<AxumResponse, AuthError> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(AuthError::Missing)?;
    let raw = header.strip_prefix("Bearer ").ok_or(AuthError::Malformed)?;
    let principal = mgr.verify(raw).await?;
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

#[allow(dead_code)]
fn _avoid_unused() {
    let _: StatusCode = StatusCode::OK;
    let _: Option<Response> = None;
}
