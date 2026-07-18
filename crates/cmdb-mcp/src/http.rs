//! HTTP/SSE transport (Streamable HTTP variant).
//!
//! POST /mcp  -> JSON-RPC request in body, JSON-RPC response back in body.
//! GET  /mcp  -> opens an SSE stream; for P1 we only support the
//!               request/response shape; full SSE streaming arrives in P1.1
//!               when we wire NATS change events into server-pushed
//!               notifications.
//!
//! GET  /healthz -> {"ok": true}
//!
//! This is enough for fleet agents to call `tools/call` from anywhere on the
//! network. Fleet agents behind NATS can also use the ana-bridge RPC subject.

use crate::tools::McpServer;
use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    server: Arc<McpServer>,
}

pub async fn run(server: McpServer, addr: SocketAddr) -> Result<()> {
    let state = Arc::new(server);
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/mcp", post(handle_mcp))
        .route("/", get(info));
    // Note: addr is logged by the caller; we keep this silent to avoid dup.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.with_state(AppState { server: state })).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"ok": true})))
}

async fn info() -> impl IntoResponse {
    Json(serde_json::json!({
        "server": "helios-cmdb",
        "version": env!("CARGO_PKG_VERSION"),
        "endpoint": "/mcp",
        "protocol": "MCP/2025-06-18 (Streamable HTTP)",
    }))
}

async fn handle_mcp(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let raw = serde_json::to_string(&body).unwrap_or_default();
    tracing::debug!(raw = %raw, "<-- http");

    // Batch?
    if body.is_array() {
        let arr = body.as_array().cloned().unwrap_or_default();
        let mut responses = Vec::with_capacity(arr.len());
        for req in arr {
            let s = serde_json::to_string(&req).unwrap_or_default();
            if let Some(resp) = state.server.handle(&s).await {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp) {
                    responses.push(v);
                }
            }
        }
        return Json(serde_json::Value::Array(responses)).into_response();
    }

    match state.server.handle(&raw).await {
        Some(resp) => match serde_json::from_str::<serde_json::Value>(&resp) {
            Ok(v) => Json(v).into_response(),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response(),
        },
        None => Json(serde_json::json!({})).into_response(),
    }
}
