//! stdio transport: newline-delimited JSON-RPC over process stdin/stdout.

use crate::protocol::Response;
use crate::tools::McpServer;
use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

pub async fn run(server: McpServer) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let stdout = std::sync::Arc::new(Mutex::new(stdout));

    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        tracing::debug!(raw = %trimmed, "<-- stdio");

        // Handle batched requests (array of requests).
        let is_batch = trimmed.starts_with('[');
        let mut responses: Vec<String> = Vec::new();

        if is_batch {
            let reqs: Vec<serde_json::Value> = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    let r = Response::err(
                        crate::protocol::Id::Num(0),
                        crate::protocol::PARSE_ERROR,
                        e.to_string(),
                    );
                    responses.push(serde_json::to_string(&r).unwrap_or_default());
                    Vec::new()
                }
            };
            for req in reqs {
                let s = serde_json::to_string(&req).unwrap_or_default();
                if let Some(resp) = server.handle(&s).await {
                    responses.push(resp);
                }
            }
        } else if let Some(resp) = server.handle(trimmed).await {
            responses.push(resp);
        }

        if responses.is_empty() {
            continue;
        }

        let out = if is_batch {
            format!("[{}]", responses.join(","))
        } else {
            responses.into_iter().next().unwrap()
        };

        tracing::debug!(raw = %out, "--> stdio");
        {
            let mut so = stdout.lock().await;
            so.write_all(out.as_bytes()).await?;
            so.write_all(b"\n").await?;
            so.flush().await?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn _unused() {
    let _ = Response::ok(crate::protocol::Id::Num(0), serde_json::Value::Null);
}
