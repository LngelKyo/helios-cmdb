//! helios-cmdb MCP server.
//!
//! Implements the Model Context Protocol over two transports:
//!   - stdio (newline-delimited JSON-RPC; for Claude Code, Cursor, etc.)
//!   - HTTP + SSE (Streamable HTTP transport; for remote fleet agents)
//!
//! Tool list (11 implemented for P1; `cypher` deferred to P3 with Apache AGE):
//!   list_types | describe_type | get_entity | query | search | traverse
//!   | upsert_entity | upsert_fact | relate | unrelate | history

pub mod http;
pub mod protocol;
pub mod stdio;
pub mod tools;

pub use tools::{serve_http, serve_stdio};
