//! Embedded Web UI (HTML+JS+CSS, no build step).
//! Uses rust-embed to bake the assets into the binary at compile time.

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "src/ui_assets"]
struct UiAsset;

pub async fn index() -> Response {
    serve("index.html")
}

pub async fn asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    serve(path)
}

fn serve(path: &str) -> Response {
    match UiAsset::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, HeaderValue::from_str(mime.essence_str()).unwrap())],
                Body::from(file.data.into_owned()),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            "asset not found",
        )
            .into_response(),
    }
}
