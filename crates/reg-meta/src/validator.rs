//! The cache validators of `serve`: every GET operation and download sits behind
//! [`guard`], which answers a matching `If-None-Match` with a 304 before the route
//! runs, so a revalidation does no DB work and no serialization
//! (`reg_webapp/DESIGN.md`, "`ETag` / `Cache-Control`").
//!
//! A response is a pure function of the server binary, the admitted catalog and the
//! request URL, and none of them changes for the process's lifetime. So the `ETag` is
//! derived from a boot-time token over the first two and the request's path and
//! query, not from the body. The token hashes the files' bytes, not their identity
//! fields: a catalog's `generation_id` does not cover `catalog_version`, which reaches
//! responses. The image hashes them when it is built and hands the digest over in
//! [`CONTENT_DIGEST_ENV`]: on Fly's throttled rootfs (about 8 MiB/s cold) hashing the
//! catalog at boot would outlast the entrypoint's readiness deadline.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use reg_catalog::hex;
use reg_catalog::ops::{Cache, Server};
use sha2::{Digest, Sha256};

use crate::{EDGE_VERSION_PARAM, VERSION};

/// The SHA-256 the image computed over the served binary and database
/// (`reg_webapp/Dockerfile`); unset, `serve` hashes them itself at boot.
const CONTENT_DIGEST_ENV: &str = "REG_META_CONTENT_DIGEST";

pub struct Validators {
    /// The `ETag`'s readable head: the server version and the generation.
    head: String,
    /// The SHA-256 of everything a response depends on besides its request.
    token: [u8; 32],
}

impl Validators {
    /// The validators of one server process. The binary's own digest stands for the
    /// code: an unreleased image change keeps `VERSION` but must not answer 304 to
    /// a body it no longer serves.
    ///
    /// # Panics
    ///
    /// When [`CONTENT_DIGEST_ENV`] is set but not 64 lowercase hex digits, or a served
    /// file cannot be read.
    pub fn new(server: &Server) -> Arc<Self> {
        let content = if let Ok(digest) = std::env::var(CONTENT_DIGEST_ENV) {
            assert!(
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "{CONTENT_DIGEST_ENV} is not a SHA-256 hex digest"
            );
            digest.into_bytes()
        } else {
            let exe = std::env::current_exe().expect("the server binary's path");
            let mut files = Sha256::new();
            files.update(file_digest(&exe));
            files.update(file_digest(server.catalog.path()));
            files.finalize().to_vec()
        };
        Arc::new(Self {
            head: format!("{VERSION}-{}", server.catalog.generation()),
            token: Sha256::digest(&content).into(),
        })
    }

    /// The strong `ETag` of a GET of `uri`: the boot token and the request's path and
    /// query, without the edge's cache-generation parameter (an edge redeploy must
    /// not invalidate a browser's copy of an unchanged answer).
    fn etag(&self, uri: &Uri) -> String {
        let query = uri
            .query()
            .unwrap_or_default()
            .split('&')
            .filter(|pair| pair.split('=').next() != Some(EDGE_VERSION_PARAM))
            .collect::<Vec<_>>()
            .join("&");
        let digest = Sha256::new()
            .chain_update(self.token)
            .chain_update(uri.path())
            .chain_update("?")
            .chain_update(query)
            .finalize();
        format!("\"{}-{}\"", self.head, hex(&digest[..8]))
    }
}

/// The SHA-256 of a file's bytes, read in 1 MiB blocks.
fn file_digest(path: &Path) -> [u8; 32] {
    let mut file = File::open(path).expect("open a served file");
    let mut buffer = vec![0; 1 << 20];
    let mut digest = Sha256::new();
    loop {
        match file.read(&mut buffer).expect("read a served file") {
            0 => return digest.finalize().into(),
            n => digest.update(&buffer[..n]),
        }
    }
}

/// A GET's validators: a 304 for an `If-None-Match` naming its `ETag`, before the
/// route runs; otherwise the route's answer, a 200 with its `ETag` and `cache` tier.
/// `*` matches only a representation that exists, so it is answered after the route.
pub async fn guard(
    State((validators, cache)): State<(Arc<Validators>, Cache)>,
    request: Request,
    next: Next,
) -> Response {
    let etag = validators.etag(request.uri());
    let tags = request
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(',').map(str::trim).collect::<Vec<_>>())
        .unwrap_or_default();
    // RFC 9110's weak comparison: Cloudflare weakens the ETag when it compresses.
    let listed = tags
        .iter()
        .any(|tag| tag.strip_prefix("W/").unwrap_or(tag) == etag);
    let any = tags.contains(&"*");
    let mut response = if listed {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let response = next.run(request).await;
        if response.status() != StatusCode::OK {
            return response;
        }
        if any {
            StatusCode::NOT_MODIFIED.into_response()
        } else {
            response
        }
    };
    let headers = response.headers_mut();
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("ASCII ETag"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache.header()),
    );
    response
}
