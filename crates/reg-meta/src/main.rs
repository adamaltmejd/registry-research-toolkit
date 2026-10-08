//! `reg-meta`: run modes only, no query CLI (`RUST_RUNTIME_SPEC.md` sections 6 and 7).
//!
//! ```text
//! reg-meta serve --db DIR [--catalog NAME] --stewards DIR --port N
//! ```
//!
//! `serve` admits `DIR/reg_meta.db` (as `--catalog NAME` when given), loads the
//! catalog's branding from `DIR/<catalog>/steward.json` under `--stewards`, and serves
//! every registered operation plus `/openapi.json` on 127.0.0.1. A refusal prints the
//! error document on stderr and exits with the code's status.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use reg_catalog::ops::{self, Meta, Operation, Server, Steward};
use reg_catalog::{Catalog, Error, Scope};
use sha2::{Digest, Sha256};

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Args {
    db: PathBuf,
    catalog: Option<String>,
    stewards: PathBuf,
    port: u16,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, Error> {
    if args.next().as_deref() != Some("serve") {
        return Err(Error::invalid_parameter("mode"));
    }
    let (mut db, mut catalog, mut stewards, mut port) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--db" => &mut db,
            "--catalog" => &mut catalog,
            "--stewards" => &mut stewards,
            "--port" => &mut port,
            _ => return Err(Error::invalid_parameter(&flag)),
        };
        *slot = Some(args.next().ok_or_else(|| Error::invalid_parameter(&flag))?);
    }
    Ok(Args {
        db: db.ok_or_else(|| Error::invalid_parameter("--db"))?.into(),
        catalog,
        stewards: stewards
            .ok_or_else(|| Error::invalid_parameter("--stewards"))?
            .into(),
        port: port
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| Error::invalid_parameter("--port"))?,
    })
}

fn admit(args: &Args) -> Result<Server, Error> {
    let catalog = Catalog::open(&args.db, args.catalog.as_deref())?;
    let steward = Steward::load(&args.stewards, catalog.name())?;
    Ok(Server {
        catalog,
        steward,
        version: VERSION,
    })
}

#[tokio::main]
async fn main() {
    let started =
        parse_args(std::env::args().skip(1)).and_then(|args| Ok((admit(&args)?, args.port)));
    let (server, port) = started.unwrap_or_else(|err| {
        eprintln!("{}", serde_json::to_string(&err).expect("Error serializes"));
        std::process::exit(err.exit());
    });
    let openapi = ops::openapi(VERSION).to_json().expect("OpenAPI serializes");
    let mut app = Router::new().route("/openapi.json", get(|| async move { json(openapi) }));
    for op in ops::all() {
        app = app.route(
            op.path,
            get(move |state, query, headers| answer(op, state, query, headers)),
        );
    }
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind the port");
    axum::serve(listener, app.with_state(Arc::new(server)))
        .await
        .expect("serve");
}

fn json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// One operation over HTTP: `{data, meta}` with today's `ETag` and `Cache-Control`
/// policy and a 304 for a matching `If-None-Match`, or `{error, meta}` with the
/// code's status.
async fn answer(
    op: &'static Operation,
    State(server): State<Arc<Server>>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> Response {
    let task = Arc::clone(&server);
    let (scope, result) = tokio::task::spawn_blocking(move || ops::call(&task, op, &query))
        .await
        .expect("operation task");
    let meta = Meta::new(&server.catalog, scope);
    let data = match result {
        Ok(data) => data,
        Err(err) => {
            let body = serde_json::json!({"error": err, "meta": meta}).to_string();
            let status = StatusCode::from_u16(err.status()).expect("catalogued status");
            return (status, json(body)).into_response();
        }
    };
    let body = serde_json::json!({"data": data, "meta": meta}).to_string();
    let etag = etag(&server, scope, body.as_bytes());
    let revalidated = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| etag_matches(value, &etag));
    let mut response = if revalidated {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        json(body)
    };
    let validators = response.headers_mut();
    validators.insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("ASCII ETag"),
    );
    validators.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_control(op.path)),
    );
    response
}

/// Today's `etag.py`: a strong validator over the server version, catalog, generation,
/// effective scope and the first 16 hex digits of the body's SHA-256.
fn etag(server: &Server, scope: Scope, body: &[u8]) -> String {
    let mut digest = String::new();
    for byte in &Sha256::digest(body)[..8] {
        write!(digest, "{byte:02x}").expect("write to String");
    }
    let scope = serde_json::to_value(scope).expect("Scope serializes");
    format!(
        "\"{VERSION}-{}-{}-{}-{digest}\"",
        server.catalog.name(),
        server.catalog.generation(),
        scope.as_str().expect("Scope is a string"),
    )
}

/// RFC 9110's weak comparison for `If-None-Match`, with its list and `*` forms.
fn etag_matches(if_none_match: &str, etag: &str) -> bool {
    if_none_match
        .split(',')
        .map(str::trim)
        .any(|tag| tag == "*" || tag.strip_prefix("W/").unwrap_or(tag) == etag)
}

/// Today's three tiers: identity reads revalidate every request, fold-bearing reads
/// get a 60 s window, and the rest (documents) a day.
fn cache_control(path: &str) -> &'static str {
    if path == "/api/context" {
        "no-cache"
    } else if path.starts_with("/api/catalog") || path.starts_with("/api/search") {
        "public, max-age=60, must-revalidate"
    } else {
        "public, max-age=86400, must-revalidate"
    }
}
