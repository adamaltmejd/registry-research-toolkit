//! `reg-meta`: run modes only, no query CLI (`RUST_RUNTIME_SPEC.md` sections 6 and 7).
//!
//! ```text
//! reg-meta serve --db DIR [--catalog NAME] --stewards DIR --port N
//! reg-meta mcp --db DIR [--catalog NAME]
//! ```
//!
//! Both admit `DIR/reg_meta.db` (as `--catalog NAME` when given). `serve` loads the
//! catalog's branding from `DIR/<catalog>/steward.json` under `--stewards` and serves
//! every registered operation, `/openapi.json` and MCP at `/mcp` on 127.0.0.1; `mcp`
//! serves the MCP tools over stdio. A refusal prints the error document on stderr and
//! exits with the code's status.

mod mcp;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use reg_catalog::ops::{self, Meta, Operation, Server, Steward};
use reg_catalog::{Catalog, Error, Scope, hex};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const VERSION: &str = env!("CARGO_PKG_VERSION");

enum Mode {
    Serve { stewards: PathBuf, port: u16 },
    Mcp,
}

struct Args {
    db: PathBuf,
    catalog: Option<String>,
    mode: Mode,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, Error> {
    let mode = args.next();
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
    let mode = match mode.as_deref() {
        Some("serve") => Mode::Serve {
            stewards: stewards
                .ok_or_else(|| Error::invalid_parameter("--stewards"))?
                .into(),
            port: port
                .and_then(|p| p.parse().ok())
                .ok_or_else(|| Error::invalid_parameter("--port"))?,
        },
        // `mcp` loads no branding and listens on no port.
        Some("mcp") if stewards.is_some() => return Err(Error::invalid_parameter("--stewards")),
        Some("mcp") if port.is_some() => return Err(Error::invalid_parameter("--port")),
        Some("mcp") => Mode::Mcp,
        _ => return Err(Error::invalid_parameter("mode")),
    };
    Ok(Args {
        db: db.ok_or_else(|| Error::invalid_parameter("--db"))?.into(),
        catalog,
        mode,
    })
}

/// Print the refusal's error document on stderr and exit with the code's status.
fn refuse(err: &Error) -> ! {
    eprintln!("{}", serde_json::to_string(err).expect("Error serializes"));
    std::process::exit(err.exit());
}

#[tokio::main]
async fn main() {
    let args = parse_args(std::env::args().skip(1)).unwrap_or_else(|err| refuse(&err));
    let catalog =
        Catalog::open(&args.db, args.catalog.as_deref()).unwrap_or_else(|err| refuse(&err));
    match args.mode {
        Mode::Serve { stewards, port } => {
            let steward =
                Steward::load(&stewards, catalog.name()).unwrap_or_else(|err| refuse(&err));
            let server = Server {
                catalog,
                steward: Some(steward),
                version: VERSION,
            };
            serve(Arc::new(server), port).await;
        }
        Mode::Mcp => {
            let server = Server {
                catalog,
                steward: None,
                version: VERSION,
            };
            mcp::stdio(Arc::new(server)).await;
        }
    }
}

async fn serve(server: Arc<Server>, port: u16) {
    let openapi = ops::openapi(VERSION).to_json().expect("OpenAPI serializes");
    let mut app = Router::new().route("/openapi.json", get(|| async move { json(openapi) }));
    for op in ops::all() {
        app = app.route(
            op.path,
            get(move |state, query, headers| answer(op, state, query, headers)),
        );
    }
    let app = app
        .with_state(Arc::clone(&server))
        .merge(mcp::router(server));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind the port");
    // The client address keys the `/mcp` rate limit.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("serve");
}

/// A response document: `{data, meta}` with status 200, or `{error, meta}` with the
/// code's status. HTTP and MCP send the same document.
struct Answer {
    scope: Scope,
    status: StatusCode,
    body: Value,
}

impl Answer {
    fn new(server: &Server, scope: Scope, result: Result<Value, Error>) -> Self {
        let meta = Meta::new(&server.catalog, scope);
        let (status, body) = match result {
            Ok(data) => (StatusCode::OK, json!({"data": data, "meta": meta})),
            Err(err) => (
                StatusCode::from_u16(err.status()).expect("catalogued status"),
                json!({"error": err, "meta": meta}),
            ),
        };
        Self {
            scope,
            status,
            body,
        }
    }
}

/// Answer `op` for the request's parameters on the blocking pool.
async fn run(server: &Arc<Server>, op: &'static Operation, query: Vec<(String, String)>) -> Answer {
    let task = Arc::clone(server);
    let (scope, result) = tokio::task::spawn_blocking(move || ops::call(&task, op, &query))
        .await
        .expect("operation task");
    Answer::new(server, scope, result)
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
    let answer = run(&server, op, query).await;
    let body = answer.body.to_string();
    if answer.status != StatusCode::OK {
        return (answer.status, json(body)).into_response();
    }
    let etag = etag(&server, answer.scope, body.as_bytes());
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
    let digest = hex(&Sha256::digest(body)[..8]);
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
