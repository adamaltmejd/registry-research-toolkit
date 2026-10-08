//! `reg-meta`: run modes only, no query CLI (`RUST_RUNTIME_SPEC.md` sections 6 and 7).
//!
//! ```text
//! reg-meta serve --db DIR [--catalog NAME] --stewards DIR --port N [--host ADDR]
//!                [--public-host HOST]
//! reg-meta mcp --db DIR [--catalog NAME]
//! ```
//!
//! Both admit `DIR/reg_meta.db` (as `--catalog NAME` when given) and, when present,
//! `DIR/reg_meta_docs.db`. `serve` loads the catalog's branding from
//! `DIR/<catalog>/steward.json` under `--stewards` and serves every registered
//! operation and download, `/openapi.json` and MCP at `/mcp` on `--host` (default
//! 127.0.0.1); `/mcp` admits the `Host` header `--public-host` besides the loopback
//! names, and the edge token in `REG_META_EDGE_TOKEN` (`mcp.rs`). `mcp` serves the MCP
//! tools over stdio. A refusal prints the error document on stderr and exits with the
//! code's status.

mod mcp;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use reg_catalog::ops::{self, Cache, Download, Meta, Operation, Param, Raw, Run, Server, Steward};
use reg_catalog::{Catalog, Docs, Error, Scope, hex};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const VERSION: &str = env!("CARGO_PKG_VERSION");

enum Mode {
    Serve {
        stewards: PathBuf,
        port: u16,
        host: IpAddr,
        public_host: Option<String>,
    },
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
    let (mut host, mut public_host) = (None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--db" => &mut db,
            "--catalog" => &mut catalog,
            "--stewards" => &mut stewards,
            "--port" => &mut port,
            "--host" => &mut host,
            "--public-host" => &mut public_host,
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
            host: match host {
                Some(host) => host
                    .parse()
                    .map_err(|_| Error::invalid_parameter("--host"))?,
                None => Ipv4Addr::LOCALHOST.into(),
            },
            public_host,
        },
        // `mcp` loads no branding and listens on no port.
        Some("mcp") => {
            let serve_only = [
                ("--stewards", &stewards),
                ("--port", &port),
                ("--host", &host),
                ("--public-host", &public_host),
            ];
            if let Some((flag, _)) = serve_only.iter().find(|(_, value)| value.is_some()) {
                return Err(Error::invalid_parameter(flag));
            }
            Mode::Mcp
        }
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
    let docs = Docs::open(&args.db).unwrap_or_else(|err| refuse(&err));
    match args.mode {
        Mode::Serve {
            stewards,
            port,
            host,
            public_host,
        } => {
            let steward =
                Steward::load(&stewards, catalog.name()).unwrap_or_else(|err| refuse(&err));
            let server = Server {
                catalog,
                docs,
                steward: Some(steward),
                version: VERSION,
            };
            serve(Arc::new(server), (host, port).into(), public_host).await;
        }
        Mode::Mcp => {
            let server = Server {
                catalog,
                docs,
                steward: None,
                version: VERSION,
            };
            mcp::stdio(Arc::new(server)).await;
        }
    }
}

async fn serve(server: Arc<Server>, addr: SocketAddr, public_host: Option<String>) {
    let openapi = ops::openapi(VERSION).to_json().expect("OpenAPI serializes");
    let mut app = Router::new().route("/openapi.json", get(|| async move { json(openapi) }));
    for op in ops::all() {
        app = app.route(
            &axum_route(op.path),
            get(move |state, path, query, headers| answer(op, state, path, query, headers)),
        );
    }
    for download in ops::downloads() {
        app = app.route(
            &axum_route(download.path),
            get(move |state, path, query, headers| fetch(download, state, path, query, headers)),
        );
    }
    let app = app
        .with_state(Arc::clone(&server))
        .merge(mcp::router(server, public_host));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind the port");
    // The peer address keys the `/mcp` rate limit of a request not proven to come
    // through the edge.
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

/// Answer a route's parameters with `run` on the blocking pool.
async fn call<T: Send + 'static>(
    server: &Arc<Server>,
    params: &'static [Param],
    run: Run<T>,
    query: Vec<(String, String)>,
) -> (Scope, Result<T, Error>) {
    let task = Arc::clone(server);
    tokio::task::spawn_blocking(move || ops::call(&task, params, run, &query))
        .await
        .expect("operation task")
}

/// Answer `op` for the request's parameters.
async fn run(server: &Arc<Server>, op: &'static Operation, query: Vec<(String, String)>) -> Answer {
    let (scope, result) = call(server, op.params, op.run, query).await;
    Answer::new(server, scope, result)
}

fn json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// The edge worker's cache-generation parameter (`reg_webapp/edge/src/index.ts`):
/// part of the edge cache key, never an operation parameter.
const EDGE_VERSION_PARAM: &str = "__edge_v";

/// An operation-table route as an axum route: `{ref}` spans segments, so it and the
/// segments after it are one wildcard, which [`request_params`] splits again.
fn axum_route(template: &str) -> String {
    template.split_once("{ref}").map_or_else(
        || template.to_owned(),
        |(head, _)| format!("{head}{{*ref}}"),
    )
}

/// A request's parameters: the path's, named by `template`, then the query's
/// without the edge's cache parameter. `{ref}` keeps the segments the names after
/// it leave.
fn request_params(
    template: &str,
    path: Vec<(String, String)>,
    mut query: Vec<(String, String)>,
) -> Vec<(String, String)> {
    query.retain(|(name, _)| name != EDGE_VERSION_PARAM);
    let Some((_, after)) = template.split_once("{ref}") else {
        return [path, query].concat();
    };
    let tail = path.into_iter().map(|(_, tail)| tail).collect::<String>();
    let mut rest = tail.as_str();
    let mut params = Vec::new();
    for name in after
        .rsplit('/')
        .filter_map(|segment| segment.strip_prefix('{')?.strip_suffix('}'))
    {
        let Some((head, last)) = rest.rsplit_once('/') else {
            break;
        };
        params.push((name.to_owned(), last.to_owned()));
        rest = head;
    }
    params.push(("ref".to_owned(), rest.to_owned()));
    [params, query].concat()
}

/// One operation over HTTP: `{data, meta}` with today's `ETag` and `Cache-Control`
/// policy and a 304 for a matching `If-None-Match`, or `{error, meta}` with the
/// code's status.
async fn answer(
    op: &'static Operation,
    State(server): State<Arc<Server>>,
    Path(path): Path<Vec<(String, String)>>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> Response {
    let answer = run(&server, op, request_params(op.path, path, query)).await;
    let body = answer.body.to_string();
    if answer.status != StatusCode::OK {
        return (answer.status, json(body)).into_response();
    }
    let raw = Raw {
        bytes: body.into_bytes(),
        headers: Vec::new(),
    };
    cached(
        &server,
        answer.scope,
        op.cache,
        &headers,
        "application/json",
        raw,
    )
}

/// One download over HTTP: the raw bytes with its media type, under the same
/// validators and tier as an operation, or `{error, meta}` with the code's status.
async fn fetch(
    download: &'static Download,
    State(server): State<Arc<Server>>,
    Path(path): Path<Vec<(String, String)>>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> Response {
    let params = request_params(download.path, path, query);
    match call(&server, download.params, download.run, params).await {
        (scope, Ok(raw)) => cached(
            &server,
            scope,
            download.cache,
            &headers,
            download.media_type,
            raw,
        ),
        (scope, Err(err)) => {
            let answer = Answer::new(&server, scope, Err(err));
            (answer.status, json(answer.body.to_string())).into_response()
        }
    }
}

/// A 200 of `raw` with its `ETag`, `Cache-Control` and extra headers, or a 304 for a
/// matching `If-None-Match`.
fn cached(
    server: &Server,
    scope: Scope,
    cache: Cache,
    request: &HeaderMap,
    media_type: &'static str,
    raw: Raw,
) -> Response {
    let Raw {
        bytes: body,
        headers: extra,
    } = raw;
    let etag = etag(server, scope, &body);
    let revalidated = request
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| etag_matches(value, &etag));
    let mut response = if revalidated {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let mut response = ([(header::CONTENT_TYPE, media_type)], body).into_response();
        for (name, value) in extra {
            let value = HeaderValue::from_str(&value).expect("a header value");
            response.headers_mut().insert(name, value);
        }
        response
    };
    let validators = response.headers_mut();
    validators.insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("ASCII ETag"),
    );
    validators.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache.header()),
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
