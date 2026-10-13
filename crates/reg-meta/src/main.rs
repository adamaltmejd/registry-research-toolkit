//! `reg-meta`: run modes only, no query CLI (`crates/DESIGN.md`, "Agent and web interface").
//!
//! ```text
//! reg-meta serve --db DIR [--catalog NAME] --stewards DIR --port N [--host ADDR]
//!                [--public-host HOST] [--write-limit N] [--mmap-size BYTES]
//! reg-meta mcp --db DIR [--catalog NAME]
//! ```
//!
//! Both admit `DIR/reg_meta.db` (as `--catalog NAME` when given) and, when present,
//! `DIR/reg_meta_docs.db`. `serve` loads the catalog's branding from
//! `DIR/<catalog>/steward.json` under `--stewards` and serves every registered
//! operation and download, `/openapi.json` and MCP at `/mcp` on `--host` (default
//! 127.0.0.1); `/mcp` admits the `Host` header `--public-host` besides the loopback
//! names. `/mcp` and every POST share one rate limit per client address, keyed with
//! the edge token in `REG_META_EDGE_TOKEN` (`limit.rs`): `--write-limit` tokens
//! (default 60), refilled one a second. `--mmap-size` sets each SQLite connection's
//! `PRAGMA mmap_size` (default 2 GiB, `reg_catalog::DEFAULT_MMAP_SIZE`; 0 disables
//! memory-mapping). Once bound, `serve` prints `listening on ADDR:PORT` on stderr,
//! the port the kernel chose under `--port 0`. `mcp` serves the MCP
//! tools over stdio. A refusal prints the error document on stderr and exits with the
//! code's status.

mod limit;
mod mcp;
mod validator;

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use reg_catalog::ops::{
    self, Cache, Download, Meta, Operation, Param, Raw, Run, Server, Steward, body,
};
use reg_catalog::{Catalog, DEFAULT_MMAP_SIZE, Docs, Error, Scope};
use serde_json::{Value, json};

use crate::limit::Limits;
use crate::validator::Validators;

const VERSION: &str = env!("CARGO_PKG_VERSION");

enum Mode {
    Serve {
        stewards: PathBuf,
        port: u16,
        host: IpAddr,
        public_host: Option<String>,
        write_limit: u64,
    },
    Mcp,
}

struct Args {
    db: PathBuf,
    catalog: Option<String>,
    /// `serve --mmap-size`; `mcp` keeps the default.
    mmap_size: i64,
    mode: Mode,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, Error> {
    let mode = args.next();
    let (mut db, mut catalog, mut stewards, mut port) = (None, None, None, None);
    let (mut host, mut public_host, mut write_limit, mut mmap_size) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--db" => &mut db,
            "--catalog" => &mut catalog,
            "--stewards" => &mut stewards,
            "--port" => &mut port,
            "--host" => &mut host,
            "--public-host" => &mut public_host,
            "--write-limit" => &mut write_limit,
            "--mmap-size" => &mut mmap_size,
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
            write_limit: match write_limit {
                Some(limit) => limit
                    .parse()
                    .ok()
                    .filter(|&limit| limit > 0)
                    .ok_or_else(|| Error::invalid_parameter("--write-limit"))?,
                None => limit::DEFAULT_PER_MINUTE,
            },
        },
        // `mcp` loads no branding and listens on no port.
        Some("mcp") => {
            let serve_only = [
                ("--stewards", &stewards),
                ("--port", &port),
                ("--host", &host),
                ("--public-host", &public_host),
                ("--write-limit", &write_limit),
                ("--mmap-size", &mmap_size),
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
        mmap_size: match mmap_size {
            Some(bytes) => bytes
                .parse()
                .ok()
                .filter(|&bytes| bytes >= 0)
                .ok_or_else(|| Error::invalid_parameter("--mmap-size"))?,
            None => DEFAULT_MMAP_SIZE,
        },
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
    let catalog = Catalog::open(&args.db, args.catalog.as_deref(), args.mmap_size)
        .unwrap_or_else(|err| refuse(&err));
    let docs = Docs::open(&args.db, args.mmap_size).unwrap_or_else(|err| refuse(&err));
    match args.mode {
        Mode::Serve {
            stewards,
            port,
            host,
            public_host,
            write_limit,
        } => {
            let steward =
                Steward::load(&stewards, catalog.name()).unwrap_or_else(|err| refuse(&err));
            let server = Server {
                catalog,
                docs,
                steward: Some(steward),
                version: VERSION,
            };
            serve(
                Arc::new(server),
                (host, port).into(),
                public_host,
                write_limit,
            )
            .await;
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

async fn serve(
    server: Arc<Server>,
    addr: SocketAddr,
    public_host: Option<String>,
    write_limit: u64,
) {
    let openapi = ops::openapi(VERSION).to_json().expect("OpenAPI serializes");
    let limits = Limits::new(Arc::clone(&server), write_limit);
    let validators = Validators::new(&server);
    // A POST is behind the write limits, as `/mcp` is: the rate limit, then the cap.
    let write = |handler: MethodRouter<Arc<Server>>| -> MethodRouter<Arc<Server>> {
        handler
            .layer::<_, Infallible>(from_fn_with_state(Arc::clone(&limits), limit::guard))
            .layer(DefaultBodyLimit::max(body::MAX_BYTES))
    };
    // A GET is behind its validators, which can answer it without running it. A route
    // layer, so another method's 405 is not one of them.
    let read = |handler: MethodRouter<Arc<Server>>, cache: Cache| -> MethodRouter<Arc<Server>> {
        handler.route_layer(from_fn_with_state(
            (Arc::clone(&validators), cache),
            validator::guard,
        ))
    };
    let mut app = Router::new().route("/openapi.json", get(|| async move { json(openapi) }));
    for op in ops::all() {
        for &route in op.paths {
            let handler = if op.body().is_some() {
                write(post(move |state, query, body| {
                    submit(op, route, state, query, body)
                }))
            } else {
                read(
                    get(move |state, path, query| answer(op, route, state, path, query)),
                    op.cache,
                )
            };
            app = app.route(&axum_route(route), handler);
        }
    }
    for download in ops::downloads() {
        let handler = if download.body().is_some() {
            write(post(move |state, query, body| {
                submit_download(download, state, query, body)
            }))
        } else {
            read(
                get(move |state, path, query| fetch(download, state, path, query)),
                download.cache,
            )
        };
        app = app.route(&axum_route(download.path), handler);
    }
    let app = app
        .with_state(Arc::clone(&server))
        .merge(mcp::router(server, public_host, limits));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind the port");
    // One stable line naming the bound address, so `--port 0` is usable: the
    // conformance harness will start servers that way and read this line once the
    // G1 pinned baseline binary prints it (#1529).
    eprintln!(
        "listening on {}",
        listener.local_addr().expect("the bound address")
    );
    // The peer address keys the write rate limit of a request not proven to come
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
        Self { status, body }
    }
}

/// Answer a route's parameters with `run` on the blocking pool.
async fn call<T: Send + 'static>(
    server: &Arc<Server>,
    params: Vec<&'static Param>,
    run: Run<T>,
    query: Vec<(String, String)>,
) -> (Scope, Result<T, Error>) {
    let task = Arc::clone(server);
    tokio::task::spawn_blocking(move || ops::call(&task, &params, run, &query))
        .await
        .expect("operation task")
}

/// Answer `op` for the request's parameters, declared by `params`.
async fn run(
    server: &Arc<Server>,
    op: &'static Operation,
    params: Vec<&'static Param>,
    query: Vec<(String, String)>,
) -> Answer {
    let (scope, result) = call(server, params, op.run, query).await;
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

/// One operation over HTTP: `{data, meta}`, or `{error, meta}` with the code's
/// status. [`validator::guard`] adds the 200's `ETag` and `Cache-Control`.
async fn answer(
    op: &'static Operation,
    route: &'static str,
    State(server): State<Arc<Server>>,
    Path(path): Path<Vec<(String, String)>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    let params = request_params(route, path, query);
    let answer = run(&server, op, op.route_params(route), params).await;
    (answer.status, json(answer.body.to_string())).into_response()
}

/// One POST operation over HTTP: its body, capped by [`limit::guard`] and parsed
/// strictly ([`body::parse`]), is the body parameter. Answered `{data, meta}` or
/// `{error, meta}` without cache validators: a POST answer is not cached.
async fn submit(
    op: &'static Operation,
    route: &'static str,
    State(server): State<Arc<Server>>,
    Query(query): Query<Vec<(String, String)>>,
    bytes: Bytes,
) -> Response {
    let body = op.body().expect("a POST operation has a body");
    let params = match posted(&server, body, route, query, &bytes) {
        Ok(params) => params,
        Err(refused) => return *refused,
    };
    let answer = run(&server, op, op.route_params(route), params).await;
    (answer.status, json(answer.body.to_string())).into_response()
}

/// One POST download over HTTP: its body as [`submit`] takes it, answered with the
/// raw bytes and no cache validators, or `{error, meta}` with the code's status.
async fn submit_download(
    download: &'static Download,
    State(server): State<Arc<Server>>,
    Query(query): Query<Vec<(String, String)>>,
    bytes: Bytes,
) -> Response {
    let body = download.body().expect("a POST download has a body");
    let params = match posted(&server, body, download.path, query, &bytes) {
        Ok(params) => params,
        Err(refused) => return *refused,
    };
    let declared = download.params.iter().collect();
    match call(&server, declared, download.run, params).await {
        (_, Ok(raw)) => raw_response(download.media_type, raw),
        (scope, Err(err)) => refusal(&server, scope, err),
    }
}

/// A POST's request parameters: the query's, then its body, parsed strictly
/// ([`body::parse`]), as the parameter `body`; or the response refusing the body.
fn posted(
    server: &Server,
    body: &Param,
    route: &str,
    query: Vec<(String, String)>,
    bytes: &[u8],
) -> Result<Vec<(String, String)>, Box<Response>> {
    let project = body::parse(bytes)
        .map_err(|err| Box::new(refusal(server, server.catalog.default_scope(), err)))?;
    let mut params = request_params(route, Vec::new(), query);
    params.push((body.name.to_owned(), project.to_string()));
    Ok(params)
}

/// `{error, meta}` with the code's status.
fn refusal(server: &Server, scope: Scope, err: Error) -> Response {
    let answer = Answer::new(server, scope, Err(err));
    (answer.status, json(answer.body.to_string())).into_response()
}

/// One download over HTTP: the raw bytes with its media type, or `{error, meta}` with
/// the code's status. [`validator::guard`] adds the 200's `ETag` and `Cache-Control`.
async fn fetch(
    download: &'static Download,
    State(server): State<Arc<Server>>,
    Path(path): Path<Vec<(String, String)>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    let params = request_params(download.path, path, query);
    match call(
        &server,
        download.params.iter().collect(),
        download.run,
        params,
    )
    .await
    {
        (_, Ok(raw)) => raw_response(download.media_type, raw),
        (scope, Err(err)) => refusal(&server, scope, err),
    }
}

/// A 200 of `raw`'s bytes as `media_type`, with its extra headers.
fn raw_response(media_type: &'static str, raw: Raw) -> Response {
    let mut response = ([(header::CONTENT_TYPE, media_type)], raw.bytes).into_response();
    for (name, value) in raw.headers {
        let value = HeaderValue::from_str(&value).expect("a header value");
        response.headers_mut().insert(name, value);
    }
    response
}
