//! MCP (section 7): `reg-meta mcp` over stdio and `/mcp` on `serve` over streamable
//! HTTP. Every registered operation with a `tool` is one tool. Its schemas are the
//! operation's `OpenAPI` ones, and a call returns the HTTP response document: `{data,
//! meta}`, or `{error, meta}` as a tool error.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequest, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use reg_catalog::ops::{self, Server};
use reg_catalog::{Code, Error};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, JsonObject,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt};
use serde_json::{Map, Value, json};

use crate::{Answer, VERSION, run};

/// `/mcp` requests per client address: a token bucket of this many tokens, refilled
/// one per second. Sized for agent tool calls, apart from any SPA limit.
const RATE_PER_MINUTE: u64 = 60;
/// The `/mcp` body cap, today's `limits.py` cap on write bodies.
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// simplify: buckets that have refilled are dropped only once this many addresses are
/// tracked; make it a time-ordered sweep if a hosted burst of addresses shows in RSS
/// or in `/mcp` latency.
const MAX_TRACKED: usize = 10_000;

/// The tool handler: one per stdio process, and per request over stateless HTTP.
#[derive(Clone)]
struct Tools {
    server: Arc<Server>,
    tools: Arc<[Tool]>,
}

impl Tools {
    fn new(server: Arc<Server>) -> Self {
        let openapi = serde_json::to_value(ops::openapi(VERSION)).expect("OpenAPI serializes");
        let tools = ops::all()
            .filter_map(|op| {
                let operation = &openapi["paths"][op.path]["get"];
                Some(tool(&openapi, operation, op.tool?, op.description))
            })
            .collect();
        Self { server, tools }
    }
}

/// A tool from its operation's `OpenAPI` entry: the query parameters as the input
/// object, and the success and error envelopes as the output.
fn tool(openapi: &Value, operation: &Value, name: &'static str, description: &str) -> Tool {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for param in operation["parameters"].as_array().expect("parameters") {
        let name = param["name"].as_str().expect("parameter name");
        properties.insert(name.to_owned(), param["schema"].clone());
        if param["required"] == true {
            required.push(name);
        }
    }
    let input = json!({"type": "object", "properties": properties, "required": required});
    let envelope = |status: &str| {
        operation["responses"][status]["content"]["application/json"]["schema"].clone()
    };
    let output = json!({"type": "object", "oneOf": [envelope("200"), envelope("default")]});
    Tool::new_with_raw(
        name,
        Some(description.to_owned().into()),
        with_defs(input, openapi),
    )
    .with_raw_output_schema(with_defs(output, openapi))
}

/// `schema` self-contained: the components it references, transitively, become its
/// `$defs` and every `$ref` points there.
fn with_defs(mut schema: Value, openapi: &Value) -> Arc<JsonObject> {
    let mut pending = Vec::new();
    rebase(&mut schema, &mut pending);
    let mut defs = Map::new();
    while let Some(name) = pending.pop() {
        if !defs.contains_key(&name) {
            let mut def = openapi["components"]["schemas"][&name].clone();
            rebase(&mut def, &mut pending);
            defs.insert(name, def);
        }
    }
    let Value::Object(mut schema) = schema else {
        unreachable!("a tool schema is an object")
    };
    if !defs.is_empty() {
        schema.insert("$defs".into(), Value::Object(defs));
    }
    Arc::new(schema)
}

/// Point every `#/components/schemas/X` reference in `value` at `#/$defs/X`,
/// collecting the names.
fn rebase(value: &mut Value, names: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(target)) = map.get_mut("$ref")
                && let Some(name) = target.strip_prefix("#/components/schemas/")
            {
                names.push(name.to_owned());
                *target = format!("#/$defs/{name}");
            }
            map.values_mut().for_each(|v| rebase(v, names));
        }
        Value::Array(items) => items.iter_mut().for_each(|v| rebase(v, names)),
        _ => {}
    }
}

/// The tool arguments as an HTTP query: a string as is, a number in its JSON spelling
/// (`limit: 5` is `limit=5`); any other value is `invalid_parameter`.
fn query(arguments: JsonObject) -> Result<Vec<(String, String)>, Error> {
    arguments
        .into_iter()
        .map(|(name, value)| match value {
            Value::String(text) => Ok((name, text)),
            Value::Number(number) => Ok((name, number.to_string())),
            _ => Err(Error::invalid_parameter(&name)),
        })
        .collect()
}

impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("reg-meta", VERSION))
    }

    fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(self.tools.to_vec())))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let op = ops::all()
            .find(|op| op.tool == Some(&*request.name))
            .ok_or_else(|| {
                ErrorData::invalid_params(format!("Unknown tool {:?}.", request.name), None)
            })?;
        let answer = match query(request.arguments.unwrap_or_default()) {
            Ok(query) => run(&self.server, op, query).await,
            Err(err) => Answer::new(&self.server, self.server.catalog.default_scope(), Err(err)),
        };
        Ok(if answer.status == StatusCode::OK {
            CallToolResult::structured(answer.body)
        } else {
            CallToolResult::structured_error(answer.body)
        }
        .into())
    }
}

/// Serve the tools over stdio until the client closes it.
pub async fn stdio(server: Arc<Server>) {
    Tools::new(server)
        .serve(rmcp::transport::stdio())
        .await
        .expect("MCP initialize over stdio")
        .waiting()
        .await
        .expect("MCP session over stdio");
}

/// `/mcp`: streamable HTTP without sessions (each POST stands alone and is answered
/// with JSON), behind the rate limit and then the body cap.
pub fn router(server: Arc<Server>) -> Router {
    let tools = Tools::new(Arc::clone(&server));
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    let service = StreamableHttpService::new(
        move || Ok(tools.clone()),
        Arc::new(NeverSessionManager::default()),
        config,
    );
    let limits = Arc::new(Limits {
        server,
        buckets: Mutex::default(),
    });
    Router::new()
        .route_service("/mcp", service)
        .layer(from_fn_with_state(limits, guard))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

struct Limits {
    server: Arc<Server>,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

struct Bucket {
    tokens: u64,
    refilled: Instant,
}

impl Bucket {
    /// Add a token per whole second since the last refill, up to the capacity.
    fn refill(&mut self, now: Instant) -> &mut Self {
        let seconds = now.duration_since(self.refilled).as_secs();
        self.tokens = (self.tokens + seconds).min(RATE_PER_MINUTE);
        self.refilled += Duration::from_secs(seconds);
        self
    }
}

impl Limits {
    /// Take a token from `client`'s bucket; false when it is empty.
    fn allow(&self, client: IpAddr) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("rate buckets");
        if buckets.len() >= MAX_TRACKED {
            // A full bucket is the same as none. simplify: while this many addresses
            // are active, every request sweeps them all under the lock; replace the
            // sweep (see MAX_TRACKED) if it shows in `/mcp` latency.
            buckets.retain(|_, bucket| bucket.refill(now).tokens < RATE_PER_MINUTE);
        }
        let bucket = buckets
            .entry(client)
            .or_insert(Bucket {
                tokens: RATE_PER_MINUTE,
                refilled: now,
            })
            .refill(now);
        let allowed = bucket.tokens > 0;
        bucket.tokens = bucket.tokens.saturating_sub(1);
        allowed
    }

    /// `err` as `{error, meta}` with its status.
    fn refuse(&self, err: Error) -> Response {
        let answer = Answer::new(&self.server, self.server.catalog.default_scope(), Err(err));
        (answer.status, crate::json(answer.body.to_string())).into_response()
    }
}

/// The `/mcp` limits, answered with the error document: `rate_limited` per client
/// address, then `payload_too_large` over [`MAX_BODY_BYTES`] (read through
/// `DefaultBodyLimit`, which the MCP service itself does not consult).
async fn guard(
    State(limits): State<Arc<Limits>>,
    ConnectInfo(client): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if !limits.allow(client.ip()) {
        let err = Error::new(
            Code::RateLimited,
            format!("More than {RATE_PER_MINUTE} MCP requests a minute from this address."),
            vec![1.into()],
        );
        return ([(header::RETRY_AFTER, "1")], limits.refuse(err)).into_response();
    }
    let (parts, body) = request.into_parts();
    match Bytes::from_request(Request::from_parts(parts.clone(), body), &()).await {
        Ok(bytes) => next.run(Request::from_parts(parts, bytes.into())).await,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            limits.refuse(Error::new(
                Code::PayloadTooLarge,
                format!("The request body exceeds {MAX_BODY_BYTES} bytes."),
                vec![MAX_BODY_BYTES.into()],
            ))
        }
        Err(rejection) => rejection.into_response(),
    }
}
