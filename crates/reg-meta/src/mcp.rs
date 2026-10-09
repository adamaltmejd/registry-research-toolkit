//! MCP (section 7): `reg-meta mcp` over stdio and `/mcp` on `serve` over streamable
//! HTTP. A tool exposes the registered operations that name it. Its schemas are the
//! operations' `OpenAPI` ones, and a call returns the HTTP response document: `{data,
//! meta}`, or `{error, meta}` as a tool error.

use std::sync::Arc;

use crate::limit::{Limits, guard};
use crate::{Answer, VERSION, run};
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use reg_catalog::Error;
use reg_catalog::ops::{self, Operation, Server, Type, body};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, JsonObject,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt};
use serde_json::{Map, Value, json};

/// The tool handler: one per stdio process, and per request over stateless HTTP.
#[derive(Clone)]
struct Tools {
    server: Arc<Server>,
    tools: Arc<[Tool]>,
}

impl Tools {
    fn new(server: Arc<Server>) -> Self {
        let openapi = serde_json::to_value(ops::openapi(VERSION)).expect("OpenAPI serializes");
        let tools = ops::tools()
            .into_iter()
            .map(|(name, ops)| tool(&openapi, name, &ops))
            .collect();
        Self { server, tools }
    }
}

/// A tool from its operations' `OpenAPI` entries: the parameters as the input object,
/// and the success and error envelopes as the output. A tool of several operations
/// takes an `operation` argument naming one, and its other arguments are that
/// operation's parameters. They stay one flat object, since agent APIs reject a
/// top-level `oneOf` input; each operation's parameters are listed in the description
/// and checked per call. A POST operation's body is the argument its body parameter
/// names.
fn tool(openapi: &Value, name: &'static str, ops: &[&Operation]) -> Tool {
    // The last route names every path parameter (`Operation`).
    let entries: Vec<&Value> = ops
        .iter()
        .map(|op| {
            let method = if op.body().is_some() { "post" } else { "get" };
            &openapi["paths"][op.paths[op.paths.len() - 1]][method]
        })
        .collect();
    let mut properties = Map::new();
    let mut required = Vec::new();
    let mut signatures = Vec::new();
    for (op, entry) in ops.iter().zip(&entries) {
        let mut signature = Vec::new();
        let mut params: Vec<(&str, Value)> = entry["parameters"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|param| {
                let name = param["name"].as_str().expect("parameter name");
                (name, param["schema"].clone())
            })
            .collect();
        if let Some(body) = op.body() {
            let content = &entry["requestBody"]["content"]["application/json"];
            params.push((body.name, content["schema"].clone()));
        }
        for (name, schema) in params {
            let previous = properties.insert(name.to_owned(), schema.clone());
            assert!(
                previous.is_none_or(|previous| previous == schema),
                "parameter {name:?} has two schemas"
            );
            // Required by the operation, not the route: `show`'s `ref` is a
            // required path segment of one route and absent from the other.
            let is_required = op.params.iter().any(|p| p.name == name && p.required);
            let optional = if is_required { "" } else { "?" };
            signature.push(format!("{name}{optional}"));
            if is_required && ops.len() == 1 {
                required.push(name.to_owned());
            }
        }
        signatures.push(signature.join(", "));
    }
    let description = if let [op] = ops {
        op.description.to_owned()
    } else {
        properties.insert(
            "operation".to_owned(),
            json!({"type": "string", "enum": ops.iter().map(|op| op.name).collect::<Vec<_>>()}),
        );
        required.push("operation".to_owned());
        let lines = ops
            .iter()
            .zip(&signatures)
            .map(|(op, signature)| format!("- `{}` ({signature}): {}", op.name, op.description));
        std::iter::once(
            "`operation` names one of these operations; the other arguments are its \
            parameters (`?` marks an optional one)."
                .to_owned(),
        )
        .chain(lines)
        .collect::<Vec<_>>()
        .join("\n")
    };
    let input = json!({"type": "object", "properties": properties, "required": required});
    let envelope = |entry: &Value, status: &str| {
        entry["responses"][status]["content"]["application/json"]["schema"].clone()
    };
    let mut outputs: Vec<Value> = entries.iter().map(|entry| envelope(entry, "200")).collect();
    outputs.push(envelope(entries[0], "default"));
    let output = json!({"type": "object", "oneOf": outputs});
    Tool::new_with_raw(name, Some(description.into()), with_defs(input, openapi))
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

/// The operation a call names among its tool's `ops`: the only one, or the one the
/// `operation` argument of a tool of several names (taken out of `arguments`).
fn operation(
    ops: &[&'static Operation],
    arguments: &mut JsonObject,
) -> Result<&'static Operation, Error> {
    if let [op] = ops {
        return Ok(op);
    }
    let name = arguments.remove("operation");
    ops.iter()
        .find(|op| name.as_ref().and_then(Value::as_str) == Some(op.name))
        .copied()
        .ok_or_else(|| Error::invalid_parameter("operation"))
}

/// The tool arguments as the request's parameters: a string as is, a number in its
/// JSON spelling (`limit: 5` is `limit=5`), an array parameter's non-empty array of
/// strings as one parameter per string, as HTTP repeats its key, and the body
/// parameter's object as its JSON text; any other value, an array for another
/// parameter or a non-object body included, is `invalid_parameter`.
fn query(op: &Operation, arguments: JsonObject) -> Result<Vec<(String, String)>, Error> {
    let body = op.body().map(|param| param.name);
    let mut query = Vec::new();
    for (name, value) in arguments {
        let is_array = op
            .params
            .iter()
            .any(|p| p.name == name && matches!(p.ty, Type::Strings));
        if body == Some(name.as_str()) {
            if !value.is_object() {
                return Err(Error::invalid_parameter(&name));
            }
            let text = value.to_string();
            query.push((name, text));
            continue;
        }
        match value {
            Value::String(text) => query.push((name, text)),
            Value::Number(number) => query.push((name, number.to_string())),
            Value::Array(items) if is_array && !items.is_empty() => {
                for item in items {
                    let Value::String(text) = item else {
                        return Err(Error::invalid_parameter(&name));
                    };
                    query.push((name.clone(), text));
                }
            }
            _ => return Err(Error::invalid_parameter(&name)),
        }
    }
    Ok(query)
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
        let ops: Vec<&Operation> = ops::all()
            .filter(|op| op.tool == Some(&*request.name))
            .collect();
        if ops.is_empty() {
            return Err(ErrorData::invalid_params(
                format!("Unknown tool {:?}.", request.name),
                None,
            ));
        }
        let mut arguments = request.arguments.unwrap_or_default();
        let call = operation(&ops, &mut arguments)
            .and_then(|op| query(op, arguments).map(|query| (op, query)));
        let answer = match call {
            Ok((op, query)) => run(&self.server, op, op.params.iter().collect(), query).await,
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
/// with JSON), behind the write limits (`limit.rs`). `Host` may be a loopback name or
/// `public_host`.
pub fn router(server: Arc<Server>, public_host: Option<String>, limits: Arc<Limits>) -> Router {
    let tools = Tools::new(server);
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    config.allowed_hosts.extend(public_host);
    let service = StreamableHttpService::new(
        move || Ok(tools.clone()),
        Arc::new(NeverSessionManager::default()),
        config,
    );
    Router::new()
        .route_service("/mcp", service)
        .layer(from_fn_with_state(limits, guard))
        .layer(DefaultBodyLimit::max(body::MAX_BYTES))
}
