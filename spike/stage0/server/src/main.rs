//! Stage-0 spike binary: one operation (`search`) over HTTP, MCP over HTTP and MCP over
//! stdio. Run modes only, no query CLI (RUST_RUNTIME_SPEC.md §6–§7).
//!
//!   reg-meta-spike serve --db DIR [--port N]   HTTP API at /api/search, MCP at /mcp
//!   reg-meta-spike mcp --db DIR                MCP over stdio
//!   reg-meta-spike once --db DIR QUERY...      spike harness: one search as JSON

mod catalog;

use std::{path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use catalog::{Catalog, Envelope, ErrorDoc, SearchParams, SearchResult};
use rmcp::{
    ErrorData, ServerHandler, ServiceExt,
    handler::server::{
        router::tool::ToolRouter,
        wrapper::{Json as McpJson, Parameters},
    },
    model::{ServerCapabilities, ServerConfig},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};

#[derive(Clone)]
struct Tools {
    catalog: Arc<Catalog>,
    #[allow(dead_code, reason = "read by the tool_handler macro")]
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl Tools {
    fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Search register variables by name, definition and description. \
        Each word matches as a prefix. Returns ranked variables with their FQIDs."
    )]
    async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<McpJson<Envelope<SearchResult>>, ErrorData> {
        let catalog = self.catalog.clone();
        let result = tokio::task::spawn_blocking(move || catalog.search_variables(&params))
            .await
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
        match result {
            Ok(data) => Ok(McpJson(self.catalog.envelope(data))),
            Err(err) => Err(ErrorData::invalid_params(
                err.message.clone(),
                Some(serde_json::to_value(&err).unwrap()),
            )),
        }
    }
}

#[tool_handler]
impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }
}

struct ApiError(ErrorDoc);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.class.as_str() {
            "usage" => StatusCode::BAD_REQUEST,
            "config" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({ "error": self.0 }))).into_response()
    }
}

async fn http_search(
    State(catalog): State<Arc<Catalog>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Envelope<SearchResult>>, ApiError> {
    let c = catalog.clone();
    let data = tokio::task::spawn_blocking(move || c.search_variables(&params))
        .await
        .map_err(|e| {
            ApiError(ErrorDoc {
                code: "internal_error".into(),
                class: "internal".into(),
                message: e.to_string(),
                remediation: String::new(),
            })
        })?
        .map_err(ApiError)?;
    Ok(Json(catalog.envelope(data)))
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn fail(err: ErrorDoc) -> ! {
    eprintln!("{}", serde_json::json!({ "error": err }));
    std::process::exit(10);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().cloned().unwrap_or_default();
    let db = PathBuf::from(arg(&args, "--db").unwrap_or_else(|| ".".into()));
    let catalog = Arc::new(Catalog::admit(db).unwrap_or_else(|e| fail(e)));
    match mode.as_str() {
        "serve" => {
            let port = arg(&args, "--port").unwrap_or_else(|| "8787".into());
            let mcp_catalog = catalog.clone();
            let mcp = StreamableHttpService::new(
                move || Ok(Tools::new(mcp_catalog.clone())),
                LocalSessionManager::default().into(),
                StreamableHttpServerConfig::default(),
            );
            let app = Router::new()
                .route("/api/search", get(http_search))
                .with_state(catalog)
                .nest_service("/mcp", mcp);
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                .await
                .unwrap();
            eprintln!(
                "serving on 127.0.0.1:{port} (sqlite {})",
                Catalog::sqlite_version()
            );
            axum::serve(listener, app).await.unwrap();
        }
        "mcp" => {
            let running = Tools::new(catalog)
                .serve(rmcp::transport::stdio())
                .await
                .unwrap();
            running.waiting().await.unwrap();
        }
        "once" => {
            let query = args[1..]
                .iter()
                .filter(|a| {
                    !a.starts_with("--") && Some(a.as_str()) != arg(&args, "--db").as_deref()
                })
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            let params = SearchParams {
                query,
                limit: Some(50),
            };
            match catalog.search_variables(&params) {
                Ok(data) => println!(
                    "{}",
                    serde_json::to_string(&catalog.envelope(data)).unwrap()
                ),
                Err(e) => fail(e),
            }
        }
        _ => fail(ErrorDoc {
            code: "usage_error".into(),
            class: "usage".into(),
            message: format!("unknown run mode {mode:?}"),
            remediation: "Use serve, mcp or once.".into(),
        }),
    }
}
