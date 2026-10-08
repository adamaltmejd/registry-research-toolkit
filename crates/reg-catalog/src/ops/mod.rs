//! The operation set (section 7). Each slice registers its operations in its own file
//! (`slice_3a.rs`, ...), and both transports are generated from the registrations: the
//! HTTP routes and `/openapi.json` here and in `reg-meta`, and the MCP tools in
//! `reg-meta`. Shared request pieces live in their own modules: refs (`refs.rs`) and
//! cursors (`cursor.rs`).

mod cursor;
mod docs;
mod refs;
mod search;
pub mod slice_3a;
pub mod slice_3b;

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::{
    ComponentsBuilder, ContentBuilder, HttpMethod, InfoBuilder, ObjectBuilder, OpenApi,
    OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, Required, Schema, Type as Json,
};

use crate::{CONTRACT_VERSION, Catalog, Code, Docs, Error, Scope};

/// What a running server answers from: the admitted catalog and its docs database
/// (when present), its steward branding (`serve` only: `mcp` loads none, since
/// branding is read only by `context`, which has no tool) and the server's version
/// (`reg_meta_version`).
pub struct Server {
    pub catalog: Catalog,
    pub docs: Option<Docs>,
    pub steward: Option<Steward>,
    pub version: &'static str,
}

/// Deployment branding, `reg_webapp/stewards/<catalog>/steward.json`.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct Steward {
    pub id: String,
    pub name: String,
    pub long_name: String,
}

impl Steward {
    /// Load the branding of catalog `name` from `stewards/<name>/steward.json`.
    ///
    /// # Errors
    ///
    /// `invalid_parameter` (`stewards`) when the file is missing, malformed or names
    /// another id.
    pub fn load(stewards: &Path, name: &str) -> Result<Self, Error> {
        let path = stewards.join(name).join("steward.json");
        let steward: Self = std::fs::read(&path)
            .map_err(|err| err.to_string())
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|err| err.to_string()))
            .map_err(|err| {
                Error::new(
                    Code::InvalidParameter,
                    format!(
                        "No steward branding for {name:?} at {}: {err}",
                        path.display()
                    ),
                    vec!["stewards".into()],
                )
            })?;
        if steward.id != name {
            return Err(Error::new(
                Code::InvalidParameter,
                format!("{}: id {:?} is not {name:?}", path.display(), steward.id),
                vec!["stewards".into()],
            ));
        }
        Ok(steward)
    }
}

/// A parameter: a query parameter, or a path parameter when the route names it as
/// `{name}`.
pub struct Param {
    pub name: &'static str,
    pub ty: Type,
    pub required: bool,
}

impl Param {
    #[must_use]
    pub const fn required(name: &'static str, ty: Type) -> Self {
        Self {
            name,
            ty,
            required: true,
        }
    }

    #[must_use]
    pub const fn optional(name: &'static str, ty: Type) -> Self {
        Self {
            name,
            ty,
            required: false,
        }
    }
}

/// A parameter type of `operations.toml`, which fixes its schema.
#[derive(Clone, Copy)]
pub enum Type {
    String,
    /// A FQID or a bare name (section 7).
    Ref,
    /// The FQID/project period grammar.
    Period,
    Scope,
    Limit,
    /// An opaque `next_cursor`.
    Cursor,
    Enum(&'static [&'static str]),
}

/// The `Cache-Control` tier of a 200 (today's three): identity reads revalidate every
/// request, fold-bearing reads get a minute, and rebuild-stable documents a day.
#[derive(Clone, Copy)]
pub enum Cache {
    Revalidate,
    Minute,
    Day,
}

impl Cache {
    #[must_use]
    pub fn header(self) -> &'static str {
        match self {
            Self::Revalidate => "no-cache",
            Self::Minute => "public, max-age=60, must-revalidate",
            Self::Day => "public, max-age=86400, must-revalidate",
        }
    }
}

type Components = Vec<(String, RefOr<Schema>)>;
/// An operation's validated parameters, `scope` excluded.
pub type Params<'a> = BTreeMap<&'a str, &'a str>;
/// The function that answers a route.
pub type Run<T> = fn(&Server, Scope, &Params) -> Result<T, Error>;

/// One operation: its name, routes and MCP tool (`operations.toml`), the description
/// both transports publish, its parameters, its `Cache-Control` tier, the function
/// that answers it and its `data` schema. The last route names every path
/// parameter; the MCP tool takes that route's parameters.
pub struct Operation {
    pub name: &'static str,
    pub paths: &'static [&'static str],
    pub tool: Option<&'static str>,
    pub description: &'static str,
    pub params: &'static [Param],
    pub cache: Cache,
    pub run: Run<Value>,
    pub result: fn(&mut Components) -> RefOr<Schema>,
}

/// A raw-bytes response (section 7): the body and the headers it adds besides its
/// download's media type.
pub struct Raw {
    pub bytes: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
}

/// A download route (`operations.toml`'s `[[download]]`): raw bytes whose metadata
/// its operation answers as JSON.
pub struct Download {
    pub path: &'static str,
    pub description: &'static str,
    pub media_type: &'static str,
    pub params: &'static [Param],
    pub cache: Cache,
    pub run: Run<Raw>,
}

impl Operation {
    /// The parameters `route` takes: a parameter another of the operation's routes
    /// names in its path is a path parameter only, so a route without it omits it
    /// (`GET /api/catalog` takes no `ref`).
    #[must_use]
    pub fn route_params(&self, route: &str) -> Vec<&'static Param> {
        self.params
            .iter()
            .filter(|p| {
                let placeholder = format!("{{{}}}", p.name);
                route.contains(&placeholder)
                    || !self.paths.iter().any(|other| other.contains(&placeholder))
            })
            .collect()
    }
}

/// Every registered operation.
pub fn all() -> impl Iterator<Item = &'static Operation> {
    slice_3a::OPERATIONS.iter().chain(slice_3b::OPERATIONS)
}

/// Every registered download.
pub fn downloads() -> impl Iterator<Item = &'static Download> {
    slice_3b::DOWNLOADS.iter()
}

/// Every MCP tool with its operations, in registration order.
#[must_use]
pub fn tools() -> Vec<(&'static str, Vec<&'static Operation>)> {
    let mut tools: Vec<(&str, Vec<&Operation>)> = Vec::new();
    for op in all() {
        let Some(tool) = op.tool else { continue };
        match tools.iter_mut().find(|(name, _)| *name == tool) {
            Some((_, ops)) => ops.push(op),
            None => tools.push((tool, vec![op])),
        }
    }
    tools
}

/// Answer a route's request parameters (path and query) with `run`, returning the
/// effective scope (the artifact's default when the request's scope is not usable)
/// with the result.
pub fn call<T>(
    server: &Server,
    declared: &[&Param],
    run: Run<T>,
    query: &[(String, String)],
) -> (Scope, Result<T, Error>) {
    let mut scope = server.catalog.default_scope();
    let result = (|| {
        let mut params = BTreeMap::new();
        for (name, value) in query {
            // Unknown and repeated parameters are errors, never ignored.
            if !declared.iter().any(|p| p.name == name)
                || params.insert(name.as_str(), value.as_str()).is_some()
            {
                return Err(Error::invalid_parameter(name));
            }
        }
        if let Some(missing) = declared
            .iter()
            .find(|p| p.required && !params.contains_key(p.name))
        {
            return Err(Error::invalid_parameter(missing.name));
        }
        if declared.iter().any(|p| p.name == "scope") {
            scope = server.catalog.scope(params.remove("scope"))?;
        }
        run(server, scope, &params)
    })();
    (scope, result)
}

/// The response `meta` (`shape.Meta`).
#[derive(Serialize, ToSchema)]
pub struct Meta {
    pub contract_version: &'static str,
    pub generation: String,
    pub scope: Scope,
}

impl Meta {
    #[must_use]
    pub fn new(catalog: &Catalog, scope: Scope) -> Self {
        Self {
            contract_version: CONTRACT_VERSION,
            generation: catalog.generation().to_owned(),
            scope,
        }
    }
}

/// Register `T` and its dependencies as components; return a reference to it.
fn component<T: ToSchema>(components: &mut Components) -> RefOr<Schema> {
    components.push((T::name().into_owned(), T::schema()));
    T::schemas(components);
    Ref::from_schema_name(T::name()).into()
}

fn param_schema(ty: Type, components: &mut Components) -> RefOr<Schema> {
    let string = || ObjectBuilder::new().schema_type(Json::String);
    match ty {
        Type::Scope => component::<Scope>(components),
        // A ref, a period and a cursor are strings in their grammars.
        Type::String | Type::Ref | Type::Period | Type::Cursor => string().into(),
        Type::Enum(members) => string().enum_values(Some(members.iter().copied())).into(),
        Type::Limit => ObjectBuilder::new()
            .schema_type(Json::Integer)
            .minimum(Some(1))
            .maximum(Some(200))
            .into(),
    }
}

/// `route`'s parameters as `OpenAPI` parameters: in the path when the route names
/// them, else in the query.
fn parameters(
    mut operation: OperationBuilder,
    route: &str,
    params: &[&Param],
    components: &mut Components,
) -> OperationBuilder {
    for param in params {
        let located = if route.contains(&format!("{{{}}}", param.name)) {
            ParameterIn::Path
        } else {
            ParameterIn::Query
        };
        operation = operation.parameter(
            ParameterBuilder::new()
                .name(param.name)
                .parameter_in(located)
                .required(if param.required {
                    Required::True
                } else {
                    Required::False
                })
                .schema(Some(param_schema(param.ty, components))),
        );
    }
    operation
}

fn error_response() -> ResponseBuilder {
    envelope("error", Ref::from_schema_name("Error").into())
        .description("An error in `api/errors.toml`")
}

fn envelope(key: &str, schema: RefOr<Schema>) -> ResponseBuilder {
    let body = ObjectBuilder::new()
        .property(key, schema)
        .property("meta", Ref::from_schema_name("Meta"))
        .required(key)
        .required("meta");
    ResponseBuilder::new().content(
        "application/json",
        ContentBuilder::new().schema(Some(body)).build(),
    )
}

/// The `OpenAPI` document of every registered operation.
#[must_use]
pub fn openapi(version: &str) -> OpenApi {
    let mut components = Components::new();
    component::<Meta>(&mut components);
    component::<Error>(&mut components);
    let mut paths = PathsBuilder::new();
    for op in all() {
        for route in op.paths {
            let operation = OperationBuilder::new()
                .operation_id(Some(op.name))
                .description(Some(op.description));
            let operation = parameters(operation, route, &op.route_params(route), &mut components);
            let data = (op.result)(&mut components);
            let operation = operation
                .response("200", envelope("data", data).description("Success"))
                .response("default", error_response());
            paths = paths.path(*route, PathItem::new(HttpMethod::Get, operation));
        }
    }
    // A download has no operation id of its own: it serves its operation's bytes.
    for download in downloads() {
        let operation = OperationBuilder::new().description(Some(download.description));
        let params: Vec<&Param> = download.params.iter().collect();
        let operation = parameters(operation, download.path, &params, &mut components);
        let bytes = ResponseBuilder::new()
            .description("The raw bytes")
            .content(download.media_type, ContentBuilder::new().build());
        let operation = operation
            .response("200", bytes)
            .response("default", error_response());
        paths = paths.path(download.path, PathItem::new(HttpMethod::Get, operation));
    }
    OpenApiBuilder::new()
        .info(InfoBuilder::new().title("reg-meta").version(version))
        .paths(paths)
        .components(Some(
            ComponentsBuilder::new()
                .schemas_from_iter(components)
                .build(),
        ))
        .build()
}
