//! The operation set (section 7). Each slice registers its operations in its own file
//! (`slice_3a.rs`, ...), and both transports are generated from the registrations: the
//! HTTP routes and `/openapi.json` here and in `reg-meta`, and the MCP tools in
//! `reg-meta`. Shared request pieces live in their own modules: refs (`refs.rs`) and
//! cursors (`cursor.rs`). An operation or download with a [`Type::Project`] parameter
//! is a POST that takes it as the JSON body; every other is a GET.

pub mod body;
mod coded;
mod coverage;
mod cursor;
mod docs;
mod graph;
mod lineage;
mod order;
mod refs;
mod resolve;
mod schema;
mod search;
mod show;
pub mod slice_3a;
pub mod slice_3b;
pub mod slice_3c;
pub mod slice_3d;
pub mod slice_3e;
mod states;
mod validate;
mod values;
mod warnings;

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::{
    ArrayBuilder, ComponentsBuilder, ContentBuilder, HttpMethod, InfoBuilder, ObjectBuilder,
    OpenApi, OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, Required, Schema, Type as Json,
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
    Boolean,
    /// A storage id, spelled as the decimal string results carry (ids pass 2^53).
    StorageId,
    Enum(&'static [&'static str]),
    /// `string[]`: up to [`MAX_ITEMS`] strings, repeated keys over HTTP and a JSON
    /// array over MCP.
    Strings,
    /// A `project_data.json` document: a POST's JSON body, handed to the operation as
    /// its JSON text.
    Project,
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
/// An operation's validated parameters, `scope` excluded: each parameter's values in
/// request order, one unless it is a [`Type::Strings`].
#[derive(Default)]
pub struct Params<'a> {
    values: BTreeMap<&'a str, Vec<&'a str>>,
}

impl<'a> Params<'a> {
    /// A parameter's value.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&&'a str> {
        self.values.get(name).and_then(|values| values.first())
    }

    /// An array parameter's values; none when it is absent.
    #[must_use]
    pub fn list(&self, name: &str) -> &[&'a str] {
        self.values.get(name).map_or(&[], Vec::as_slice)
    }
}

/// A required parameter's value, which the transport has checked is present.
impl<'a> std::ops::Index<&str> for Params<'a> {
    type Output = &'a str;

    fn index(&self, name: &str) -> &Self::Output {
        self.get(name).expect("a required parameter")
    }
}

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

/// The parameter sent as the JSON body, which makes a route a POST.
fn body(params: &'static [Param]) -> Option<&'static Param> {
    params.iter().find(|p| matches!(p.ty, Type::Project))
}

impl Download {
    /// The parameter sent as the JSON body, which makes the download a POST.
    #[must_use]
    pub fn body(&self) -> Option<&'static Param> {
        body(self.params)
    }
}

impl Operation {
    /// The parameter sent as the JSON body, which makes the operation a POST.
    #[must_use]
    pub fn body(&self) -> Option<&'static Param> {
        body(self.params)
    }

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
    slice_3a::OPERATIONS
        .iter()
        .chain(slice_3b::OPERATIONS)
        .chain(slice_3c::OPERATIONS)
        .chain(slice_3d::OPERATIONS)
        .chain(slice_3e::OPERATIONS)
}

/// Every registered download.
pub fn downloads() -> impl Iterator<Item = &'static Download> {
    slice_3b::DOWNLOADS.iter().chain(slice_3e::DOWNLOADS)
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
        let mut params = Params::default();
        for (name, value) in query {
            // Unknown and repeated parameters are errors, never ignored; a repeat is
            // an array's next value.
            let Some(param) = declared.iter().find(|p| p.name == name) else {
                return Err(Error::invalid_parameter(name));
            };
            let values = params.values.entry(param.name).or_default();
            values.push(value.as_str());
            let most = if matches!(param.ty, Type::Strings) {
                MAX_ITEMS
            } else {
                1
            };
            if values.len() > most {
                return Err(Error::invalid_parameter(name));
            }
        }
        if let Some(missing) = declared
            .iter()
            .find(|p| p.required && !params.values.contains_key(p.name))
        {
            return Err(Error::invalid_parameter(missing.name));
        }
        if declared.iter().any(|p| p.name == "scope") {
            let requested = params.values.remove("scope").map(|values| values[0]);
            scope = server.catalog.scope(requested)?;
        }
        run(server, scope, &params)
    })();
    (scope, result)
}

/// A page's `limit` when unset, and its largest accepted value (`Type::Limit`).
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
/// The most values an array parameter takes (`Type::Strings`).
const MAX_ITEMS: usize = 200;

/// `limit` (`operations.toml`): 1 to [`MAX_LIMIT`], default [`DEFAULT_LIMIT`].
pub(crate) fn limit(params: &Params) -> Result<usize, Error> {
    params.get("limit").map_or(Ok(DEFAULT_LIMIT), |v| {
        v.parse()
            .ok()
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| Error::invalid_parameter("limit"))
    })
}

/// The period parameter `name`, in the FQID/project period grammar; a refusal
/// names `name`.
pub(crate) fn period(params: &Params, name: &str) -> Result<Option<reg_core::Period>, Error> {
    params
        .get(name)
        .map(|p| {
            p.parse().map_err(|err| {
                Error::new(
                    Code::InvalidPeriod,
                    format!("Invalid {name} {p:?}: {err}."),
                    vec![name.into()],
                )
            })
        })
        .transpose()
}

/// `variant`: a register variant's slug, or `_default` (today's
/// `validate_slug(allow_default=True)`).
pub(crate) fn variant<'a>(params: &Params<'a>) -> Result<Option<&'a str>, Error> {
    match params.get("variant").copied() {
        Some(v) if v != "_default" && !reg_core::is_slug(v) => {
            Err(Error::invalid_parameter("variant"))
        }
        v => Ok(v),
    }
}

/// `value_set_version`: a free-text label, so only sanity-checked as today's
/// `parse_value_set_version`: not blank, at most 200 characters, no C0, DEL or C1
/// control characters.
pub(crate) fn value_set_version<'a>(params: &Params<'a>) -> Result<Option<&'a str>, Error> {
    match params.get("value_set_version").copied() {
        Some(v)
            if reg_core::py_strip(v).is_empty()
                || v.chars().count() > 200
                || v.chars()
                    .any(|c| c < ' ' || ('\u{7f}'..='\u{9f}').contains(&c)) =>
        {
            Err(Error::invalid_parameter("value_set_version"))
        }
        v => Ok(v),
    }
}

/// The longest `q` (today's `QUERY_MAX_LEN`).
const MAX_QUERY_CHARS: usize = 200;
/// `Type::StorageId`'s pattern.
const STORAGE_ID: &str = "^-?[0-9]+$";

/// `q`: at most [`MAX_QUERY_CHARS`] characters and no NUL; absent is empty.
pub(crate) fn q<'a>(params: &Params<'a>) -> Result<&'a str, Error> {
    let q = params.get("q").copied().unwrap_or_default();
    if q.contains('\0') || q.chars().count() > MAX_QUERY_CHARS {
        return Err(Error::invalid_parameter("q"));
    }
    Ok(q)
}

/// The storage-id parameter `name`: an optional sign and decimal digits that fit
/// an `i64`.
pub(crate) fn storage_id(params: &Params, name: &str) -> Result<Option<i64>, Error> {
    params
        .get(name)
        .map(|v| {
            let digits = v.strip_prefix('-').unwrap_or(v);
            (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
                .then(|| v.parse().ok())
                .flatten()
                .ok_or_else(|| Error::invalid_parameter(name))
        })
        .transpose()
}

/// A boolean parameter, `true` or `false` (default false).
pub(crate) fn flag(params: &Params, name: &str) -> Result<bool, Error> {
    match params.get(name).copied() {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(Error::invalid_parameter(name)),
    }
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
        Type::Boolean => ObjectBuilder::new().schema_type(Json::Boolean).into(),
        Type::StorageId => string().pattern(Some(STORAGE_ID)).into(),
        Type::Strings => ArrayBuilder::new()
            .items(string())
            .max_items(Some(MAX_ITEMS))
            .into(),
        Type::Limit => ObjectBuilder::new()
            .schema_type(Json::Integer)
            .minimum(Some(1))
            .maximum(Some(MAX_LIMIT))
            .into(),
        Type::Project => ObjectBuilder::new()
            .schema_type(Json::Object)
            .description(Some("A project_data.json document"))
            .into(),
    }
}

/// `route`'s parameters as `OpenAPI` parameters: in the path when the route names
/// them, else in the query; a body parameter as the request body.
fn parameters(
    mut operation: OperationBuilder,
    route: &str,
    params: &[&Param],
    components: &mut Components,
) -> OperationBuilder {
    for param in params {
        if matches!(param.ty, Type::Project) {
            let body = ContentBuilder::new()
                .schema(Some(param_schema(param.ty, components)))
                .build();
            operation = operation.request_body(Some(
                RequestBodyBuilder::new()
                    .content("application/json", body)
                    .required(Some(Required::True))
                    .build(),
            ));
            continue;
        }
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

/// A route's operation id, unique as `OpenAPI` requires: the operation's name on the
/// route that takes every parameter, and on a route without some path parameters the
/// name with `_without_` and those parameters (`show_without_ref` for
/// `GET /api/catalog`).
fn operation_id(op: &Operation, route: &str) -> String {
    let taken = op.route_params(route);
    let missing: Vec<&str> = op
        .params
        .iter()
        .filter(|p| !taken.iter().any(|t| t.name == p.name))
        .map(|p| p.name)
        .collect();
    if missing.is_empty() {
        op.name.to_owned()
    } else {
        format!("{}_without_{}", op.name, missing.join("_"))
    }
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
                .operation_id(Some(operation_id(op, route)))
                .description(Some(op.description));
            let operation = parameters(operation, route, &op.route_params(route), &mut components);
            let data = (op.result)(&mut components);
            let operation = operation
                .response("200", envelope("data", data).description("Success"))
                .response("default", error_response());
            let method = if op.body().is_some() {
                HttpMethod::Post
            } else {
                HttpMethod::Get
            };
            paths = paths.path(*route, PathItem::new(method, operation));
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
        let method = if download.body().is_some() {
            HttpMethod::Post
        } else {
            HttpMethod::Get
        };
        paths = paths.path(download.path, PathItem::new(method, operation));
    }
    // Two types with one schema name would silently replace each other's schema
    // (3c.3's `Coverage` once replaced `show`'s): refuse instead.
    let mut named: BTreeMap<&str, &RefOr<Schema>> = BTreeMap::new();
    for (name, schema) in &components {
        if let Some(other) = named.insert(name, schema) {
            assert!(other == schema, "two types share the schema name {name}");
        }
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
