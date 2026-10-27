//! The operation set. Each operation registers itself in its own file (`context.rs`,
//! `search.rs`, `show.rs`, ...), and both transports are generated from the
//! registrations: the HTTP routes and `/openapi.json` here and in `reg-meta`, and the
//! MCP tools in `reg-meta`. Shared request pieces live in their own modules: refs
//! (`refs.rs`), cursors (`cursor.rs`), result shapes (`shape.rs`) and the FTS5 query
//! (`fts.rs`). An operation or download with a [`Type::Project`] parameter is a POST
//! that takes it as the JSON body; every other is a GET.

pub mod body;
pub mod context;
pub mod coverage;
pub mod cursor;
pub mod docs;
pub mod findings;
pub mod fts;
pub mod graph;
pub mod order;
pub mod refs;
pub mod resolve;
pub mod schema;
pub mod search;
pub mod segments;
pub mod shape;
pub mod show;
pub mod values;

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::{
    ArrayBuilder, ComponentsBuilder, ContentBuilder, HttpMethod, InfoBuilder, ObjectBuilder,
    OpenApi, OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, Required, Schema, Type as Json,
};

use crate::{CONTRACT_VERSION, Catalog, Code, Error};

/// What a running server answers from: the admitted catalog, the `context` computed
/// when it was admitted and the server's version (`reg_meta_version`).
pub struct Server {
    pub catalog: Catalog,
    context: context::Context,
    pub version: &'static str,
    /// `meta` `open_start` and `open_end`: the day numbers a result period renders as
    /// `null`.
    open: (i64, i64),
}

impl Server {
    /// Serve `catalog`, computing what is answered from memory.
    ///
    /// # Errors
    ///
    /// `internal_error` when `open_start` or `open_end` is not a day number, or the
    /// `context` queries fail.
    pub fn new(catalog: Catalog, version: &'static str) -> Result<Self, Error> {
        let day = |key: &str| {
            catalog.meta(key).parse::<i64>().map_err(|_| {
                Error::new(
                    Code::InternalError,
                    format!("meta {key} is not a day number: {:?}", catalog.meta(key)),
                    vec![],
                )
            })
        };
        let open = (day("open_start")?, day("open_end")?);
        let context = context::Context::load(&catalog, open, version)?;
        Ok(Self {
            catalog,
            context,
            version,
            open,
        })
    }

    /// A result period of the day numbers `[start, end)` (`shape.Period`).
    #[must_use]
    pub fn period(&self, start: i64, end: i64) -> Period {
        Period::new(self.open, start, end)
    }
}

/// `shape.Period`: a half-open `[start, end)` of ISO days, `null` for an open end.
#[derive(Clone, Serialize, ToSchema)]
pub struct Period {
    #[schema(required = true)]
    start: Option<String>,
    #[schema(required = true)]
    end: Option<String>,
}

impl Period {
    /// The period of the day numbers `[start, end)`; an end equal to its `open`
    /// sentinel (`meta` `open_start`, `open_end`) is `null`.
    #[must_use]
    pub(crate) fn new(open: (i64, i64), start: i64, end: i64) -> Self {
        Self {
            start: (start != open.0).then(|| iso_day(start)),
            end: (end != open.1).then(|| iso_day(end)),
        }
    }
}

/// The ISO date of a catalog day number (days from 1970-01-01): the inverse of
/// `reg_core::iso_day_number`, Howard Hinnant's `civil_from_days`.
fn iso_day(day: i64) -> String {
    let z = day + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
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
    /// A public id, or a bare name matched on its folded last segment (`refs.rs`).
    Ref,
    /// The FQID/project period grammar.
    Period,
    Limit,
    /// An opaque `next_cursor`.
    Cursor,
    /// A content hash: 64 lowercase hex digits.
    Hash,
    Boolean,
    Enum(&'static [&'static str]),
    /// `string[]`: up to [`MAX_ITEMS`] strings, repeated keys over HTTP and a JSON
    /// array over MCP.
    Strings,
    /// A `project_data.json` document: a POST's JSON body, handed to the operation as
    /// its JSON text.
    Project,
}

/// The `Cache-Control` tier of a 200 (today's three): identity reads revalidate every
/// request, fold-bearing reads get a minute in the browser and a day in a shared
/// cache (the edge keys them by deploy generation), and rebuild-stable documents a
/// day.
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
            Self::Minute => "public, max-age=60, s-maxage=86400, must-revalidate",
            Self::Day => "public, max-age=86400, must-revalidate",
        }
    }
}

type Components = Vec<(String, RefOr<Schema>)>;
/// An operation's validated parameters: each parameter's values in request order, one
/// unless it is a [`Type::Strings`].
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
pub type Run<T> = fn(&Server, &Params) -> Result<T, Error>;

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

/// A raw-bytes response: the body and the headers it adds besides its
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
///
/// The order is `operations.toml`'s row order, which fixes the MCP `tools/list` order.
pub fn all() -> impl Iterator<Item = &'static Operation> {
    search::OPERATIONS
        .iter()
        .chain(context::OPERATIONS)
        .chain(show::OPERATIONS)
        .chain(segments::OPERATIONS)
        .chain(findings::OPERATIONS)
        .chain(values::OPERATIONS)
        .chain(graph::OPERATIONS)
        .chain(coverage::OPERATIONS)
        .chain(schema::OPERATIONS)
        .chain(resolve::OPERATIONS)
        .chain(order::OPERATIONS)
        .chain(docs::OPERATIONS)
}

/// Every registered download.
pub fn downloads() -> impl Iterator<Item = &'static Download> {
    docs::DOWNLOADS.iter()
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

/// Answer a route's request parameters (path and query) with `run`.
///
/// # Errors
///
/// `invalid_parameter` for an unknown, repeated or missing parameter; else `run`'s.
pub fn call<T>(
    server: &Server,
    declared: &[&Param],
    run: Run<T>,
    query: &[(String, String)],
) -> Result<T, Error> {
    let mut params = Params::default();
    for (name, value) in query {
        // Unknown and repeated parameters are errors, never ignored; a repeat is an
        // array's next value.
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
    run(server, &params)
}

/// A page's `limit` when unset, and its largest accepted value (`Type::Limit`).
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
/// The most values an array parameter takes (`Type::Strings`).
const MAX_ITEMS: usize = 200;

/// `limit` (`operations.toml`): 1 to [`MAX_LIMIT`], default [`DEFAULT_LIMIT`].
///
/// # Errors
///
/// `invalid_parameter` (`limit`) outside that range or not a number.
pub fn limit(params: &Params) -> Result<usize, Error> {
    params.get("limit").map_or(Ok(DEFAULT_LIMIT), |v| {
        v.parse()
            .ok()
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| Error::invalid_parameter("limit"))
    })
}

/// The period parameter `name`, in the FQID/project period grammar; a refusal
/// names `name`.
///
/// # Errors
///
/// `invalid_period` when the value is not in the grammar.
pub fn period(params: &Params, name: &str) -> Result<Option<reg_core::Period>, Error> {
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

/// A request period as half-open day numbers `[first, last + 1)`; none when absent.
///
/// # Panics
///
/// If the period grammar produces bounds that are not ISO days.
///
/// # Errors
///
/// `invalid_period` when the value is not in the grammar.
pub fn days(params: &Params, name: &str) -> Result<Option<(i64, i64)>, Error> {
    let day = |iso: &str| reg_core::iso_day_number(iso).expect("a grammar period's ISO bounds");
    Ok(period(params, name)?.map(|p| {
        let (first, last) = p.iso_bounds();
        (day(&first), day(&last) + 1)
    }))
}

/// A boolean parameter, `true` or `false` (default false).
///
/// # Errors
///
/// `invalid_parameter` (`name`) for any other value.
pub fn flag(params: &Params, name: &str) -> Result<bool, Error> {
    match params.get(name).copied() {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(Error::invalid_parameter(name)),
    }
}

/// A `hash` parameter (64 lowercase hex digits); `None` when absent.
///
/// # Errors
///
/// `invalid_parameter` (`name`) for any other value.
pub fn hash<'a>(params: &Params<'a>, name: &str) -> Result<Option<&'a str>, Error> {
    match params.get(name).copied() {
        None => Ok(None),
        Some(h) if h.len() == 64 && h.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) => {
            Ok(Some(h))
        }
        Some(_) => Err(Error::invalid_parameter(name)),
    }
}

/// The response `meta` (`shape.Meta`).
#[derive(Serialize, ToSchema)]
pub struct Meta {
    pub contract_version: &'static str,
    pub generation: String,
    pub catalog_version: String,
}

impl Meta {
    #[must_use]
    pub fn new(catalog: &Catalog) -> Self {
        Self {
            contract_version: CONTRACT_VERSION,
            generation: catalog.generation().to_owned(),
            catalog_version: catalog.meta("catalog_version").to_owned(),
        }
    }
}

/// Register `T` and its dependencies as components; return a reference to it.
fn component<T: ToSchema>(components: &mut Components) -> RefOr<Schema> {
    components.push((T::name().into_owned(), T::schema()));
    T::schemas(components);
    Ref::from_schema_name(T::name()).into()
}

fn param_schema(ty: Type) -> RefOr<Schema> {
    let string = || ObjectBuilder::new().schema_type(Json::String);
    match ty {
        // A ref, a period and a cursor are strings in their grammars.
        Type::String | Type::Ref | Type::Period | Type::Cursor => string().into(),
        Type::Enum(members) => string().enum_values(Some(members.iter().copied())).into(),
        Type::Boolean => ObjectBuilder::new().schema_type(Json::Boolean).into(),
        Type::Strings => ArrayBuilder::new()
            .items(string())
            .max_items(Some(MAX_ITEMS))
            .into(),
        Type::Hash => string().pattern(Some("^[0-9a-f]{64}$")).into(),
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
fn parameters(mut operation: OperationBuilder, route: &str, params: &[&Param]) -> OperationBuilder {
    for param in params {
        if matches!(param.ty, Type::Project) {
            let body = ContentBuilder::new()
                .schema(Some(param_schema(param.ty)))
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
                .schema(Some(param_schema(param.ty))),
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
///
/// # Panics
///
/// When two types register different schemas under one name.
#[must_use]
pub fn openapi(version: &str) -> OpenApi {
    let mut components = Components::new();
    component::<Meta>(&mut components);
    component::<Error>(&mut components);
    // Referenced by no operation (the validate body stays a free object): published
    // so the SPA's types are generated, not hand-written.
    component::<reg_core::project::ProjectData>(&mut components);
    let mut paths = PathsBuilder::new();
    for op in all() {
        for route in op.paths {
            let operation = OperationBuilder::new()
                .operation_id(Some(operation_id(op, route)))
                .description(Some(op.description));
            let operation = parameters(operation, route, &op.route_params(route));
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
        let operation = parameters(operation, download.path, &params);
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

#[cfg(test)]
mod tests {
    /// Fails when a result period's ISO day drifts from the day number the builder
    /// wrote (`reg_core::iso_day_number`): an off-by-one at a leap day, a century or
    /// before 1970, which the `context` case's two January firsts do not reach.
    #[test]
    fn iso_day_inverts_iso_day_number() {
        let first = reg_core::iso_day_number("1600-01-01").unwrap();
        let last = reg_core::iso_day_number("2400-12-31").unwrap();
        for day in first..=last {
            assert_eq!(reg_core::iso_day_number(&super::iso_day(day)), Some(day));
        }
    }
}
