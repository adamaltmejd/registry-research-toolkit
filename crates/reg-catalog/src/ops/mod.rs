//! The operation set (section 7). Each slice registers its operations in its own file
//! (`slice_3a.rs`, ...), and both transports are generated from the registrations: the
//! HTTP routes and `/openapi.json` here and in `reg-meta`, the MCP tools later.

pub mod slice_3a;

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::{
    ComponentsBuilder, ContentBuilder, HttpMethod, InfoBuilder, ObjectBuilder, OpenApi,
    OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, Required, Schema,
};

use crate::{CONTRACT_VERSION, Catalog, Code, Error, Scope};

/// What a running server answers from: the admitted catalog, its steward branding
/// and the server's version (`reg_meta_version`).
pub struct Server {
    pub catalog: Catalog,
    pub steward: Steward,
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

/// A query parameter. Its schema follows from its name: one parameter per concept,
/// the same everywhere (section 7).
pub struct Param {
    pub name: &'static str,
    pub required: bool,
}

type Components = Vec<(String, RefOr<Schema>)>;
/// An operation's validated parameters, `scope` excluded.
pub type Params<'a> = BTreeMap<&'a str, &'a str>;

/// One operation: its name and route (`operations.toml`), its parameters, the
/// function that answers it and its `data` schema.
pub struct Operation {
    pub name: &'static str,
    pub path: &'static str,
    pub params: &'static [Param],
    pub run: fn(&Server, Scope, &Params) -> Result<Value, Error>,
    pub result: fn(&mut Components) -> RefOr<Schema>,
}

/// Every registered operation.
pub fn all() -> impl Iterator<Item = &'static Operation> {
    slice_3a::OPERATIONS.iter()
}

/// Answer `op` for the request's parameters, returning the effective scope (the
/// artifact's default when the request's scope is not usable) with the result.
pub fn call(
    server: &Server,
    op: &Operation,
    query: &[(String, String)],
) -> (Scope, Result<Value, Error>) {
    let mut scope = server.catalog.default_scope();
    let result = (|| {
        let mut params = BTreeMap::new();
        for (name, value) in query {
            // Unknown and repeated parameters are errors, never ignored.
            if !op.params.iter().any(|p| p.name == name)
                || params.insert(name.as_str(), value.as_str()).is_some()
            {
                return Err(Error::invalid_parameter(name));
            }
        }
        if let Some(missing) = op
            .params
            .iter()
            .find(|p| p.required && !params.contains_key(p.name))
        {
            return Err(Error::invalid_parameter(missing.name));
        }
        if op.params.iter().any(|p| p.name == "scope") {
            scope = server.catalog.scope(params.remove("scope"))?;
        }
        (op.run)(server, scope, &params)
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

fn param_schema(name: &str, components: &mut Components) -> RefOr<Schema> {
    match name {
        "scope" => component::<Scope>(components),
        _ => panic!("no schema for parameter {name:?}"),
    }
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
        let mut operation = OperationBuilder::new().operation_id(Some(op.name));
        for param in op.params {
            operation = operation.parameter(
                ParameterBuilder::new()
                    .name(param.name)
                    .parameter_in(ParameterIn::Query)
                    .required(if param.required {
                        Required::True
                    } else {
                        Required::False
                    })
                    .schema(Some(param_schema(param.name, &mut components))),
            );
        }
        let data = (op.result)(&mut components);
        operation = operation
            .response("200", envelope("data", data).description("Success"))
            .response(
                "default",
                envelope("error", Ref::from_schema_name("Error").into())
                    .description("An error in `api/errors.toml`"),
            );
        paths = paths.path(op.path, PathItem::new(HttpMethod::Get, operation));
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
