//! The error catalog (`conformance/api/errors.toml`): one enum, one row per code.
//! `tests/api_spec.rs` keeps it equal to the TOML.

use serde::Serialize;
use serde_json::{Map, Value};
use utoipa::ToSchema;

macro_rules! codes {
    ($($variant:ident $name:literal $class:literal $status:literal $exit:literal
       [$($field:literal),*] $remediation:literal;)*) => {
        /// A stable error code.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Code { $($variant),* }

        impl Code {
            pub const ALL: &[Code] = &[$(Code::$variant),*];

            /// The code's row: name, class, HTTP status and process exit status (0 when
            /// the code has none), `fields` keys and remediation.
            #[must_use]
            pub fn spec(self) -> Spec {
                match self {
                    $(Code::$variant => Spec {
                        name: $name,
                        class: $class,
                        status: $status,
                        exit: $exit,
                        fields: &[$($field),*],
                        remediation: $remediation,
                    }),*
                }
            }
        }
    };
}

pub struct Spec {
    pub name: &'static str,
    pub class: &'static str,
    pub status: u16,
    pub exit: i32,
    pub fields: &'static [&'static str],
    pub remediation: &'static str,
}

codes! {
    InvalidParameter "invalid_parameter" "usage" 422 0 ["parameter"]
        "Correct the named parameter; see /openapi.json for the accepted parameters.";
    InvalidRef "invalid_ref" "usage" 422 0 ["ref"]
        "Pass an FQID or a bare name of a kind this operation takes.";
    InvalidPeriod "invalid_period" "usage" 422 0 ["parameter"]
        "Use the period grammar, for example 2019, 2015..2019 or 2019-03.";
    InvalidCursor "invalid_cursor" "usage" 422 0 []
        "Pass a next_cursor from the same request, or restart without cursor.";
    ScopeUnavailable "scope_unavailable" "usage" 422 0 ["scope"]
        "Use scope=reference, or select a steward catalog.";
    ProjectInvalid "project_invalid" "usage" 422 0 ["issues"]
        "Fix the listed issues; validate reports them.";
    NotFound "not_found" "not_found" 404 0 ["ref"]
        "Search for the entity and use its FQID.";
    DocsUnavailable "docs_unavailable" "not_found" 404 0 []
        "This catalog has no documentation index.";
    AmbiguousRef "ambiguous_ref" "conflict" 409 0 ["ref", "candidates"]
        "Repeat the call with one of the candidates' FQIDs.";
    StaleCursor "stale_cursor" "conflict" 409 0 ["generation"]
        "The catalog changed; restart without cursor.";
    OrderBlocked "order_blocked" "order_blocked" 422 0 ["findings"]
        "Resolve the listed findings and order again.";
    CatalogUnavailable "catalog_unavailable" "unavailable" 503 0 []
        "Retry later; the catalog file could not be read.";
    InternalError "internal_error" "internal" 500 0 []
        "Report the request; this is a defect.";
    MalformedRequest "malformed_request" "usage" 400 0 []
        "Send one UTF-8 JSON object without a byte-order mark or duplicate keys.";
    PayloadTooLarge "payload_too_large" "limit" 413 0 ["limit_bytes"]
        "Send a smaller body.";
    RateLimited "rate_limited" "limit" 429 0 ["retry_after_seconds"]
        "Wait the given number of seconds and retry.";
    DbNotFound "db_not_found" "unavailable" 0 10 []
        "Pass --db with the directory that holds reg_meta.db.";
    SchemaIncompatible "schema_incompatible" "unavailable" 0 10 ["schema_version", "supported"]
        "Install a catalog with a supported schema version.";
    CatalogUnpublishable "catalog_unpublishable" "unavailable" 0 10 []
        "Install a complete, publishable catalog.";
    CatalogMismatch "catalog_mismatch" "unavailable" 0 10 ["configured", "artifact"]
        "Select the artifact's own catalog name, or another artifact.";
    DocSchemaIncompatible "doc_schema_incompatible" "unavailable" 0 10 []
        "Install a documentation database with a supported schema version.";
}

/// The error document `{code, class, message, remediation, fields}` (section 7).
#[derive(Debug, Serialize, ToSchema)]
pub struct Error {
    #[serde(skip)]
    kind: Code,
    code: &'static str,
    class: &'static str,
    message: String,
    remediation: &'static str,
    #[schema(value_type = Object)]
    fields: Map<String, Value>,
}

impl Error {
    /// An error whose `fields` pairs the code's field names with `values`, in order.
    ///
    /// # Panics
    ///
    /// When `values` does not have one value per field name (a defect).
    #[must_use]
    pub fn new(kind: Code, message: impl Into<String>, values: Vec<Value>) -> Self {
        let spec = kind.spec();
        assert_eq!(spec.fields.len(), values.len(), "{} fields", spec.name);
        Self {
            kind,
            code: spec.name,
            class: spec.class,
            message: message.into(),
            remediation: spec.remediation,
            fields: spec
                .fields
                .iter()
                .map(|f| (*f).to_owned())
                .zip(values)
                .collect(),
        }
    }

    #[must_use]
    pub fn invalid_parameter(name: &str) -> Self {
        Self::new(
            Code::InvalidParameter,
            format!("Invalid parameter {name:?}."),
            vec![name.into()],
        )
    }

    /// The HTTP status (500 for a code without one).
    #[must_use]
    pub fn status(&self) -> u16 {
        match self.kind.spec().status {
            0 => 500,
            status => status,
        }
    }

    /// The process exit status; a bad argument (a code without one) exits 2.
    #[must_use]
    pub fn exit(&self) -> i32 {
        match self.kind.spec().exit {
            0 => 2,
            exit => exit,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::new(
            Code::InternalError,
            format!("Catalog query failed: {err}"),
            vec![],
        )
    }
}
