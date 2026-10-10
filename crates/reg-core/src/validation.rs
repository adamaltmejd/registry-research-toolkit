//! The project validation result (`conformance/api/operations.toml`, `validate`:
//! today's `{ok, issues}`), shared by the structural and the semantic layer.
//!
//! With the `openapi` feature the types carry the `Validation`, `ValidationIssue` and
//! `IssueLevel` schemas the server publishes; a doc comment on a type or field here is
//! its schema description, so the comments that are not descriptions are plain ones.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

// How much an issue blocks. The structural layer emits only errors; 3e.2's semantic
// layer emits warnings and infos too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum IssueLevel {
    Error,
    Warning,
    Info,
}

/// One finding about the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ValidationIssue {
    pub level: IssueLevel,
    /// A stable identifier, such as `period_outside_state_validity`.
    pub code: &'static str,
    /// An RFC 6901 JSON pointer into the document; empty for the whole document.
    pub path: String,
    pub message: String,
    /// The successor a `variable_replaced` finding names; null otherwise.
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub successor_fqid: Option<String>,
}

/// Every issue a validation found, in the order the layers emitted them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationResult {
    pub issues: Vec<ValidationIssue>,
}

impl ValidationResult {
    /// No issue is an error. Warnings and infos do not block, so `ok` is not a clean
    /// bill of health.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.issues.iter().all(|i| i.level != IssueLevel::Error)
    }
}

impl Serialize for ValidationResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("ValidationResult", 2)?;
        s.serialize_field("ok", &self.ok())?;
        s.serialize_field("issues", &self.issues)?;
        s.end()
    }
}

/// The wire shape [`Serialize`] writes: `ok` is computed, so no field derives it.
#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for ValidationResult {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, Type};
        use utoipa::openapi::{Ref, RefOr};
        RefOr::T(
            ObjectBuilder::new()
                .description(Some(
                    "The validation result: `ok` when no issue is an error.",
                ))
                .property("ok", ObjectBuilder::new().schema_type(Type::Boolean))
                .required("ok")
                .property(
                    "issues",
                    ArrayBuilder::new()
                        .items(Ref::from_schema_name("ValidationIssue"))
                        .description(Some(
                            "In emission order: per source, its variant and period, then \
                             per binding.",
                        )),
                )
                .required("issues")
                .into(),
        )
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for ValidationResult {
    fn name() -> std::borrow::Cow<'static, str> {
        "Validation".into()
    }

    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        use utoipa::PartialSchema;
        schemas.push((
            ValidationIssue::name().into_owned(),
            ValidationIssue::schema(),
        ));
        ValidationIssue::schemas(schemas);
    }
}
