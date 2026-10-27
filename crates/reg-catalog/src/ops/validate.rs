//! `validate`: every issue of a `project_data.json` document. For now only the
//! structural layer of `reg-core` runs, without the catalog (the supported-version
//! decision first, then the structural rules); v5's catalog rules come with the order
//! check. An invalid project is a result (`ok: false`), never an error.

use reg_core::ValidationResult;
use reg_core::project::check;
use serde_json::Value;

use super::{Params, Server};
use crate::{Error, Scope};

pub fn validate(_server: &Server, _scope: Scope, params: &Params) -> Result<Value, Error> {
    // The transports hand the body over as JSON text: HTTP after refusing malformed
    // bytes, MCP from its object argument.
    let raw: Value = serde_json::from_str(params["project"])
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| Error::invalid_parameter("project"))?;
    let result = check(&raw)
        .err()
        .unwrap_or(ValidationResult { issues: vec![] });
    Ok(serde_json::to_value(result).expect("ValidationResult serializes"))
}
