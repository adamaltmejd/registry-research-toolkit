//! Local mock data from a `project_data.json` (#707, crates/reg-mock/DESIGN.md): the
//! mock rules and spec contracts and `compile`, which `reg-meta mock` runs offline on
//! the researcher's machine. Reads no data rows: only catalog metadata and the rules.

pub mod compile;
pub mod input;
pub mod rules;
pub mod spec;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// A contract document read from JSON, refusing a struct written as an array: serde
/// reads `[a, b]` as `{"x": a, "y": b}` by position, which `deny_unknown_fields` does
/// not catch. Every object of the read value must be an object in the input.
///
/// # Errors
/// The serde error, or the path of the first struct written as an array.
pub fn from_json<T: DeserializeOwned + Serialize>(input: &Value) -> Result<T, String> {
    let value: T = serde_json::from_value(input.clone()).map_err(|e| e.to_string())?;
    let read = serde_json::to_value(&value).map_err(|e| e.to_string())?;
    objects_as_objects(input, &read, "")?;
    Ok(value)
}

fn objects_as_objects(input: &Value, read: &Value, at: &str) -> Result<(), String> {
    match (input, read) {
        (Value::Object(given), Value::Object(read)) => {
            for (key, value) in read {
                if let Some(given) = given.get(key) {
                    objects_as_objects(given, value, &format!("{at}/{key}"))?;
                }
            }
            Ok(())
        }
        (Value::Array(given), Value::Array(read)) => {
            for (i, (given, value)) in given.iter().zip(read).enumerate() {
                objects_as_objects(given, value, &format!("{at}/{i}"))?;
            }
            Ok(())
        }
        (_, Value::Object(_)) => Err(format!("{at}: an object is written as another value")),
        _ => Ok(()),
    }
}

/// A located refusal: a stable `code`, where (`at`: a file, source, table or column),
/// and what to do. Never holds a data value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal {
    pub code: &'static str,
    pub at: String,
    pub message: String,
}

impl Refusal {
    #[must_use]
    pub fn new(code: &'static str, at: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            at: at.into(),
            message: message.into(),
        }
    }
}
