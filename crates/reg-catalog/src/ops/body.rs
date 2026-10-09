//! A POST's JSON body: one UTF-8 JSON object without a byte-order mark, duplicate keys
//! at any depth (last-wins would silently validate the wrong value) or nesting past
//! `serde_json`'s depth limit (128 levels; frozen Python's recursion limit allowed about
//! 1,000, so a project nested between the two is refused here and validated there).
//! Anything else is `malformed_request`. Today's
//! `reg_meta.order.parse_project`. The HTTP transport applies it to a POST operation's
//! body; MCP takes the body as an object argument, already parsed.

use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use crate::{Code, Error};

/// The cap on a request body, POST operations' and `/mcp`'s: today's `limits.py`
/// cap on write bodies.
pub const MAX_BYTES: usize = 1024 * 1024;

/// `payload_too_large`: a body over [`MAX_BYTES`].
#[must_use]
pub fn too_large() -> Error {
    Error::new(
        Code::PayloadTooLarge,
        format!("The request body exceeds {MAX_BYTES} bytes."),
        vec![MAX_BYTES.into()],
    )
}

/// `bytes` as a JSON object.
///
/// # Errors
///
/// `malformed_request`, saying what is wrong.
pub fn parse(bytes: &[u8]) -> Result<Value, Error> {
    let malformed = |message: String| Error::new(Code::MalformedRequest, message, vec![]);
    if bytes.starts_with(b"\xEF\xBB\xBF") {
        return Err(malformed(
            "The body must be UTF-8 without a byte-order mark.".into(),
        ));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|err| malformed(format!("The body is not valid UTF-8: {err}.")))?;
    let Strict(value) = serde_json::from_str(text)
        .map_err(|err| malformed(format!("The body is not valid JSON: {err}.")))?;
    if !value.is_object() {
        return Err(malformed("The body must be a JSON object.".into()));
    }
    Ok(value)
}

/// A JSON value that refuses a duplicate object key.
struct Strict(Value);

impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictVisitor).map(Strict)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(v.into())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(Strict(item)) = seq.next_element()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let Strict(value) = map.next_value()?;
            if object.contains_key(&key) {
                return Err(de::Error::custom(format_args!(
                    "duplicate key {}",
                    reg_core::quote(&key)
                )));
            }
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}
