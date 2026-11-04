//! An insertion-ordered JSON value, written with Python's `json.dumps` separators.
//!
//! `serde_json::Value` sorts object keys unless its `preserve_order` feature is on, and
//! that feature would unify into every workspace crate built alongside this one. `gh`
//! emits comment keys in its own order, which the gate must pass through unchanged.

use std::fmt;
use std::io::{self, Write};

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Number;
use serde_json::ser::Formatter;

#[derive(Clone)]
pub enum Json {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// The value of `key` in an object; the last one wins, as in Python's `json.loads`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    /// Serialized like Python's `json.dumps(value, ensure_ascii=False)`.
    ///
    /// # Panics
    ///
    /// Never: the writer is a `Vec` and a `Json` holds no non-finite float.
    #[must_use]
    pub fn dumps(&self) -> String {
        let mut buf = Vec::new();
        let mut ser = serde_json::Serializer::with_formatter(&mut buf, PythonFormatter);
        self.serialize(&mut ser)
            .expect("writing to a Vec cannot fail");
        String::from_utf8(buf).expect("serde_json writes UTF-8")
    }
}

// simplify: floats print as serde_json does (`1e16`), not as Python's repr (`1e+16`);
// `gh issue view` payloads carry no floats. Revisit if a requested field gains one.
impl Serialize for Json {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Json::Null => serializer.serialize_unit(),
            Json::Bool(b) => serializer.serialize_bool(*b),
            Json::Number(n) => n.serialize(serializer),
            Json::String(s) => serializer.serialize_str(s),
            Json::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Json::Object(members) => {
                let mut map = serializer.serialize_map(Some(members.len()))?;
                for (key, value) in members {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_bool<E>(self, b: bool) -> Result<Json, E> {
        Ok(Json::Bool(b))
    }

    fn visit_i64<E>(self, n: i64) -> Result<Json, E> {
        Ok(Json::Number(n.into()))
    }

    fn visit_u64<E>(self, n: u64) -> Result<Json, E> {
        Ok(Json::Number(n.into()))
    }

    fn visit_f64<E>(self, n: f64) -> Result<Json, E> {
        // JSON text holds no NaN or infinity, so this never maps to null in practice.
        Ok(Number::from_f64(n).map_or(Json::Null, Json::Number))
    }

    fn visit_str<E>(self, s: &str) -> Result<Json, E> {
        Ok(Json::String(s.to_owned()))
    }

    fn visit_string<E>(self, s: String) -> Result<Json, E> {
        Ok(Json::String(s))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Json::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut members = Vec::new();
        while let Some(member) = map.next_entry()? {
            members.push(member);
        }
        Ok(Json::Object(members))
    }
}

/// Python's default `json.dumps` separators: `", "` between items, `": "` after a key.
/// String escaping already agrees with `ensure_ascii=False`.
struct PythonFormatter;

impl Formatter for PythonFormatter {
    fn begin_array_value<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }
}
