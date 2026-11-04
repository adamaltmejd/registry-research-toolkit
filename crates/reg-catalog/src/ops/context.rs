//! `context`: what the site shell shows, computed once when the catalog is admitted
//! and answered from memory.

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Params, Period, Server, component};
use crate::{Catalog, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "context",
    paths: &["/api/context"],
    tool: None,
    description: "The catalog's period span, the server version and the steward's presentation.",
    params: &[],
    cache: Cache::Revalidate,
    run: context,
    result: component::<Context>,
}];

/// What the site shell shows. Catalog identity is in every response's `meta`.
#[derive(Serialize, ToSchema)]
pub struct Context {
    /// The hull of the periods served: in steward mode the years held, else the
    /// registers' periods. A timeless period does not count; an ongoing one leaves
    /// `end` null. `null` when every period served is timeless, or when the
    /// hull is open at both ends.
    #[schema(required = true)]
    period_span: Option<Period>,
    /// The server's version, which seeds a new project's `reg_meta_version`.
    reg_meta_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = crate::holdings::StewardConfig)]
    steward_config: Option<Value>,
}

impl Context {
    /// `catalog`'s context; `open` is its `open_start` and `open_end`.
    pub(crate) fn load(catalog: &Catalog, open: (i64, i64), version: &str) -> Result<Self, Error> {
        // Steward mode spans the placements, not the reference registers: the
        // reference catalog covers years the steward does not hold.
        let (conn, from) = match catalog.connect_holdings()? {
            Some(held) => (held, "holding_mapping"),
            None => (catalog.connect()?, "register"),
        };
        // A timeless period (both bounds open) has no years; one open bound (an
        // ongoing register) stays open. Dropping only whole periods keeps the hull
        // ordered, which dropping open bounds one by one does not.
        let hull: (Option<i64>, Option<i64>) = conn.query_row(
            &format!("SELECT MIN(start), MAX(end) FROM {from} WHERE NOT (start = ?1 AND end = ?2)"),
            [open.0, open.1],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(Self {
            period_span: match hull {
                // Mixed open bounds (one period open at the start, another at the
                // end) give a both-open hull, which would read as timeless.
                (Some(start), Some(end)) if (start, end) != open => {
                    Some(Period::new(open, start, end))
                }
                _ => None,
            },
            reg_meta_version: version.to_owned(),
            steward_config: catalog.steward_config(),
        })
    }
}

#[expect(clippy::unnecessary_wraps, reason = "an operation's `Run` signature")]
fn context(server: &Server, _: &Params) -> Result<Value, Error> {
    Ok(serde_json::to_value(&server.context).expect("Context serializes"))
}
