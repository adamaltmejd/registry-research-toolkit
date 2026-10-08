//! Refs (section 7): a FQID or a bare name, resolved in the read scope. Today: a
//! register ref (`search`'s `register`, `docs_related`).

use reg_core::Fqid;
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;

use crate::held;
use crate::{Code, Error, Scope};

/// A resolved register.
pub(crate) struct Register {
    pub id: i64,
    /// Its slug; a register without one has no FQID.
    pub slug: Option<String>,
}

/// Today's register lookup for `--register`, as a ref: a FQID with a `/` resolves
/// by slugs; a one-segment ref is a bare name, matched on `fold_identity`. Either
/// must name a register in scope.
pub(crate) fn register(conn: &Connection, scope: Scope, value: &str) -> Result<Register, Error> {
    let in_scope = held::register(scope, "r.register_id");
    let not_found = || {
        Error::new(
            Code::NotFound,
            format!("No register {value:?} in this scope."),
            vec![value.into()],
        )
    };
    if value.contains('/') {
        let Ok(Fqid::Register { provider, register }) = value.parse() else {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is not a register FQID or name."),
                vec![value.into()],
            ));
        };
        let sql = format!(
            "SELECT r.register_id FROM register r JOIN provider p USING(provider_id) \
             WHERE p.slug = ? AND r.slug = ? AND {in_scope}"
        );
        let id: Option<i64> = conn
            .query_row(&sql, [&provider, &register], |row| row.get(0))
            .optional()?;
        return id
            .map(|id| Register {
                id,
                slug: Some(register),
            })
            .ok_or_else(not_found);
    }
    let sql = format!(
        "SELECT r.register_id, p.slug, r.slug, r.name FROM register r \
         JOIN provider p USING(provider_id) \
         WHERE fold_identity(r.name) = fold_identity(?) AND {in_scope} ORDER BY r.register_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let found: Vec<(i64, Option<String>, Option<String>, String)> = stmt
        .query_map([value], |row| {
            let slug: Option<String> = row.get(2)?;
            let fqid = fqid(&[row.get(1)?, slug.clone()]);
            Ok((row.get(0)?, slug, fqid, row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    match found.as_slice() {
        [] => Err(not_found()),
        [(id, slug, ..)] => Ok(Register {
            id: *id,
            slug: slug.clone(),
        }),
        _ => Err(Error::new(
            Code::AmbiguousRef,
            format!("{} registers are named {value:?}.", found.len()),
            vec![
                value.into(),
                found
                    .iter()
                    .map(|(_, _, fqid, name)| {
                        json!({"fqid": fqid, "kind": "register", "name": name})
                    })
                    .collect(),
            ],
        )),
    }
}

/// A FQID from its slugs, or None when one is missing or not a slug (today's
/// `try_emit`).
pub(crate) fn fqid(slugs: &[Option<String>]) -> Option<String> {
    let joined = slugs
        .iter()
        .map(|s| s.as_deref().filter(|s| !s.is_empty()))
        .collect::<Option<Vec<_>>>()?
        .join("/");
    joined.parse::<Fqid>().ok().map(|_| joined)
}
