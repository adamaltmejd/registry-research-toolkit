//! Refs (section 7): a FQID, a group ref or a bare name, resolved in the read scope.
//! [`resolve`] takes any kind (`show`); [`register`] takes a register only
//! (`search`'s `register`, `docs_related`).

use std::collections::BTreeSet;

use reg_core::Fqid;
use rusqlite::{Connection, OptionalExtension, params_from_iter};
use serde_json::json;

use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

/// The first segment of classification FQIDs and of classification group refs.
const CLASS: &str = "class";
/// The first segment of group refs.
const GROUP: &str = "group";

/// What a ref names, resolved in the read scope.
pub(crate) enum Target {
    /// No ref: the catalog root.
    Root,
    Provider {
        id: i64,
        slug: String,
    },
    Register {
        id: i64,
        provider: String,
        slug: String,
    },
    Variable {
        id: i64,
    },
    /// `class`: every classification.
    ClassificationRoot,
    Classification {
        id: i64,
        slug: String,
    },
    /// `group/<provider>/<register>/<key>`; whether the group has members in scope
    /// is the caller's to decide.
    Group {
        register_id: i64,
        provider: String,
        register: String,
        key: String,
    },
    /// `group/class/<key>`: a classification group or family, or nothing.
    ClassificationGroup {
        key: String,
    },
}

pub(crate) fn not_found(value: &str) -> Error {
    Error::new(
        Code::NotFound,
        format!("Nothing named {value:?} in this scope."),
        vec![value.into()],
    )
}

fn invalid(value: &str) -> Error {
    Error::new(
        Code::InvalidRef,
        format!("{value:?} is not a FQID, a group ref or a name."),
        vec![value.into()],
    )
}

/// Resolve `value` (`None` is the root): `class`; a group ref; a FQID, where a
/// retired register or variable resolves to its terminal successor when that is in
/// scope ([`successor`]), and one live outside the scope is `not_found`; otherwise
/// a bare name. A one-segment ref is a provider first, then a bare name, so an exact
/// ref never turns ambiguous.
pub(crate) fn resolve(
    conn: &Connection,
    scope: Scope,
    value: Option<&str>,
) -> Result<Target, Error> {
    let Some(value) = value else {
        return Ok(Target::Root);
    };
    if value == CLASS {
        return Ok(Target::ClassificationRoot);
    }
    let segments: Vec<&str> = value.split('/').collect();
    if segments[0] == GROUP {
        return group(conn, value, &segments[1..]);
    }
    match value.parse::<Fqid>() {
        Ok(Fqid::Provider { provider }) => {
            let sql = format!(
                "SELECT provider_id FROM provider p WHERE slug = ? AND {}",
                held::provider(scope, "p.provider_id")
            );
            if let Some(id) = conn
                .query_row(&sql, [&provider], |row| row.get(0))
                .optional()?
            {
                return Ok(Target::Provider { id, slug: provider });
            }
            bare(conn, scope, value)
        }
        Ok(Fqid::Register { provider, register }) => {
            let (id, slugs) =
                live_or_terminal(conn, scope, value, &[provider, register], &REGISTER)?;
            let [provider, slug] = <[String; 2]>::try_from(slugs).expect("a register pair");
            Ok(Target::Register { id, provider, slug })
        }
        Ok(Fqid::Variable {
            provider,
            register,
            variable,
        }) => {
            let (id, _) = live_or_terminal(
                conn,
                scope,
                value,
                &[provider, register, variable],
                &VARIABLE,
            )?;
            Ok(Target::Variable { id })
        }
        // Classifications are scope-independent. The build refuses a succession or
        // same_as edge to a slug with no row, so a missing slug has no successor.
        Ok(Fqid::Classification { classification }) => conn
            .query_row(
                "SELECT id FROM classification WHERE slug = ?",
                [&classification],
                |row| row.get(0),
            )
            .optional()?
            .map(|id| Target::Classification {
                id,
                slug: classification,
            })
            .ok_or_else(|| not_found(value)),
        Err(_) if !value.is_empty() && !value.contains('/') => bare(conn, scope, value),
        Err(_) => Err(invalid(value)),
    }
}

/// `group/class/<key>` or `group/<provider>/<register>/<key>`.
fn group(conn: &Connection, value: &str, rest: &[&str]) -> Result<Target, Error> {
    match rest {
        [CLASS, key] if !key.is_empty() => Ok(Target::ClassificationGroup {
            key: (*key).to_owned(),
        }),
        [provider, register, key] if !key.is_empty() => {
            let Ok(Fqid::Register { provider, register }) =
                format!("{provider}/{register}").parse()
            else {
                return Err(invalid(value));
            };
            // Membership in scope decides the group; the register need only exist.
            let register_id = register_id(
                conn,
                Scope::Reference,
                &[provider.clone(), register.clone()],
            )?
            .ok_or_else(|| not_found(value))?;
            Ok(Target::Group {
                register_id,
                provider,
                register,
                key: (*key).to_owned(),
            })
        }
        _ => Err(invalid(value)),
    }
}

type Lookup = fn(&Connection, Scope, &[String]) -> Result<Option<i64>, Error>;

/// How a register or a variable is looked up and succeeded.
struct Grain {
    kind: &'static str,
    /// The id at the slugs, in the scope.
    lookup: Lookup,
    /// The successors' slugs by predecessor slugs; `{policy}` is the policy year.
    successors: &'static str,
    /// The name at the slugs, for an `ambiguous_ref` candidate.
    name: &'static str,
}

const REGISTER: Grain = Grain {
    kind: "register",
    lookup: register_id,
    successors: "SELECT successor_provider, successor_register FROM register_replaced_by \
        WHERE predecessor_provider = ? AND predecessor_register = ? \
        AND (effective_year IS NULL OR effective_year <= {policy}) ORDER BY 1, 2",
    name: "SELECT r.name FROM register r JOIN provider p USING(provider_id) \
        WHERE p.slug = ? AND r.slug = ?",
};

const VARIABLE: Grain = Grain {
    kind: "variable",
    lookup: variable_id,
    successors: "SELECT successor_provider, successor_register, successor_variable \
        FROM variable_replaced_by WHERE predecessor_provider = ? \
        AND predecessor_register = ? AND predecessor_variable = ? \
        AND (effective_year IS NULL OR effective_year <= {policy}) ORDER BY 1, 2, 3",
    name: "SELECT v.name FROM variable v WHERE v.register_id IN (SELECT r.register_id \
        FROM register r JOIN provider p USING(provider_id) WHERE p.slug = ? AND r.slug = ?) \
        AND v.slug = ?",
};

/// The entity at `slugs` in scope; else, when it is live in no scope, its terminal
/// successor ([`successor`]) when that is in scope; else `not_found`. Returns the id
/// and the slugs it was found at.
fn live_or_terminal(
    conn: &Connection,
    scope: Scope,
    value: &str,
    slugs: &[String],
    grain: &Grain,
) -> Result<(i64, Vec<String>), Error> {
    let found = if let Some(id) = (grain.lookup)(conn, scope, slugs)? {
        Some((id, slugs.to_vec()))
    } else if scope == Scope::Holdings && (grain.lookup)(conn, Scope::Reference, slugs)?.is_some() {
        None
    } else {
        match successor(conn, value, slugs, grain)? {
            Some(terminal) => (grain.lookup)(conn, scope, &terminal)?.map(|id| (id, terminal)),
            None => None,
        }
    };
    found.ok_or_else(|| not_found(value))
}

/// The terminal successor of a retired register or variable at the policy year
/// ([`policy_year`]): follow the one active successor to the chain's end. An edge
/// dated after the policy year is not active, so it is not followed; a split
/// (several active successors) is `ambiguous_ref` with the successors as
/// candidates. None when `start` has no active successor.
fn successor(
    conn: &Connection,
    value: &str,
    start: &[String],
    grain: &Grain,
) -> Result<Option<Vec<String>>, Error> {
    let sql = grain
        .successors
        .replace("{policy}", &policy_year(conn)?.to_string());
    let mut stmt = conn.prepare(&sql)?;
    let mut seen = BTreeSet::from([start.to_vec()]);
    let mut current = start.to_vec();
    loop {
        let next: Vec<Vec<String>> = stmt
            .query_map(params_from_iter(&current), |row| {
                (0..current.len())
                    .map(|i| row.get(i))
                    .collect::<rusqlite::Result<Vec<String>>>()
            })?
            .collect::<rusqlite::Result<_>>()?;
        match next.as_slice() {
            [] => break,
            // A cycle (a malformed artifact) stops the walk.
            [one] if !seen.insert(one.clone()) => break,
            [one] => current.clone_from(one),
            split => {
                let candidates = split
                    .iter()
                    .map(|slugs| {
                        let name: Option<String> = conn
                            .query_row(grain.name, params_from_iter(slugs), |row| row.get(0))
                            .optional()?;
                        Ok(json!({"fqid": slugs.join("/"), "kind": grain.kind, "name": name}))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                return Err(Error::new(
                    Code::AmbiguousRef,
                    format!("{value:?} was split into {} successors.", split.len()),
                    vec![value.into(), candidates.into()],
                ));
            }
        }
    }
    Ok((current != start).then_some(current))
}

/// The manifest's succession policy year, `classification_succession_as_of_year`
/// (today's reader default when the manifest has none).
pub(crate) fn policy_year(conn: &Connection) -> Result<i64, Error> {
    const DEFAULT: i64 = 2026;
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM import_manifest WHERE key = 'classification_succession_as_of_year'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(value.and_then(|v| v.parse().ok()).unwrap_or(DEFAULT))
}

fn register_id(conn: &Connection, scope: Scope, slugs: &[String]) -> Result<Option<i64>, Error> {
    let sql = format!(
        "SELECT r.register_id FROM register r JOIN provider p USING(provider_id) \
         WHERE p.slug = ? AND r.slug = ? AND {}",
        held::register(scope, "r.register_id")
    );
    Ok(conn
        .query_row(&sql, params_from_iter(slugs), |row| row.get(0))
        .optional()?)
}

fn variable_id(conn: &Connection, scope: Scope, slugs: &[String]) -> Result<Option<i64>, Error> {
    // The register subquery keys idx_variable_slug(register_id, slug).
    let sql = format!(
        "SELECT v.variable_id FROM variable v WHERE v.register_id IN (SELECT r.register_id \
         FROM register r JOIN provider p USING(provider_id) WHERE p.slug = ? AND r.slug = ?) \
         AND v.slug = ? AND {}",
        held::variable(scope, "v.variable_id", Narrow::default())
    );
    Ok(conn
        .query_row(&sql, params_from_iter(slugs), |row| row.get(0))
        .optional()?)
}

/// A bare name: the registers, variables (by `fold_identity` of their name) and
/// classifications (of their short name or name) in scope. One resolves; several
/// are `ambiguous_ref` with their FQIDs. Delivered column names are not names
/// (`resolve` maps them).
fn bare(conn: &Connection, scope: Scope, value: &str) -> Result<Target, Error> {
    // simplify: fold_identity over every variable name per request; give `variable`
    // a folded-name column if the measured request time on the real artifact grows.
    let sql = format!(
        "SELECT 'register', p.slug, r.slug, NULL, r.name FROM register r \
         JOIN provider p USING(provider_id) \
         WHERE r.slug IS NOT NULL AND fold_identity(r.name) = fold_identity(?1) AND {} \
         UNION ALL \
         SELECT 'variable', p.slug, r.slug, v.slug, v.name FROM variable v \
         JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
         WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL \
         AND fold_identity(v.name) = fold_identity(?1) AND {} \
         UNION ALL \
         SELECT 'classification', '{CLASS}', c.slug, NULL, c.short_name FROM classification c \
         WHERE c.slug IS NOT NULL AND (fold_identity(c.short_name) = fold_identity(?1) \
         OR fold_identity(c.name) = fold_identity(?1))",
        held::register(scope, "r.register_id"),
        held::variable(scope, "v.variable_id", Narrow::default()),
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut found: Vec<(String, &'static str, Option<String>)> = stmt
        .query_map([value], |row| {
            let kind: String = row.get(0)?;
            let slugs: [Option<String>; 3] = [row.get(1)?, row.get(2)?, row.get(3)?];
            let present: Vec<Option<String>> = slugs.into_iter().flatten().map(Some).collect();
            let kind = match kind.as_str() {
                "register" => "register",
                "variable" => "variable",
                _ => "classification",
            };
            Ok((fqid(&present).unwrap_or_default(), kind, row.get(4)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    found.retain(|(fqid, ..)| !fqid.is_empty());
    found.sort();
    match found.as_slice() {
        [] => Err(not_found(value)),
        [(fqid, ..)] => resolve(conn, scope, Some(fqid)),
        _ => Err(Error::new(
            Code::AmbiguousRef,
            format!("{} entities are named {value:?}.", found.len()),
            vec![
                value.into(),
                found
                    .iter()
                    .map(|(fqid, kind, name)| json!({"fqid": fqid, "kind": kind, "name": name}))
                    .collect(),
            ],
        )),
    }
}

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
        return register_id(conn, scope, &[provider, register.clone()])?
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
