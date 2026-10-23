//! A source's variant succession family (#1840): the members a dated source period
//! reaches, and which member serves each requested day (crates/DESIGN.md, "Order
//! manifest", step 1). The family is `show`'s succession component
//! ([`variant_families`]), or the named variant alone; `_default` resolves the named
//! variant exactly.
//!
//! The build refuses a family whose members deliver a shared day (`validate_built_db`),
//! so each day has at most one delivering member and the split needs no tie-break.

use std::collections::BTreeMap;

use reg_core::{Interval, gaps, merge, overlap};
use rusqlite::{Connection, OptionalExtension, params};

use super::show::{rows, variant_families};
use crate::Error;

/// One member's share of a request: its variant id and the requested days it serves.
pub(super) type Part = (i64, Vec<Interval>);

/// The variants a source's `register_variant` reaches.
pub(super) struct VariantFamily {
    /// The variant the coordinate names.
    pub(super) named: i64,
    members: Vec<Member>,
}

/// A family member: its id, slug and the days its states deliver.
struct Member {
    id: i64,
    slug: String,
    delivered: Vec<Interval>,
}

impl VariantFamily {
    /// Each member's share of `requested`, ordered by first day. A day goes to the
    /// member whose states deliver it, else to the named variant, where availability
    /// then clips it: no variant is invented. A day the delivering member holds no
    /// table for stays with it and blocks as a coverage gap, even where another
    /// member holds some table that day.
    pub(super) fn split(&self, requested: &[Interval]) -> Vec<Part> {
        if self.members.len() == 1 {
            return vec![(self.named, requested.to_vec())];
        }
        let mut rest = requested.to_vec();
        let mut shares: BTreeMap<i64, Vec<Interval>> = BTreeMap::new();
        for member in &self.members {
            let share = overlap(&rest, &member.delivered);
            if !share.is_empty() {
                rest = gaps(&rest, share.clone());
                shares.entry(member.id).or_default().extend(share);
            }
        }
        shares.entry(self.named).or_default().extend(rest);
        let mut parts: Vec<Part> = shares
            .into_iter()
            .map(|(id, days)| (id, merge(days)))
            .filter(|(_, days)| !days.is_empty())
            .collect();
        parts.sort_by(|a, b| a.1.cmp(&b.1));
        parts
    }

    /// A member's slug.
    pub(super) fn slug(&self, id: i64) -> &str {
        &self
            .members
            .iter()
            .find(|m| m.id == id)
            .expect("a member of the family")
            .slug
    }
}

/// The family of `<provider>/<register>/<variant>`; none when it names no variant.
pub(super) fn variant_family(
    conn: &Connection,
    coordinate: &str,
) -> Result<Option<VariantFamily>, Error> {
    let parts: Vec<&str> = coordinate.split('/').collect();
    let [provider, register, variant] = parts[..] else {
        unreachable!("the structural validator admits only 3-part register variants")
    };
    let Some((register_id, named)): Option<(i64, i64)> = conn
        .prepare_cached(
            "SELECT r.register_id, rv.register_variant_id FROM register_variant rv \
             JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
             WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
        )?
        .query_row([provider, register, variant], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?
    else {
        return Ok(None);
    };
    let families = variant_families(conn, register_id, provider, register)?;
    let Some((head, _)) = families.get(variant) else {
        let members = vec![Member {
            id: named,
            slug: variant.to_owned(),
            delivered: Vec::new(),
        }];
        return Ok(Some(VariantFamily { named, members }));
    };
    let mut members = Vec::new();
    for (slug, _) in families.iter().filter(|(_, (key, _))| key == head) {
        let id: i64 = conn
            .prepare_cached(
                "SELECT register_variant_id FROM register_variant \
                 WHERE register_id = ? AND slug = ?",
            )?
            .query_row(params![register_id, slug], |row| row.get(0))?;
        members.push(Member {
            id,
            slug: slug.clone(),
            delivered: delivered(conn, id)?,
        });
    }
    Ok(Some(VariantFamily { named, members }))
}

/// The windows of `variant`'s dated states.
fn delivered(conn: &Connection, variant: i64) -> Result<Vec<Interval>, Error> {
    Ok(merge(rows(
        conn,
        "SELECT DISTINCT valid_from, valid_to FROM variable_state \
         WHERE register_variant_id = ? AND period_scope = 'intervals' \
         AND valid_from IS NOT NULL AND valid_to IS NOT NULL",
        [variant],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?))
}
