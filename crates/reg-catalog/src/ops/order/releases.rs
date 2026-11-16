//! Concurrent tables (crates/DESIGN.md, "Concurrent releases"): a day two primary
//! releases of a serving table share is served by neither until the order names one, so
//! the source blocks there with `release_ambiguous`, listing the releases that share it.

use std::collections::BTreeSet;

use reg_core::{Interval, merge, overlap};
use rusqlite::Connection;

use super::{Serving, interval};
use crate::{Error, rows};

/// The requested days of serving table `s` that two of its primary releases share, one
/// per run, each with the names of the releases sharing it in start order.
// simplify: the base database's releases only; a holdings database whose own releases
// of a base table share days is the holdings schema's to model (holdings PR).
pub(super) fn shared(
    conn: &Connection,
    s: &Serving,
) -> Result<Vec<(Interval, Vec<String>)>, Error> {
    let primaries: Vec<(Interval, String)> = rows(
        conn,
        "SELECT r.start, r.end, n.body FROM main.release r
         JOIN main.text n ON n.id = r.name_text
         WHERE r.table_id = ?1 AND r.is_primary = 1 AND r.start IS NOT NULL
         ORDER BY r.start, n.body",
        [s.table.id],
        |row| Ok((interval(row.get(0)?, row.get(1)?), row.get(2)?)),
    )?;
    // Each pair's shared requested days, with the pair.
    let mut pairs: Vec<(Interval, [usize; 2])> = Vec::new();
    for (i, a) in primaries.iter().enumerate() {
        for (j, b) in primaries.iter().enumerate().skip(i + 1) {
            if b.0.0 > a.0.1 {
                break;
            }
            for days in overlap(
                &overlap(std::slice::from_ref(&a.0), std::slice::from_ref(&b.0)),
                &s.requested,
            ) {
                pairs.push((days, [i, j]));
            }
        }
    }
    Ok(merge(pairs.iter().map(|(days, _)| days.clone()).collect())
        .into_iter()
        .map(|run| {
            let sharing: BTreeSet<usize> = pairs
                .iter()
                .filter(|(days, _)| {
                    !overlap(std::slice::from_ref(days), std::slice::from_ref(&run)).is_empty()
                })
                .flat_map(|(_, pair)| *pair)
                .collect();
            let names = sharing
                .into_iter()
                .map(|i| primaries[i].1.clone())
                .collect();
            (run, names)
        })
        .collect())
}
