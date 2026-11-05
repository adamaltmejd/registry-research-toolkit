//! `search`'s depth bound, which only a real artifact has the hits to reach.

use std::collections::HashSet;

use serde_json::Value;

use crate::{artifact, get, target};

const DEPTH: usize = 1000;
const PAGE: usize = 200;

#[test]
fn paging_stops_at_depth() {
    // Fails if paging passes the 1000th hit (the fifth full page still has a
    // `next_cursor`), or if offset paging repeats a hit across pages.
    let artifact = artifact();
    let port = artifact.port();
    let limit = PAGE.to_string();
    let deep = ('a'..='z').find_map(|letter| {
        let q = letter.to_string();
        let mut pages: Vec<Value> = Vec::new();
        let mut cursor: Option<String> = None;
        while pages.len() < DEPTH / PAGE {
            let mut query = vec![("q", q.as_str()), ("limit", limit.as_str())];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor));
            }
            let page = get(port, &target("/api/search", &query))
                .unwrap_or_else(|e| panic!("{letter} page {}: {e}", pages.len()))
                .get_mut("data")
                .map(Value::take)
                .expect("a search page");
            cursor = page["next_cursor"].as_str().map(str::to_owned);
            pages.push(page);
            if cursor.is_none() {
                break;
            }
        }
        let full = pages.len() == DEPTH / PAGE
            && pages
                .last()
                .is_some_and(|p| p["items"].as_array().map_or(0, Vec::len) == PAGE);
        full.then_some((letter, pages))
    });
    let Some((letter, pages)) = deep else {
        // A real catalog reaching no depth would mean a broken index: fail there.
        assert!(!artifact.real, "no single letter reaches {DEPTH} hits");
        eprintln!("paging_stops_at_depth: skipped, the fixture has fewer than {DEPTH} hits");
        return;
    };
    assert!(
        pages.last().expect("a page")["next_cursor"].is_null(),
        "{letter}: a cursor past depth {DEPTH}"
    );
    let hits: Vec<String> = pages
        .iter()
        .flat_map(|page| page["items"].as_array().cloned().unwrap_or_default())
        .map(|hit| hit.to_string())
        .collect();
    let distinct: HashSet<&String> = hits.iter().collect();
    assert_eq!(
        distinct.len(),
        hits.len(),
        "{letter}: {} repeated hits",
        hits.len() - distinct.len()
    );
}
