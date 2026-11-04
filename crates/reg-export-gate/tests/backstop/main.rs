//! The identifier scan's grammar, pinned as data: `shapes.json` holds inputs
//! and whether the R gate's pattern matches them, computed once by running
//! that pattern (`reg_export_gate/export_gate.R`, `IDENTIFIER`). A scan that
//! drifts from it (a day range, the separator, the century, a digit
//! boundary) fails a row.

use reg_export_gate::identifier_shape;
use serde_json::Value;

#[test]
fn identifier_shapes_match_the_r_pattern() {
    let table: Vec<Value> = serde_json::from_str(include_str!("shapes.json")).expect("JSON");
    let wrong: Vec<String> = table
        .iter()
        .filter_map(|row| {
            let text = row["text"].as_str().expect("a text");
            let want = row["match"].as_bool().expect("a match");
            (identifier_shape(text) != want).then(|| format!("{text:?}: expected {want}"))
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
