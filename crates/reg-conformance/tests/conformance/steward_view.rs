//! The steward view's read rules over HTTP on the paired fixture, beside the same
//! requests on its base catalog (crates/DESIGN.md, "A steward deployment serves its
//! holdings only").

use std::sync::OnceLock;

use serde_json::{Value, json};

use crate::common::http_case::json_eq;
use crate::common::{client, read_json, server, steward};

/// A register's `sets` as `{fqid, members}` and a variable's `sets` as fqids.
fn sets_of(port: u16, register: &str, member: &str) -> (Value, Value) {
    let get = |path: &str| {
        let (status, answer) = client::json(port, "GET", path, &[], None).unwrap();
        assert_eq!(status, 200, "{path}: {answer}");
        answer
    };
    let register = get(&format!("/api/catalog/{register}"));
    let register_sets: Vec<Value> = register["data"]["sets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|set| json!({"fqid": set["fqid"], "members": set["members"]}))
        .collect();
    let member = get(&format!("/api/catalog/{member}"));
    // An empty `sets` is left out.
    let member_sets: Vec<Value> = member["data"]["sets"]
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .map(|set| set["fqid"].clone())
        .collect();
    (Value::from(register_sets), Value::from(member_sets))
}

#[test]
fn a_set_with_one_held_member_reads_as_that_variable() {
    // Fails if a steward serves a set with fewer than two held members: in its
    // register's sets, in its member's sets (the SPA then opens the variable as a
    // group page) or by its address; or if a set with two held members stops being
    // served; or if the base catalog stops serving the set the steward drops (#2410).
    let expected =
        read_json(&reg_conformance::holdings_case().join("single-member-set.json")).unwrap();
    let (register, member) = (
        expected["register"].as_str().unwrap(),
        expected["member"].as_str().unwrap(),
    );
    let paired = steward::paired().unwrap();
    let (register_sets, member_sets) = sets_of(paired, register, member);
    assert!(
        json_eq(&register_sets, &expected["paired"]["register_sets"]),
        "{register_sets}"
    );
    assert!(
        json_eq(&member_sets, &expected["paired"]["member_sets"]),
        "{member_sets}"
    );
    let lone = format!("/api/catalog/{}", expected["lone"].as_str().unwrap());
    let (status, answer) = client::json(paired, "GET", &lone, &[], None).unwrap();
    assert_eq!(status, 404, "{answer}");
    assert_eq!(answer["error"]["code"], expected["paired"]["lone_code"]);

    let base =
        reg_conformance::cached_reader_artifact(&reg_conformance::holdings_case().join("base"))
            .unwrap();
    let base = server::server(base.parent().unwrap()).unwrap().port;
    let (register_sets, member_sets) = sets_of(base, register, member);
    assert!(
        json_eq(&register_sets, &expected["base_only"]["register_sets"]),
        "{register_sets}"
    );
    assert!(
        json_eq(&member_sets, &expected["base_only"]["member_sets"]),
        "{member_sets}"
    );
}

/// The server of a steward whose blocked files are the only ones of `demo/projected`
/// and `demo/owners/empty`, built once. Parent and Child (`steward/holdings.toml`) are
/// the only files of `demo/projected`: both row keys are held here. Pool's row key is
/// held too (`row-key-hold.toml`): Pool (2017..2020) alone reaches `demo/owners/empty`,
/// while Owners places `demo/owners` over 2017..2019.
fn blocked() -> u16 {
    static DIR: OnceLock<reg_conformance::Scratch> = OnceLock::new();
    let dir = DIR.get_or_init(|| {
        let dir = reg_conformance::Scratch::new("blocked-view").unwrap();
        let root = steward::locked_copy(dir.path()).unwrap();
        let topology = root.join("holdings.toml");
        for (entity, follow_up) in [
            ("[\"demo/projected/id\"]", "synthetic:parent-key"),
            (
                "[\"demo/projected/id\", \"demo/projected/child-id\"]",
                "synthetic:child-key",
            ),
        ] {
            let key = format!("[[table.key]]\nentity = {entity}\ntime = []\n");
            let held = format!("{key}hold = \"Key not reviewed\"\nfollow_up = \"{follow_up}\"\n");
            steward::edit(&topology, &key, &held, false).unwrap();
        }
        steward::edit(
            &topology,
            "[[table.key]]\nentity = [\"demo/held/id\"]\ntime = []\nperiod_key = \"column\"\n\
             locator = \"fixture:declared-entity-time-key\"\n",
            &steward::read(&reg_conformance::holdings_case().join("row-key-hold.toml")).unwrap(),
            false,
        )
        .unwrap();
        steward::compile(dir.path()).unwrap();
        dir
    });
    server::steward_server(dir.path(), "fixture").unwrap().port
}

/// `GET path` on `port`, asserted 200; its `data`.
fn data(port: u16, path: &str) -> Value {
    let (status, answer) = client::json(port, "GET", path, &[], None).unwrap();
    assert_eq!(status, 200, "{path}: {answer}");
    answer["data"].clone()
}

/// Each listed file with its reason and its first hold's follow-up, in file order.
fn files(listed: &Value) -> Vec<(Value, Value, Value)> {
    listed
        .as_array()
        .unwrap_or_else(|| panic!("no not_orderable: {listed}"))
        .iter()
        .map(|f| {
            (
                f["file"].clone(),
                f["reason"].clone(),
                f["holds"][0]["follow_up"].clone(),
            )
        })
        .collect()
}

#[test]
fn a_register_held_only_in_blocked_files_is_shown_with_the_reason() {
    // Fails if a steward drops a register it holds only in files no order can draw on
    // (#2568: the register vanished, so the researcher entered its tables by hand), or
    // shows it without the stored reason and the steward's own holds: on the root
    // list, the register, its table, a search hit or `segments`. Also fails if showing
    // it widens possession, so its variables read as held, or a variable held nowhere
    // and not documented in the blocked table is admitted there (#2574 review).
    let port = blocked();
    let expected = vec![
        (
            json!("Child"),
            json!("row_key_held"),
            json!("synthetic:child-key"),
        ),
        (
            json!("Parent"),
            json!("row_key_held"),
            json!("synthetic:parent-key"),
        ),
    ];
    let shown = data(port, "/api/catalog");
    let listed = shown["providers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|p| p["registers"].as_array().unwrap())
        .find(|r| r["fqid"] == "demo/projected")
        .unwrap_or_else(|| panic!("demo/projected not listed: {shown}"))
        .clone();
    assert_eq!(listed["not_orderable"], true, "{listed}");
    let register = data(port, "/api/catalog/demo/projected");
    assert_eq!(register["not_orderable"], true, "{register}");
    assert_eq!(register["variables"], json!([]), "{register}");
    assert_eq!(files(&register["tables"][0]["not_orderable"]), expected);
    // Its one table is implicit: the register's pid names it.
    let pid = register["tables"][0]["fqid"].as_str().unwrap().to_owned();
    let segments = data(
        port,
        &format!("/api/segments/{pid}?variable=demo/projected/id"),
    );
    assert_eq!(files(&segments["not_orderable"]), expected);
    let (status, answer) = client::json(
        port,
        "GET",
        "/api/segments/demo/owners/empty?variable=demo/projected/id",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        (status, &answer["error"]["code"]),
        (404, &json!("not_found")),
        "{answer}"
    );
    let search = data(port, "/api/search?q=Projection%20reference&type=register");
    assert_eq!(search["items"][0]["fqid"], "demo/projected", "{search}");
    assert_eq!(search["items"][0]["not_orderable"], true, "{search}");
    // The rest of the steward is placed, so it carries no mark.
    assert!(
        data(port, "/api/catalog/demo/held")
            .get("not_orderable")
            .is_none()
    );
}

#[test]
fn a_placed_register_matches_search_over_its_placements_only() {
    // Fails if a register search for a period only a blocked table covers matches the
    // placed register through that table (#2574 review), or if a table search for that
    // period misses the blocked table. Owners places `demo/owners` over 2017..2019;
    // Pool, blocked, alone reaches `demo/owners/empty` through 2020.
    let port = blocked();
    let hits = |query: &str| -> Vec<(Value, Value)> {
        data(port, &format!("/api/search?{query}"))["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| (hit["fqid"].clone(), hit["not_orderable"].clone()))
            .collect()
    };
    let owners = "q=Interval-local%20rival%20owners&type=register";
    assert!(hits(owners).contains(&(json!("demo/owners"), Value::Null)));
    assert!(
        !hits(&format!("{owners}&period=2020"))
            .iter()
            .any(|(fqid, _)| fqid == "demo/owners")
    );
    let empty = "q=Segmentless%20routed%20table&type=table&period=2020";
    assert!(hits(empty).contains(&(json!("demo/owners/empty"), json!(true))));
}

#[test]
fn a_placed_table_lists_its_blocked_files_beside_its_orderable_years() {
    // Fails if a table with orderable years hides a blocked file that also routes to it
    // (the blocked years read as never held), drops that file's years, lets them widen
    // the table's own period, or admits a variable the steward holds nowhere because the
    // table has blocked files. Part A and Part B place `demo/held` over 2021; Pool
    // (delivery 2017..2020, row key held by `row-key-hold.toml`) routes there too.
    let port = blocked();
    let pool = json!([{
        "file": "Pool",
        "reason": "row_key_held",
        "spans": [{"start": "2017-01-01", "end": "2021-01-01"}],
        "message": "Held by the data holder, but not orderable yet: the columns that \
                    identify its rows are on hold. Ask the data holder.",
        "holds": [{
            "hold": "required_time_key_absent: EventDate",
            "follow_up": "synthetic:request-event-key",
        }],
    }]);
    let register = data(port, "/api/catalog/demo/held");
    let table = &register["tables"][0];
    assert_eq!(table["not_orderable"], pool, "{register}");
    assert_eq!(
        table["period"],
        json!({"start": "2021-01-01", "end": "2022-01-01"})
    );
    let segments = data(port, "/api/segments/demo/held?variable=demo/held/id");
    assert_eq!(segments["not_orderable"], pool, "{segments}");
    let (status, answer) = client::json(
        port,
        "GET",
        "/api/segments/demo/held?variable=demo/held/late",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        (status, &answer["error"]["code"]),
        (404, &json!("not_found")),
        "{answer}"
    );
}
