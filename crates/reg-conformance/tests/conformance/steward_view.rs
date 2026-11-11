//! The steward view's read rules over HTTP on the paired fixture, beside the same
//! requests on its base catalog (crates/DESIGN.md, "A steward deployment serves its
//! holdings only").

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
