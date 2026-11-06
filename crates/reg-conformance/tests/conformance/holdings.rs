//! Holdings compilation, publication verification and pinned source admission
//! (`cases/holdings`): `reg-build holdings` and `reg-build verify --steward-repo` on
//! the synthetic steward.

use std::fs;
use std::path::Path;
use std::process::Command;

use reg_conformance::{Scratch, cached_reader_artifact, fixture, holdings_case, pinned_holdings};
use serde_json::Value;

use crate::common::cli::{Ran, column, exec, execute, verify_holdings};
use crate::common::steward::{self, append, edit, locked_copy, read, splice_slot_family, write};
use crate::common::{read_json, run_all};

/// The `verify-refusals.json` cases of `variant`: each damages a copy of the published
/// `db` with its SQL, and verify must refuse it with its located code.
pub fn verify_refusals(db: &Path, base: &Path, root: &Path, variant: &str) {
    let cases = read_json(&holdings_case().join("verify-refusals.json")).unwrap();
    let cases: Vec<_> = cases
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["variant"].as_str().unwrap_or("base") == variant)
        .collect();
    assert!(!cases.is_empty(), "no {variant} verify refusals");
    let dir = Scratch::new("verify-refusals").unwrap();
    for (n, case) in cases.iter().enumerate() {
        let damaged = dir.path().join(format!("{n}.db"));
        fs::copy(db, &damaged).unwrap();
        execute(&damaged, case["sql"].as_str().unwrap());
        verify_holdings(&damaged, base, root).refused(&[
            case["code"].as_str().unwrap(),
            case["location"].as_str().unwrap(),
        ]);
    }
}

/// `reg-build holdings` of the steward pinned in `dir` into `out`.
fn compile(dir: &Path, out: &Path) -> Ran {
    let holdings = pinned_holdings(dir).unwrap();
    exec(holdings.command().arg("--out").arg(out))
}

#[test]
fn compiled_pair_is_deterministic_and_verified() {
    // Fails if the compiler emits nondeterministic bytes, if verify refuses the pair
    // or accepts it against another base, or if verify trusts compiler counters
    // instead of the pinned source files (`verify-refusals.json`: exact-name/type
    // corruption and additions/removals keep those counters consistent, a duplicate
    // placement beside its disjoint allowed twin, a withheld required key, stored
    // delivery bounds replacing authored ones, a child data cell carried as a key, a
    // projection losing its parent key, swapped Code/Year associations, Late shortened
    // by a day, Code's coding caveat removed, a text body naming a steward working file,
    // a stored space renamed away from the config).
    let pair = steward::pair().unwrap();
    let holdings = &pair.holdings;
    let published = pair.dir.join("reg_meta_fixture.db");
    let dir = Scratch::new("holdings-twice").unwrap();
    let again = dir.path().join("again.db");
    exec(holdings.command().arg("--out").arg(&again)).ok();
    assert!(
        fs::read(&published).unwrap() == fs::read(&again).unwrap(),
        "two compiles of one pinned steward differ"
    );
    verify_holdings(&published, &holdings.base, &holdings.root).ok();
    let wrong_base = cached_reader_artifact(&fixture("base")).unwrap();
    verify_holdings(&published, &wrong_base, &holdings.root)
        .refused(&["holdings_catalog_mismatch"]);
    verify_refusals(&published, &holdings.base, &holdings.root, "base");
}

/// Replace the steward's declared entity-time key with the case file `name`.
fn replace_key(root: &Path, name: &str) {
    edit(
        &root.join("holdings.toml"),
        "[[table.key]]\nentity = [\"demo/held/id\"]\ntime = []\nperiod_key = \"column\"\n\
         locator = \"fixture:declared-entity-time-key\"\n",
        &read(&holdings_case().join(name)).unwrap(),
        false,
    )
    .unwrap();
}

#[test]
fn verify_refuses_a_mapping_beside_its_held_row_key() {
    // Fails if a stored mapping is admitted beside its pinned held row-key claim: an
    // explicit missing-time-key hold must hold every column of the received table.
    let dir = Scratch::new("held-key").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    replace_key(&root, "row-key-hold.toml");
    let out = dir.path().join("held.db");
    compile(dir.path(), &out).ok();
    let base = cached_reader_artifact(&holdings_case().join("base")).unwrap();
    verify_holdings(&out, &base, &root).ok();
    verify_refusals(&out, &base, &root, "held-key");
}

/// `text` with the `[[table.column]]` block named `name` replaced by `with`.
fn replace_column(text: &str, name: &str, with: &str) -> String {
    let start = text
        .find(&format!("[[table.column]]\nname = \"{name}\""))
        .unwrap_or_else(|| panic!("no column {name}"));
    let end = start + 1 + text[start + 1..].find("[[table.column]]").unwrap();
    format!("{}{with}{}", &text[..start], &text[end..])
}

#[test]
fn held_binding_needs_its_follow_up() {
    // Fails if a held binding on an unrouted table is admitted without its follow-up:
    // a hold is a trust boundary, not permission to omit its reason. The table keeps a
    // valid earlier coding beside the held later meaning, as authored.
    let expected = read_json(&holdings_case().join("unrouted-pool.json")).unwrap();
    let dir = Scratch::new("unrouted").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    let path = root.join("holdings.toml");
    let text = read(&path).unwrap();
    let start = text.find("[[table]]\nphysical_name = \"Pool\"").unwrap();
    let end = start
        + text[start..]
            .find("[[table]]\nphysical_name = \"Part A\"")
            .unwrap();
    let text = format!(
        "{}{}{}",
        &text[..start],
        read(&holdings_case().join("unrouted-pool.toml")).unwrap(),
        &text[end..]
    );
    let text = replace_column(
        &text,
        "Code",
        &read(&holdings_case().join("mixed-code.toml")).unwrap(),
    );
    let text = replace_column(
        &text,
        "Old",
        &read(&holdings_case().join("held-old-binding.toml")).unwrap(),
    );
    let text = text.replacen("table_pids = []", "table_pids = [\"demo/held\"]", 1);
    write(&path, &text).unwrap();
    edit(
        &path,
        "follow_up = \"https://example.invalid/issues/held-meaning\"",
        "follow_up = \" \"",
        true,
    )
    .unwrap();
    compile(dir.path(), &dir.path().join("held.db")).refused(&[&format!(
        "{}: {}:",
        expected["invalid_hold_column"].as_str().unwrap(),
        expected["invalid_hold_code"].as_str().unwrap(),
    )]);
}

#[test]
fn census_replacement_refuses_vanished_columns() {
    // Fails if a new input pin silently drops a stale reviewed route, if the old pair
    // stops demanding its recorded source commit, or if a refused compile replaces the
    // published pair. Expectations come from the second census.
    let expected = read_json(&holdings_case().join("census-update.json")).unwrap();
    let text = |key: &str| expected[key].as_str().unwrap();
    let dir = Scratch::new("census-update").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    let out = dir.path().join("published.db");
    compile(dir.path(), &out).ok();
    let original = fs::read(&out).unwrap();
    fs::copy(
        holdings_case().join("census-next.csv"),
        root.join("census.csv"),
    )
    .unwrap();
    let holdings = pinned_holdings(dir.path()).unwrap();
    verify_holdings(&out, &holdings.base, &root).refused(&[text("previous_pin_refusal")]);
    exec(holdings.command().arg("--out").arg(&out)).refused(&[
        text("refusal"),
        text("route_locator"),
        text("decision_locator"),
    ]);
    assert!(
        fs::read(&out).unwrap() == original,
        "a refusal replaced the pair"
    );
}

/// The `renames.json` placements of `case`.
fn rename_expected(case: &str) -> Vec<String> {
    let cases = read_json(&holdings_case().join("renames.json")).unwrap();
    serde_json::from_value(cases[case].clone()).unwrap()
}

/// Pool's placements of its physical column `name`, as `table variable start end`.
fn pool_placements(db: &Path, name: &str) -> Vec<String> {
    column(
        db,
        &format!(
            "SELECT m.table_pid||' '||m.variable_pid||' '||m.start||' '||m.end
             FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
             JOIN holding_table t ON t.id=c.holding_id
             WHERE t.physical_name='Pool' AND c.name='{name}' ORDER BY 1"
        ),
    )
}

#[test]
fn renamed_physical_column_matches_its_catalog_name() {
    // Fails if `[[rename]]` is ignored (the column lands unmatched), if it is looked up
    // after `[[rewrite]]` (the `Received_` strip leaves `Olde`, which no rename names),
    // or if the catalog name replaces the emitted physical name. The expected
    // placements (`renames.json`, `catalog`) come from the base frame and Pool's
    // authored delivery.
    let expected = rename_expected("catalog");
    let dir = Scratch::new("census-rename").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    edit(&root.join("census.csv"), ",Old,", ",Received_Olde,", false).unwrap();
    edit(&root.join("storage.csv"), ",Old,", ",Received_Olde,", false).unwrap();
    append(
        &root.join("curation/readers/holdings.toml"),
        "\n[[rename]]\nphysical = \"Received_Olde\"\ncatalog = \"Old\"\nevidence = \"Synthetic census renames Old.\"\n",
    )
    .unwrap();
    let out = dir.path().join("renamed.db");
    compile(dir.path(), &out).ok();
    assert_eq!(pool_placements(&out, "Received_Olde"), expected);
}

#[test]
fn variable_rename_places_every_documented_spelling() {
    // Fails if a `variable` rename resolves its FQID to one spelling and folds by it:
    // `demo/held/value` is spelled `Old` to 2018 and `New` from 2019, so a single
    // spelling places only one of the Pool's two intervals. The expected placements
    // (`renames.json`, `variable`) come from the base frame and Pool's authored delivery.
    let expected = rename_expected("variable");
    let dir = Scratch::new("census-variable-rename").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    edit(&root.join("census.csv"), ",Old,New,", ",Value,,", false).unwrap();
    let storage = root.join("storage.csv");
    edit(
        &storage,
        "Pool,BASE TABLE,Old,",
        "Pool,BASE TABLE,Value,",
        false,
    )
    .unwrap();
    edit(&storage, "dbo,Pool,BASE TABLE,New,int,,10,0,3\n", "", false).unwrap();
    append(
        &root.join("curation/readers/holdings.toml"),
        "\n[[rename]]\nphysical = \"Value\"\nvariable = \"demo/held/value\"\nevidence = \"Synthetic census drops the spelling change.\"\n",
    )
    .unwrap();
    let out = dir.path().join("variable.db");
    compile(dir.path(), &out).ok();
    assert_eq!(pool_placements(&out, "Value"), expected);
}

#[test]
fn schema_keyed_census_holds_one_name_in_two_schemas() {
    // Fails if a schema-keyed census still keys tables by name (the aux twin of
    // Excluded refuses as a duplicate, or the unique physical name refuses it at
    // insert), if file names ignore the schema (both twins slug to `excluded`), or if
    // a repeated (schema, table) is admitted.
    let dir = Scratch::new("census-schema").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    let census = root.join("census.csv");
    edit(&census, "Synthetic,", "dbo,", true).unwrap();
    append(&census, "aux,Excluded,Excluded,Secret,,,,,,,,\n").unwrap();
    edit(
        &root.join("curation/readers/holdings.toml"),
        "census_columns = \"V\"\n",
        "census_columns = \"V\"\ncensus_schema = \"Category\"\n",
        false,
    )
    .unwrap();
    let topology = root.join("holdings.toml");
    edit(
        &topology,
        "[[table]]\n",
        "[[table]]\nschema_name = \"dbo\"\n",
        true,
    )
    .unwrap();
    append(
        &topology,
        "\n[[table]]\nschema_name = \"aux\"\nphysical_name = \"Excluded\"\n\
         disposition = \"excluded\"\ntable_pids = []\nreason = \"Outside the authored observation scope\"\n\
         locator = \"fixture:excluded-aux\"\n\n[[table.column]]\nname = \"Secret\"\n\
         unmap = \"Not observation data\"\nlocator = \"fixture:excluded-aux-secret\"\n",
    )
    .unwrap();
    let out = dir.path().join("schemas.db");
    compile(dir.path(), &out).ok();
    assert_eq!(
        column(
            &out,
            "SELECT schema_name||' '||file_name FROM holding_table
             WHERE physical_name='Excluded' ORDER BY 1"
        ),
        ["aux aux_excluded", "dbo dbo_excluded"]
    );
    let base = cached_reader_artifact(&holdings_case().join("base")).unwrap();
    verify_holdings(&out, &base, &root).ok();
    append(&census, "dbo,Excluded,Excluded,Secret,,,,,,,,\n").unwrap();
    compile(dir.path(), &dir.path().join("repeated.db"))
        .refused(&["census", "row=12: duplicate physical table"]);
}

#[test]
fn one_placement_in_two_spaces_is_two_databases_copies() {
    // Fails if the duplicate-placement check ignores the space (the aux copy of Part A
    // refuses beside the dbo original even in its own space), if it stops refusing
    // within one space (the single-space compile publishes), or if a table is stored
    // in another space than its schema's.
    let dir = Scratch::new("census-spaces").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    steward::split_schemas(&root).unwrap();
    let out = dir.path().join("spaces.db");
    compile(dir.path(), &out)
        .refused(&["inventory_cell_conflict: undeclared duplicate physical placement"]);
    let holdings = pinned_holdings(dir.path()).unwrap();
    steward::declare_spaces(&root).unwrap();
    exec(holdings.command().arg("--out").arg(&out)).ok();
    assert_eq!(
        column(
            &out,
            "SELECT h.schema_name||' '||h.space||' '||s.ord FROM holding_table h
             JOIN holding_space s ON s.id=h.space WHERE h.physical_name='Part A' ORDER BY 1"
        ),
        ["aux aux 2", "dbo main 1"]
    );
}

/// Apply the `refusals.toml` edits `edits` to the steward checkout at `root`.
fn apply(root: &Path, edits: &Value) -> Result<(), String> {
    for change in edits.as_array().into_iter().flatten() {
        let text = |key: &str| change.get(key).and_then(Value::as_str);
        let path = root.join(text("file").ok_or("an edit names no file")?);
        if let Some(from) = text("from") {
            let all = change.get("all").and_then(Value::as_bool) == Some(true);
            edit(&path, from, text("to").ok_or("`from` without `to`")?, all)?;
        } else if let Some(drop) = text("drop") {
            let kept = read(&path)?;
            if !kept.contains(drop) {
                return Err(format!("{}: no {drop:?} to drop", path.display()));
            }
            let kept: String = kept
                .split_inclusive('\n')
                .filter(|l| !l.contains(drop))
                .collect();
            write(&path, &kept)?;
        } else if let Some(tail) = text("append") {
            append(&path, tail)?;
        } else if let Some(case) = text("append_case") {
            append(&path, &read(&holdings_case().join(case))?)?;
        } else {
            return Err(format!("an edit of {} changes nothing", path.display()));
        }
    }
    Ok(())
}

#[test]
fn holdings_refuse_invalid_authored_inputs() {
    // Each case in `refusals.toml` names the regression its edit catches. Also fails
    // if a refusal replaces the previous publication or leaves a staging file.
    let corpus = crate::common::read_toml(&holdings_case().join("refusals.toml")).unwrap();
    let cases: Vec<Value> = serde_json::to_value(&corpus["refusal"])
        .unwrap()
        .as_array()
        .unwrap()
        .clone();
    run_all(
        "holdings refusals",
        &cases,
        |case| case["name"].as_str().unwrap_or("?").to_owned(),
        |case| {
            let dir = Scratch::new("holdings-refusal")?;
            let root = locked_copy(dir.path())?;
            if case["slot_family"] == true {
                splice_slot_family(&root)?;
            }
            apply(&root, &case["edit"])?;
            let holdings = pinned_holdings(dir.path())?;
            if let Some(file) = case["assume_unchanged"].as_str() {
                exec(Command::new("git").arg("-C").arg(&root).args([
                    "update-index",
                    "--assume-unchanged",
                    file,
                ]))
                .ok();
            }
            apply(&root, &case["after_pin"])?;
            let out = dir.path().join("refused.db");
            fs::write(&out, b"previous publication").unwrap();
            let located: Vec<&str> = case["located"]
                .as_array()
                .ok_or("no located")?
                .iter()
                .filter_map(Value::as_str)
                .collect();
            exec(holdings.command().arg("--out").arg(&out)).refused(&located);
            assert_eq!(fs::read(&out).unwrap(), b"previous publication");
            let staged: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with('.') && name.contains(".holdings-"))
                .collect();
            assert!(staged.is_empty(), "staging left behind: {staged:?}");
            Ok(())
        },
    );
}

#[test]
fn verify_refuses_invented_family_evidence() {
    // Changing every stored slot family keeps distinct indices and agreement inside
    // the artifact. Fails if verify trusts that compiler output instead of refusing
    // it against the pinned steward source. The family's served placements are the
    // materialize `slot-family` case.
    let refusal =
        &read_json(&holdings_case().join("slot-family/expected.json")).unwrap()["verify_refusal"];
    let dir = Scratch::new("slot-family").unwrap();
    let root = locked_copy(dir.path()).unwrap();
    splice_slot_family(&root).unwrap();
    let out = dir.path().join("family.db");
    compile(dir.path(), &out).ok();
    let base = cached_reader_artifact(&holdings_case().join("base")).unwrap();
    verify_holdings(&out, &base, &root).ok();
    execute(&out, refusal["sql"].as_str().unwrap());
    verify_holdings(&out, &base, &root).refused(&[
        refusal["code"].as_str().unwrap(),
        refusal["location"].as_str().unwrap(),
    ]);
}

#[test]
fn holdings_base_change_invalidates_lineage_lock() {
    // Fails if curation ignores the external source's exact file identity: the base's
    // public ids and advertised generation stay unchanged, only its bytes move.
    let dir = Scratch::new("base-change").unwrap();
    let holdings = steward::holdings(dir.path()).unwrap();
    let changed = dir.path().join("changed-base.db");
    fs::copy(&holdings.base, &changed).unwrap();
    let mut mode = fs::metadata(&changed).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut mode, 0o644);
    fs::set_permissions(&changed, mode).unwrap();
    execute(
        &changed,
        "UPDATE meta SET value='changed' WHERE key='catalog_version'",
    );
    let out = dir.path().join("not-published.db");
    exec(
        Command::new(reg_conformance::reg_build())
            .arg("holdings")
            .arg("--base")
            .arg(&changed)
            .args(["--steward", "fixture", "--steward-repo"])
            .arg(&holdings.root)
            .args(["--config", "steward.toml"])
            .arg("--out")
            .arg(&out),
    )
    .refused(&["premise-changed"]);
    assert!(!out.exists());
}

#[test]
fn pin_refresh_readmits_a_relocked_steward() {
    // Fails if `reg-build pin` computes a digest the compiler does not (a directory pin
    // over another file walk), leaves a changed pin stale, adds or drops a pin, or
    // rewrites the config's own formatting (a trailing comment on the changed pin): the
    // steward bump re-locks, refreshes the pins and must then compile.
    let dir = Scratch::new("pin-refresh").unwrap();
    let holdings = steward::holdings(dir.path()).unwrap();
    let config = holdings.root.join("steward.toml");
    let text = read(&config).unwrap();
    let start = text.find("\"curation\" = \"").unwrap();
    let end = start + text[start..].find('\n').unwrap();
    let comment = " # the curation tree";
    write(
        &config,
        &format!("{}{comment}{}", &text[..end], &text[end..]),
    )
    .unwrap();
    let pins = |text: &str| {
        let value: toml::Value = toml::from_str(text).unwrap();
        value["pins"].as_table().unwrap().clone()
    };
    let before = pins(&read(&config).unwrap());
    append(
        &holdings.root.join("curation/readers/holdings.toml"),
        "\n# re-locked\n",
    )
    .unwrap();
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(&holdings.root)
            .status()
            .unwrap();
        assert!(status.success());
    };
    git(&["commit", "--quiet", "-am", "re-lock"]);
    let out = dir.path().join("pinned.db");
    exec(holdings.command().arg("--out").arg(&out)).refused(&["holdings_input_digest_mismatch"]);
    exec(
        Command::new(reg_conformance::reg_build())
            .args(["pin", "--steward-repo"])
            .arg(&holdings.root)
            .args(["--config", "steward.toml"]),
    )
    .ok();
    let text = read(&config).unwrap();
    let after = pins(&text);
    assert!(before.keys().eq(after.keys()), "pin added or dropped a pin");
    assert_ne!(
        before["curation"], after["curation"],
        "the changed pin is stale"
    );
    assert!(text.contains(comment), "the config's comment was rewritten");
    git(&["commit", "--quiet", "-am", "refresh pins"]);
    exec(holdings.command().arg("--out").arg(&out)).ok();
}

#[test]
fn pin_names_the_repository_not_the_transport() {
    // Fails if the pin records the origin URL as given, so a pair built from an HTTPS
    // clone (steward CI) is refused by verify on an SSH clone (a local checkout), or if
    // an origin that names no repository (a local path) is admitted as the pin.
    let dir = Scratch::new("origin-transport").unwrap();
    let holdings = steward::holdings(dir.path()).unwrap();
    let out = dir.path().join("https.db");
    exec(holdings.command().arg("--out").arg(&out)).ok();
    let origin = |url: &str| {
        let status = Command::new("git")
            .args(["remote", "set-url", "origin", url])
            .current_dir(&holdings.root)
            .status()
            .unwrap();
        assert!(status.success());
    };
    origin("git@Example.invalid:fixture/Steward.git");
    verify_holdings(&out, &holdings.base, &holdings.root).ok();
    origin("/srv/git/steward.git");
    verify_holdings(&out, &holdings.base, &holdings.root).refused(&["holdings_pin_invalid"]);
}
