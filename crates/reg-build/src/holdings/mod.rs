//! Steward compilation after the immutable reference catalog. The ordinary semantic
//! pipeline emits the supplemental part; this stage adds possession facts only.
mod compile;
mod input;
mod references;
mod verify;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, params};
use sha2::{Digest, Sha256};

pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let flags = flags(args)?;
    let need = |name: &str| {
        flags
            .get(name)
            .cloned()
            .ok_or(format!("{name} is required"))
    };
    if flags.keys().any(|k| {
        ![
            "--base",
            "--steward",
            "--steward-repo",
            "--out",
            "--registry",
            "--report",
        ]
        .contains(&k.as_str())
    }) {
        return Err("holdings: unknown flag".into());
    }
    let base = PathBuf::from(need("--base")?);
    let root = PathBuf::from(need("--steward-repo")?);
    let out = PathBuf::from(need("--out")?);
    let owner = need("--steward")?;
    let report = flags.get("--report").map(PathBuf::from);
    let registry = PathBuf::from(
        flags
            .get("--registry")
            .map_or("curation/stewards.toml", String::as_str),
    );
    let pins: BTreeMap<String, input::Pin> =
        crate::toml_file(&registry).map_err(|_| "holdings_registry_invalid".to_owned())?;
    let pin = pins.get(&owner).ok_or("holdings_steward_unknown")?;
    let config = input::admit(&root, pin, &owner)?;
    crate::verify::catalog(&base, None)?;
    if std::iter::once(&out)
        .chain(report.iter())
        .any(|path| path == &base || path.canonicalize().ok() == base.canonicalize().ok())
    {
        return Err("holdings_output_invalid: output must differ from base".into());
    }
    // The scratch is only supplemental metadata, never a reference-catalog copy.
    let name = out
        .file_name()
        .ok_or("holdings_output_invalid")?
        .to_string_lossy();
    let scratch = out.with_file_name(format!(".{name}.holdings-scratch"));
    let staged = out.with_file_name(format!(".{name}.holdings-staged"));
    if scratch.exists() || staged.exists() {
        return Err("holdings_output_invalid: scratch output already exists".into());
    }
    let result = write(
        &base,
        &root,
        &scratch,
        &staged,
        &owner,
        pin,
        &config,
        report.as_deref(),
    )
    .and_then(|()| verify::pair(&staged, &base, &root))
    .and_then(|()| {
        let summary = {
            let db = open(&staged)?;
            format!(
                "holdings: tables={} columns={} mapped={} unmatched={} storage_extras={}",
                meta(&db, "census_tables")?,
                meta(&db, "census_columns")?,
                meta(&db, "mapped_columns")?,
                meta(&db, "unmatched_columns")?,
                meta(&db, "storage_extra_columns")?
            )
        };
        std::fs::rename(&staged, &out).map_err(|e| e.to_string())?;
        eprintln!("{summary}");
        Ok(())
    });
    for file in [&scratch, &staged] {
        if file.exists() {
            let _ = std::fs::remove_file(file);
        }
    }
    result
}

fn flags(mut args: impl Iterator<Item = String>) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    while let Some(flag) = args.next() {
        if !flag.starts_with("--") {
            return Err("holdings: expected named arguments".into());
        }
        let value = args.next().ok_or("holdings: option needs a value")?;
        if out.insert(flag, value).is_some() {
            return Err("holdings: duplicate option".into());
        }
    }
    Ok(out)
}

pub(crate) fn verify_args(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let file = PathBuf::from(
        args.next()
            .ok_or("usage: reg-build verify FILE --catalog FILE --steward-repo DIR")?,
    );
    let flags = flags(args)?;
    if flags
        .keys()
        .any(|key| !["--catalog", "--steward-repo"].contains(&key.as_str()))
    {
        return Err("usage: reg-build verify FILE --catalog FILE --steward-repo DIR".into());
    }
    let base = flags.get("--catalog").ok_or("--catalog is required")?;
    let root = flags
        .get("--steward-repo")
        .ok_or("holdings_source_required: --steward-repo is required")?;
    verify::pair(&file, Path::new(base), Path::new(root))
}

pub(super) fn open(path: &Path) -> Result<Connection, String> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| e.to_string())
}

pub(super) fn meta(db: &Connection, key: &str) -> Result<String, String> {
    db.query_row("SELECT value FROM meta WHERE key=?", [key], |r| r.get(0))
        .map_err(|_| format!("holdings_meta_invalid: missing {key}"))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn write(
    base: &Path,
    root: &Path,
    scratch: &Path,
    staged: &Path,
    owner: &str,
    pin: &input::Pin,
    config: &input::Config,
    report: Option<&Path>,
) -> Result<(), String> {
    let curation = input::path(root, &config.curation_root)?
        .parent()
        .ok_or("holdings_input_path_invalid")?
        .to_path_buf();
    let context = crate::external::Context::open(base)?;
    crate::build_with_provenance(
        &crate::Args {
            command: "build".into(),
            inputs: root.to_path_buf(),
            curation,
            out: scratch.to_path_buf(),
            assets: None,
            report: report.map(Path::to_path_buf),
            catalog_version: "supplemental".into(),
            release: true,
            registers: vec![],
            base: None,
        },
        Some((&pin.repo, &pin.commit)),
        Some(&context),
    )?;
    let semantic = open(scratch)?;
    let base_db = open(base)?;
    let semantic_generation = meta(&semantic, "generation_id")?;
    let semantic_sha256 = crate::sha256_file(scratch)?.0;
    let declarations = references::read(root, config)?;
    drop(semantic);
    let semantic = Connection::open(scratch).map_err(|e| e.to_string())?;
    references::write(&semantic, &declarations)?;
    verify::distinct(&semantic, &base_db)?;
    let base_generation = meta(&base_db, "generation_id")?;
    let (base_sha, _) = crate::sha256_file(base)?;
    drop(semantic);
    let topology: input::Topology = crate::toml_file(&input::path(root, &config.inputs.holdings)?)
        .map_err(|_| "holdings_topology_invalid: malformed authored topology".to_owned())?;
    let census = reg_readers::holdings::read(
        &input::path(root, &config.inputs.census)?,
        &input::path(root, &config.inputs.storage_export)?,
        &input::path(root, &config.inputs.census_reader)?,
    )
    .map_err(|e| e.to_string())?;
    let mut db = Connection::open(scratch).map_err(|e| e.to_string())?;
    db.execute_batch("PRAGMA foreign_keys=ON;")
        .map_err(|e| e.to_string())?;
    db.execute_batch(include_str!("schema.sql"))
        .map_err(|e| e.to_string())?;
    db.execute_batch(include_str!("relations.sql"))
        .map_err(|e| e.to_string())?;
    let counts = compile::write(&mut db, &base_db, topology, census.tables)?;
    let mut hasher = Sha256::new();
    hasher.update(crate::sha256_file(&input::path(root, &pin.config)?)?.0);
    for part in [
        &base_generation,
        &base_sha,
        &semantic_generation,
        &semantic_sha256,
        &pin.commit,
        &owner.to_owned(),
        &reg_core::HOLDINGS_SCHEMA_VERSION.to_string(),
        &compiler_digest(),
    ] {
        hasher.update(part);
        hasher.update(b"\n");
    }
    for (path, sha) in &config.pins {
        hasher.update(format!("{path}\t{sha}\n"));
    }
    let generation = reg_frame::hex(&hasher.finalize());
    let config_json = |relative: &str, json: bool| -> Result<String, String> {
        let text =
            std::fs::read_to_string(input::path(root, relative)?).map_err(|e| e.to_string())?;
        let value = if json {
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|_| "holdings_presentation_invalid".to_owned())?
        } else {
            serde_json::to_value(
                toml::from_str::<toml::Value>(&text)
                    .map_err(|_| "holdings_scope_invalid".to_owned())?,
            )
            .map_err(|e| e.to_string())?
        };
        serde_json::to_string(&value).map_err(|e| e.to_string())
    };
    let metadata = BTreeMap::from([
        (
            "schema_version",
            reg_core::HOLDINGS_SCHEMA_VERSION.to_string(),
        ),
        (
            "semantic_schema_version",
            reg_core::CATALOG_SCHEMA_VERSION.to_string(),
        ),
        ("generation_id", generation),
        ("semantic_generation", semantic_generation),
        ("semantic_sha256", semantic_sha256),
        ("catalog_generation", base_generation),
        ("catalog_sha256", base_sha),
        ("owner_kind", "steward".into()),
        ("owner_id", owner.into()),
        ("steward_repo", pin.repo.clone()),
        ("steward_commit", pin.commit.clone()),
        ("steward_config", pin.config.clone()),
        ("compiler_digest", compiler_digest()),
        (
            "input_pins",
            serde_json::to_string(&config.pins).map_err(|e| e.to_string())?,
        ),
        ("steward", config_json(&config.presentation.steward, true)?),
        ("scope", config_json(&config.scope, false)?),
        (
            "logo",
            std::fs::read_to_string(input::path(root, &config.presentation.logo)?)
                .map_err(|e| e.to_string())?,
        ),
        ("census_tables", counts.0.to_string()),
        ("census_columns", counts.1.to_string()),
        ("mapped_columns", counts.2.to_string()),
        ("unmatched_columns", counts.3.to_string()),
        (
            "storage_extra_columns",
            census.storage_extra_columns.to_string(),
        ),
    ]);
    let tx = db.transaction().map_err(|e| e.to_string())?;
    for (key, value) in metadata {
        tx.execute(
            "INSERT OR REPLACE INTO meta VALUES (?,?)",
            params![key, value],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.pragma_update(None, "user_version", reg_core::HOLDINGS_SCHEMA_VERSION)
        .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    db.execute_batch("ANALYZE;").map_err(|e| e.to_string())?;
    db.execute("VACUUM INTO ?", [staged.to_string_lossy().as_ref()])
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn compiler_digest() -> String {
    let sources = [
        include_str!("mod.rs"),
        include_str!("input.rs"),
        include_str!("references.rs"),
        include_str!("compile.rs"),
        include_str!("verify.rs"),
        include_str!("schema.sql"),
        include_str!("../main.rs"),
        include_str!("../emit.rs"),
        include_str!("../verify.rs"),
        include_str!("../../../reg-readers/src/holdings.rs"),
        include_str!("../../../reg-readers/src/transcription.rs"),
        include_str!("../../../reg-resolve/src/lib.rs"),
        include_str!("../../../reg-core/src/lib.rs"),
        include_str!("../../../reg-core/src/grammar.rs"),
        include_str!("../../../reg-frame/src/lib.rs"),
        include_str!("../../../../Cargo.lock"),
        include_str!("../external.rs"),
        include_str!("relations.sql"),
        include_str!("../curation.rs"),
        include_str!("../link/curated.rs"),
    ];
    let mut hasher = Sha256::new();
    hasher.update(env!("CARGO_PKG_VERSION"));
    for source in sources {
        hasher.update(source);
    }
    reg_frame::hex(&hasher.finalize())
}
