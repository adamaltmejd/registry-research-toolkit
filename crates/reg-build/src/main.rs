//! `reg-build`: the catalog builder (`crates/DESIGN.md`, "The catalog builder").
//!
//! ```text
//! reg-build read  --inputs DIR --curation DIR --out DIR [--assets DIR]
//! reg-build build --inputs DIR --curation DIR --out FILE [--assets DIR] [--report FILE]
//!                 [--catalog-version V] [--release]
//! reg-build lock [REGISTER...] --inputs DIR --curation DIR [--assets DIR] [--base FILE]
//! reg-build verify FILE [--previous FILE]
//! reg-build verify FILE --catalog FILE --steward-repo DIR
//! reg-build holdings --base FILE --steward NAME --steward-repo DIR --config FILE
//!                    --out FILE [--report FILE]
//! reg-build pin --steward-repo DIR --config FILE
//! reg-build diff [--holdings] OLD NEW [--json] [-n N]
//! ```
//!
//! `verify` checks a written catalog file (`verify.rs`) and, with `--previous`, its ids
//! against the previous release's, reporting id changes when the previous release is pre-v1.
//! `read` and `build` run every reader of the manifest `<curation>/inputs.toml` over the inputs root.
//! An input that is a release asset of the inputs repository is read from
//! `<assets>/<release>/<path>`, fetched there beforehand (`gh release download`).
//! `read` writes each input's frame as `<name>.jsonl` and its warnings as
//! `<name>.warnings` under `--out`. `build` resolves each register with its ledger
//! `<curation>/registers/<provider>/<register>/ids.toml`, matches the frames' links and
//! notices across the resolved registers (`link.rs`), and writes the catalog file to `--out`
//! and the build report (JSON) to `--report`. `--catalog-version` (default `dev`) is
//! recorded in the file; `--release` refuses unplaced or split ledger ids and variables
//! without a ledger id. A refusal
//! prints the error on stderr and exits 1. `diff` compares two catalog files by natural
//! key (exit 0 same, 1 different, 2 error); see `diff.rs`. `holdings` compiles a
//! steward's holdings database against a base catalog (a release build in production),
//! and `verify --catalog`
//! checks that pair; `lock --base` locks a steward repository's curation against a base
//! catalog (`holdings/`).

mod curation;
mod diff;
mod docs;
mod emit;
mod error;
mod external;
mod holdings;
mod id_targets;
mod link;
mod proposals;
mod retired;
mod search;
mod sets;
mod timing;
mod verify;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::error::Result;
use reg_frame::Record;
use reg_readers::curation::Lines;
use serde::{Deserialize, Serialize};

/// `curation/inputs.toml`: the pinned inputs and the providers they belong to.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    /// The inputs repository and the commit the inputs root is checked out at.
    repo: String,
    commit: String,
    provider: Vec<Provider>,
    input: Vec<reg_readers::Input>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    pid: toml::Spanned<String>,
    name: String,
    url: Option<String>,
}

struct Args {
    command: String,
    inputs: PathBuf,
    curation: PathBuf,
    out: PathBuf,
    assets: Option<PathBuf>,
    report: Option<PathBuf>,
    catalog_version: String,
    release: bool,
    registers: Vec<String>,
    base: Option<PathBuf>,
}

#[derive(Serialize)]
struct BuildRefusal {
    register: Option<String>,
    kind: &'static str,
    details: Vec<String>,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args> {
    let command = args
        .next()
        .ok_or("usage: reg-build read|build --inputs DIR --curation DIR --out PATH")?;
    let (mut inputs, mut curation, mut out, mut assets, mut report, mut version) =
        (None, None, None, None, None, None);
    let mut release = false;
    let mut registers = Vec::new();
    let mut base = None;
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--inputs" => &mut inputs,
            "--curation" => &mut curation,
            "--out" => &mut out,
            "--assets" => &mut assets,
            "--report" => &mut report,
            "--catalog-version" => &mut version,
            "--base" if command == "lock" => &mut base,
            "--release" => {
                release = true;
                continue;
            }
            other if command == "lock" && !other.starts_with('-') => {
                registers.push(other.to_owned());
                continue;
            }
            other => return Err(format!("unknown flag {other:?}").into()),
        };
        *slot = Some(args.next().ok_or(format!("{flag} needs a value"))?);
    }
    let need =
        |v: Option<String>, flag: &str| v.map(PathBuf::from).ok_or(format!("{flag} is required"));
    let out = if command == "lock" {
        PathBuf::new()
    } else {
        need(out, "--out")?
    };
    Ok(Args {
        command,
        inputs: need(inputs, "--inputs")?,
        curation: need(curation, "--curation")?,
        out,
        assets: assets.map(PathBuf::from),
        report: report.map(PathBuf::from),
        catalog_version: version.unwrap_or_else(|| "dev".to_owned()),
        release,
        registers,
        base: base.map(PathBuf::from),
    })
}

fn toml_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(reg_readers::config(path)?)
}

/// Every input read, in manifest order.
fn ledger_path(curation: &Path, provider: &str, register: &str) -> PathBuf {
    curation
        .join("registers")
        .join(provider)
        .join(register)
        .join("ids.toml")
}

/// Locate a reported id using TOML spans, including quoted keys and multiline anchors.
fn ledger_refusal(path: &Path, id: &str, code: &str, reason: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct Locations {
        variables: BTreeMap<toml::Spanned<String>, Vec<String>>,
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let locations: Locations =
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let key = locations
        .variables
        .keys()
        .find(|key| key.get_ref() == id)
        .ok_or_else(|| {
            format!(
                "{}: ledger id {id:?} has no source location",
                path.display()
            )
        })?;
    let line = Lines::new(&text).at(key.span().start);
    Ok(format!(
        "{}:{line}: {code}: ledger id {id:?} {reason} (--release)",
        path.display()
    ))
}

fn read_all(args: &Args, manifest: &Manifest) -> Result<Vec<reg_readers::ReadOutput>> {
    let check_provider = |provider: &str, input: &str, at: &str| {
        if matches!(provider, "set" | "class" | "group") {
            Err(format!(
                "{input}:{at}: provider-reserved: provider {provider:?} is a reserved leading public-id segment"
            ))
        } else {
            Ok(())
        }
    };
    let path = args.curation.join("inputs.toml");
    let lines = Lines::new(&std::fs::read_to_string(&path)?);
    for provider in &manifest.provider {
        let line = lines.at(provider.pid.span().start);
        check_provider(
            provider.pid.get_ref(),
            &path.to_string_lossy(),
            &line.to_string(),
        )?;
    }
    let readers = args.curation.join("readers");
    let read_one = |input: &reg_readers::Input| {
        let output = reg_readers::read(&args.inputs, args.assets.as_deref(), &readers, input)?;
        for record in &output.frame.records {
            let Some((provider, _, at)) = curation::coordinates(record) else {
                continue;
            };
            check_provider(provider, &at.input, &at.at)?;
        }
        Ok(output)
    };
    // Readers are independent and pure, so they run on all cores; outputs and the first
    // error keep manifest order, as a serial read would (#2196).
    let next = std::sync::atomic::AtomicUsize::new(0);
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .min(manifest.input.len());
    let mut slots: Vec<Option<std::thread::Result<Result<reg_readers::ReadOutput>>>> =
        (0..manifest.input.len()).map(|_| None).collect();
    std::thread::scope(|scope| {
        let handles = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(input) = manifest.input.get(i) else {
                            return done;
                        };
                        // A reader panic counts only if no earlier input failed, as when
                        // the inputs were read one after another.
                        let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            read_one(input)
                        }));
                        done.push((i, output));
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            for (i, output) in handle.join().expect("a reader thread") {
                slots[i] = Some(output);
            }
        }
    });
    slots
        .into_iter()
        .map(|slot| {
            slot.expect("every input read")
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
        .collect()
}

fn read(args: &Args) -> Result<()> {
    let manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    std::fs::create_dir_all(&args.out)?;
    for (input, output) in manifest.input.iter().zip(read_all(args, &manifest)?) {
        let write = |ext: &str, text: String| {
            let path = args.out.join(format!("{}.{ext}", input.name));
            std::fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))
        };
        write("jsonl", output.frame.to_jsonl())?;
        write(
            "warnings",
            output
                .warnings
                .iter()
                .fold(String::new(), |s, w| s + w + "\n"),
        )?;
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<(String, i64)> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let len = i64::try_from(bytes.len())?;
    Ok((reg_core::sha256_hex(&bytes), len))
}

/// The digest of a set of files: SHA-256 over `path\tsha256\n` lines in path order,
/// and their total size. A single file is its own SHA-256.
fn digest(root: &Path, files: &BTreeSet<String>) -> Result<(String, i64)> {
    if let [only] = files.iter().collect::<Vec<_>>()[..] {
        return sha256_file(&root.join(only));
    }
    let (mut lines, mut total) = (String::new(), 0);
    for file in files {
        let (sha, bytes) = sha256_file(&root.join(file))?;
        writeln!(lines, "{file}\t{sha}").expect("write to String");
        total += bytes;
    }
    Ok((reg_core::sha256_hex(lines), total))
}

/// Every file under `dir`, relative to it, `/`-separated; symlinks are followed and
/// dotfiles (host litter such as `.DS_Store`) skipped.
fn tree(dir: &Path, prefix: &str, out: &mut BTreeSet<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if entry.path().is_dir() {
            tree(&entry.path(), &rel, out)?;
        } else {
            out.insert(rel);
        }
    }
    Ok(())
}

/// A steward commit covers its own generated transcription inputs; putting that
/// same commit inside its manifest would make a self-referential Git pin.
#[allow(clippy::too_many_lines)]
fn build_with_provenance(
    args: &Args,
    admitted_pin: Option<(&str, &str)>,
    base: Option<&external::Context>,
) -> Result<()> {
    let t = std::time::Instant::now();
    let retired = retired::load(&args.curation)?;
    let statements = reg_readers::curation::load(&args.curation)?;
    let tags = sets::vocabulary(&args.curation)?;
    sets::statements(&tags, &statements)?;
    let mut manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    if let Some((repo, commit)) = admitted_pin {
        repo.clone_into(&mut manifest.repo);
        commit.clone_into(&mut manifest.commit);
    }
    timing::done("load curation", t);
    let t = std::time::Instant::now();
    let outputs = read_all(args, &manifest)?;
    timing::done("read inputs", t);
    let t = std::time::Instant::now();
    let mut applied = curation::run(
        &args.curation,
        &retired,
        &statements,
        &outputs,
        false,
        &[],
        base,
    )?;
    timing::done("apply curation", t);
    let t = std::time::Instant::now();
    if !args.release {
        for finding in &applied.findings {
            eprintln!("reg-build: {finding}");
        }
    }

    let mut inputs = Vec::new();
    let mut warnings = Vec::new();
    warnings.extend(applied.findings.iter().cloned());
    for (input, output) in manifest.input.iter().zip(&outputs) {
        // A release asset is its pin (the reader verified it); its locators name
        // archive members, not files under the inputs root.
        let (sha256, bytes) = match (&input.release, &input.sha256) {
            (Some(_), Some(pin)) => {
                let path = input.locate(&args.inputs, args.assets.as_deref())?;
                let len = std::fs::metadata(&path)
                    .map_err(|e| format!("{}: {e}", path.display()))?
                    .len();
                (pin.clone(), i64::try_from(len)?)
            }
            _ => digest(&args.inputs, &output.files.iter().cloned().collect())?,
        };
        inputs.push(emit::InputRow {
            name: input.name.clone(),
            repo: manifest.repo.clone(),
            commit: input
                .release
                .clone()
                .unwrap_or_else(|| manifest.commit.clone()),
            path: input.path.clone(),
            sha256,
            bytes,
        });
        warnings.extend(output.warnings.iter().cloned());
    }
    // Input ids follow name order; frames keep manifest order.
    let mut order: Vec<usize> = (0..inputs.len()).collect();
    order.sort_by(|&a, &b| inputs[a].name.cmp(&inputs[b].name));
    let mut rank = vec![0; inputs.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i] = r;
    }
    let mut file_input = BTreeMap::new();
    for (i, output) in outputs.iter().enumerate() {
        for file in &output.files {
            file_input.entry(file.clone()).or_insert(rank[i]);
        }
    }

    // Preserve each reader's attribution even when two manifest inputs read the
    // same file. Curation adds no synthetic manifest inputs.
    let mut valueset_names = BTreeMap::<String, BTreeSet<(usize, String)>>::new();
    for (i, output) in outputs.iter().enumerate() {
        for record in &output.frame.records {
            if let Record::ValueSet(v) = record {
                valueset_names
                    .entry(v.hash.clone())
                    .or_default()
                    .extend(v.names.iter().map(|name| (rank[i], name.clone())));
            }
        }
    }

    // Register handles only: a name collision check reads them (#2196 dropped the
    // per-register record copies nothing read).
    let mut registers: BTreeSet<(String, String)> = BTreeSet::new();
    let mut valuesets = BTreeMap::new();
    // Links and notices cross registers: the link stage reads them, not resolve. So do
    // documents, which are merged by hash first.
    let mut links = Vec::new();
    let mut docs = Vec::new();
    for record in &applied.records {
        let key = match record {
            Record::Link(_) | Record::Notice(_) => {
                links.push(record.clone());
                continue;
            }
            Record::Doc(d) => {
                docs.push(d.clone());
                continue;
            }
            Record::Register(r) => (r.provider.clone(), r.register.clone()),
            Record::Table(t) => (t.provider.clone(), t.register.clone()),
            Record::Release(r) => (r.provider.clone(), r.register.clone()),
            Record::Occurrence(o) => (o.provider.clone(), o.register.clone()),
            Record::ValueSet(v) => {
                let entry = valuesets
                    .entry(v.hash.clone())
                    .or_insert_with(|| (v.clone(), BTreeSet::new()));
                if let Some(names) = valueset_names.get(&v.hash) {
                    entry.1.extend(names.iter().cloned());
                }
                continue;
            }
        };
        registers.insert(key);
    }

    timing::done("digest and group", t);
    let t = std::time::Instant::now();
    let precomputed = applied.resolved.take();
    let mut resolved = curation::complete_findings(
        &args.curation,
        &retired,
        &applied.records,
        &applied,
        precomputed,
        base,
    )?;
    timing::done("resolve", t);
    let t = std::time::Instant::now();
    let mut report = BTreeMap::new();
    let mut refusals = Vec::new();
    if args.release {
        let mut groups = BTreeMap::<_, Vec<_>>::new();
        for refusal in &applied.refusals {
            groups
                .entry(refusal.register.clone())
                .or_default()
                .push(refusal.message.clone());
        }
        refusals.extend(groups.into_iter().map(|(register, details)| BuildRefusal {
            register,
            kind: "curation",
            details,
        }));
    }
    for r in &mut resolved {
        let (provider, register) = (&r.register.provider, &r.register.register);
        let path = ledger_path(&args.curation, provider, register);
        if args.release {
            for (ids, kind, reason) in [
                (
                    &r.report.unplaced_ids,
                    "ledger-id-unplaced",
                    "has no placed variable",
                ),
                (
                    &r.report.split_ids,
                    "ledger-id-split",
                    "anchors more than one variable",
                ),
            ] {
                if !ids.is_empty() {
                    refusals.push(BuildRefusal {
                        register: Some(format!("{provider}/{register}")),
                        kind,
                        details: ids
                            .iter()
                            .map(|id| ledger_refusal(&path, id, kind, reason))
                            .collect::<Result<_, _>>()?,
                    });
                }
            }
            if !r.report.unminted.is_empty() {
                refusals.push(BuildRefusal {
                    register: Some(format!("{provider}/{register}")),
                    kind: "unminted-variable",
                    details: r.report.unminted.clone(),
                });
            }
        }
    }
    // A public name is one register's within its provider: no other register's public
    // name or handle.
    let mut named: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for r in &resolved {
        let (provider, handle) = (r.register.provider.as_str(), r.register.register.as_str());
        if let Some(other) = named.insert((provider, r.public.as_str()), handle) {
            refusals.push(BuildRefusal {
                register: Some(format!("{provider}/{handle}")),
                kind: "public-name-collision",
                details: vec![format!(
                    "{}: public name {:?} of {provider}/{handle} is also {provider}/{other}'s",
                    ledger_path(&args.curation, provider, handle).display(),
                    r.public
                )],
            });
        }
    }
    for r in &resolved {
        let (provider, handle) = (r.register.provider.as_str(), r.register.register.as_str());
        if r.public != handle && registers.contains(&(provider.to_owned(), r.public.clone())) {
            refusals.push(BuildRefusal {
                register: Some(format!("{provider}/{handle}")),
                kind: "public-name-collision",
                details: vec![format!(
                    "{}: public name {:?} of {provider}/{handle} is another register's handle",
                    ledger_path(&args.curation, provider, handle).display(),
                    r.public
                )],
            });
        }
    }
    sets::final_members(&resolved, &statements)?;
    let (docs, doc_warnings) = docs::merge(docs)?;
    warnings.extend(doc_warnings);
    timing::done("refusals and docs", t);
    let t = std::time::Instant::now();
    let mut linked = link::resolve(&resolved, &links, &docs)?;
    let mut external_relations = Vec::new();
    if let Some(curated) = applied.linked {
        linked.relations = curated.relations;
        external_relations = curated.external;
        applied.statements.extend(curated.statements);
    }
    let link_unresolved = linked.warnings.len();
    let succession_dropped = linked.succession_warnings.len();
    let doc_subject_unresolved = linked.doc_warnings.len();
    warnings.extend(linked.warnings);
    warnings.extend(linked.succession_warnings);
    warnings.extend(linked.doc_warnings);
    for r in &resolved {
        report.insert(
            format!("{}/{}", r.register.provider, r.register.register),
            serde_json::to_value(&r.report)?,
        );
    }

    timing::done("link", t);
    let t = std::time::Instant::now();
    let mut curation = BTreeSet::new();
    tree(&args.curation, "", &mut curation)?;
    let (curation_digest, _) = digest(&args.curation, &curation)?;
    let schema_version = reg_core::CATALOG_SCHEMA_VERSION.to_string();
    let builder_version = env!("CARGO_PKG_VERSION");
    let sqlite_version = rusqlite::version();
    let mut generation = format!("{schema_version}\n{builder_version}\n{sqlite_version}\n");
    for &i in &order {
        let row = &inputs[i];
        writeln!(
            generation,
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.name, row.repo, row.commit, row.path, row.sha256, row.bytes
        )
        .expect("write to String");
    }
    generation += &curation_digest;
    if let Some(base) = base {
        generation += &base.generation;
        generation += &base.sha256;
    }
    let generation_id = reg_core::sha256_hex(generation);
    let day = |iso| reg_core::iso_day_number(iso).expect("an ISO day");
    let meta = BTreeMap::from([
        ("schema_version", schema_version),
        ("generation_id", generation_id.clone()),
        ("catalog_version", args.catalog_version.clone()),
        ("builder_version", builder_version.to_owned()),
        ("sqlite_version", sqlite_version.to_owned()),
        ("open_start", day(reg_frame::OPEN_START).to_string()),
        ("open_end", (day(reg_frame::OPEN_END) + 1).to_string()),
        ("curation_digest", curation_digest),
        ("period_grammar", "reg-core".to_owned()),
        // The grammar is the readers' `[names]` tables, inside the digested curation tree.
        ("delivery_name_grammar", "curation_digest".to_owned()),
    ]);

    let mut providers: Vec<emit::ProviderRow> = manifest
        .provider
        .into_iter()
        .map(|p| emit::ProviderRow {
            pid: p.pid.into_inner(),
            name: p.name,
            url: p.url,
        })
        .collect();
    providers.sort_by(|a, b| a.pid.cmp(&b.pid));
    let mut sorted_inputs: Vec<emit::InputRow> = Vec::new();
    let mut by_index: Vec<Option<emit::InputRow>> = inputs.into_iter().map(Some).collect();
    for &i in &order {
        sorted_inputs.push(by_index[i].take().expect("each input once"));
    }
    let provenance = reg_resolve::provenance::finalize(
        &resolved,
        &valuesets.values().map(|(set, _)| set).collect::<Vec<_>>(),
        &applied.classifications,
        &applied.overrides,
    );
    timing::done("digest and provenance", t);
    let catalog = emit::Catalog {
        set_presentation: applied.sets,
        retired,
        tags,
        meta,
        inputs: sorted_inputs,
        providers,
        registers: resolved,
        valuesets,
        sentinels: curation::sentinel_codes(&applied.sentinels),
        file_input,
        relations: linked.relations,
        notices: linked.notices,
        docs,
        doc_links: linked.doc_links,
        statements: applied.statements,
        provenance,
        classifications: applied.classifications,
        external_relations,
        occurrence_locators: admitted_pin.is_none(),
    };
    if let Some(path) = &args.report {
        let doc = serde_json::json!({
            "generation_id": generation_id,
            "registers": report,
            "warnings": warnings,
            "link_unresolved": link_unresolved,
            "succession_dropped": succession_dropped,
            "doc_subject_unresolved": doc_subject_unresolved,
            "refusals": refusals,
        });
        let text = serde_json::to_string_pretty(&doc)?;
        std::fs::write(path, text + "\n").map_err(|e| format!("{}: {e}", path.display()))?;
    }
    if !refusals.is_empty() {
        for refusal in &refusals {
            eprintln!(
                "reg-build: {}: {} {}",
                refusal.register.as_deref().unwrap_or("curation"),
                refusal.details.len(),
                refusal.kind,
            );
            if base.is_some()
                && let Some(code) = ["premise-changed", "premise-missing"]
                    .into_iter()
                    .find(|code| refusal.details.iter().any(|detail| detail.contains(code)))
            {
                eprintln!("reg-build: {code}");
            }
        }
        return Err(match &args.report {
            Some(path) => format!("build refused; report: {}", path.display()).into(),
            None => "build refused; use --report FILE for details".into(),
        });
    }
    emit::write(&catalog, &args.out)
}

fn lock(args: &Args) -> Result<()> {
    let base = args
        .base
        .as_deref()
        .map(external::Context::open)
        .transpose()?;
    let retired = retired::load(&args.curation)?;
    let statements = reg_readers::curation::load(&args.curation)?;
    let tags = sets::vocabulary(&args.curation)?;
    sets::statements(&tags, &statements)?;
    let manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    let outputs = read_all(args, &manifest)?;
    let applied = curation::run(
        &args.curation,
        &retired,
        &statements,
        &outputs,
        true,
        &args.registers,
        base.as_ref(),
    )?;
    for finding in &applied.findings {
        eprintln!("reg-build: {finding}");
    }
    Ok(())
}

/// `verify FILE [--previous FILE]`, after the command word.
fn parse_verify(mut args: impl Iterator<Item = String>) -> Result<(PathBuf, Option<PathBuf>)> {
    let usage = "usage: reg-build verify FILE [--previous FILE]";
    let (mut file, mut previous) = (None, None);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--previous" if previous.is_none() => {
                previous = Some(PathBuf::from(args.next().ok_or(usage)?));
            }
            flag if flag.starts_with("--") => {
                return Err(format!("unknown flag {flag:?}; {usage}").into());
            }
            _ if file.is_none() => file = Some(PathBuf::from(arg)),
            _ => return Err(usage.into()),
        }
    }
    Ok((file.ok_or(usage)?, previous))
}

fn main() -> ExitCode {
    timing::start();
    let mut args = std::env::args().skip(1).peekable();
    // `diff` has its own exit codes (1 = the catalogs differ, 2 = error).
    if args.next_if_eq("diff").is_some() {
        return diff::run(&args.collect::<Vec<_>>());
    }
    let result = if args.next_if_eq("holdings").is_some() {
        holdings::run(args)
    } else if args.next_if_eq("pin").is_some() {
        holdings::pin_args(args)
    } else if args.next_if_eq("verify").is_some() {
        let args: Vec<String> = args.collect();
        if args.iter().any(|arg| arg == "--catalog") {
            holdings::verify_args(args.into_iter())
        } else {
            parse_verify(args.into_iter())
                .and_then(|(file, previous)| verify::catalog(&file, previous.as_deref()))
        }
    } else {
        parse_args(args).and_then(|args| match args.command.as_str() {
            "read" => read(&args),
            "build" => build_with_provenance(&args, None, None),
            "lock" => lock(&args),
            other => Err(format!("unknown command {other:?}").into()),
        })
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("reg-build: {e}");
            ExitCode::FAILURE
        }
    }
}
