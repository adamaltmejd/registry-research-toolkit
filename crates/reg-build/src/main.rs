//! `reg-build`: the catalog builder (`crates/DESIGN.md`, "The catalog builder").
//!
//! ```text
//! reg-build read  --inputs DIR --curation DIR --out DIR [--assets DIR]
//! reg-build build --inputs DIR --curation DIR --out FILE [--assets DIR] [--report FILE]
//!                 [--catalog-version V] [--release]
//! reg-build lock [REGISTER...] --inputs DIR --curation DIR [--assets DIR]
//! reg-build verify FILE [--previous FILE]
//! reg-build diff OLD NEW [--json] [-n N]
//! ```
//!
//! `verify` checks a written catalog file (`verify.rs`) and, with `--previous`, its ids
//! against the previous release's; it prints nothing on success. `read` and `build` run every reader of the manifest `<curation>/inputs.toml` over the inputs root.
//! An input that is a release asset of the inputs repository is read from
//! `<assets>/<release>/<path>`, fetched there beforehand (`gh release download`).
//! `read` writes each input's frame as `<name>.jsonl` and its warnings as
//! `<name>.warnings` under `--out`. `build` resolves each register with its ledger
//! `<curation>/registers/<provider>/<register>/ids.toml`, matches the frames' links and
//! notices across the resolved registers (`link.rs`), and writes the catalog file to `--out`
//! and the build report (JSON) to `--report`. `--catalog-version` (default `dev`) is
//! recorded in the file; `--release` refuses a variable without a ledger id. A refusal
//! prints the error on stderr and exits 1. `diff` compares two catalog files by natural
//! key (exit 0 same, 1 different, 2 error); see `diff.rs`.

mod curation;
mod diff;
mod docs;
mod emit;
mod link;
mod search;
mod verify;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use reg_frame::Record;
use serde::Deserialize;
use sha2::{Digest, Sha256};

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
    pid: String,
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
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let command = args
        .next()
        .ok_or("usage: reg-build read|build --inputs DIR --curation DIR --out PATH")?;
    let (mut inputs, mut curation, mut out, mut assets, mut report, mut version) =
        (None, None, None, None, None, None);
    let mut release = false;
    let mut registers = Vec::new();
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--inputs" => &mut inputs,
            "--curation" => &mut curation,
            "--out" => &mut out,
            "--assets" => &mut assets,
            "--report" => &mut report,
            "--catalog-version" => &mut version,
            "--release" => {
                release = true;
                continue;
            }
            other if command == "lock" && !other.starts_with('-') => {
                registers.push(other.to_owned());
                continue;
            }
            other => return Err(format!("unknown flag {other:?}")),
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
    })
}

fn toml_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    reg_readers::config(path).map_err(|e| e.to_string())
}

/// Every input read, in manifest order.
fn ledger_path(curation: &Path, provider: &str, register: &str) -> PathBuf {
    curation
        .join("registers")
        .join(provider)
        .join(register)
        .join("ids.toml")
}

fn read_all(args: &Args, manifest: &Manifest) -> Result<Vec<reg_readers::ReadOutput>, String> {
    let readers = args.curation.join("readers");
    manifest
        .input
        .iter()
        .map(|input| {
            reg_readers::read(&args.inputs, args.assets.as_deref(), &readers, input)
                .map_err(|e| e.to_string())
        })
        .collect()
}

fn read(args: &Args) -> Result<(), String> {
    let manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    std::fs::create_dir_all(&args.out).map_err(|e| e.to_string())?;
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

fn sha256_file(path: &Path) -> Result<(String, i64), String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let len = i64::try_from(bytes.len()).map_err(|e| e.to_string())?;
    Ok((reg_frame::hex(&Sha256::digest(&bytes)), len))
}

/// The digest of a set of files: SHA-256 over `path\tsha256\n` lines in path order,
/// and their total size. A single file is its own SHA-256.
fn digest(root: &Path, files: &BTreeSet<String>) -> Result<(String, i64), String> {
    if let [only] = files.iter().collect::<Vec<_>>()[..] {
        return sha256_file(&root.join(only));
    }
    let mut hasher = Sha256::new();
    let mut total = 0;
    for file in files {
        let (sha, bytes) = sha256_file(&root.join(file))?;
        hasher.update(format!("{file}\t{sha}\n"));
        total += bytes;
    }
    Ok((reg_frame::hex(&hasher.finalize()), total))
}

/// Every file under `dir`, relative to it, `/`-separated; symlinks are followed and
/// dotfiles (host litter such as `.DS_Store`) skipped.
fn tree(dir: &Path, prefix: &str, out: &mut BTreeSet<String>) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
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

#[allow(clippy::too_many_lines)]
fn build(args: &Args) -> Result<(), String> {
    let statements = reg_readers::curation::load(&args.curation).map_err(|e| e.to_string())?;
    let manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    let outputs = read_all(args, &manifest)?;
    let applied = curation::run(
        &args.curation,
        &statements,
        &outputs,
        false,
        &[],
        args.release,
    )?;

    let mut inputs = Vec::new();
    let mut warnings = Vec::new();
    warnings.extend(applied.findings);
    for (input, output) in manifest.input.iter().zip(&outputs) {
        // A release asset is its pin (the reader verified it); its locators name
        // archive members, not files under the inputs root.
        let (sha256, bytes) = match (&input.release, &input.sha256) {
            (Some(_), Some(pin)) => {
                let path = input
                    .locate(&args.inputs, args.assets.as_deref())
                    .map_err(|e| e.to_string())?;
                let len = std::fs::metadata(&path)
                    .map_err(|e| format!("{}: {e}", path.display()))?
                    .len();
                (pin.clone(), i64::try_from(len).map_err(|e| e.to_string())?)
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

    let mut registers: BTreeMap<(String, String), Vec<Record>> = BTreeMap::new();
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
        registers.entry(key).or_default().push(record.clone());
    }

    let set_by_hash: BTreeMap<&str, &reg_frame::ValueSet> = valuesets
        .iter()
        .map(|(hash, (set, _))| (hash.as_str(), set))
        .collect();
    let mut resolved = Vec::new();
    let mut report = BTreeMap::new();
    for ((provider, register), records) in &registers {
        let path = ledger_path(&args.curation, provider, register);
        let ledger = if path.is_file() {
            toml_file(&path)?
        } else {
            reg_resolve::Ledger::default()
        };
        let r = reg_resolve::resolve(records, &ledger, &set_by_hash)
            .map_err(|e| format!("{provider}/{register}: {e}"))?;
        if args.release && !r.report.unminted.is_empty() {
            return Err(format!(
                "{provider}/{register}: {} variables have no ledger id (--release)",
                r.report.unminted.len()
            ));
        }
        resolved.push(r);
    }
    // A public name is one register's within its provider: no other register's public
    // name or handle.
    let mut named: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for r in &resolved {
        let (provider, handle) = (r.register.provider.as_str(), r.register.register.as_str());
        if let Some(other) = named.insert((provider, r.public.as_str()), handle) {
            return Err(format!(
                "{}: public name {:?} of {provider}/{handle} is also {provider}/{other}'s",
                ledger_path(&args.curation, provider, handle).display(),
                r.public
            ));
        }
    }
    for r in &resolved {
        let (provider, handle) = (r.register.provider.as_str(), r.register.register.as_str());
        if r.public != handle && registers.contains_key(&(provider.to_owned(), r.public.clone())) {
            return Err(format!(
                "{}: public name {:?} of {provider}/{handle} is another register's handle",
                ledger_path(&args.curation, provider, handle).display(),
                r.public
            ));
        }
    }
    let (docs, doc_warnings) = docs::merge(docs)?;
    warnings.extend(doc_warnings);
    let linked = link::resolve(&resolved, &links, &docs)?;
    let link_unresolved = linked.warnings.len();
    let succession_dropped = linked.succession_warnings.len();
    let doc_subject_unresolved = linked.doc_warnings.len();
    warnings.extend(linked.warnings);
    warnings.extend(linked.succession_warnings);
    warnings.extend(linked.doc_warnings);
    for r in &resolved {
        report.insert(
            format!("{}/{}", r.register.provider, r.register.register),
            serde_json::to_value(&r.report).map_err(|e| e.to_string())?,
        );
    }

    let mut curation = BTreeSet::new();
    tree(&args.curation, "", &mut curation)?;
    let (curation_digest, _) = digest(&args.curation, &curation)?;
    let schema_version = reg_core::CATALOG_SCHEMA_VERSION.to_string();
    let builder_version = env!("CARGO_PKG_VERSION");
    let sqlite_version = rusqlite::version();
    let mut hasher = Sha256::new();
    hasher.update(format!(
        "{schema_version}\n{builder_version}\n{sqlite_version}\n"
    ));
    for &i in &order {
        let row = &inputs[i];
        hasher.update(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            row.name, row.repo, row.commit, row.path, row.sha256, row.bytes
        ));
    }
    hasher.update(&curation_digest);
    let generation_id = reg_frame::hex(&hasher.finalize());
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
            pid: p.pid,
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
    let catalog = emit::Catalog {
        meta,
        inputs: sorted_inputs,
        providers,
        registers: resolved,
        valuesets,
        file_input,
        relations: linked.relations,
        notices: linked.notices,
        docs,
        doc_links: linked.doc_links,
        statements: applied.statements,
        provenance,
        classifications: applied.classifications,
    };
    if let Some(path) = &args.report {
        let doc = serde_json::json!({
            "generation_id": generation_id,
            "registers": report,
            "warnings": warnings,
            "link_unresolved": link_unresolved,
            "succession_dropped": succession_dropped,
            "doc_subject_unresolved": doc_subject_unresolved,
        });
        let text = serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?;
        std::fs::write(path, text + "\n").map_err(|e| format!("{}: {e}", path.display()))?;
    }
    emit::write(&catalog, &args.out)
}

fn lock(args: &Args) -> Result<(), String> {
    let statements = reg_readers::curation::load(&args.curation).map_err(|e| e.to_string())?;
    let manifest: Manifest = toml_file(&args.curation.join("inputs.toml"))?;
    let outputs = read_all(args, &manifest)?;
    curation::run(
        &args.curation,
        &statements,
        &outputs,
        true,
        &args.registers,
        true,
    )?;
    Ok(())
}

/// `verify FILE [--previous FILE]`, after the command word.
fn parse_verify(
    mut args: impl Iterator<Item = String>,
) -> Result<(PathBuf, Option<PathBuf>), String> {
    let usage = "usage: reg-build verify FILE [--previous FILE]";
    let (mut file, mut previous) = (None, None);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--previous" if previous.is_none() => {
                previous = Some(PathBuf::from(args.next().ok_or(usage)?));
            }
            flag if flag.starts_with("--") => {
                return Err(format!("unknown flag {flag:?}; {usage}"));
            }
            _ if file.is_none() => file = Some(PathBuf::from(arg)),
            _ => return Err(usage.to_owned()),
        }
    }
    Ok((file.ok_or(usage)?, previous))
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1).peekable();
    // `diff` has its own exit codes (1 = the catalogs differ, 2 = error).
    if args.next_if_eq("diff").is_some() {
        return diff::run(&args.collect::<Vec<_>>());
    }
    let result = if args.next_if_eq("verify").is_some() {
        parse_verify(args).and_then(|(file, previous)| verify::catalog(&file, previous.as_deref()))
    } else {
        parse_args(args).and_then(|args| match args.command.as_str() {
            "read" => read(&args),
            "build" => build(&args),
            "lock" => lock(&args),
            other => Err(format!("unknown command {other:?}")),
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
