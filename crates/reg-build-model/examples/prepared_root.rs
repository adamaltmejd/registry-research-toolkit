//! Read a prepared root back: `cargo run --release -p reg-build-model --example
//! prepared_root -- ROOT [--digests SOURCE]`.
//!
//! Without `--digests` it compares what the root holds with its manifests and exits 1
//! on a difference. With it, it prints `record_id<TAB>evidence_sha256` for every
//! occurrence of SOURCE, to compare with the Python builder's `evidence_sha256`.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use reg_build_model::prepared::PreparedRoot;
use reg_build_model::record_evidence_sha256;

fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: prepared_root ROOT [--digests SOURCE]")?,
    );
    let prepared = PreparedRoot::open(&root)?;
    if let Some(flag) = args.next() {
        if flag != "--digests" {
            return Err(format!("unknown argument {flag}").into());
        }
        let source = args.next().ok_or("--digests needs a SOURCE")?;
        let mut out = std::io::BufWriter::new(std::io::stdout().lock());
        prepared
            .records()
            .for_each_record(Some(&source), |record| {
                writeln!(
                    out,
                    "{}\t{}",
                    record.record_id,
                    record_evidence_sha256(&record)
                )
                .map_err(reg_build_model::Error::from)
            })?;
        return Ok(ExitCode::SUCCESS);
    }
    let counts = prepared.counts()?;
    let records = prepared.records_manifest();
    let catalog = prepared.catalog();
    let rows = [
        ("records", counts.records, records.record_count),
        ("tables", counts.tables, records.table_count),
        ("table_rows", counts.table_rows, records.table_row_count),
        ("evidence", counts.evidence, catalog.evidence_count),
        ("values", counts.values, catalog.values.len() as u64),
    ];
    let mut same = true;
    for (name, held, stated) in rows {
        println!("{name}\theld {held}\tmanifest {stated}");
        same &= held == stated;
    }
    println!("prepared_manifest_sha256\t{}", prepared.catalog_sha256());
    Ok(if same {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
