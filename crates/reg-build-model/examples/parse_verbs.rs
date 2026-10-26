//! Parse converted register curation files as the neutral verbs:
//! `cargo run -p reg-build-model --example parse_verbs -- FILE...`.
//!
//! The converter (`reg_meta_build.port.convert`) writes them; this reads each file's verb
//! tables with `Verbs` (unknown fields denied) and the naming pins and unchanged verbs
//! as plain TOML. It prints the entry count per file and exits 1 on the first file that
//! does not parse. Deleted with the converter at phase 4.

use std::path::PathBuf;
use std::process::ExitCode;

use reg_build_model::verbs::Verbs;

const VERB_TABLES: [&str; 5] = ["identity", "table", "errata", "representation", "coding"];

fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let files: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if files.is_empty() {
        return Err("usage: parse_verbs FILE...".into());
    }
    for file in files {
        let text = std::fs::read_to_string(&file)?;
        let mut table: toml::Table = match text.parse() {
            Ok(table) => table,
            Err(error) => {
                eprintln!("{}: {error}", file.display());
                return Ok(ExitCode::FAILURE);
            }
        };
        table.retain(|name, _| VERB_TABLES.contains(&name));
        let verbs: Verbs = match table.try_into() {
            Ok(verbs) => verbs,
            Err(error) => {
                eprintln!("{}: {error}", file.display());
                return Ok(ExitCode::FAILURE);
            }
        };
        let entries = verbs.identity.assign.len()
            + verbs.table.assign.len()
            + verbs.errata.table.len()
            + verbs.errata.field.len()
            + verbs.errata.period.len()
            + verbs.errata.delivered.len()
            + verbs.errata.delivery.len()
            + verbs.errata.column.len()
            + verbs.errata.support.len()
            + verbs.representation.parallel.len()
            + verbs.representation.period_family.len()
            + verbs.representation.delivery_metadata.len()
            + verbs.representation.matrix.len()
            + [
                &verbs.coding.warning,
                &verbs.coding.choice,
                &verbs.coding.uncoded,
                &verbs.coding.omit,
                &verbs.coding.extend,
                &verbs.coding.documented,
                &verbs.coding.sentinel,
                &verbs.coding.support,
            ]
            .iter()
            .map(|entries| entries.len())
            .sum::<usize>();
        println!("{}\t{entries}", file.display());
    }
    Ok(ExitCode::SUCCESS)
}
