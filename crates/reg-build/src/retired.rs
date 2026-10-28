//! Permanent published-id retirements, authored in `curation/retired.toml`.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    kind: String,
    #[serde(rename = "in")]
    version: String,
    reason: String,
}

pub struct Retired {
    pub pid: String,
    pub kind: String,
    pub version: String,
    pub reason: String,
    location: String,
}

impl Retired {
    pub fn live_error(&self) -> String {
        format!(
            "{}: retired-id-live: retired id {:?} is also live in ident",
            self.location, self.pid
        )
    }
}

pub fn load(root: &Path) -> Result<Vec<Retired>, String> {
    let path = root.join("retired.toml");
    // Synthetic curation trees with no published history need no retirements.
    if !path
        .try_exists()
        .map_err(|e| format!("{}: {e}", path.display()))?
    {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let line_starts: Vec<_> = std::iter::once(0)
        .chain(text.match_indices('\n').map(|(offset, _)| offset + 1))
        .collect();
    let line = |offset| line_starts.partition_point(|&start| start <= offset);
    let entries: BTreeMap<toml::Spanned<String>, Entry> = toml::from_str(&text).map_err(|e| {
        format!(
            "{}:{}: {e}",
            path.display(),
            line(e.span().map_or(0, |s| s.start))
        )
    })?;
    entries
        .into_iter()
        .map(|(pid, entry)| {
            let location = format!("{}:{}", path.display(), line(pid.span().start));
            if !matches!(
                entry.kind.as_str(),
                "provider"
                    | "register"
                    | "table"
                    | "variable"
                    | "set"
                    | "tag"
                    | "class_family"
                    | "class_version"
            ) {
                return Err(format!(
                    "{location}: retired-kind: unknown kind {:?}",
                    entry.kind
                ));
            }
            if !matches!(entry.reason.as_str(), "merged" | "split" | "withdrawn") {
                return Err(format!(
                    "{location}: retired-reason: unknown reason {:?}",
                    entry.reason
                ));
            }
            Ok(Retired {
                pid: pid.into_inner(),
                kind: entry.kind,
                version: entry.version,
                reason: entry.reason,
                location,
            })
        })
        .collect()
}
