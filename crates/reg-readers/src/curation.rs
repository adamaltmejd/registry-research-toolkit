//! Register-local curation statements. Loading validates syntax, never resolves selectors.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use reg_frame::statement::Statement;
use serde::Deserialize;

use crate::{ReadError, error};

/// A validated statement and its authoring location; identity excludes the location.
#[derive(Debug, Clone)]
pub struct LocatedStatement {
    pub provider: String,
    pub register: String,
    pub path: PathBuf,
    pub line: usize,
    pub identity: String,
    pub statement: Statement,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    statement: Vec<toml::Spanned<toml::Table>>,
}

/// Load every `registers/<provider>/<register>/curation.toml`, in path order.
///
/// # Errors
/// Unreadable files, invalid statements or duplicate normalized content in a register.
/// Refusals include the file and statement's starting line.
pub fn load(root: &Path) -> Result<Vec<LocatedStatement>, ReadError> {
    let mut out = Vec::new();
    let registers = root.join("registers");
    if !registers
        .try_exists()
        .map_err(|e| error(&registers.display().to_string(), e.to_string()))?
    {
        return Ok(out);
    }
    for provider in directories(&registers)? {
        for register in directories(&provider)? {
            let path = register.join("curation.toml");
            if !path
                .try_exists()
                .map_err(|e| error(&path.display().to_string(), e.to_string()))?
            {
                continue;
            }
            let name = path.display().to_string();
            let text = std::fs::read_to_string(&path).map_err(|e| error(&name, e.to_string()))?;
            let parsed: File = toml::from_str(&text).map_err(|e: toml::de::Error| {
                let line = line_at(&text, e.span().map_or(0, |s| s.start));
                error(&format!("{name}:{line}"), e.message())
            })?;
            let mut seen = BTreeMap::new();
            for raw in parsed.statement {
                let line = line_at(&text, raw.span().start);
                let location = format!("{name}:{line}");
                let statement: Statement = toml::Value::Table(raw.into_inner())
                    .try_into()
                    .map_err(|e: toml::de::Error| error(&location, e.message()))?;
                statement.validate().map_err(|e| error(&location, e))?;
                let identity = statement.identity();
                if let Some(previous) = seen.insert(identity.clone(), line) {
                    return Err(error(
                        &location,
                        format!("duplicate statement; first at {name}:{previous}"),
                    ));
                }
                out.push(LocatedStatement {
                    provider: directory_name(&provider)?,
                    register: directory_name(&register)?,
                    path: path.clone(),
                    line,
                    identity,
                    statement,
                });
            }
        }
    }
    Ok(out)
}

fn line_at(text: &str, offset: usize) -> usize {
    text[..offset].chars().filter(|&c| c == '\n').count() + 1
}

fn directory_name(path: &Path) -> Result<String, ReadError> {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| error(&path.display().to_string(), "directory needs a name"))
}

fn directories(path: &Path) -> Result<Vec<PathBuf>, ReadError> {
    let name = path.display().to_string();
    let mut out = Vec::new();
    for entry in std::fs::read_dir(path).map_err(|e| error(&name, e.to_string()))? {
        let entry = entry.map_err(|e| error(&name, e.to_string()))?;
        if entry
            .file_type()
            .map_err(|e| error(&name, e.to_string()))?
            .is_dir()
        {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}
