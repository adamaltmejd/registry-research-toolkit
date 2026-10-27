//! Documents the source publishes about its registers, in two input formats, one reader
//! each: [`markdown`] (a directory of parsed markdown texts, configured by
//! `curation/readers/markdown.toml`) and [`pdf_list`] (a list of PDFs pinned by hash,
//! configured by `curation/readers/pdf-list.toml`).
//!
//! A document's subject is stated in source coordinates and resolved by the link stage:
//! a markdown document whose frontmatter names a variable is about that variable of the
//! input's register, with the key folded by [`name_key`] (the key the LISA reader gives
//! its columns); one without is about the register. A listed PDF is about the register
//! whose handle is its list key. A subject that matches nothing leaves the document
//! searchable, and the link stage warns.
//!
//! Markdown is one document per `*.md` in file-name order, with a licence and original
//! URL looked up in the sources file by the frontmatter `source` (one trailing `.md`
//! stripped).

use std::collections::BTreeMap;
use std::path::Path;

use reg_frame::{Doc, DocSection, DocSubject, Endpoint, Frame, Locator, Record, hex};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{ReadError, ReadOutput, config, error, name_key};

/// `curation/readers/markdown.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkdownConfig {
    /// The sources file's name: each document's original URL, and the licence of the
    /// markdown derived from them.
    sources: String,
    frontmatter: Frontmatter,
}

/// The frontmatter keys read.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    variable: String,
    title: String,
    source: String,
}

impl MarkdownConfig {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// `curation/readers/pdf-list.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PdfListConfig {
    /// The document-list file's name.
    list: String,
}

impl PdfListConfig {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// The sources file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sources {
    licence: String,
    sources: BTreeMap<String, Source>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    url: String,
    // The source's own title; a document is titled by its frontmatter.
    #[allow(dead_code)]
    title: String,
}

/// The document-list file: `[[register.<handle>.document]]` entries.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    register: BTreeMap<String, Register>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Register {
    document: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    title: String,
    filename: String,
    source_url: String,
    license: String,
    // When the file was fetched; provenance for the maintainer, not catalog data.
    #[allow(dead_code)]
    fetched: String,
    sha256: String,
    byte_size: u64,
    #[serde(default = "yes")]
    required: bool,
}

fn yes() -> bool {
    true
}

/// Read the markdown documents under `rel`, about `register` of `provider`.
///
/// # Errors
/// An unreadable directory or sources file, markdown whose frontmatter does not close or
/// that has no sources licence.
pub fn markdown(
    root: &Path,
    rel: &str,
    provider: &str,
    register: &str,
    config: &MarkdownConfig,
) -> Result<ReadOutput, ReadError> {
    let dir = root.join(rel);
    let sources_path = format!("{rel}/{}", config.sources);
    let sources: Sources = config_at(root, &sources_path)?;
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .map_err(|e| error(rel, e.to_string()))?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| {
            !name.starts_with('.') && Path::new(name).extension().is_some_and(|e| e == "md")
        })
        .collect();
    names.sort();
    let mut out = ReadOutput::default();
    out.files.push(sources_path.clone());
    let mut records = Vec::new();
    for name in names {
        let path = format!("{rel}/{name}");
        let bytes = std::fs::read(root.join(&path)).map_err(|e| error(&path, e.to_string()))?;
        let text = std::str::from_utf8(&bytes).map_err(|e| error(&path, e.to_string()))?;
        out.files.push(path.clone());
        let (meta, body) = frontmatter(text).map_err(|m| error(&path, m))?;
        if body.trim().is_empty() {
            out.warnings.push(format!("{path}: empty body; skipped"));
            continue;
        }
        let key = |k: &str| meta.get(k).filter(|v| !v.is_empty());
        let source = key(&config.frontmatter.source);
        let url = source.and_then(|s| {
            sources
                .sources
                .get(s.strip_suffix(".md").unwrap_or(s))
                .map(|s| s.url.clone())
        });
        if url.is_none() {
            out.warnings.push(format!(
                "{path}: source {:?} is not in {}; no original URL",
                source.map_or("", String::as_str),
                config.sources
            ));
        }
        let endpoint = |kind: &str, key: Option<String>| Endpoint {
            kind: kind.to_owned(),
            provider: provider.to_owned(),
            register: Some(register.to_owned()),
            register_name: None,
            table: None,
            key,
        };
        let subject = match key(&config.frontmatter.variable) {
            Some(variable) => endpoint("variable", Some(name_key(variable))),
            None => endpoint("register", None),
        };
        records.push(Record::Doc(Doc {
            hash: hex(&Sha256::digest(&bytes)),
            kind: "parsed".to_owned(),
            title: key(&config.frontmatter.title)
                .cloned()
                .unwrap_or_else(|| name.trim_end_matches(".md").to_owned()),
            licence: sources.licence.clone(),
            bytes: bytes.len() as u64,
            url,
            sections: sections(body),
            content: None,
            subjects: vec![DocSubject {
                endpoint: subject,
                page: 0,
            }],
            locator: Locator {
                input: path,
                at: String::new(),
            },
        }));
    }
    out.frame = Frame::new(records);
    Ok(out)
}

/// Read the PDFs the list file under `rel` pins, each about the register its list key
/// names.
///
/// # Errors
/// A missing or invalid list file, a listed file that is absent (and required), unsafe
/// or does not match its pinned hash and size.
pub fn pdf_list(
    root: &Path,
    rel: &str,
    provider: &str,
    config: &PdfListConfig,
) -> Result<ReadOutput, ReadError> {
    let list_path = format!("{rel}/{}", config.list);
    let list: List = config_at(root, &list_path)?;
    let mut out = ReadOutput::default();
    out.files.push(list_path.clone());
    let mut records = Vec::new();
    for (handle, register) in list.register {
        for entry in register.document {
            if entry.filename.contains(['/', '\\']) || entry.filename.starts_with('.') {
                return Err(error(
                    &list_path,
                    format!(
                        "{handle}: filename {:?} is not a bare file name",
                        entry.filename
                    ),
                ));
            }
            let path = format!("{rel}/{handle}/{}", entry.filename);
            let bytes = match std::fs::read(root.join(&path)) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && !entry.required => {
                    out.warnings
                        .push(format!("{path}: absent and not required; skipped"));
                    continue;
                }
                Err(e) => return Err(error(&path, e.to_string())),
            };
            let hash = hex(&Sha256::digest(&bytes));
            let size = bytes.len() as u64;
            if hash != entry.sha256 || size != entry.byte_size {
                return Err(error(
                    &path,
                    format!(
                        "has sha256 {hash} and {size} bytes, pinned {} and {}",
                        entry.sha256, entry.byte_size
                    ),
                ));
            }
            out.files.push(path.clone());
            records.push(Record::Doc(Doc {
                hash,
                kind: "pdf".to_owned(),
                title: entry.title,
                licence: entry.license,
                bytes: size,
                url: Some(entry.source_url),
                sections: Vec::new(),
                content: Some(hex(&bytes)),
                subjects: vec![DocSubject {
                    endpoint: Endpoint {
                        kind: "register".to_owned(),
                        provider: provider.to_owned(),
                        register: Some(handle.clone()),
                        register_name: None,
                        table: None,
                        key: None,
                    },
                    page: 0,
                }],
                locator: Locator {
                    input: path,
                    at: String::new(),
                },
            }));
        }
    }
    out.frame = Frame::new(records);
    Ok(out)
}

fn config_at<T: serde::de::DeserializeOwned>(root: &Path, rel: &str) -> Result<T, ReadError> {
    let text = std::fs::read_to_string(root.join(rel)).map_err(|e| error(rel, e.to_string()))?;
    toml::from_str(&text).map_err(|e| error(rel, e.to_string()))
}

// simplify: a hand-rolled reader of the flat frontmatter the LISA markdown carries; use a
// YAML crate if frontmatter from another source appears.
/// The frontmatter's `key: value` pairs (a quoted value unquoted, a wrapped or block
/// scalar folded to one line; list items are not read) and the body after it. No
/// opening `---` line is no frontmatter.
fn frontmatter(text: &str) -> Result<(BTreeMap<String, String>, &str), String> {
    let mut meta = BTreeMap::new();
    let mut lines = text.split_inclusive('\n');
    let Some(first) = lines.next().filter(|l| l.trim_end() == "---") else {
        return Ok((meta, text));
    };
    let mut consumed = first.len();
    let mut key: Option<String> = None;
    for line in lines {
        consumed += line.len();
        let trimmed = line.trim_end();
        if trimmed == "---" {
            return Ok((meta, &text[consumed..]));
        }
        let indented = trimmed.starts_with([' ', '\t']);
        let item = trimmed.trim_start().starts_with("- ");
        if item {
            continue;
        }
        if indented {
            // A wrapped or block scalar continues the previous key's value.
            if let Some(value) = key.as_ref().and_then(|k| meta.get_mut(k)) {
                if !value.is_empty() {
                    value.push(' ');
                }
                value.push_str(trimmed.trim());
            }
        } else if let Some((k, v)) = trimmed.split_once(':') {
            let v = v.trim();
            let v = if v.starts_with(['>', '|']) {
                ""
            } else {
                unquote(v)
            };
            key = Some(k.trim().to_owned());
            meta.insert(k.trim().to_owned(), v.to_owned());
        }
    }
    Err("frontmatter does not close".to_owned())
}

fn unquote(v: &str) -> &str {
    for q in ['"', '\''] {
        if let Some(inner) = v.strip_prefix(q).and_then(|v| v.strip_suffix(q)) {
            return inner;
        }
    }
    v
}

/// `body` split at ATX headings. Text before the first heading is a section without a
/// heading, dropped when blank.
fn sections(body: &str) -> Vec<DocSection> {
    let mut out: Vec<DocSection> = Vec::new();
    let mut heading: Option<String> = None;
    let mut text: Vec<&str> = Vec::new();
    let flush = |heading: Option<String>, text: &mut Vec<&str>, out: &mut Vec<DocSection>| {
        let body = text.join("\n").trim().to_owned();
        text.clear();
        if heading.is_some() || !body.is_empty() {
            out.push(DocSection {
                heading,
                page: None,
                body,
            });
        }
    };
    // An open code fence as (marker, length); it closes on the same marker, at least as long.
    let mut fence: Option<(char, usize)> = None;
    for line in body.lines() {
        let trimmed = line.trim_start();
        let marker = trimmed.chars().next().filter(|&c| c == '`' || c == '~');
        let run = marker.map_or(0, |m| trimmed.chars().take_while(|&c| c == m).count());
        if let (Some(m), true) = (marker, run >= 3) {
            match fence {
                None => fence = Some((m, run)),
                Some((open, len))
                    if open == m && run >= len && trimmed[run..].trim().is_empty() =>
                {
                    fence = None;
                }
                Some(_) => {}
            }
            text.push(line);
            continue;
        }
        if fence.is_some() {
            text.push(line);
            continue;
        }
        let hashes = line.chars().take_while(|&c| c == '#').count();
        let rest = &line[hashes..];
        if (1..=6).contains(&hashes) && (rest.is_empty() || rest.starts_with(' ')) {
            flush(heading.take(), &mut text, &mut out);
            heading = Some(rest.trim().to_owned());
        } else {
            text.push(line);
        }
    }
    flush(heading, &mut text, &mut out);
    out
}
