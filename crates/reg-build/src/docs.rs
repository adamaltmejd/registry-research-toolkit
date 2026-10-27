//! Documents: the `Doc` records of every input, merged by content hash. Two records of
//! one hash are one document: the first by locator order stands and the subjects are
//! the union.

use std::collections::BTreeMap;

use reg_frame::Doc;

/// A merged document: the standing record and its decoded bytes (`pdf` only).
pub struct Document {
    pub doc: Doc,
    pub content: Option<Vec<u8>>,
}

/// Documents in hash order, and one warning per extra record whose title differs.
///
/// # Errors
/// A `pdf` record without content, a `parsed` one with content, or content that is not
/// lowercase hex.
pub fn merge(docs: Vec<Doc>) -> Result<(Vec<Document>, Vec<String>), String> {
    let mut by_hash: BTreeMap<String, Vec<Doc>> = BTreeMap::new();
    for doc in docs {
        by_hash.entry(doc.hash.clone()).or_default().push(doc);
    }
    let mut merged = Vec::new();
    let mut warnings = Vec::new();
    for (hash, mut group) in by_hash {
        group.sort_by(|a, b| a.locator.cmp(&b.locator));
        let mut first = group.remove(0);
        for other in group {
            if other.title != first.title {
                warnings.push(format!(
                    "doc {hash}: {:?} at {}:{} and {:?} at {}:{} differ in title; the first stands",
                    first.title,
                    first.locator.input,
                    first.locator.at,
                    other.title,
                    other.locator.input,
                    other.locator.at
                ));
            }
            first.subjects.extend(other.subjects);
        }
        first.subjects.sort();
        first.subjects.dedup();
        let located = |m: &str| format!("{}:{}: {m}", first.locator.input, first.locator.at);
        let content = match (first.kind.as_str(), &first.content) {
            ("pdf", Some(hex)) => {
                Some(reg_frame::unhex(hex).ok_or_else(|| located("content is not lowercase hex"))?)
            }
            ("pdf", None) => return Err(located("a pdf document has no content")),
            (_, Some(_)) => return Err(located("only a pdf document has content")),
            (_, None) => None,
        };
        merged.push(Document {
            doc: first,
            content,
        });
    }
    Ok((merged, warnings))
}
