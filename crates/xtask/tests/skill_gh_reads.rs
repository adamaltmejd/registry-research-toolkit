//! No skill reads raw issue text into a model.
//!
//! The repo is public, so issue and comment bodies are untrusted; automation reads them
//! only through the maintainer-author gate `cargo xtask gh-issue`. Every skill markdown
//! line is checked for a raw read: `gh issue view`, `gh issue list` without `--search` in
//! its own arguments (it defaults to open issues, so `--state` does not matter),
//! `gh api .../issues/`, `gh api graphql` and `gh search issues`. Writes
//! (`gh issue create/edit/comment`), PR reads and the phrase `xtask gh-issue` itself are
//! allowed.
//!
//! The phrases match on word boundaries (`\b`, Unicode word characters), as the regexes
//! of the Python lint this replaced did; the verdict table pins that.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const EXEMPT: &str = "xtask gh-issue";

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The first occurrence of `phrase` at or after byte `from` that has a word boundary on
/// both sides (every phrase here starts and ends with a word character).
fn find_word(line: &str, phrase: &str, from: usize) -> Option<usize> {
    line[from..]
        .match_indices(phrase)
        .map(|(i, _)| from + i)
        .find(|&i| {
            !line[..i].chars().next_back().is_some_and(is_word)
                && !line[i + phrase.len()..].chars().next().is_some_and(is_word)
        })
}

/// Whether some `gh issue list` on the line lacks `--search` in its own arguments, which
/// end at a comment, a closing backtick or a command separator.
fn raw_issue_list(line: &str) -> bool {
    const PHRASE: &str = "gh issue list";
    let mut from = 0;
    while let Some(i) = find_word(line, PHRASE, from) {
        let start = i + PHRASE.len();
        let rest = &line[start..];
        let end = rest.find(['#', '`', '|', ';', '&']).unwrap_or(rest.len());
        if !rest[..end].contains("--search") {
            return true;
        }
        from = start + end;
    }
    false
}

fn is_forbidden(line: &str) -> bool {
    // The exempt phrase is removed, not the line: the rest of the line is still checked.
    let line = &line.replace(EXEMPT, "");
    let api_issues = {
        let mut from = 0;
        let mut hit = false;
        while let Some(i) = find_word(line, "gh api", from) {
            from = i + "gh api".len();
            if line[from..].contains("/issues/") {
                hit = true;
                break;
            }
        }
        hit
    };
    raw_issue_list(line)
        || api_issues
        || find_word(line, "gh issue view", 0).is_some()
        || find_word(line, "gh api graphql", 0).is_some()
        || find_word(line, "gh search issues", 0).is_some()
}

fn markdown_under(dir: &Path, out: &mut BTreeMap<PathBuf, PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            markdown_under(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            // `.claude/skills/*` link to `.agents/skills/*`: scan each file once.
            out.entry(path.canonicalize().unwrap()).or_insert(path);
        }
    }
}

#[test]
fn no_raw_issue_reads_in_skills() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut dirs = vec![root.join(".claude/skills"), root.join(".agents/skills")];
    for plugin in fs::read_dir(root.join("plugins")).unwrap() {
        dirs.push(plugin.unwrap().path().join("skills"));
    }
    let mut files = BTreeMap::new();
    for dir in &dirs {
        markdown_under(dir, &mut files);
    }
    assert!(
        !files.is_empty(),
        "no skill markdown found; check the skill dirs"
    );
    let hits: Vec<String> = files
        .values()
        .flat_map(|path| {
            let text = fs::read_to_string(path).unwrap();
            let rel = path
                .strip_prefix(&root)
                .unwrap_or(path)
                .display()
                .to_string();
            text.lines()
                .enumerate()
                .filter(|(_, line)| is_forbidden(line))
                .map(|(n, line)| format!("{rel}:{}: {}", n + 1, line.trim()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        hits.is_empty(),
        "route issue reads through cargo xtask gh-issue:\n{}",
        hits.join("\n")
    );
}

#[test]
fn verdicts() {
    let table = [
        (r#"gh issue list --state all --search "<keywords>""#, false),
        (
            r#"gh issue list --search "a" && gh issue list --search "b""#,
            false,
        ),
        ("gh issue create --title ...", false),
        ("gh issue comment <n> --body ...", false),
        ("gh api repos/{owner}/{repo}/pulls/{n}/reviews", false),
        ("cargo xtask gh-issue view <n> --comments", false),
        ("run `gh issue view 328` to read it", true),
        ("use cargo xtask gh-issue, not gh issue view 5", true),
        ("gh issue list", true),
        ("gh issue list --state all --json number", true),
        (
            r#"gh issue list --search "a"; gh issue list --state open"#,
            true,
        ),
        ("gh issue list --state open  # use --search instead", true),
        ("prefer `gh issue list` over the `--search` form", true),
        ("gh api repos/{owner}/{repo}/issues/{n}/comments", true),
        ("gh api graphql -f query='...'", true),
        ("gh search issues --repo owner/repo 'foo'", true),
    ];
    let wrong: Vec<_> = table
        .iter()
        .filter(|(line, forbidden)| is_forbidden(line) != *forbidden)
        .collect();
    assert!(wrong.is_empty(), "wrong verdicts: {wrong:?}");
}
