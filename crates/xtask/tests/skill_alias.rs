//! Claude Code (`.claude/skills/`) and Codex (`.agents/skills/`) serve the same skills.
//!
//! Each skill is a real directory in one catalog and a relative symlink to it in the
//! other, so the two cannot drift and the link survives any clone or worktree. The
//! canonical directory also holds the Codex `agents/openai.yaml`. Fails on a skill
//! present in only one catalog, a copied directory, or an absolute or repointed link.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const CATALOGS: [&str; 2] = [".claude", ".agents"];

#[test]
fn skill_is_real_in_one_catalog_and_aliased_in_the_other() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let skills: BTreeSet<String> = CATALOGS
        .iter()
        .flat_map(|c| fs::read_dir(root.join(c).join("skills")).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        !skills.is_empty(),
        "no skills under .claude/skills or .agents/skills"
    );

    let mut errors = Vec::new();
    for skill in &skills {
        let paths = CATALOGS.map(|c| root.join(c).join("skills").join(skill));
        let links: Vec<usize> = (0..2).filter(|&i| paths[i].is_symlink()).collect();
        let [alias_at] = links[..] else {
            errors.push(format!("{skill}: exactly one catalog must hold a symlink"));
            continue;
        };
        let (alias, canonical) = (&paths[alias_at], CATALOGS[1 - alias_at]);
        let target = PathBuf::from(format!("../../{canonical}/skills/{skill}"));
        if fs::read_link(alias).ok() != Some(target.clone()) {
            errors.push(format!(
                "{skill}: {} must link to {}",
                alias.display(),
                target.display()
            ));
        }
        let front = fs::read_to_string(alias.join("SKILL.md")).unwrap_or_default();
        if !front.starts_with(&format!("---\nname: {skill}\n")) {
            errors.push(format!("{skill}: SKILL.md must open with `name: {skill}`"));
        }
        if !paths[1 - alias_at].join("agents/openai.yaml").is_file() {
            errors.push(format!("{skill}: no agents/openai.yaml in {canonical}"));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
