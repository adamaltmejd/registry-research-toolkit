"""The repository's own slug directory. Slug-file loading and its located refusals
are `cases/curation_toml/slugs-*`."""

from __future__ import annotations

from reg_meta_build.fqid_slugs import repo_slug_dir


def test_repo_layout_resolves():
    """`repo_slug_dir()` returns the live curation tree in a repo checkout. Fails
    if the default `--slug-dir` stops finding the committed register files."""
    result = repo_slug_dir()
    assert result is not None
    assert result.is_dir()
    assert (result / "registers" / "scb" / "lisa.toml").is_file()
