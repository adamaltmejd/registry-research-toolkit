#!/usr/bin/env bash
# Verify version consistency across the monorepo.
#
# 1. For each package, pyproject.toml version must match __init__.py __version__.
# 2. If GITHUB_REF_NAME is a release tag (<pkg>/v<ver>), the tagged version
#    must match the package version and the tag must use <pkg>/vX.Y.Z syntax.
set -euo pipefail

errors=0
err() { echo "ERROR: $*" >&2; errors=$((errors + 1)); }

# --- Package definitions: dir  pyproject_path  init_path ---
packages=(
    "regmeta regmeta/pyproject.toml regmeta/src/regmeta/__init__.py"
    "mock_data_wizard mock_data_wizard/pyproject.toml mock_data_wizard/src/mock_data_wizard/__init__.py"
)

extract_pyproject_version() {
    # Matches: version = "X.Y.Z" (with optional pre-release suffixes)
    sed -n 's/^version *= *"\([^"]*\)"/\1/p' "$1" | head -1
}

extract_init_version() {
    sed -n 's/^__version__ *= *"\([^"]*\)"/\1/p' "$1" | head -1
}

# 1. Check version consistency within each package
for entry in "${packages[@]}"; do
    read -r pkg_name pyproject init <<<"$entry"
    v_pyproject=$(extract_pyproject_version "$pyproject")
    v_init=$(extract_init_version "$init")

    if [[ -z "$v_pyproject" ]]; then
        err "$pkg_name: could not read version from $pyproject"
        continue
    fi
    if [[ -z "$v_init" ]]; then
        err "$pkg_name: could not read __version__ from $init"
        continue
    fi
    if [[ "$v_pyproject" != "$v_init" ]]; then
        err "$pkg_name: pyproject.toml ($v_pyproject) != __init__.py ($v_init)"
    else
        echo "OK: $pkg_name v$v_pyproject"
    fi
done

# 2. On release: validate tag format and version match
tag="${GITHUB_REF_NAME:-}"
if [[ -n "$tag" && "$tag" == *"/v"* ]]; then
    # Expected format: <package>/vX.Y.Z (with optional pre-release)
    pkg_from_tag="${tag%%/v*}"
    ver_from_tag="${tag#*/v}"

    if ! [[ "$ver_from_tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        err "Tag '$tag' does not match <package>/vX.Y.Z format"
    fi

    # Find the matching package
    matched=false
    for entry in "${packages[@]}"; do
        read -r pkg_name pyproject _ <<<"$entry"
        # Allow tag prefix to match with hyphens or underscores
        tag_slug="${pkg_from_tag//-/_}"
        if [[ "$tag_slug" == "$pkg_name" ]]; then
            v_pyproject=$(extract_pyproject_version "$pyproject")
            if [[ "$ver_from_tag" != "$v_pyproject" ]]; then
                err "Tag version ($ver_from_tag) != $pkg_name pyproject.toml ($v_pyproject)"
            else
                echo "OK: tag $tag matches $pkg_name v$v_pyproject"
            fi
            matched=true
            break
        fi
    done

    if [[ "$matched" == false ]]; then
        err "Tag '$tag': unknown package '$pkg_from_tag'"
    fi
fi

if [[ "$errors" -gt 0 ]]; then
    echo "FAILED: $errors error(s)" >&2
    exit 1
fi
echo "All version checks passed."
