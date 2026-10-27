"""Shared provider TOML, slug pins, session base and run entry for the `extend_db` tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta_build.extend_db import (
    extend_db,
)
from reg_meta_build.id import mint

if TYPE_CHECKING:
    from pathlib import Path

STEWARD = "swecov"
BANK = "swedbank"

BASE_TOML = """\
[provider]
name = "Swedbank AB"
source_label = "swecov-inventory-test"

[[register]]
key = "transaktioner"
name = "Transaktioner"
purpose = "Bank delivery"
description = "Bankkontotransaktioner."

  [[register.variant]]
  key = "_default"
  name = "Transaktioner"
  description = "One table"

  [[register.variable]]
  key = "belopp"
  name = "Belopp"
  description = "Transaktionsbelopp i SEK."
  variants = ["_default"]

    [[register.variable.state]]
    column = "BELOPP"
    data_type = "float"

  [[register.variable]]
  key = "kontonr"
  name = "Kontonummer"
  is_identifier = true
  is_sensitive = true
  variants = ["_default"]

    [[register.variable.state]]
    column = "KONTO"
    data_type = "varchar"
    valid_from = "2018"

[[register]]
key = "kunder"
name = "Kunder"

  [[register.variant]]
  key = "privat"
  name = "Privatkunder"

  [[register.variant]]
  key = "foretag"
  name = "Företagskunder"

  [[register.variable]]
  key = "personnr"
  name = "Personnummer"
  is_identifier = true
  is_sensitive = true
  variants = ["privat"]

    [[register.variable.state]]
    column = "PersonNr"

  [[register.variable]]
  key = "personnr"
  name = "Personnummer"
  is_identifier = true
  is_sensitive = true
  variants = ["foretag"]

    [[register.variable.state]]
    column = "PersonNr"
"""


def provider_ids(provider: str = BANK) -> dict[str, int]:
    return {
        "provider": mint("provider", provider),
        "register": mint("register", provider, "transaktioner"),
        "variant": mint("variant", provider, "transaktioner", "_default"),
        "belopp": mint("variable", provider, "transaktioner", "belopp"),
        "kontonr": mint("variable", provider, "transaktioner", "kontonr"),
        "pooled_register": mint("register", provider, "kunder"),
        "privat": mint("variant", provider, "kunder", "privat"),
        "foretag": mint("variant", provider, "kunder", "foretag"),
        "personnr": mint("variable", provider, "kunder", "personnr"),
    }


def providers(
    tmp_path: Path,
    text: str = BASE_TOML,
    *,
    name: str = "providers",
    provider: str = BANK,
) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    (directory / f"{provider}.toml").write_text(text, encoding="utf-8")
    return directory


def write_slug_dir(path: Path, provider: str = BANK) -> None:
    ids = provider_ids(provider)
    path.mkdir()
    (path / ".snapshot.json").write_text("{}\n", encoding="utf-8")
    (path / f"{provider}.toml").write_text(
        f'[register."{ids["register"]}"]\nslug = "transaktioner"\n'
        f'[register_variant."{ids["register"]}.{ids["variant"]}"]\n'
        'slug = "transaktioner-default"\n'
        f'[register."{ids["pooled_register"]}"]\nslug = "kunder"\n'
        f'[register_variant."{ids["pooled_register"]}.{ids["privat"]}"]\n'
        'slug = "kunder-privat"\n'
        f'[register_variant."{ids["pooled_register"]}.{ids["foretag"]}"]\n'
        'slug = "kunder-foretag"\n',
        encoding="utf-8",
    )


# The session base: a published-shape catalog from the conformance reader fixtures,
# one that carries a classification book (`classifications.json` there: SUN2020,
# "Svensk utbildningsnomenklatur"). The cache stores it read-only; each worker
# overlays a private copy so no test can touch the cached bytes.
BASE_FIXTURE = "reader/classification-editions"
# A word of exactly one base book's name (SUN2020's, in the fixture's
# classifications.json).
BASE_BOOK_TERM = "utbildningsnomenklatur"


@pytest.fixture(scope="session", name="base_db")
def base_db_fixture(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_reader_artifact(
        tmp_path_factory.mktemp("base"),
        BASE_FIXTURE,
        "catalog",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )


def run(
    tmp_path: Path,
    base_db: Path,
    text: str = BASE_TOML,
    *,
    provider: str = BANK,
    relations: Path | None = None,
) -> tuple[dict, Path]:
    providers_dir = providers(tmp_path, text, name="out-providers", provider=provider)
    slug_dir = tmp_path / "out-slugs"
    write_slug_dir(slug_dir, provider)
    out = tmp_path / "out"
    result = extend_db(
        base_db=base_db,
        providers_dir=providers_dir,
        db_dir=out,
        steward=STEWARD,
        slug_dir=slug_dir,
        diagnostic=True,
        relations=relations,
    )
    return result, out / "reg_meta.db"
