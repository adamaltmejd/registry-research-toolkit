"""The shipped SEKTORKOD and SEKTOR2000 books keep the code sets a binding relies on."""

from pathlib import Path

from reg_meta_build.classifications import load_valid_codes

BOOKS = Path(__file__).resolve().parent.parent / "input_data" / "classifications"


def test_sektorkod_cohort_is_not_a_sektor2000_subset() -> None:
    """Y-170 scope against the SHIPPED books: the observed 11-code SEKTORKOD cohort
    (00, 11-15, 21-25) conforms to SEKTORKOD and severs against SEKTOR2000, because
    INSEKT's Undersektor level reuses 11-14 and 21-25 with other meanings but has no
    15 and no 00 (an accepted binding alone cannot make 15 canonical).

    This pins committed book content, not builder behavior: how a binding conforms or
    extends is pinned by the build case
    `classification-bindings-conform-extend-or-stay-unbound-per-register`. Fails if
    sektorkod.csv changes its codes or 15 or 00 is added to sektor2000.csv.
    """
    sektorkod = set(load_valid_codes(BOOKS / "sektorkod.csv"))
    insekt = set(load_valid_codes(BOOKS / "sektor2000.csv"))
    assert sektorkod == {
        "00",
        "11",
        "12",
        "13",
        "14",
        "15",
        "21",
        "22",
        "23",
        "24",
        "25",
    }
    assert sektorkod - insekt == {"00", "15"}
