"""The shipped curation/relations.toml carries the moved curated edges."""

from __future__ import annotations

from pathlib import Path

from reg_meta.fqid import FqidKind
from reg_meta_build.relations import (
    load_relations,
)

# reg_meta_build/ package root (tests/ sits beside the curation/ dir).
_ROOT = Path(__file__).resolve().parent.parent


class TestMovedEdges:
    def test_repo_file_carries_the_moved_edges(self) -> None:
        rel = load_relations(_ROOT / "curation" / "relations.toml")
        # 11 variable replaced_by (the #375 LISA succession chain) + 21 #931
        # LISA SNI-coding succession edges + 2 variable replaced_by (the #400
        # SSYK 96 → SSYK 2012 J16 succession) + 3
        # classification replaced_by (the #579 sun1996 → niva/inriktning/grupp
        # split) + 3 #770/#768 ICD/KS disease-classification succession edges + 7
        # #814 iot disponibel-inkomst 2004-års-definition succession edges + 1
        # #875 KSju lgrp → NgGr1 representation-grain succession edge + 1
        # #846 RTB PNR → PersonNr representation-grain rename edge + 2 #846 FRIDA
        # firm-key variant-scoped gap-fill round-trip edges + 1 #376 LISA
        # register_variant succession edge + 3 #1122 LISA FÅMANS KU→AGI source
        # succession edges + 8 Y-88 curated LISA succession edges + the ULF
        # sampling-frame succession retained in e83a178d.
        assert len(rel.replaced_by) == 64
        [ulf] = [e for e in rel.replaced_by if e.predecessor.register == "ulf"]
        assert (ulf.predecessor_variant, ulf.successor_variant) == (
            "individer-16-74-ar-ulf",
            "individer-16-84-ar-ulf",
        )
        assert "1975-1977" in ulf.note
        # #508 (615) + #737 (232) - 6 mixed SUN peer edges - 2 stale course
        # relations - 13 retired aggregate identities = 826 same_as edges; all
        # variable-grain with a non-empty note; max connected component stays
        # ≤32 FQIDs.
        assert len(rel.same_as) == 826
        retired_aggregates = {
            "scb/rams/naringsgren-foretag",
            "scb/livsmedelsforsaljning/naringsgren-for-statistiken",
            "scb/slh/yrkesuppgift",
        }
        assert not retired_aggregates.intersection(
            endpoint
            for edge in rel.same_as
            for endpoint in (edge.a_fqid(), edge.b_fqid())
        )
        assert len(rel.derived_from) == 1
        assert (str(rel.derived_from[0].derived), str(rel.derived_from[0].source)) == (
            "class/ks87-p",
            "class/icd-9-ks87",
        )
        assert all(
            e.grain is FqidKind.VARIABLE_BINDING
            and e.a_variable
            and e.b_variable
            and e.note
            for e in rel.same_as
        )
        # Spot-check one moved edge of each type.
        assert ("scb/lisa/anninkf", "scb/lisa/anninkf04") in {
            (str(e.predecessor), str(e.successor)) for e in rel.replaced_by
        }
        faman_edges = {
            (str(e.predecessor), str(e.successor), e.effective_year)
            for e in rel.replaced_by
            if e.predecessor.register == "lisa"
            and (e.predecessor.variable or "").endswith("faman")
        }
        assert faman_edges == {
            ("scb/lisa/ku1faman", "scb/lisa/agi1faman", 2019),
            ("scb/lisa/ku2faman", "scb/lisa/agi2faman", 2019),
            ("scb/lisa/ku3faman", "scb/lisa/agi3faman", 2019),
        }
        # Y-88's curated LISA round, read off the succession-candidates worklist
        # as `(predecessor, successor, from_column, to_column, variant, year)` —
        # a tuple that also carries the GRAIN each edge landed at. Four
        # `cross_var_id` re-mints are VARIABLE-grain (one measure recut under a
        # new SCB VarId: no columns, hence no variant scope); four
        # never-co-delivered renames are REPRESENTATION-grain, scoped to the
        # individ variant, since LISA's other variants keep delivering the
        # predecessor column.
        y88_expected = {
            (
                "scb/lisa/antal-anstallda-ku",
                "scb/lisa/antal-anstallda",
                None,
                None,
                "",
                2022,
            ),
            ("scb/lisa/forvink-aktiv", "scb/lisa/forvink", None, None, "", 2022),
            (
                "scb/lisa/forvink-ers-aktiv",
                "scb/lisa/forvink-ers",
                None,
                None,
                "",
                2022,
            ),
            ("scb/lisa/yrkesbaserad-seg", "scb/lisa/eseg", None, None, "", 2016),
            (
                "scb/lisa/raks-huvinkkalla",
                "scb/lisa/huvudsaklig-inkomstkalla",
                "Raks_HuvInkKalla",
                "HuvInkKalla",
                "individer-15plus",
                2022,
            ),
            (
                "scb/lisa/sysselsattningsstatus-november",
                "scb/lisa/syssstat19",
                "SyssStat11",
                "SyssStat19",
                "individer-15plus",
                2019,
            ),
            (
                "scb/lisa/cfar-nummer",
                "scb/lisa/cfar-nummer-2",
                "CfarNr",
                "CfarNr_LISA",
                "individer-15plus",
                2016,
            ),
            (
                "scb/lisa/person-orgnr",
                "scb/lisa/person-orgnr-2",
                "PeOrgNr",
                "PeOrgNr_LISA",
                "individer-15plus",
                2016,
            ),
        }
        y88_predecessors = {pred for pred, *_ in y88_expected}
        y88 = [e for e in rel.replaced_by if str(e.predecessor) in y88_predecessors]
        assert {
            (
                str(e.predecessor),
                str(e.successor),
                e.predecessor_column,
                e.successor_column,
                e.variant,
                e.effective_year,
            )
            for e in y88
        } == y88_expected
        # `note` is optional on a replaced_by edge; a curation round owes one.
        assert all(e.note for e in y88)
        # The #579 1→many classification split: one predecessor, three successors
        # (all three SUN 2000 dimensions), parsed as `class/<slug>` (CLASSIFICATION
        # grain).
        sun_succ = {
            str(e.successor)
            for e in rel.replaced_by
            if str(e.predecessor) == "class/sun1996"
        }
        assert sun_succ == {
            "class/sun2000-niva",
            "class/sun2000-inriktning",
            "class/sun2000-grupp",
        }
        assert all(
            e.predecessor.kind is FqidKind.CLASSIFICATION
            for e in rel.replaced_by
            if str(e.predecessor) == "class/sun1996"
        )
        icd_edge = next(
            e
            for e in rel.replaced_by
            if (str(e.predecessor), str(e.successor))
            == ("class/icd-10-se", "class/icd-11-se")
        )
        assert icd_edge.effective_year == 2027
        # The #846 RTB representation-grain rename: a variable-grain edge carrying
        # BOTH `from_column`/`to_column`, parsed onto `predecessor_column` /
        # `successor_column` (the representation arm of `replaced_by`).
        rtb = next(
            e
            for e in rel.replaced_by
            if (str(e.predecessor), str(e.successor))
            == ("scb/rtb/pnr", "scb/rtb/personnr")
        )
        assert (rtb.predecessor_column, rtb.successor_column) == ("PNR", "PersonNr")
        # The #875 KSju grouped-SNI handoff is also representation-grain, but
        # crosses sibling variables inside one register rather than columns inside
        # one variable.
        ksju = next(
            e
            for e in rel.replaced_by
            if str(e.predecessor) == "scb/ksju/naringsgren-grupperad-2009"
        )
        assert str(ksju.successor) == "scb/ksju/naringsgren"
        assert (ksju.predecessor_column, ksju.successor_column) == ("lgrp", "NgGr1")
        # The #846 FRIDA firm-key gap-fill: a variant-SCOPED representation
        # round-trip (`borgnr` → `PERSORGNR` → `borgnr`) on the
        # punktskatter-for-energi variant, time-ordered 2014 < 2018. Verifies the
        # variant arm of `replaced_by` parses end-to-end from the repo file.
        frida = [
            e
            for e in rel.replaced_by
            if e.predecessor.register == "frida" and e.variant
        ]
        assert {(e.predecessor_column, e.successor_column) for e in frida} == {
            ("borgnr", "PERSORGNR"),
            ("PERSORGNR", "borgnr"),
        }
        assert all(e.variant == "punktskatter-for-energi" for e in frida)
        assert {e.effective_year for e in frida} == {2014, 2018}
