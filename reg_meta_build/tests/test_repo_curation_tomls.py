"""The REAL repo curation TOMLs must always parse.

Synthetic fixtures use explicit catalog rows and do not read repository
curation. Load the actual maintainer files directly so malformed entries are
caught without a real-data build.

Scope: load-time validation only (TOML shape, canonical ints, folded-column
group rules). The build-time half (named columns exist for the var) needs the
real corpus and stays maintainer-build-only by design.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest
from _curation_fixtures import write_lisa_errata
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidKind
from reg_meta_build._curation import repo_curation_path
from reg_meta_build.concept_groups import (
    load_classification_groups,
    load_code_label_pairs,
    load_concept_groups,
    load_worklist_concept_groups,
)
from reg_meta_build.curation_tree import CurationTree, load_curation_tree
from reg_meta_build.doc_db import (
    _require_doc_source_str,
    load_doc_sources,
    load_related_documents,
)
from reg_meta_build.relations import _SAME_AS_MAX_COMPONENT, load_relations
from reg_meta_build.scb_errata import load_scb_errata

from reg_meta_build.fqid_slugs import (
    declared_column_ownership,
    load_lineage_config,
    load_slug_dir,
    repo_slug_dir,
)

# reg_meta_build/ package root (tests/ sits beside the curation/ directory).
_ROOT = Path(__file__).resolve().parent.parent
_CURATION = _ROOT / "curation"


@pytest.fixture(scope="module")
def repo_tree() -> CurationTree:
    return load_curation_tree(_CURATION)


# Y-304: the 75 reviewed IT-användning (258) recurrent-question identity
# families. Each value is the native-question split owner leaf and the complete
# set of delivered literals that leaf owns in that native family.
_IT_RECURRENT_QUESTION_OWNERS: dict[str, tuple[str, tuple[str, ...]]] = {
    "258.15918": ("datc1", ("B1", "B1a", "C1", "DatC1")),
    "258.15958": ("anvant-internet-sokvaror", ("DatC5B", "DatC5D", "IUIF")),
    "258.15959": (
        "anvant-internet-resor",
        ("B6o", "C4l", "DatC5C", "DatC5E", "DatC5M", "IUHOLS"),
    ),
    "258.15962": ("anvant-nedladdning-programvara", ("DatC5D", "DatC5F", "DatC5H")),
    "258.15963": (
        "anvant-internet-nyheter",
        (
            "B3f",
            "B4E",
            "B4e",
            "B6d",
            "C4e",
            "DatC5B",
            "DatC5E",
            "DatC5I",
            "DatC5J",
            "IUNW",
            "IUNW1",
        ),
    ),
    "258.15966": (
        "anvant-internet-halsoinformation",
        ("B3m", "B3o", "B4f", "B6m", "DatC5C", "DatC5G", "DatC5K", "DatC5L"),
    ),
    "258.15968": (
        "utfort-bankarenden-pa-natet",
        (
            "B3r",
            "B3t",
            "B4N",
            "B4l",
            "B5m",
            "C4n",
            "DatC5H",
            "DatC5M",
            "DatC5P",
            "IUBK",
        ),
    ),
    "258.15969": (
        "anvant-internet-salja-varor",
        ("B3q", "B3s", "B4k", "B5l", "B6p", "C4m", "DatC5I", "DatC5N", "IUSELL"),
    ),
    "258.15998": (
        "nar-handlade-du-senast-via-internet",
        ("D1", "DatD1", "DatE1", "E1", "IBUY"),
    ),
    "258.15999": ("kopt-lakemedel", ("BMED", "D2C", "D2c", "DatD2C", "DatE2C", "E2c")),
    "258.16000": (
        "kopt-mat-eller-specerivaror",
        ("BFOOD", "D2A", "D2a", "DatD2A", "DatE2A", "E2a"),
    ),
    "258.16001": (
        "kopt-bocker-tidningar-ink-e-bocker",
        ("BBOOKNL", "D2L", "D2l", "DatD2D", "DatD2E", "DatE2D", "DatE2E", "E2l"),
    ),
    "258.16002": (
        "kopt-klader-eller-sportartiklar",
        (
            "BCLOT",
            "D2D",
            "D2d",
            "DatD2E",
            "DatD2F",
            "DatD2G",
            "DatE2E",
            "DatE2G",
            "E2d",
        ),
    ),
    "258.16004": (
        "kopt-datorer-eller-datautrustning",
        (
            "BHARD",
            "D2E",
            "D2e",
            "DatD2G",
            "DatD2I",
            "DatD2J",
            "DatE2G",
            "DatE2J",
            "E2e",
        ),
    ),
    "258.16005": (
        "kopt-hemelektronik-eller-kameror",
        (
            "BEEQU",
            "D2F",
            "D2f",
            "DatD2H",
            "DatD2J",
            "DatD2K",
            "DatE2H",
            "DatE2K",
            "E2f",
        ),
    ),
    "258.16008": (
        "kopt-biljetter-till-evenemang",
        (
            "BTICK",
            "D2J",
            "D2j",
            "DatD2K",
            "DatD2O",
            "DatD2P",
            "DatE2K",
            "DatE2P",
            "E2j",
        ),
    ),
    "258.16010": (
        "kopt-andra-varor-eller-tjanster",
        (
            "BOTHTH",
            "D2O",
            "D2o",
            "D2p",
            "DatD2M",
            "DatD2P",
            "DatD2Q",
            "DatE2M",
            "DatE2Q",
            "E2o",
        ),
    ),
    "258.16011": ("kopt-nedladdad-film-musik", ("BFILMO", "DatD3A", "DatE3A")),
    "258.16088": (
        "antal-personer-under-16-ar-i-hushallet",
        (
            "DatE11",
            "DatF11",
            "DatF9",
            "DatG13",
            "DatG14",
            "H13",
            "H13antal",
            "H15",
            "HH_CHILD",
        ),
    ),
    "258.17510": (
        "kopt-hushallsvaror",
        ("BFURN", "D2B", "D2b", "DatD2B", "DatE2B", "E2b"),
    ),
    "258.18036": (
        "myndighetskontakt-lamnat-uppgifter",
        ("C1C", "C1c", "D1c", "DatC6C", "DatD1C", "GOV12RT", "IGOV12RT"),
    ),
    "258.18087": (
        "kopt-varor-tjanster-fran-sverige",
        ("BFDOM", "D3a", "D4A", "DatD4A", "DatE4A", "E4a"),
    ),
    "258.18088": (
        "kopt-varor-tjanster-fran-eu",
        ("BFEU", "D3b", "D4B", "DatD4B", "DatE4B", "E4b"),
    ),
    "258.18089": (
        "kopt-varor-tjanster-utanfor-eu",
        ("BFWRLD", "D3c", "D4C", "DatD4C", "DatE4C", "E4c"),
    ),
    "258.18090": (
        "kopt-varor-tjanster-okant-land",
        ("BFUNK", "D3d", "D4D", "DatD4D", "DatE4D", "E4d"),
    ),
    "258.21112": (
        "internet-snabbmeddelanden",
        ("B3d", "B4D", "B4d", "DatC5C", "IUFORIM"),
    ),
    "258.21120": (
        "kopt-researrangemang-fardbiljetter",
        ("BOTA", "D2I", "D2i", "DatD2N", "DatD2O", "DatE2O", "E2i"),
    ),
    "258.21175": (
        "kopt-filmer-musik-internet",
        ("BFILM", "D2K", "D2k", "DatD2D", "DatE2D", "E2k"),
    ),
    "258.21176": (
        "kopt-video-dataspel-internet",
        ("BGSOFT", "D2N", "D2n", "DatD2G", "DatD2H", "DatE2H", "E2n"),
    ),
    "258.23396": (
        "anvant-internet-sokvaror-tjanster",
        ("B3e", "B4G", "B4g", "B5d", "B6e", "C4g", "DatC5D", "DatC5E", "IUIF"),
    ),
    "258.23397": (
        "kopt-el-bestallt-datorbaserade-laromedel",
        ("BLRN", "D2M", "D2m", "DatD2F", "DatE2F", "E2m"),
    ),
    "258.23477": ("yrke", ("H8", "Sy2")),
    "258.26891": ("andel-omsattning-fran-edi", ("AXSVALPCT", "AXVALPCT")),
    "258.27978": (
        "delta-pa-sociala-natverkssajter",
        ("B3c", "B4C", "B4c", "B5c", "B6c", "C4c", "DatC5A"),
    ),
    "258.27994": (
        "flyttat-filer-dator-annan-enhet",
        ("CXFER", "DatF3G", "E1A", "E1a", "F1a"),
    ),
    "258.28013": (
        "datd2a-2",
        ("C2A", "C2a", "D2a", "DatD2A", "GOV12RTX_NAP", "IGOV12RTX_NAP"),
    ),
    "258.28014": (
        "datd2b-2",
        ("C2B", "C2b", "D2b", "DatD2B", "GOV12RTX_SNA", "IGOV12RTX_SNA"),
    ),
    "258.28016": (
        "datd2d",
        ("C2C", "C2c", "D2c", "DatD2D", "GOV12RTX_SKL", "IGOV12RTX_SKL"),
    ),
    "258.28017": (
        "datd2e",
        ("C2D", "C2d", "D2d", "DatD2E", "GOV12RTX_SEC", "IGOV12RTX_SEC"),
    ),
    "258.28861": (
        "anvant-internet-skicka-ta-emot-epost",
        ("B3a", "B4A", "B4a", "B5a", "C4a", "IUEM"),
    ),
    "258.29967": (
        "andel-webb-forsaljning-konsumenter",
        ("AWSVALCPCT", "AWSVAL_B2CPCT", "E_AWSVAL_B2C"),
    ),
    "258.29988": (
        "andel-webb-forsaljning-foretag-offentlig",
        ("AWSVALBGPCT", "AWSVAL_B2BGPCT", "E_AWSVAL_B2BG"),
    ),
    "258.30112": ("igov12rtx-sign", ("C2e", "D2e", "IGOV12RTX_SIGN")),
    "258.30113": (
        "myndighet-nagon-annan-skickade-blankett",
        ("C2E", "C2e", "C2f", "C2g", "D2f", "IGOV12RTX_DEL"),
    ),
    "258.35520": ("kop-belopp-internet", ("D5", "D6", "E5", "E7")),
    "258.35530": ("anvant-ordbehandling", ("E2B", "E2a", "E2b", "F2a", "F2b")),
    "258.35551": ("studiematerial-natet", ("B4b", "B5b", "B7B", "B8b", "C6b")),
    "258.35554": ("kommunicerat-larare", ("B4c", "B5c", "B7C", "B8c", "C6c")),
    "258.35557": ("andra-utbildn-internet", ("B8d", "C6d")),
    "258.35562": ("lagringsutrymme-internet", ("B4", "B5_1", "B6", "B7", "C5", "CC")),
    "258.37000": (
        "telefon-videosamtal",
        ("B3b", "B4B", "B4b", "B5b", "B6b", "C4b", "IUPH1"),
    ),
    "258.37031": ("andrat-installningar", ("E1C", "E1c", "F1c")),
    "258.37032": ("skapat-presentationer", ("E2C", "E2c", "F2c")),
    "258.37033": ("anvant-kalkylprogram", ("E2D", "E2c", "E2d", "F2c", "F2d")),
    "258.39489": ("tagit-nagon-kurs-pa-internet", ("B4a", "B5a", "B7A", "B8a")),
    "258.41678": (
        "ej-utbildningsaktiviteter",
        ("B4c2", "B5b2", "B5c2", "B5d2", "B8c2"),
    ),
    "258.41723": ("kopt-sportartiklar", ("D2b", "E2b")),
    "258.41724": ("kopt-klader-skor-accessoarer", ("D2a", "E2a")),
    "258.41725": ("kopt-barnleksaker-eller-barnartiklar", ("D2c", "E2c")),
    "258.41726": ("kopt-mobler-inredning-tradgard", ("D2d", "E2d")),
    "258.41727": ("kopt-cd-eller-vinylskivor", ("D2e", "E2e")),
    "258.41729": ("kopt-tryckta-bocker-tidningar", ("D2g", "E2g")),
    "258.41730": ("kopt-datorer-surfplattor-mobiler", ("D2h", "E2h")),
    "258.41731": ("kopt-hushallsapparater-eller-vitvaror", ("D2i", "E2i")),
    "258.41732": ("kopt-lakemedel-eller-kosttillskott", ("D2j", "E2j")),
    "258.41733": ("kopt-leveranser-fran-restauranger", ("D2k", "E2k")),
    "258.41734": ("kopt-mat-eller-dryck", ("D2l", "E2l")),
    "258.41735": ("kopt-kosmetik-skonhet", ("D2m", "E2m")),
    "258.41736": ("kopt-rengoring-hygienprodukter", ("D2n", "E2n")),
    "258.41737": ("kopt-cyklar-mopeder-bilar", ("D2o", "E2o")),
    "258.41738": ("kopt-andra-fysiska-varor", ("D2p", "E2p")),
    "258.41740": ("betalat-musik-streaming", ("D5a", "E5a")),
    "258.41741": ("betalat-filmer-serier-streaming", ("D5b", "E5b")),
    "258.44638": ("anvant-internet-asikter-politik", ("B3g", "B4h")),
    "258.44639": ("aktivt-delta-politisk-diskussion", ("B3h", "B4i")),
}

# Y-310: the 58 IoT native families whose individual (X), ordinary-household (XHB)
# and modelled shared-residence (XVXHB) calculation bases are separated. Each
# value is (base literal, existing source-name stem, physical originals).
_IOT_Y310_CALCULATION_BASES: dict[str, tuple[str, str, int]] = {
    "25.5655": ("SBEGR", "betald-begravningsavgift", 138),
    "25.21514": ("CTRAN04", "negativa-transfereringar-2004", 123),
    "25.1413": ("ISMBID", "studiemedel-studiebidrag", 125),
    "25.5610": ("SKYRK", "kyrkoavgift-inklusive-begravningsavgift", 128),
    "25.39337": ("SREDSA", "skattereduktion-sjuk-aktivitet", 50),
    "25.4866": ("CSTUDT", "studiestod", 126),
    "25.22277": ("COVRN04", "ovriga-negativa-transfereringar-2004", 123),
    "25.714": ("SREDSJO", "medgiven-skattereduktion-sjoinkomst", 144),
    "25.4387": ("PPENSSP", "pension-och-livranta-skattepliktig", 126),
    "25.36641": ("CTRAPSFOV", "ovriga-transfereringar-skattefria", 67),
    "25.712": ("SREDPEN", "skattereduktion-allman-pensionsavgift", 138),
    "25.22247": ("PLIVRTA", "livranta-harledd", 119),
    "25.21824": ("SREDARB", "skattereduktion-for-arbetsinkomst", 117),
    "25.36637": ("CBRUTTO15", "bruttoinkomst-definition-2015", 67),
    "25.45919": ("SREDARBT", "tillfallig-skattereduktion-arbete", 16),
    "25.38226": ("ISA", "sjuk-aktivitetsersattning-skattefri", 56),
    "25.22242": ("ISHBID", "studiehjalp-bidrag", 125),
    "25.1484": ("PALLP", "allman-pension-inklusive-delpension", 119),
    "25.1295": ("CTRAP", "summa-positiva-transfereringar", 129),
    "25.1486": ("PBARN", "barnpension-skattepliktig-och-skattefri", 119),
    "25.25618": ("KUTHYRM", "uthyrning-privatbostad-inkl-avdrag", 98),
    "25.22249": ("PAENKP", "ankepension-skapad", 119),
    "25.21711": ("KFBRUT", "kapitalforlust-brutto", 113),
    "25.1615": ("TPENBID", "pensioner-bidrag-exkl-arbetsmarkn", 156),
    "25.1390": ("IALDF", "aldreforsorjningsstod", 129),
    "25.1483": ("PALDP", "alderspension", 119),
    "25.1078": ("CKAP", "rantor-utdelningar-exkl-rantebidrag", 129),
    "25.21701": ("ISMLAN", "studiemedel-lan-m-m", 125),
    "25.26974": ("ISHIN", "inackorderingsbidrag", 83),
    "25.30826": ("IKBOBDR", "bostadstillagg-sarskilt-pensionarer", 90),
    "25.44792": ("SSLUTU", "skatt-utlandsk", 23),
    "25.30674": ("CDIVSF", "skattefri-pension-pensionstillagg", 54),
    "25.44791": ("TPENSAU", "pensioner-utlandskt-beskattade", 23),
    "25.88": ("TFORP", "foraldrapenning-skattepliktig", 90),
    "25.1630": ("TARBST", "totalt-arbetsmarknadsstod", 159),
    "25.4523": ("COVRN", "ovriga-negativa-transfereringar", 54),
    "25.21878": ("SDEBUTL", "debiterad-utlandsk-skatt", 119),
    "25.1493": ("PEFLEVB", "efterlevandepension-till-barn", 129),
    "25.44462": ("ISTIP", "stipendium-skattefritt", 38),
    "25.4871": ("PRESTA", "presta", 126),
    "25.4386": ("PPENSSF", "pension-och-livranta-skattefri", 126),
    "25.21861": ("POMSTP", "omstallningspension-skapad", 119),
    "25.8274": ("SKLFVI", "kommunal-inkomstskatt-forvarvsinkomst", 123),
    "25.21729": ("KVBRUT", "kapitalvinst-brutto", 113),
    "25.45923": ("PIPT", "inkomstpensionstillagg", 32),
    "25.732": ("SPENAVG", "allman-pensionsavgift", 153),
    "25.630": ("AAPENS", "underlag-allman-pensionsavgift", 140),
    "25.4874": ("PRESTPR", "prestpr", 126),
    "25.30640": ("UUHBID", "givet-underhallsbidrag", 54),
    "25.1508": ("PGARP", "garantipension", 119),
    "25.1632": ("TARBFOR", "arbetsmarknadsforsakringar", 156),
    "25.22241": ("ISHEXT", "extra-tillagg-studiemedel-bidrag", 125),
    "25.30639": ("IUHBID", "mottaget-underhallsbidrag", 90),
    "25.101": ("TSA", "sjuk-och-aktivitetsersattning", 129),
    "25.42415": ("IMILI", "skattefri-ersattning-forsvar", 44),
    "25.2504": ("PPRIV", "frivilliga-pensioner", 119),
    "25.1485": ("PAVTAL", "avtalspension", 119),
    "25.23618": ("PFOMSTP", "forlangd-omstallningspension-summerad", 119),
}

# Y-315: eight IoT household-only native families whose ordinary-household (XHB)
# and modelled shared-residence (XVXHB) calculation bases are separated, both in
# variant 25.1153. Each value is (HB literal, VXHB literal, existing
# household-native stem); the ordinary owner keeps the stem and the VX owner adds
# the explicit -vaxelvis-boende suffix. No individual branch exists.
_IOT_Y315_HOUSEHOLD_PAIRS: dict[str, tuple[str, str, str]] = {
    "25.3188": ("BHTYPHB", "BHTYPVXHB", "hushallstyp"),
    "25.4381": ("IBOSTBHB", "IBOSTBVXHB", "bostadsbidrag"),
    "25.4499": ("IFAMHB", "IFAMVXHB", "familjestod-totalt"),
    "25.4783": (
        "CTRAPSFHB",
        "CTRAPSFVXHB",
        "skattefria-transfereringar-hushall",
    ),
    "25.4784": (
        "CTRAPSPHB",
        "CTRAPSPVXHB",
        "skattepliktiga-transfereringar-hushall",
    ),
    "25.22335": ("BKE04HB", "BKE04VXHB", "konsumtionsenhetsvikt-2004"),
    "25.22349": ("ISOCBHB", "ISOCBVXHB", "ekonomiskt-bistand-socialbidrag"),
    "25.30835": (
        "BINKSTATHB",
        "BINKSTATVXHB",
        "hushall-inkluderas-inkomststatistiken",
    ),
}

# Y-316: nine IoT employer-reporting native families whose pre-2019 annual
# control-statement (KU) basis and post-2019 monthly AGI + remaining annual KU
# basis are separated, all in variant 25.763. Each value is (old KU literal,
# new literal, existing native stem); the old owner keeps the stem and the new
# owner adds the explicit -agi-ku suffix.
_IOT_Y316_SOURCE_BASIS_PAIRS: dict[str, tuple[str, str, str]] = {
    "25.1195": ("TKULONO", "TLONO", "skattepliktiga-formaner-ej-lon"),
    "25.6093": ("KKUHYR", "KHYR", "hyresersattning"),
    "25.19855": ("BKUAORG", "BAORG", "ordinarie-organisationsnummer"),
    "25.21209": ("AKUHUV", "AHUV", "underlag-skattereduktion-rut-forman"),
    "25.21829": ("TAKUAV", "TAAV", "avdrag"),
    "25.21830": ("TKUBILF", "TBILF", "bilforman-utom-drivmedel"),
    "25.21831": ("TKUDRIV", "TDRIV", "drivmedel-vid-bilforman"),
    "25.21879": (
        "TKUOVE",
        "TOVE",
        "skattepliktiga-ersattningar-ej-sociala",
    ),
    "25.24169": ("AKUROT", "AROT", "underlag-skattereduktion-rot-forman"),
}


# Y-311: the 74 reviewed Innovation i foretag (257) native-question identity
# families. Each value is the native-question split owner leaf and the complete
# set of delivered literals that leaf owns in that native family.
_INNOVATION_RECURRENT_QUESTION_OWNERS: dict[str, tuple[str, tuple[str, ...]]] = {
    "257.29": ("fenr", ("FENr", "SCBID")),
    "257.690": ("antal-anstallda", ("EMP", "EMP2022")),
    "257.2468": ("outlier", ("OUTLFL", "Outlier")),
    "257.4032": ("antal-anstallda-foretaget", ("A4", "Q4FDB")),
    "257.4045": ("koncernmarkering", ("A1", "GP", "Q1")),
    "257.4256": ("nettoomsattning", ("TUR", "TUR2022")),
    "257.7705": (
        "utgifter-for-egen-fou",
        ("E11a", "EXP_INNO_RND_IH", "Q11a", "RRDINX"),
    ),
    "257.7708": (
        "utgifter-for-utlagd-fou",
        ("E11b", "EXP_INNO_RND_CONTR_OUT", "Q11b", "RRDEXX"),
    ),
    "257.15555": ("geografisk-marknad-nationellt", ("A2b", "MARNAT", "Q2b")),
    "257.15556": ("geografisk-marknad-eu-efta", ("A2c", "MAREUR", "Q2c")),
    "257.15559": ("geografisk-marknad-ovriga-lander", ("A2d", "MAROTH", "Q2d")),
    "257.15562": (
        "produktinnovation-ny-for-marknad",
        ("B6a", "INNO_PRD_NEW_MKT", "NEWMKT", "Q6a"),
    ),
    "257.15563": (
        "produktinnovation-ny-for-foretaget",
        ("B6b", "INNO_PRD_NEW_ENT", "NEWFRM", "Q6b"),
    ),
    "257.15568": (
        "omsattningsandel-inno-ny-for-marknad",
        ("B7a", "Q7a", "TURNMAR", "TUR_PRD_NEW_MKT"),
    ),
    "257.15569": (
        "omsattningsandel-inno-ny-for-foretaget",
        ("B7b", "Q7b", "TURNIN", "TUR_PRD_NEW_ENT"),
    ),
    "257.15570": (
        "omsattningsandel-oforandrade-varor",
        ("B7c", "Q7c", "TURNTOT", "TUR_PRD_NINN"),
    ),
    "257.15571": (
        "introduktion-av-produktionsmetoder",
        ("C8a", "INNO_PCS_PRD", "INPSPD", "Q8a"),
    ),
    "257.15578": (
        "introduktion-av-leveransmetoder",
        ("C8b", "INNO_PCS_LOG", "INPSLG", "Q8b"),
    ),
    "257.15579": (
        "introduktion-av-stodverksamheter",
        ("C8c", "INNO_PCS_ACCT", "INPSSU", "Q8c"),
    ),
    "257.15580": ("utvecklare-av-processinnovationer", ("C81", "Q81")),
    "257.15582": ("introduktion-av-varor", ("B5a", "INNO_PRD_GD", "INPDGD", "Q5a")),
    "257.15583": (
        "introduktion-av-tjanster",
        ("B5b", "INNO_PRD_SERV", "INPDSV", "Q5b"),
    ),
    "257.15584": ("utvecklare-av-produktinnovationer", ("B51", "Q51")),
    "257.15593": ("huvudkontoret-belaget", ("A1a", "Q1a")),
    "257.15600": ("innovationssamarbete", ("CO", "F12", "Q12")),
    "257.15612": ("utgifter-forvarv-maskiner-programvara", ("E11c", "Q11c")),
    "257.15613": ("utgifter-forvarv-existerande-kunskap", ("E11d", "Q11d", "ROEKX")),
    "257.15614": ("inna-egen-fou", ("E10a", "EGFOU", "INNA_IH_RND", "Q10a")),
    "257.15616": ("inna-utlagd-fou", ("E10b", "INNA_RND_CONTR_OUT", "Q10b", "RRDEX")),
    "257.15617": ("inna-forvarv-maskiner", ("E10c", "Q10c", "RMAC")),
    "257.15618": ("inna-forvarv-existerande-kunskap", ("E10d", "Q10d", "ROEK")),
    "257.15619": ("inna-utbildning", ("E10e", "Q10e", "RTR")),
    "257.15620": ("inna-marknadsintroduktion", ("E10f", "Q10f", "RMAR")),
    "257.15621": ("inna-andra-forberedelser", ("E10g", "Q10g")),
    "257.15642": ("geografisk-marknad-regional-lokal", ("A2a", "MARLOC", "Q2a")),
    "257.15643": ("totala-innovationsutgifter", ("E11e", "Q11e", "RALLX")),
    "257.15665": ("mest-vardefull-samarbetspartner", ("F14", "PMOS", "Q14")),
    "257.17402": ("koncerntillhorighet", ("ENTGRP_PART", "Koncern")),
    "257.20900": ("avbruten-innovationsaktivitet", ("D9a", "INABA", "INNA_ABDN")),
    "257.20901": ("pagaende-innovationsaktivitet", ("D9b", "INNA_ONGO", "INONG")),
    "257.20940": ("introduktion-av-nya-affarsmetoder", ("H16a", "ORGBUP")),
    "257.20941": (
        "introduktion-metoder-ansvar-beslut",
        ("H16b", "INNO_PCS_WR_DEC_HRM", "ORGWKP"),
    ),
    "257.20942": (
        "introduktion-metoder-externa-relationer",
        ("H16c", "INNO_PCS_OPROC_EXTREL", "ORGEXR"),
    ),
    "257.20948": ("introduktion-forandringar-utformning", ("I18a", "MKTDGP")),
    "257.20949": (
        "introduktion-metoder-marknadsforing",
        ("I18b", "INNO_PCS_SLS_SERV", "MKTPDP"),
    ),
    "257.20950": ("introduktion-metoder-produktplacering", ("I18c", "MKTPDL")),
    "257.20951": ("introduktion-metoder-prissattning", ("I18d", "MKTPRI")),
    "257.20955": (
        "miljoinnovation-minskad-materialatgang",
        ("ECO_MAT", "ECO_MAT_SG", "J20a"),
    ),
    "257.20957": ("j20c", ("ECO_ENO", "ECO_ENO_SG", "J20c")),
    "257.20958": (
        "miljoinnovation-materialersattning",
        ("ECO_SUB", "ECO_SUB_SG", "J20d"),
    ),
    "257.20959": (
        "miljoinnovation-fororeningar-foretaget",
        ("ECO_POL", "ECO_POL_SG", "J20e"),
    ),
    "257.20960": (
        "miljoinnovation-atervinning-avfall",
        ("ECO_REC", "ECO_REC_SG", "J20f"),
    ),
    "257.20962": (
        "miljoinno-fororeningar-slutkonsumtion",
        ("ECO_POS", "ECO_POS_SG", "J20h"),
    ),
    "257.29913": (
        "utgifter-ovrig-innovationsverksamhet",
        ("EXP_INNO_INN_XRND", "ROTRX"),
    ),
    "257.29914": (
        "stod-for-innovationsverksamhet-kommun",
        ("FUND_AUT_LOC_REG_RNDINN", "FUNLOC"),
    ),
    "257.32713": ("ipr-patent", ("IPR_OUT_PAT", "PROPAT")),
    "257.36983": (
        "stod-for-innovationsverksamhet-staten",
        ("FUND_GOV_CTL_RNDINN", "FUNGMT"),
    ),
    "257.36984": ("stod-for-innovationsverksamhet-eu", ("FUND_EU_OTH_RNDINN", "FUNEU")),
    "257.37076": ("ipr-monsterskydd", ("IPR_OUT_IDESG", "PRODSG")),
    "257.37077": ("ipr-varumarke", ("IPR_OUT_TRDM", "PROTM")),
    "257.37078": ("ipr-salt", ("IPR_OUT_SELL", "PROLEX")),
    "257.37079": ("ipr-kopt", ("IPR_IN_ENT", "PROLIN")),
    "257.39094": (
        "stod-innovationsaktiviteter-eu-h2020",
        ("FUND_EU_HP2020_RNDINN", "FUND_EU_HP_RNDINN"),
    ),
    "257.39096": ("stod-fran-eu-h2020", ("FUND_EU_HP", "FUND_EU_HP2020")),
    "257.39102": ("samarbete-inom-ovriga-aktiviteter", ("COOP_NO", "COOP_OTH")),
    "257.39126": (
        "coop-pub-clcu-neu-nefta",
        ("COOP_PUB_CLCU_NEU_EFTA", "COOP_PUB_CLCU_NEU_NEFTA"),
    ),
    "257.46481": ("miljoinno-livscykel", ("ECO_EXT", "ECO_EXT_SG")),
    "257.46482": ("miljoinno-atervinning", ("ECO_REA", "ECO_REA_SG")),
    "257.46483": ("eco-enu-sg", ("ECO_ENU", "ECO_ENU_SG")),
    "257.46484": ("miljoinno-mangfald-kons", ("ECO_BIU", "ECO_BIU_SG")),
    "257.46485": ("miljoinno-mangfald-intern", ("ECO_BIO", "ECO_BIO_SG")),
    "257.46492": ("miljoinno-fossila", ("ECO_REP", "ECO_REP_SG")),
    "257.46650": ("turpop", ("LillaNs_TUR", "TURPOP")),
    "257.46654": ("turresp", ("StoraN_TUR", "TURRESP")),
}


def test_catalog_overlays_share_one_directory() -> None:
    names = {
        "classification_groups.toml",
        "lineage.toml",
        "relations.toml",
        "tags.toml",
        "slug_state.toml",
    }
    assert {path.name for path in _CURATION.glob("*.toml")} == names
    assert all(repo_curation_path(name) == _CURATION / name for name in names)
    assert not any((_ROOT / name).is_file() for name in names)
    assert not any(
        path.is_file()
        for path in (
            _ROOT / "classification_links.toml",
            _ROOT / "code_label_pairs.toml",
            _ROOT / "delivery_enrichment.toml",
        )
    )


def test_repo_classification_files_count_and_stay_unique(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    books = [entry.classification for entry in tree.classifications]
    labels = [
        label
        for entry in tree.classifications
        for label in entry.binding.value_set_labels
    ]
    bound = [
        binding.variable
        for entry in tree.classifications
        for binding in entry.binding.variable
    ]
    assert len(list((_CURATION / "classifications").iterdir())) == len(books) == 83
    assert len({book.slug for book in books}) == 83
    assert len(labels) == len(set(labels)) == 111
    assert sum(len(book.sentinel_codes) for book in books) == 543
    assert sum(1 for book in books if book.sentinel_codes) == 65
    assert len(bound) == len(set(bound)) == 10
    books_dir = _ROOT / "input_data" / "classifications"
    assert all((books_dir / book.codes_file).is_file() for book in books)


def test_repo_rtb_named_edition_splits_load(repo_tree: CurationTree) -> None:
    tree = repo_tree
    rtb = next(
        register for register in tree.registers if register.register_info.slug == "rtb"
    )
    splits = {
        entry.variant: entry
        for entry in rtb.identity.edition_split
        if entry.split.endswith("-kvartal") or entry.variant in {"2.66", "2.1028"}
    }
    quarterly = {
        "2.53": ("folkbokforda-personer-kvartal", "Kvartal 1-3 fr.o.m. 2009", 58),
        "2.56": ("civilstandsandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 33),
        "2.57": ("doda-kvartal", "Kvartal 1-3 fr.o.m. 2010", 58),
        "2.60": ("utvandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 28),
        "2.61": ("fodda-kvartal", "Kvartal 1-3 fr.o.m. 2010", 29),
        "2.62": ("invandringar-kvartal", "Kvartal 1-3 fr.o.m 2010", 28),
        "2.63": ("inrikes-flyttningar-kvartal", "Kvartal 1-3 fr.o.m 2010", 58),
        "2.64": ("medborgarskapsandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 58),
    }
    assert {key: len(entry.editions) for key, entry in splits.items()} == {
        "2.66": 36,
        "2.1028": 24,
        **dict.fromkeys(quarterly, 1),
    }
    assert splits["2.66"].source_editions == [str(year) for year in range(1987, 2026)]
    assert splits["2.1028"].source_editions == [str(year) for year in range(2002, 2026)]
    variants = {variant.native_id: variant for variant in rtb.variant}
    assert {entry.split for entry in splits.values()} <= variants.keys()
    for parent_id, (slug, edition, retained_count) in quarterly.items():
        split = splits[parent_id]
        assert split.editions == [edition]
        assert len(split.source_editions) == retained_count
        assert edition not in split.source_editions
        assert split.split == f"{parent_id}.{slug}"
        assert split.noted == "2026-09-27"
        parent = variants[parent_id]
        child = variants[split.split]
        assert child.slug == slug
        assert child.display_group == f"{parent.display_group}, kvartal 1–3"
        assert (
            child.panel_entity_key,
            child.panel_time_key,
            child.panel_time_grain,
        ) == (
            parent.panel_entity_key,
            parent.panel_time_key,
            parent.panel_time_grain,
        )
    historical = {
        entry.split: (tuple(entry.editions), len(entry.source_editions))
        for entry in rtb.identity.edition_split
        if entry.variant == "2.56" and not entry.split.endswith("-kvartal")
    }
    assert historical == {
        "2.56.civilstandsandringar-gifta": (("Gifta 1968-1995", "Gifta 1996-1997"), 32),
        "2.56.civilstandsandringar-skilda": (("Skilda 1968-1997",), 33),
        "2.56.civilstandsandringar-ankor-anklingar": (
            ("Änkor/ änklingar 1968-1997",),
            33,
        ),
        "2.56.civilstandsandringar-registrerade-partners": (
            ("Registrerade partners 1995-1997",),
            33,
        ),
    }
    assert len(rtb.identity.edition_split) == 14
    assert "Änkor/ änklingar 1968-1997" in splits["2.56"].source_editions
    assert "1961-1997" in splits["2.61"].source_editions


def test_repo_lineage_parses_from_overlay() -> None:
    config = load_lineage_config(_CURATION / "lineage.toml")
    assert config.defaults == {("scb", "rtb"): "folkbokforda-personer"}
    assert config.overrides == {}


def test_repo_coding_windows_are_ported(repo_tree: CurationTree) -> None:
    tree = repo_tree
    coding = [
        entry
        for register in tree.registers
        for kind in (
            register.coding.choice,
            register.coding.uncoded,
            register.coding.omit,
            register.coding.extend,
            register.coding.documented,
            register.coding.support,
        )
        for entry in kind
    ]
    assert len(coding) == 302
    assert sum(len(entry.periods) for entry in coding) == 554
    assert (
        sum(
            entry.source_authority is not None
            and entry.source_authority.source_scope is not None
            for register in tree.registers
            for entry in register.coding.documented
        )
        == 2
    )
    assert (
        sum(
            len(entry.periods)
            for register in tree.registers
            for entry in register.coding.choice
        )
        == 98
    )
    assert (
        sum(
            bool(
                register.coding.choice
                or register.coding.uncoded
                or register.coding.omit
                or register.coding.extend
                or register.coding.documented
                or register.coding.support
            )
            for register in tree.registers
        )
        == 31
    )


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_CURATION)
    assert groups  # the LISA agi rank family ships with the repo
    assert len(groups) == 37
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)
    accepted_keys = {
        ("sos", "dors", "dodsorsak"),
        ("sos", "dors", "substans"),
        ("sos", "dors", "dodsorsak-position"),
        ("sos", "par", "atgardsdatum"),
        ("sos", "par", "yttre-orsakskod"),
        ("sos", "par", "anestesikod"),
        ("sos", "mfr", "barnets-diagnoskod-bk"),
        ("sos", "mfr", "diagnoskod-barnet"),
        ("sos", "mfr", "atgardskod-barnet"),
        ("sos", "mfr", "atgardskod-forlosta"),
        ("sos", "mfr", "diagnoskod-forlosta"),
        ("sos", "mfr", "diagnoskod-graviditet"),
        ("sos", "hsl", "yrkesbeteckning"),
        ("sos", "lmed", "specialistutbildningskod"),
        ("scb", "rtb", "personnrvard"),
        ("scb", "rtb", "personnrap"),
        ("scb", "breg", "personnrvard"),
        ("scb", "breg", "personnrap"),
        ("scb", "flergenreg", "personnrf"),
        ("scb", "energianvandning-fiske", "signal"),
    }
    accepted_groups = {
        (g.provider, g.register, g.key): g
        for g in groups
        if (g.provider, g.register, g.key) in accepted_keys
    }
    assert set(accepted_groups) == accepted_keys
    assert sum(len(group.members) for group in accepted_groups.values()) == 273

    lisa_groups = {
        g.key: g for g in groups if (g.provider, g.register) == ("scb", "lisa")
    }
    assert "naringsgren" in lisa_groups
    assert "naringsgren-huvudsaklig-individ" not in lisa_groups
    assert "naringsgren-huvudsaklig-arbetsstalle" not in lisa_groups
    assert "naringsgren-huvudsaklig-foretag" not in lisa_groups

    naringsgren = lisa_groups["naringsgren"]
    assert [axis for axis, _label in naringsgren.axes] == [
        "kalla",
        "population",
        "level",
        "metod",
    ]
    assert {member.variable for member in naringsgren.members} >= {
        "astsni-justerad",
        "astsni2002b-justerad",
        "astsni2002g-justerad",
        "naringsgren-storsta-agi-sni2007g",
    }
    assert all(
        axis != "edition"
        for member in naringsgren.members
        for axis, _value, _label in member.coords
    )

    expected_lisa_rank_groups = {
        "agijobbyrk",
        "agilongarant",
        "agisektorgrp",
    }
    assert expected_lisa_rank_groups <= set(lisa_groups)
    for key in expected_lisa_rank_groups:
        group = lisa_groups[key]
        assert group.axes == (("rank", "Förvärvskälla"),)
        assert [member.coords[0][1] for member in group.members] == ["1", "2", "3"]

    faman = lisa_groups["faman"]
    assert faman.axes == (
        ("kalla", "Källa"),
        ("rank", "Förvärvskälla"),
    )
    assert {member.variable for member in faman.members} == {
        "agi1faman",
        "agi2faman",
        "agi3faman",
        "ku1faman",
        "ku2faman",
        "ku3faman",
    }
    assert {
        tuple((axis, value) for axis, value, _label in member.coords)
        for member in faman.members
    } == {
        (("kalla", "agi"), ("rank", "1")),
        (("kalla", "agi"), ("rank", "2")),
        (("kalla", "agi"), ("rank", "3")),
        (("kalla", "ku"), ("rank", "1")),
        (("kalla", "ku"), ("rank", "2")),
        (("kalla", "ku"), ("rank", "3")),
    }

    antal_barn = lisa_groups["antal-barn"]
    assert antal_barn.label == "Antal hemmavarande barn per ålder"
    assert antal_barn.axes == (("alder", "Barnets ålder"),)
    expected_antal_barn_members = [
        *((f"antal-barn-{age}-ar", f"{age:02d}", f"{age} år") for age in range(22)),
        ("barn0-3", "00-03", "0-3 år"),
        ("barn4-6", "04-06", "4-6 år"),
        ("barn7-10", "07-10", "7-10 år"),
        ("barn11-15", "11-15", "11-15 år"),
        ("barn-16-17-ar", "16-17", "16-17 år"),
        ("barn-18-19-ar", "18-19", "18-19 år"),
        ("barn18plus", "18-plus", "18 år och äldre"),
        ("barn20plus", "20-plus", "20 år och äldre"),
    ]
    assert [
        (member.variable, member.coords[0][1], member.coords[0][2])
        for member in antal_barn.members
    ] == expected_antal_barn_members
    assert [member.coords[0][0] for member in antal_barn.members] == ["alder"] * len(
        expected_antal_barn_members
    )


def test_repo_concept_groups_auto_parses() -> None:
    # The auto candidates are now generator worklist input, not build curation.
    groups = load_worklist_concept_groups(
        _ROOT / "worklists" / "concept_groups.auto.toml"
    )
    assert groups
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_code_label_pairs_parses() -> None:
    # The curated code↔label pair list (#923) ships in the repo and feeds the edge
    # concept-group fold. Load-time shape (every entry sets `code`/`label` as
    # 3-segment FQIDs) is this gate; endpoint resolution + the structural guards
    # (value-set ownership, co-delivery) are maintainer-build territory.
    pairs = load_code_label_pairs(_CURATION)
    assert pairs  # the curated SCB pairs ship with the repo
    assert len(pairs) == 90
    assert all(p.code_provider and p.code_register and p.code_variable for p in pairs)
    assert all(
        p.label_provider and p.label_register and p.label_variable for p in pairs
    )
    # No duplicate (code, label) pairs (the loader also rejects this — guard the
    # committed TOML against future drift). Key on the full FQID, not just the
    # variable slug, since two registers could share a variable slug.
    pair_tuples = [
        (
            p.code_provider,
            p.code_register,
            p.code_variable,
            p.label_provider,
            p.label_register,
            p.label_variable,
        )
        for p in pairs
    ]
    assert len(pair_tuples) == len(set(pair_tuples))


def test_repo_classification_groups_parses() -> None:
    # Curated `[[classification_group]]` umbrellas (#516) live in the same
    # `concept_groups.toml`. The SUN umbrella ships with the repo; slug RESOLUTION
    # (the classifications exist) is maintainer-build territory — this gate is the
    # load-time shape (>= 2 members, unique keys/slugs). The umbrellas are now
    # AXIS-LESS (axis is None — members are distinct classifications, not points
    # on a scale), so this no longer asserts a truthy axis.
    groups = load_classification_groups(_CURATION)
    assert groups
    assert {g.key for g in groups} >= {"sun"}
    assert all(len(g.members) >= 2 for g in groups)
    assert all(g.axis is None for g in groups)


def test_repo_delivery_enrichment_parses(repo_tree: CurationTree) -> None:
    registers = repo_tree.registers
    descriptions = [d for r in registers for d in r.enrichment.description]
    aliases = [a for r in registers for a in r.enrichment.alias]
    assert (len(descriptions), len(aliases)) == (355, 45)
    # the #365 global description backfills + delivery-column aliases ship together
    assert descriptions
    assert aliases
    # Slug resolution is maintainer-build territory; load-time shape is this gate.
    assert all(d.register_fqid and d.variable for d in descriptions)
    assert all(a.register_fqid and a.delivery_column for a in aliases)


def test_repo_delivery_enrichment_keeps_issue_428_aliases(
    repo_tree: CurationTree,
) -> None:
    aliases = [a for r in repo_tree.registers for a in r.enrichment.alias]
    triples = {(a.register_fqid, a.variable, a.delivery_column) for a in aliases}

    assert {
        ("scb/gymnasieskola-betyg", "kurs", "Amneskod_omkodad"),
        ("scb/gymnasieskola-betyg", "kurs", "Kurskod_omkodad"),
        ("scb/fek", "aktier-och-andelar", "AktierOchAndelar"),
        ("scb/fek", "byggnader", "Byggnader"),
        ("scb/fek", "kundfordringar", "Kundfordringar"),
        ("scb/fek", "mark", "Mark"),
        (
            "scb/fek",
            "ovriga-kortfristiga-placeringar",
            "OvrigaKortfristigaPlaceringar",
        ),
        ("scb/fek", "skatteskulder", "Skatteskulder"),
    } <= triples


def test_repo_alias_windows_parse_and_start_with_verified_it_case(
    repo_tree: CurationTree,
) -> None:
    aliases = [a for r in repo_tree.registers for a in r.representation.alias_window]
    assert len(aliases) == 4

    assert (
        aliases[0].variable,
        aliases[0].variant,
        aliases[0].column,
        aliases[0].source_editions,
    ) == (
        "scb/it-anvandning/bestallde-varor-tjanster-webb-app",
        "it-anvandning-i-foretag",
        "AEBUY",
        ["2018"],
    )


def test_repo_delivery_enrichment_tracks_curated_lisa_sni_slugs(
    repo_tree: CurationTree,
) -> None:
    lisa_variables = {
        d.variable
        for r in repo_tree.registers
        if (r.register_info.provider, r.register_info.slug) == ("scb", "lisa")
        for d in r.enrichment.description
    }

    old_slugs = {
        "ast-sni2002b",
        "ast-sni2002g",
        "ast-sni2007g",
        "ast-sni2007u",
        "ast-sni92b",
        "ast-sni92g",
        "org-sni2002b",
        "org-sni2002g",
        "org-sni2007g",
        "org-sni2007u",
        "org-sni92b",
        "org-sni92g",
    }
    curated_slugs = {
        "naringsgren-huvud-arbetsstalle-sni2002b",
        "naringsgren-huvud-arbetsstalle-sni2002g",
        "naringsgren-huvud-arbetsstalle-sni2007g",
        "naringsgren-huvud-arbetsstalle-sni2007u",
        "naringsgren-huvud-arbetsstalle-sni92b",
        "naringsgren-huvud-arbetsstalle-sni92g",
        "naringsgren-huvud-foretag-sni2002b",
        "naringsgren-huvud-foretag-sni2002g",
        "naringsgren-huvud-foretag-sni2007g",
        "naringsgren-huvud-foretag-sni2007u",
        "naringsgren-huvud-foretag-sni92b",
        "naringsgren-huvud-foretag-sni92g",
    }

    assert old_slugs.isdisjoint(lisa_variables)
    assert curated_slugs <= lisa_variables


def test_repo_period_family_merges_parses(repo_tree: CurationTree) -> None:
    families = [f for r in repo_tree.registers for f in r.representation.period_family]
    assert families  # the #319 LISA monthly families ship with the repo
    assert len(families) == 8
    assert all(family.slug for family in families)
    # Member RESOLUTION (12 month columns exist for the stem) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(f.register_fqid and f.family_stem and f.label for f in families)


def test_repo_relations_parses() -> None:
    # The single typed `[[edge]]` surface (#522). It ships with the #375 variable
    # succession edges + the #579 sun1996 classification split (both
    # `type = "replaced_by"`) + the #508 tier-1 and #737 recall-liberal
    # cross-register curated `same_as` identity batches.
    # The gate is load-time shape — a malformed entry would otherwise surface only
    # on a real build. Endpoint RESOLUTION is maintainer-build territory (the
    # materializers fail fast).
    relations = load_relations(_CURATION / "relations.toml")
    # 615 (#508) + 232 (#737) - 6 (Y-318 mixed SUN owner) = 841 edges.
    assert len(relations.same_as) == 841
    assert all(
        e.grain is FqidKind.VARIABLE_BINDING and e.a_variable and e.b_variable
        for e in relations.same_as
    )
    # The two load-bearing same_as invariants the bare count doesn't pin:
    # (1) DISTINCT unordered pairs — no edge repeats a {a, b} pair (the loader
    #     rejects duplicates, so a regression here means the loader's dedup broke
    #     or the file was hand-edited to bypass it).
    pairs = {frozenset((e.a_fqid(), e.b_fqid())) for e in relations.same_as}
    assert len(pairs) == len(relations.same_as)
    # Y-318: five peers match LISA's SUN2000 representation. The mixed
    # Personalutbildningsstatistik owner and LISA's SUN2020 owner cannot be in
    # this identity component, including by a transitive peer link.
    sun2000_owners = (
        "scb/fasit/utbildningsniva-aggregat-old",
        "scb/lisa/utbildningsniva-aggregat-old-sun2000",
        "scb/rams/sun2000niva-old",
        "scb/slk/sun2000niva-old",
        "scb/sls/sun2000niva-old",
        "scb/stativ/sun2000niva-old",
    )
    sun2000_pairs = {
        frozenset((a, b))
        for index, a in enumerate(sun2000_owners)
        for b in sun2000_owners[index + 1 :]
    }
    assert len(sun2000_pairs) == 15
    assert {pair for pair in pairs if pair & set(sun2000_owners)} == sun2000_pairs
    assert all("scb/lisa/utbildningsniva-aggregat-old" not in pair for pair in pairs)
    # (2) COMPONENT CAP — the actual safety property same_as exists to protect: a
    #     mistaken edge welds two identity components into a runaway resolver blob.
    #     Recompute connected components (union-find over the endpoint FQIDs) and
    #     assert the max <= _SAME_AS_MAX_COMPONENT, so a bad curation edge fails the
    #     unit suite, not only the maintainer build's `materialize_same_as` guard.
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for e in relations.same_as:
        ra, rb = find(e.a_fqid()), find(e.b_fqid())
        if ra != rb:
            parent[ra] = rb
    sizes: dict[str, int] = {}
    for node in parent:
        sizes[find(node)] = sizes.get(find(node), 0) + 1
    assert max(sizes.values()) <= _SAME_AS_MAX_COMPONENT
    sun2000_component = {
        node for node in parent if find(node) == find(sun2000_owners[0])
    }
    assert sun2000_component == set(sun2000_owners)
    assert "scb/lisa/utbildningsniva-aggregat-old-sun2020" not in sun2000_component
    assert (
        "scb/personalutbildningsstatistik/utbildningsniva-aggregat-old"
        not in sun2000_component
    )
    # 11 #375 variable succession edges + 21 #931 LISA SNI-coding succession edges
    # + 2 #400 SSYK J16 succession edges
    # + 3 #579 classification split edges
    # + 3 #770/#768 ICD/KS disease-classification succession edges
    # + 7 #814 iot disponibel-inkomst 2004-års-definition succession edges
    # + 1 #875 KSju lgrp → NgGr1 representation-grain succession edge
    # + 1 #846 RTB PNR → PersonNr representation-grain rename edge
    # + 2 #846 FRIDA firm-key variant-scoped gap-fill round-trip edges
    # + 1 #376 LISA register_variant succession edge
    # + 3 #1122 LISA FÅMANS KU→AGI source succession edges
    # + 8 Y-88 curated LISA succession edges.
    assert len(relations.replaced_by) == 63
    assert all(str(e.predecessor) and str(e.successor) for e in relations.replaced_by)
    ksju_edges = [
        e
        for e in relations.replaced_by
        if str(e.predecessor) == "scb/ksju/naringsgren-grupperad-2009"
    ]
    assert len(ksju_edges) == 1
    assert str(ksju_edges[0].successor) == "scb/ksju/naringsgren"
    assert (ksju_edges[0].predecessor_column, ksju_edges[0].successor_column) == (
        "lgrp",
        "NgGr1",
    )


def test_repo_scb_errata_parses() -> None:
    # Register-file `[[errata.*]]` tables (Y-114/Y-116) are the upstream-error log; every entry
    # resolves its `register`/`variant` slugs against the register tree
    # the build reads, so a stale slug is a load-time failure here rather than a
    # maintainer-build surprise. The remaining half (the version is documented, the
    # column has / has not a real row) needs the real export and stays build-only.
    errata = load_scb_errata(
        _CURATION,
        repo_slug_dir(),
    )
    assert errata  # the verified LISA DispInkKE case ships with the repo
    assert (len(errata.delivered), len(errata.columns), len(errata.versions)) == (
        385,
        1894,
        10,
    )
    # scb/lisa "Individer, 15 år och äldre"; pin the complete coordinates of
    # these named records without assuming the multi-register file contains
    # only LISA entries.
    assert {
        (d.column, d.versions, d.register_id, d.register_variant_id)
        for d in errata.delivered
    } >= {
        ("DispInkKE", ("2010", "2011", "2012"), 34, 153),
        ("DispInkKE04", ("2010", "2011", "2012"), 34, 153),
    }
    # Y-298: scb/it-anvandning (258.556) reused the delivery literals of later
    # native owners, so the 2014 entries need their source-native owner named.
    # Pin the exact coordinates, versions and native guards.
    assert {
        (
            d.column,
            d.versions,
            d.register_id,
            d.register_variant_id,
            d.native_variable_id,
        )
        for d in errata.delivered
        if d.register_id == 258
        and d.register_variant_id == 556
        and d.column in {"EMPIUSEPCT", "SISC", "WEBORD"}
    } == {
        ("EMPIUSEPCT", ("2014",), 258, 556, 17675),
        ("SISC", ("2014",), 258, 556, 16946),
        ("WEBORD", ("2014",), 258, 556, 26864),
    }


def test_repo_scb_errata_columns_carry_both_evidence_sources() -> None:
    # Y-116 folded the SWECOV grafts and the LISA doc-coverage attaches into
    # [[column]]. Both blocks must survive a regeneration of either: the counts
    # pin the survey-wave batch (#856) and the doc set, and the LISA entries pin
    # the variant re-targeting — an entry whose `versions` name editions its
    # variant does not have fails only on a maintainer build, with the corpus.
    # The holdings count is 24 short of the grafts it converted: SCB's export
    # now carries those 24 innovation-foretag columns, which the graft pass
    # skipped in silence and [[column]] refuses (`scb_errata_now_present`), so
    # they came out. A regeneration that re-proposes them is proposing entries
    # the real corpus rejects.
    errata = load_scb_errata(
        _CURATION,
        repo_slug_dir(),
    )
    by_source = Counter(c.source for c in errata.columns)
    assert by_source == {"steward-holdings": 1860, "scb-docs": 34}

    holdings = Counter(
        (c.register_id, c.register_variant_id)
        for c in errata.columns
        if c.source == "steward-holdings"
    )
    assert sorted(holdings.values(), reverse=True)[:3] == [992, 623, 156]
    assert {
        (c.register_id, c.register_variant_id, c.column, c.versions)
        for c in errata.columns
        if c.source == "steward-holdings" and c.versions is not None
    } == {
        (122, 1628, column, ("2021", "2022"))
        for column in (
            "COVIDA",
            "COVIDAEJ2",
            "COVIDDEL",
            "COVIDE",
            "COVIDEJSOK",
            "COVIDFMH",
            "COVIDFMHB",
            "COVIDHEM",
            "COVIDTARB",
        )
    }
    peorgnrhe = next(c for c in errata.columns if c.column == "PeOrgNrHe")
    assert peorgnrhe.is_identifier

    # LISA's individual frame is `individer-16plus` (34.1335) through 2009 and
    # `individer-15plus` (34.153) from 2010, so a doc-coverage entry's versions
    # must fall inside its variant's era.
    eras = {1335: range(1990, 2010), 153: range(2010, 2024)}
    docs = [c for c in errata.columns if c.source == "scb-docs"]
    # Y-281 Q5: every doc-coverage column carries both curated flags explicitly.
    assert all(c.is_sensitive is not None and c.is_identifier is not None for c in docs)
    assert {c.register_variant_id for c in docs} == set(eras)
    for c in docs:
        assert c.versions is not None
        assert all(int(v) in eras[c.register_variant_id] for v in c.versions)
    # FastBet (1998–) and MedbLandNamn (1990–) span the change: two entries, one
    # variable each (`key` is register-scoped, not variant-scoped).
    spanning = {c.column for c in docs if sum(d.column == c.column for d in docs) > 1}
    assert spanning == {"FastBet", "MedbLandNamn"}
    assert len({c.key for c in docs if c.column in spanning}) == 2


def test_repo_fdb_gaturest_declares_two_spelling_ownership() -> None:
    # Y-167: the tracked 1.830 literal column ownership — {GatuRest, Gaturest}
    # on gaturest, {PGaturest} on pgaturest — must load family-complete against
    # the pinned auto slugs, with the counter-evidence in its reference.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    ownership = declared_column_ownership(entries, provider="scb", source_id="1.830")
    assert ownership.split_ids == ("1.830.gaturest", "1.830.pgaturest")
    assert dict(ownership.declared_columns) == {
        "GatuRest": "1.830.gaturest",
        "Gaturest": "1.830.gaturest",
        "PGaturest": "1.830.pgaturest",
    }
    assert "31477" in ownership.declaration_reference
    assert "12814" in ownership.declaration_reference
    assert "6139" in ownership.declaration_reference
    assert "383" in ownership.declaration_reference
    slugs = {
        (e.provider, e.source_id): e.slug
        for e in entries
        if e.kind == "variable" and e.slug is not None
    }
    assert slugs[("scb", "1.830.gaturest")] == "gaturest"
    assert slugs[("scb", "1.830.pgaturest")] == "pgaturest"


def test_repo_column_owning_splits_resolve_to_a_slug(repo_tree: CurationTree) -> None:
    # Register-file ownership is complete against the split slugs and each named
    # owner has an actual naming slug.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    slugged = {
        (e.provider, e.source_id)
        for e in entries
        if e.kind == "variable" and e.slug is not None
    }
    tree = repo_tree
    partitions = [
        partition
        for register in tree.registers
        for partition in register.identity.partition
    ]
    assert len(tree.registers) == 290
    assert (
        sum(len(register.identity.route) for register in tree.registers),
        sum(len(register.identity.split) for register in tree.registers),
        sum(len(register.identity.rename) for register in tree.registers),
        sum(len(register.identity.column_owner) for register in tree.registers),
    ) == (20, 21, 1, 349)
    # Y-303 rev 2: six reused PAR native names each deliver distinct source
    # concepts. AR/INDATUM/INDATUMA/ALDER/IDNR split by Deldatamängd; FODDAT
    # splits by data type because its OV/SV text originals share one
    # alphanumeric concept while the TV original is a SAS date. Each PAR panel
    # references the INDATUM owner of its own subset, never the withheld
    # unsplit `indatum` base.
    par = next(
        register for register in tree.registers if register.register_info.slug == "par"
    )
    assert {
        entry.variable: {getattr(part, entry.by): part.owner for part in entry.parts}
        for entry in par.identity.split
    } == {
        "DISTRIKT": {
            "Distrikt där patienten var folkbokförd vid tidpunkten för vårdkontakten. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-oppenvardskontakt",
            "Distrikt där patienten var folkbokförd 31 december året för vårdkontakten. Finns i årsversionerna": "5891427617861710725.DISTRIKT.distrikt",
            "Distrikt där patienten var folkbokförd vid utskrivningsdatum. Saknas utskrivningsdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-utskrivning-slutenvard",
            "Distrikt där patienten var folkbokförd vid slut av psykiatrisk vårdform. Saknas slutdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-slut-psykiatrisk-vardform",
        },
        "LK": {
            "Län och kommun där patienten var folkbokförd 31 december året för vårdkontakten. Finns i årsversionerna.": "5891427617861710725.LK.folkbokforingsort-lan-kommun",
            "Län och kommun där patienten var folkbokförd vid tidpunkten för vårdkontakten. Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-oppenvardskontakt",
            "Län och kommun där patienten var folkbokförd vid utskrivningsdatum. Saknas utskrivningsdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-utskrivning-slutenvard",
            "Län och kommun där patienten var folkbokförd vid tidpunkten för slut av psykiatrisk vårdform. Saknas slutdatum används datum då registret skapas.  Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-slut-psykiatrisk-vardform",
        },
        "ALDER_S": {
            "PAR_OV": "5891427617861710725.ALDER_S.alder-vid-arets-slut",
            "PAR_SV": "5891427617861710725.ALDER_S.alder-vid-utskrivningsarets-slut",
            "PAR_TV": "5891427617861710725.ALDER_S.alder-vid-utskrivningsarets-slut",
        },
        "HDIA": {
            "Typ av diagnos": "5891427617861710725.HDIA.typ-av-diagnos",
            "Huvuddiagnoskod": "5891427617861710725.HDIA.huvuddiagnoskod",
        },
        "START": {
            "Startdatum för psykiatrisk vårdform": "5891427617861710725.START.startdatum-psykiatrisk-vardform",
            "Startdatum för permission": "5891427617861710725.START.startdatum-permission",
            "Startdatum för avvikning": "5891427617861710725.START.startdatum-avvikning",
        },
        "SLUT": {
            "Slutdatum för psykiatrisk vårdform": "5891427617861710725.SLUT.slutdatum-psyk-vardform",
            "Slutdatum för permission": "5891427617861710725.SLUT.slutdatum-permission",
            "Slutdatum för avvikning": "5891427617861710725.SLUT.slutdatum-avvikning",
        },
        "TYP": {
            "Psykiatrisk vårdform": "5891427617861710725.TYP.typ",
            "Markör för yttre orsakskod": "5891427617861710725.TYP.markor-yttre-orsakskod",
            "Markör för avvikning": "5891427617861710725.TYP.markor-avvikning",
            "Markör för permission": "5891427617861710725.TYP.markor-permission",
        },
        "ATCO": {
            "text": "5891427617861710725.ATCO.atc-komplement-atgardskod",
            "integer": "5891427617861710725.ATCO.atg-kod-ar-atc-kod",
        },
        "DIAGNOS": {
            "PAR_OV": "5891427617861710725.DIAGNOS.diagnoskoder",
            "PAR_SV": "5891427617861710725.DIAGNOS.diagnoskoder",
            "PAR_TV": "5891427617861710725.DIAGNOS.diagnoskod-eller-atc-kod-psykiatrisk-vard",
        },
        "EKOD": {
            "PAR_OV": "5891427617861710725.EKOD.yttre-orsakskoder",
            "PAR_SV": "5891427617861710725.EKOD.yttre-orsakskoder",
            "PAR_TV": "5891427617861710725.EKOD.yttre-orsakskod-eller-atc-kod-psykiatrisk-vard",
        },
        "ATC": {
            "integer": "5891427617861710725.ATC.atc",
            "text": "5891427617861710725.ATC.atc-1",
        },
        "AR": {
            "PAR_OV": "5891427617861710725.AR.besoksar",
            "PAR_SV": "5891427617861710725.AR.utskrivningsar",
            "PAR_TV": "5891427617861710725.AR.ar-avslutad-psykiatrisk-vardform",
        },
        "INDATUM": {
            "PAR_OV": "5891427617861710725.INDATUM.besoksdatum",
            "PAR_SV": "5891427617861710725.INDATUM.inskrivningsdatum-slutenvard",
            "PAR_TV": "5891427617861710725.INDATUM.inskrivningsdatum-psykiatrisk-vardform",
        },
        "INDATUMA": {
            "PAR_OV": "5891427617861710725.INDATUMA.besoksdatum-alfanumeriskt",
            "PAR_SV": "5891427617861710725.INDATUMA.inskrivningsdatum-alfanumeriskt",
        },
        "ALDER": {
            "PAR_OV": "5891427617861710725.ALDER.alder-vid-oppenvardskontakt",
            "PAR_SV": "5891427617861710725.ALDER.alder-vid-utskrivning-slutenvard",
            "PAR_TV": "5891427617861710725.ALDER.alder-vid-utskrivning-psykiatrisk-vardform",
        },
        "IDNR": {
            "PAR_OV": "5891427617861710725.IDNR.lopnr-vardkontakt-oppenvard",
            "PAR_SV": "5891427617861710725.IDNR.lopnr-slutenvardstillfalle",
            "PAR_TV": "5891427617861710725.IDNR.lopnr-tvangsvardsform",
        },
        "FODDAT": {
            "text": "5891427617861710725.FODDAT.fodelsedatum-alfanumeriskt",
            "date": "5891427617861710725.FODDAT.fodelsedatum-sas-psykiatrisk-vardform",
        },
    }
    assert Counter(
        entry.by for entry in par.identity.split if entry.variable != "ATC"
    ) == {
        "deldatamangd": 8,
        "data_type": 2,
        "name": 4,
        "description": 2,
    }
    par_owners = {
        part.owner
        for entry in par.identity.split
        if entry.variable != "ATC"
        for part in entry.parts
    }
    assert len(par_owners) == 44
    assert {("sos", owner) for owner in par_owners} <= slugged
    assert {
        variant.display_group: variant.panel_time_key for variant in par.variant
    } == {
        "PAR_OV": "besoksdatum",
        "PAR_SV": "inskrivningsdatum-slutenvard",
        "PAR_TV": "inskrivningsdatum-psykiatrisk-vardform",
    }
    # Y-308: historical CIS2016 crosswalk components, not response recoding.
    innovation = next(
        register
        for register in tree.registers
        if register.register_info.slug == "innovation-foretag"
    )
    cis2016 = {
        "257.28054": {
            "INITGD": "257.28054.varuinnovation-utvecklad-av-foretaget",
            "INTOGD": "257.28054.varuinnovation-utvecklad-med-andra",
            "INADGD": "257.28054.varuinnovation-anpassad-av-foretaget",
            "INOTHGD": "257.28054.varuinnovation-utvecklad-av-andra",
        },
        "257.28055": {
            "INITSV": "257.28055.tjansteinnovation-utvecklad-av-foretaget",
            "INTOSV": "257.28055.tjansteinnovation-utvecklad-med-andra",
            "INADSV": "257.28055.tjansteinnovation-anpassad-av-foretaget",
            "INOTHSV": "257.28055.tjansteinnovation-utvecklad-av-andra",
        },
        "257.28056": {
            "INITPS": "257.28056.processinnovation-utvecklad-av-foretaget",
            "INTOPS": "257.28056.processinnovation-utvecklad-med-andra",
            "INADPS": "257.28056.processinnovation-anpassad-av-foretaget",
            "INOTHPS": "257.28056.processinnovation-utvecklad-av-andra",
        },
        "257.30131": {
            "PBINN": "257.30131.innovationsaktivitet-i-upphandlingsavtal",
            "PBINCT": "257.30131.innovation-kravd-i-upphandlingsavtal",
            "PBNOCT": "257.30131.innovation-ej-kravd-i-upphandlingsavtal",
        },
        "257.37092": {
            "LG": "257.37092.logistikinnovation-utebliven-huvudorsak",
            "LGFIN": "257.37092.logistikinnovation-ekonomiskt-hinder",
            "LGTEC": "257.37092.logistikinnovation-tekniskt-hinder",
            "LGREG": "257.37092.logistikinnovation-rattsligt-hinder",
            "LGOTHO": "257.37092.logistikinnovation-annat-hinder",
        },
    }
    cis2016_partitions = [
        entry for entry in innovation.identity.partition if entry.variable in cis2016
    ]
    assert len(cis2016_partitions) == len(cis2016)
    assert {
        entry.variable: dict(entry.columns) for entry in cis2016_partitions
    } == cis2016
    assert all(not entry.unassigned_columns for entry in cis2016_partitions)
    expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in cis2016.values()
        for owner in columns.values()
    }
    cis2016_names = [
        entry
        for entry in innovation.variable
        if entry.native_id.rsplit(".", 1)[0] in cis2016
    ]
    assert len(cis2016_names) == len(expected_names)
    assert {entry.native_id: entry.slug for entry in cis2016_names} == expected_names
    assert {("scb", owner) for owner in expected_names} <= slugged
    # The KU-to-AGI source-basis partitions (Y-299 34.591 and the 29 Y-302
    # employment/source families) are exact maps with two named leaves; each owner
    # must carry its own naming slug.
    pension = [partition for partition in partitions if partition.variable == "34.591"]
    assert len(pension) == 1
    assert dict(pension[0].columns) == {
        "KUPens": "34.591.tjanstepension-tjanst",
        "AGIPens": "34.591.tjanstepension-tjanst-agi",
    }
    assert {
        ("scb", "34.591.tjanstepension-tjanst"),
        ("scb", "34.591.tjanstepension-tjanst-agi"),
    } <= slugged
    # Y-302: the 29 documented LISA KU-to-AGI employment/source families. Each
    # family has one KU literal (through 2018) and one AGI literal (from 2019); the
    # columns map is exact and both owners take a distinct naming leaf.
    ku_agi = {
        "34.1933": {
            "KU1SsykAr": "34.1933.ku1ssykar",
            "AGI1SsykAr": "34.1933.agi1ssykar",
        },
        "34.1935": {
            "KU1AstKommun": "34.1935.ku1astkommun",
            "AGI1AstKommun": "34.1935.agi1astkommun",
        },
        "34.1936": {
            "KU1AstLan": "34.1936.ku1astlan",
            "AGI1AstLan": "34.1936.agi1astlan",
        },
        "34.4911": {
            "KU2YrkStalln": "34.4911.yrkesstallning-nast-forvarvskallan",
            "AGI2YrkStalln": "34.4911.yrkesstallning-nast-forvarvskallan-agi",
        },
        "34.4948": {
            "KU1YrkStalln": "34.4948.yrkesstallning-storsta-forvarvskallan",
            "AGI1YrkStalln": "34.4948.yrkesstallning-storsta-forvarvskallan-agi",
        },
        "34.15858": {
            "KU2AstNr": "34.15858.ku2astnr",
            "AGI2AstNr": "34.15858.agi2astnr",
        },
        "34.16214": {
            "KU1AstNr": "34.16214.ku1astnr",
            "AGI1AstNr": "34.16214.agi1astnr",
        },
        "34.16219": {
            "KU1PeOrgNr": "34.16219.ku1peorgnr",
            "AGI1PeOrgNr": "34.16219.agi1peorgnr",
        },
        "34.16220": {
            "KU2PeOrgNr": "34.16220.ku2peorgnr",
            "AGI2PeOrgNr": "34.16220.agi2peorgnr",
        },
        "34.16227": {
            "KU2SsykKalla": "34.16227.ku2ssykkalla",
            "AGI2SsykKalla": "34.16227.agi2ssykkalla",
        },
        "34.16237": {
            "KU1SsykKalla": "34.16237.ku1ssykkalla",
            "AGI1SsykKalla": "34.16237.agi1ssykkalla",
        },
        "34.16244": {
            "KU2SsykAr": "34.16244.ku2ssykar",
            "AGI2SsykAr": "34.16244.agi2ssykar",
        },
        "34.16249": {"KU1Ink": "34.16249.ku1ink", "AGI1Ink": "34.16249.agi1ink"},
        "34.16250": {"KU2Ink": "34.16250.ku2ink", "AGI2Ink": "34.16250.agi2ink"},
        "34.18911": {
            "KU1SsykStatus": "34.18911.ssyk-overensstammelse",
            "AGI1SsykStatus": "34.18911.ssyk-overensstammelse-agi",
        },
        "34.21004": {
            "KU1CfarNr": "34.21004.ku1cfarnr",
            "AGI1CfarNr": "34.21004.agi1cfarnr",
        },
        "34.21005": {
            "KU2CfarNr": "34.21005.ku2cfarnr",
            "AGI2CfarNr": "34.21005.agi2cfarnr",
        },
        "34.25621": {
            "KU1SektorKod": "34.25621.sektorkod-storsta-forvarvskalla",
            "AGI1SektorKod": "34.25621.sektorkod-storsta-forvarvskalla-agi",
        },
        "34.31108": {
            "KU2AstKommun": "34.31108.ku2astkommun",
            "AGI2AstKommun": "34.31108.agi2astkommun",
        },
        "34.31109": {
            "KU2AstLan": "34.31109.ku2astlan",
            "AGI2AstLan": "34.31109.agi2astlan",
        },
        "34.31118": {
            "KU2SektorKod": "34.31118.sektorkod-nast-forvarvskalla",
            "AGI2SektorKod": "34.31118.sektorkod-nast-forvarvskalla-agi",
        },
        "34.31290": {"KU3Ink": "34.31290.ku3ink", "AGI3Ink": "34.31290.agi3ink"},
        "34.31291": {
            "KU3PeOrgNr": "34.31291.ku3peorgnr",
            "AGI3PeOrgNr": "34.31291.agi3peorgnr",
        },
        "34.31292": {
            "KU3CfarNr": "34.31292.ku3cfarnr",
            "AGI3CfarNr": "34.31292.agi3cfarnr",
        },
        "34.31293": {
            "KU3AstNr": "34.31293.ku3astnr",
            "AGI3AstNr": "34.31293.agi3astnr",
        },
        "34.31294": {
            "KU3YrkStalln": "34.31294.ku3yrkesstallning",
            "AGI3YrkStalln": "34.31294.agi3yrkesstallning",
        },
        "34.31295": {
            "KU3AstKommun": "34.31295.ku3astkommun",
            "AGI3AstKommun": "34.31295.agi3astkommun",
        },
        "34.31296": {
            "KU3AstLan": "34.31296.ku3astlan",
            "AGI3AstLan": "34.31296.agi3astlan",
        },
        "34.31299": {
            "KU3SektorKod": "34.31299.sektorkod-tredje-forvarvskalla",
            "AGI3SektorKod": "34.31299.sektorkod-tredje-forvarvskalla-agi",
        },
    }
    by_variable = {partition.variable: partition for partition in partitions}
    assert set(ku_agi) <= set(by_variable)
    for variable, expected in ku_agi.items():
        assert dict(by_variable[variable].columns) == expected, variable
    assert {
        ("scb", owner) for expected in ku_agi.values() for owner in expected.values()
    } <= slugged
    # Y-313: seven LISA classification-edition and KU/AGI source-basis families.
    # Four SSYK first/second income-source families separate the delivered SSYK96
    # KU edition (2001-2013), the delivered SSYK2012 KU _2012 edition (2014-2018)
    # and the SSYK2012 AGI source basis (2019-2023); three institutional-sector
    # families separate the 1968/1999/2000/2014 Enhetsindelning coding editions
    # plus the 2014 AGI source basis. Each owner takes its own naming leaf.
    lisa_editions7 = {
        "34.1842": {
            "KU1Ssyk3_2012": "34.1842.ssyk3-storsta-2012",
            "KU1Ssyk3": "34.1842.ssyk3-storsta",
            "AGI1SSYK3_2012": "34.1842.ssyk3-storsta-2012-agi",
            "AGI1Ssyk3_2012": "34.1842.ssyk3-storsta-2012-agi",
        },
        "34.4904": {
            "KU2Ssyk3_2012": "34.4904.ssyk3-nast-2012",
            "KU2Ssyk3": "34.4904.ssyk3-nast",
            "AGI2SSYK3_2012": "34.4904.ssyk3-nast-2012-agi",
            "AGI2Ssyk3_2012": "34.4904.ssyk3-nast-2012-agi",
        },
        "34.4905": {
            "KU2Ssyk4_2012": "34.4905.ssyk4-nast-2012",
            "KU2Ssyk4": "34.4905.ssyk4-nast",
            "AGI2SSYK4_2012": "34.4905.ssyk4-nast-2012-agi",
            "AGI2Ssyk4_2012": "34.4905.ssyk4-nast-2012-agi",
        },
        "34.16224": {
            "KU1Ssyk4_2012": "34.16224.ssyk4-storsta-2012",
            "KU1Ssyk4": "34.16224.ssyk4-storsta",
            "AGI1SSYK4_2012": "34.16224.ssyk4-storsta-2012-agi",
            "AGI1Ssyk4_2012": "34.16224.ssyk4-storsta-2012-agi",
        },
        "34.31111": {
            "KU2InstKod10": "34.31111.ku2instkod-2014",
            "KU2InstKod7": "34.31111.ku2instkod-2000",
            "AGI2InstKod10": "34.31111.ku2instkod-2014-agi",
            "KU2InstKod": "34.31111.ku2instkod",
            "KU2InstKod6": "34.31111.ku2instkod-1999",
        },
        "34.31289": {
            "KU1InstKod10": "34.31289.ku1instkod-2014",
            "KU1InstKod7": "34.31289.ku1instkod-2000",
            "AGI1InstKod10": "34.31289.ku1instkod-2014-agi",
            "KU1InstKod": "34.31289.ku1instkod",
            "KU1InstKod6": "34.31289.ku1instkod-1999",
        },
        "34.31298": {
            "KU3InstKod7": "34.31298.ku3instkod-2000",
            "KU3InstKod10": "34.31298.ku3instkod-2014",
            "AGI3InstKod10": "34.31298.ku3instkod-2014-agi",
            "KU3InstKod": "34.31298.ku3instkod",
            "KU3InstKod6": "34.31298.ku3instkod-1999",
        },
    }
    assert len(lisa_editions7) == 7
    for variable, expected in lisa_editions7.items():
        assert dict(by_variable[variable].columns) == expected, variable
    expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in lisa_editions7.values()
        for owner in columns.values()
    }
    assert len(expected_names) == 27
    lisa_names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in expected_names
    }
    assert lisa_names == expected_names
    assert {("scb", owner) for owner in expected_names} <= slugged
    # Y-317: three RTB classification-basis families. Native 2.24 separates the
    # eight year-specific historical literals (one December 31 snapshot each,
    # 1990-1997, nativevariant 1524) from the two later case spellings (Famstall
    # 1998-2006, FamStall 2007-2025, nativevariant 54); natives 2.24851/2.24852
    # each separate the old Grupp text categories (2009-2010) from the current
    # KlassGrupp two-digit codes (2011-2025). Each owner takes its own naming
    # leaf. The literal map is exact and case-sensitive, and the three base
    # native declarations keep their original slugs.
    rtb_classifications3 = {
        "2.24": {
            "FST90": "2.24.familjestallning-1990-1997",
            "FST91": "2.24.familjestallning-1990-1997",
            "FST92": "2.24.familjestallning-1990-1997",
            "FST93": "2.24.familjestallning-1990-1997",
            "FST94": "2.24.familjestallning-1990-1997",
            "FST95": "2.24.familjestallning-1990-1997",
            "FST96": "2.24.familjestallning-1990-1997",
            "FST97": "2.24.familjestallning-1990-1997",
            "Famstall": "2.24.familjestallning",
            "FamStall": "2.24.familjestallning",
        },
        "2.24851": {
            "GFB_FFB_Grupp": "2.24851.gfb-grupp-ffb",
            "GFB_FFB_KlassGrupp": "2.24851.gfb-klassgruppering-ffb",
        },
        "2.24852": {
            "GFB_EFB_Grupp": "2.24852.gfb-grupp-efb",
            "GFB_EFB_KlassGrupp": "2.24852.gfb-klassgruppering-efb",
        },
    }
    assert len(rtb_classifications3) == 3
    assert sum(len(columns) for columns in rtb_classifications3.values()) == 14
    for variable, expected in rtb_classifications3.items():
        assert dict(by_variable[variable].columns) == expected, variable
    rtb_expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in rtb_classifications3.values()
        for owner in columns.values()
    }
    assert len(rtb_expected_names) == 6
    rtb_names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in rtb_expected_names
    }
    assert rtb_names == rtb_expected_names
    assert {("scb", owner) for owner in rtb_expected_names} <= slugged
    # The three existing native declarations keep their original slugs.
    rtb_base = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in {"2.24", "2.24851", "2.24852"}
    }
    assert rtb_base == {
        "2.24": "familjestallning",
        "2.24851": "gfb-klassgruppering-ffb",
        "2.24852": "gfb-klassgruppering-efb",
    }
    # Y-318: the HREG/LISA partitions are literal-based. In particular, the
    # retrospective HREG 2001/current versions retain their pre-vintage source
    # years, and the 2007/2020 citizenship groupings coexist in 2020-2023.
    # There is no observation-year gate in either partition.
    classification_versions3 = {
        "47.29507": {
            "TjKat_1995": "47.29507.anstallningskategori-1995",
            "TjKat_2001": "47.29507.anstallningskategori-2001",
            "TjKat_2008": "47.29507.anstallningskategori-2008",
            "TjKat": "47.29507.anstallningskategori-2012",
        },
        "34.774": {
            "Sun2000niva_old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2000niva_Old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2000Niva_old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2020Niva_Old": "34.774.utbildningsniva-aggregat-old-sun2020",
        },
        "34.31193": {
            "MedbGrEg3": "34.31193.medbgreg-eu27-2007",
            "MedbGrEg5": "34.31193.medbgreg-eu27-2020",
        },
    }
    selected = [
        partition
        for partition in partitions
        if partition.variable in classification_versions3
    ]
    assert len(selected) == 3
    assert {entry.variable: dict(entry.columns) for entry in selected} == (
        classification_versions3
    )
    assert all(not entry.unassigned_columns for entry in selected)
    owners = {
        owner
        for columns in classification_versions3.values()
        for owner in columns.values()
    }
    assert len(owners) == 8
    assert {("scb", owner) for owner in owners} <= slugged
    assert {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in owners
    } == {owner: owner.split(".", 2)[2] for owner in owners}
    assert {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in classification_versions3
    } == {
        "47.29507": "anstallningskategori",
        "34.774": "utbildningsniva-aggregat-old",
    }
    assert (
        classification_versions3["47.29507"]["TjKat_2001"]
        != (classification_versions3["47.29507"]["TjKat"])
    )
    assert (
        classification_versions3["34.31193"]["MedbGrEg3"]
        != (classification_versions3["34.31193"]["MedbGrEg5"])
    )
    assert (
        len(
            {
                classification_versions3["34.774"][literal]
                for literal in ("Sun2000niva_old", "Sun2000niva_Old", "Sun2000Niva_old")
            }
        )
        == 1
    )
    # Y-306: eleven RTB/IoT sequential delivery-column renames. Each family's
    # exact literals occupy disjoint windows and name one existing owner.
    sequential11 = {
        "2.243": {
            "AlderHRel": "2.243.alder-vid-handelsen-relationsperson",
            "DodMakAr": "2.243.alder-vid-handelsen-relationsperson",
        },
        "2.283": {
            "DodDat": "2.283.dodsdatum",
            "DodDatum": "2.283.dodsdatum",
        },
        "2.292": {
            "AntBord": "2.292.antal-fodda",
            "EnkFlerBord": "2.292.antal-fodda",
        },
        "2.297": {
            "CivDatMor": "2.297.civilstandsdatum-mor",
            "CivilDatumMor": "2.297.civilstandsdatum-mor",
        },
        "2.322": {
            "BarnOrdDod": "2.322.ordningsnummer-dodfodd-mor",
            "OrdnrDodF": "2.322.ordningsnummer-dodfodd-mor",
        },
        "2.346": {
            "BarnOrdLev": "2.346.ordningsnummer-mor",
            "OrdnrLevFoddaM": "2.346.ordningsnummer-mor",
        },
        "2.353": {
            "ForsamlingFg": "2.353.forsamling-tidigare",
            "ForsamlingG": "2.353.forsamling-tidigare",
        },
        "2.355": {
            "KommunFg": "2.355.kommun-tidigare",
            "KommunG": "2.355.kommun-tidigare",
        },
        "2.356": {
            "LanFg": "2.356.lan-tidigare",
            "LanG": "2.356.lan-tidigare",
        },
        "25.596": {
            "PKUTJP": "25.596.tjanstepension",
            "PTJP": "25.596.tjanstepension",
        },
        "25.1190": {
            "TALU": "25.1190.akassa-atgarder",
            "TKUALU": "25.1190.akassa-atgarder",
        },
    }
    for variable, expected in sequential11.items():
        assert dict(by_variable[variable].columns) == expected, variable
    assert {
        ("scb", owner)
        for expected in sequential11.values()
        for owner in expected.values()
    } <= slugged
    # Y-305: 22 IoT KUSOC compensation-code sequential renames. Each family's
    # two exact literals occupy disjoint windows and name one existing owner.
    kusoc = {
        "25.21785": ("PKUEN426", "PEN426", "25.21785.summa-skattefritt-skattepliktigt"),
        "25.21786": ("PKULI421", "PLI421", "25.21786.pkuli421"),
        "25.21787": ("PKULI422", "PLI422", "25.21787.pkuli422"),
        "25.21788": ("PKULI423", "PLI423", "25.21788.pkuli423"),
        "25.21789": ("PKULI424", "PLI424", "25.21789.pkuli424"),
        "25.21790": ("PKULI428", "PLI428", "25.21790.pkuli428"),
        "25.21791": ("PKULI429", "PLI429", "25.21791.pkuli429"),
        "25.21792": ("PKULI473", "PLI473", "25.21792.pkuli473"),
        "25.21793": ("PKULI481", "PLI481", "25.21793.pkuli481"),
        "25.21794": ("PKULI483", "PLI483", "25.21794.pkuli483"),
        "25.21795": ("PKULI486", "PLI486", "25.21795.pkuli486"),
        "25.21796": ("PKULI999", "PLI999", "25.21796.pkuli999"),
        "25.21797": (
            "PKUSF420",
            "PSF420",
            "25.21797.ersattningskod-420-skattefri-del",
        ),
        "25.21798": (
            "PKUSF421",
            "PSF421",
            "25.21798.ersattningskod-421-skattefri-del",
        ),
        "25.21799": (
            "PKUSF423",
            "PSF423",
            "25.21799.ersattningskod-423-skattefri-del",
        ),
        "25.21800": (
            "PKUSF426",
            "PSF426",
            "25.21800.ersattningskod-426-skattefri-del",
        ),
        "25.21801": (
            "PKUSF429",
            "PSF429",
            "25.21801.ersattningskod-429-skattefri-del",
        ),
        "25.21802": (
            "PKUSP420",
            "PSP420",
            "25.21802.ersattningskod-420-skattepliktig-del",
        ),
        "25.21803": (
            "PKUSP421",
            "PSP421",
            "25.21803.ersattningskod-421-skattepliktig-del",
        ),
        "25.21804": (
            "PKUSP423",
            "PSP423",
            "25.21804.ersattningskod-423-skattepliktig-del",
        ),
        "25.21805": (
            "PKUSP429",
            "PSP429",
            "25.21805.ersattningskod-429-skattepliktig-del",
        ),
        "25.21806": (
            "PKUSP426",
            "PSP426",
            "25.21806.ersattningskod-426-skattepliktig-del",
        ),
    }
    for variable, (old, new, owner) in kusoc.items():
        assert dict(by_variable[variable].columns) == {old: owner, new: owner}
        assert ("scb", owner) in slugged

    # Y-304: the 75 reviewed IT-användning (258) recurrent-question identity
    # families each own every listed literal through one native-question leaf.
    it_partitions = {
        partition.variable: partition
        for partition in partitions
        if partition.variable in _IT_RECURRENT_QUESTION_OWNERS
    }
    assert set(it_partitions) == set(_IT_RECURRENT_QUESTION_OWNERS)
    for native, (leaf, literals) in _IT_RECURRENT_QUESTION_OWNERS.items():
        owner = f"{native}.{leaf}"
        assert dict(it_partitions[native].columns) == dict.fromkeys(literals, owner)
    # Y-311: the 74 reviewed Innovation i foretag (257) native-question identity
    # families each own every listed literal through one native-question leaf.
    innovation_partitions = {
        partition.variable: partition
        for partition in partitions
        if partition.variable in _INNOVATION_RECURRENT_QUESTION_OWNERS
    }
    assert set(innovation_partitions) == set(_INNOVATION_RECURRENT_QUESTION_OWNERS)
    for native, (leaf, literals) in _INNOVATION_RECURRENT_QUESTION_OWNERS.items():
        owner = f"{native}.{leaf}"
        assert dict(innovation_partitions[native].columns) == dict.fromkeys(
            literals, owner
        )
    absent_owners = {
        owner
        for partition in partitions
        for owner in partition.columns.values()
        if ("scb", owner) not in slugged
    }
    assert absent_owners == set()


def test_repo_iot_y310_separates_calculation_bases(repo_tree: CurationTree) -> None:
    # Y-310: 58 IoT native families each deliver three calculation bases, not
    # spelling aliases: individual X (variant 25.763), ordinary household XHB and
    # modelled shared-residence household XVXHB (both variant 25.1153). The map,
    # the 174 qualified naming leaves and the retained unsplit naming are exact,
    # so a wrong or new literal cannot silently join a branch.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    slugged = {
        (entry.provider, entry.source_id)
        for entry in entries
        if entry.kind == "variable" and entry.slug is not None
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    by_variable = {
        partition.variable: partition for partition in iot.identity.partition
    }
    expected_maps: dict[str, dict[str, str]] = {}
    expected_names: dict[str, str] = {}
    for native, (literal, stem, _) in _IOT_Y310_CALCULATION_BASES.items():
        expected_maps[native] = {
            literal: f"{native}.individ",
            f"{literal}HB": f"{native}.hushall",
            f"{literal}VXHB": f"{native}.hushall-vaxelvis-boende",
        }
        for leaf in ("individ", "hushall", "hushall-vaxelvis-boende"):
            expected_names[f"{native}.{leaf}"] = f"{stem}-{leaf}"
    assert len(expected_maps) == 58
    assert len(expected_names) == 174
    assert set(expected_maps) <= set(by_variable)
    for native, columns in expected_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner)
        for columns in expected_maps.values()
        for owner in columns.values()
    } <= slugged
    named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in expected_names
    }
    assert named == expected_names
    # The unsplit native naming stays; only the calculation bases are split.
    base_slugs = {
        entry.source_id: entry.slug
        for entry in entries
        if entry.kind == "variable"
        and entry.provider == "scb"
        and entry.source_id in _IOT_Y310_CALCULATION_BASES
    }
    assert base_slugs == {
        native: stem for native, (_, stem, _) in _IOT_Y310_CALCULATION_BASES.items()
    }
    # Y-315: eight IoT household-only native families each deliver an ordinary
    # household HB literal and a modelled shared-residence VXHB literal under the
    # same variant 25.1153. The ordinary owner keeps the existing
    # household-native slug; the VX owner adds the explicit -vaxelvis-boende
    # suffix. The map, the 16 leaves and the retained unsplit naming are exact, so
    # a wrong or new literal cannot silently join a branch.
    pair_maps: dict[str, dict[str, str]] = {}
    pair_names: dict[str, str] = {}
    for native, (hb, vx, stem) in _IOT_Y315_HOUSEHOLD_PAIRS.items():
        pair_maps[native] = {
            hb: f"{native}.{stem}",
            vx: f"{native}.{stem}-vaxelvis-boende",
        }
        pair_names[f"{native}.{stem}"] = stem
        pair_names[f"{native}.{stem}-vaxelvis-boende"] = f"{stem}-vaxelvis-boende"
    assert len(pair_maps) == 8
    assert len(pair_names) == 16
    for native, columns in pair_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner) for columns in pair_maps.values() for owner in columns.values()
    } <= slugged
    pair_named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in pair_names
    }
    assert pair_named == pair_names
    # Y-316: nine IoT employer-reporting native families each deliver a pre-2019
    # annual-KU literal and a post-2019 monthly-AGI + remaining-annual-KU literal
    # under variant 25.763. The old owner keeps the existing native slug; the new
    # owner adds the explicit -agi-ku suffix. The map, the 18 leaves and the
    # retained unsplit naming are exact, so a wrong or new literal cannot
    # silently join a branch.
    source_maps: dict[str, dict[str, str]] = {}
    source_names: dict[str, str] = {}
    for native, (old, new, stem) in _IOT_Y316_SOURCE_BASIS_PAIRS.items():
        source_maps[native] = {
            old: f"{native}.{stem}",
            new: f"{native}.{stem}-agi-ku",
        }
        source_names[f"{native}.{stem}"] = stem
        source_names[f"{native}.{stem}-agi-ku"] = f"{stem}-agi-ku"
    assert len(source_maps) == 9
    assert len(source_names) == 18
    for native, columns in source_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner) for columns in source_maps.values() for owner in columns.values()
    } <= slugged
    source_named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in source_names
    }
    assert source_named == source_names


def test_scb_errata_repeated_version_raises_curation_error(tmp_path: Path) -> None:
    # A version named twice in one `versions` list would mint the same synthetic
    # row twice and die on the id collision mid-insert. Catch it at load, where
    # the maintainer gets a remediation instead of a primary-key error.
    body = (
        "[[errata.delivered]]\n"
        'variant = "individer-15plus"\n'
        'column = "DispInkKE"\n'
        'versions = ["2010", "2011", "2010"]\n'
        'evidence = "SWECOV holds the column in those years"\n'
        'noted = "2026-09-11"\n'
    )
    root = write_lisa_errata(tmp_path / "curation", body)
    with pytest.raises(RegMetaError) as exc:
        load_scb_errata(root, repo_slug_dir())
    assert exc.value.code == "scb_errata_invalid"
    assert exc.value.exit_code == EXIT_CONFIG
    assert "2010" in exc.value.message
    assert "once" in exc.value.remediation


def test_repo_doc_sources_parses() -> None:
    # `doc_sources.toml` (#372) maps a doc `source` slug → public SCB PDF; a
    # missing `url`/`title` key would otherwise surface only at doc-DB build
    # time. `load_doc_sources` resolves its own path relative to __file__, so
    # the in-repo file is what's exercised here. URL RESOLUTION (the PDFs 200)
    # is out of scope — load-time shape is this gate.
    sources = load_doc_sources()
    assert sources  # the #372 LISA source map ships with the repo
    assert all(entry["url"] and entry["title"] for entry in sources.values())


def test_repo_related_documents_parses() -> None:
    # `related_documents.toml` (#740) maps register-version related-document
    # binaries to provenance. Binary existence is maintainer-build territory
    # because PDFs are gitignored; this gate locks the tracked map shape.
    docs = load_related_documents(_ROOT / "related_documents.toml")
    assert "aes" in docs
    assert len(docs["aes"]) == 5
    assert all(
        doc.title and doc.filename and doc.license and doc.sha256 and doc.byte_size
        for doc in docs["aes"]
    )


def test_doc_sources_malformed_entry_raises_curation_error() -> None:
    # A missing/empty/wrong-type `url`/`title` is an actionable config error
    # (EXIT_CONFIG), not a bare KeyError. `load_doc_sources` resolves the repo
    # file by __file__, so exercise its per-entry validator directly.
    for bad in ({"title": "T"}, {"url": "", "title": "T"}, {"url": 1, "title": "T"}):
        with pytest.raises(RegMetaError) as exc_info:
            _require_doc_source_str(bad, "url", "some-slug")
        assert exc_info.value.code == "doc_sources_invalid"
        assert exc_info.value.exit_code == EXIT_CONFIG
        assert "some-slug" in exc_info.value.message


def test_related_documents_malformed_entry_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "../bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert exc_info.value.exit_code == EXIT_CONFIG
    assert "filename" in exc_info.value.message


def test_related_documents_invalid_license_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "unknown"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "license" in exc_info.value.message


def test_related_documents_noncanonical_fetched_date_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "20260623"\n'
        'sha256 = "0000000000000000000000000000000000000000000000000000000000000000"\n'
        "byte_size = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "fetched" in exc_info.value.message


def test_related_documents_missing_document_array_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        '[register.aes]\ndocuments = "bad"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "unknown field" in exc_info.value.message


def test_related_documents_unknown_top_level_key_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[registr.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "top-level" in exc_info.value.message


def test_repo_sun2020_levels_and_grouping_detail_have_exact_owners(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    expected = {
        "47.65": {
            "SUNInr1": "47.65.suninr-1",
            "SUNInr2": "47.65.suninr-2",
            "SunInr3": "47.65.suninr-3",
            "SUNInr3": "47.65.suninr-3",
            "SUNInr": "47.65.suninr-4",
        },
        "34.6416": {
            "Sun2020Grp": "34.6416.sun2020grp",
            "Sun2020Grp_Detalj": "34.6416.sun2020grp-detalj",
        },
    }
    partitions = [
        entry
        for register in tree.registers
        for entry in register.identity.partition
        if entry.variable in expected
    ]
    assert len(partitions) == 2
    assert {entry.variable: dict(entry.columns) for entry in partitions} == expected
    assert all(not entry.unassigned_columns for entry in partitions)

    hreg = expected["47.65"]
    assert hreg["SunInr3"] == hreg["SUNInr3"]
    assert len(set(hreg.values())) == 4
    lisa = expected["34.6416"]
    assert lisa["Sun2020Grp"] != lisa["Sun2020Grp_Detalj"]

    owners = {owner for columns in expected.values() for owner in columns.values()}
    assert len(owners) == 6
    slugs = {owner: owner.split(".", 2)[2] for owner in owners}
    declarations = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in owners | {"47.1315"}
    }
    assert declarations == {
        **slugs,
        "47.1315": "utbildnings-inriktning-sun-2000",
    }
    assert "47.1315" not in expected
    assert all(not owner.startswith("47.1315.") for owner in owners)

    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    named = {
        entry.source_id: entry.slug
        for entry in entries
        if entry.provider == "scb"
        and entry.kind == "variable"
        and entry.source_id in owners | {"47.65", "34.6416", "47.1315"}
    }
    assert named == {
        **slugs,
        "47.65": "suninr",
        "34.6416": "sun2020grp",
        "47.1315": "utbildnings-inriktning-sun-2000",
    }
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in owners)} == {
        f"scb/{owner}": slug for owner, slug in slugs.items()
    }
    assert snapshot["scb/47.65"] == "suninr"
    assert snapshot["scb/34.6416"] == "sun2020grp"
    assert snapshot["scb/47.1315"] == "utbildnings-inriktning-sun-2000"


def test_repo_fee_bases_and_hreg_representations_keep_distinct_owners(
    repo_tree: CurationTree,
) -> None:
    # Source descriptions distinguish total/component bases, code/text, CSN/SCB
    # classifications and two fee indicators with different blank-code meanings.
    # Literal reuse on another native identity must not broaden these maps.
    expected = {
        "25.1213": {
            "AEPAVG": "25.1213.underlag-efterlevande-pensionsavgift",
            "AEPAVG1": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-1",
            "AEPAVG2": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-2",
            "AEPAVG3": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-3",
        },
        "25.1218": {
            "ASJUKM": "25.1218.sjukforsakringsavg-lag-inkomst",
            "ASJUKM1": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-1",
            "ASJUKM2": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-2",
            "ASJUKM3": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-3",
        },
        "25.1220": {
            "ASKAD": "25.1220.underlag-arbetsskadeavgift",
            "ASKAD1": "25.1220.underlag-arbetsskadeavgift-delperiod-1",
            "ASKAD2": "25.1220.underlag-arbetsskadeavgift-delperiod-2",
            "ASKAD3": "25.1220.underlag-arbetsskadeavgift-delperiod-3",
        },
        "25.21834": {
            "ASFAVG": "25.21834.asfavg",
            "ASFAVG1": "25.21834.asfavg-delperiod-1",
            "ASFAVG2": "25.21834.asfavg-delperiod-2",
            "ASFAVG3": "25.21834.asfavg-delperiod-3",
        },
        "47.1768": {"ExTyp": "47.1768.extyp", "ExTypGrpText": "47.1768.extypgrptext"},
        "47.5249": {"Niva": "47.5249.niva-csn", "Nivå": "47.5249.niva-scb"},
        "47.29830": {
            "StudieAvg": "47.29830.studieavg",
            "AvgSkyldig": "47.29830.avgskyldig",
        },
    }
    names = {
        owner: owner.split(".", 2)[2]
        for native, columns in expected.items()
        if native.startswith("25.")
        for owner in columns.values()
    } | {
        "47.1768.extyp": "examenstyp-kod",
        "47.1768.extypgrptext": "examenstyp-klartext",
        "47.5249.niva-csn": "niva-pa-utlandsstudier-csn",
        "47.5249.niva-scb": "niva-pa-utlandsstudier-scb",
        "47.29830.studieavg": "avgiftsskyldighet-studieavg",
        "47.29830.avgskyldig": "avgiftsskyldighet-avgskyldig",
    }
    tree = repo_tree
    partitions = {
        entry.variable: entry
        for register in tree.registers
        for entry in register.identity.partition
    }
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == len(columns)
    declarations = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in names
    }
    assert declarations == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # ASKAD is also delivered as compensation; Niva also labels another variable.
    for negative in ("25.1594", "47.1279", "47.29874"):
        assert negative not in expected
        assert all(not owner.startswith(f"{negative}.") for owner in names)
    assert snapshot["scb/25.1594"] == "arbetsskadeersattning"
    assert snapshot["scb/47.29874"] == "programinriktning"


def test_repo_iot_disposable_income_keeps_capital_gain_exclusion_distinct(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partition = next(p for p in iot.identity.partition if p.variable == "25.1304")
    assert dict(partition.columns) == {
        "CDISP": "25.1304.cdisp",
        "CDISP5": "25.1304.cdisp5",
        "DIN83": "25.1304.din83",
        "DIN84": "25.1304.din83",
        "DIN86": "25.1304.din83",
        "DIN88K": "25.1304.din88k",
        "DIN91": "25.1304.din91",
        "DIND": "25.1304.dind",
        "DINKD": "25.1304.dinkd",
        "DINU82": "25.1304.dinu82",
    }
    names = {entry.native_id: entry.slug for entry in iot.variable}
    excluded = "delkomponent-disponibel-inkomst-exkl-kapitalvinst"
    assert names["25.1304.cdisp5"] == excluded
    assert names["25.1304.cdisp"] == "delkomponent-disponibel-inkomst"
    assert names["25.22307"] == "disponibel-inkomst-exkl-kapitalvinst"
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert snapshot["scb/25.1304.cdisp5"] == excluded
    group = next(g for g in iot.group if g.key == "disponibel-inkomst")
    member = next(
        m
        for m in group.members
        if m.variable == excluded and m.delivery_column == "CDISP5"
    )
    assert member.coords is not None
    assert [(c.axis, c.value) for c in member.coords] == [
        ("enhet", "individ"),
        ("hushallsbegrepp", "na"),
        ("kapitalvinst", "exkl"),
    ]
    assert any(
        m.variable == names["25.22307.individ"] and m.delivery_column == "CDISP5"
        for m in group.members
    )


def test_repo_iot_per_adult_income_retains_supplied_definition_bases(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partition = next(p for p in iot.identity.partition if p.variable == "25.2575")
    expected = {
        "CDISPP": "25.2575.cdispp",
        "DINKPP": "25.2575.dinkpp",
        "DINPP": "25.2575.dinpp",
        "DINPP81": "25.2575.definition-1981",
        "DINPP82": "25.2575.kapital-ranta-utdelning",
        "DINPP83": "25.2575.kapital-ranta-utdelning",
        "DINPP84": "25.2575.definition-1984",
        "DINPP86": "25.2575.definition-1986",
        "DINPP88K": "25.2575.dinpp88k",
        "DINPP91": "25.2575.dinpp91",
        "DINUPP": "25.2575.dinupp",
    }
    assert dict(partition.columns) == expected
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    names = {owner: snapshot[f"scb/{owner}"] for owner in expected.values()}
    assert names["25.2575.dinpp"] == "dinpp"
    assert names["25.2575.dinpp88k"] == "dinpp88k"
    declared = {entry.native_id: entry.slug for entry in iot.variable}
    for owner in (
        "25.2575.definition-1981",
        "25.2575.kapital-ranta-utdelning",
        "25.2575.definition-1984",
        "25.2575.definition-1986",
    ):
        assert declared[owner] == names[owner]
    group = next(g for g in iot.group if g.key == "disponibel-inkomst")
    for column, owner in expected.items():
        assert snapshot[f"scb/{owner}"] == names[owner]
        member = next(
            m
            for m in group.members
            if m.variable == names[owner] and m.delivery_column == column
        )
        assert member.coords is not None
        assert [(c.axis, c.value) for c in member.coords] == [
            ("enhet", "per-vuxen"),
            ("hushallsbegrepp", "familj"),
            ("kapitalvinst", "inkl"),
        ]


def test_repo_iot_income_renames_retain_the_2019_source_basis_boundary(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "25.591": {
            "PKUAPEN": "25.591.tjanstepension-tjanst",
            "KUPENS": "25.591.tjanstepension-tjanst",
            "PAPEN": "25.591.tjanstepension-tjanst-agi-ku",
        },
        "25.1192": {
            "TKUERS": "25.1192.ovriga-kostnadsersattningar",
            "KUERS": "25.1192.ovriga-kostnadsersattningar",
            "TERS": "25.1192.ovriga-kostnadsersattningar-agi-ku",
        },
        "25.1193": {
            "TKUHOBB": "25.1193.ersattning-grund-egenavgifter-tjanst",
            "THOBB": "25.1193.ersattning-grund-egenavgifter-tjanst-agi-ku",
            "KUHOBBY": "25.1193.ersattning-grund-egenavgifter-tjanst",
        },
        "25.1194": {
            "KULON": "25.1194.kontant-bruttolon-mm",
            "TKULON": "25.1194.kontant-bruttolon-mm",
            "TLON": "25.1194.kontant-bruttolon-mm-agi-ku",
        },
        "25.1196": {
            "KUOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst",
            "TKUOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst",
            "TOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst-agi-ku",
        },
        "25.21816": {
            "SAKU": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare",
            "SKUARB": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare",
            "SARB": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare-agi-ku",
        },
        "25.35587": {"SKSJO": "25.35587.sjomansskatt", "SSJO": "25.35587.sjomansskatt"},
        "25.40060": {
            "INFAST": "25.40060.inkomst-av-annan-fastighet",
            "INAF": "25.40060.inkomst-av-annan-fastighet",
            "INSFAST": "25.40060.inkomst-av-annan-fastighet",
        },
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partitions = {entry.variable: entry for entry in iot.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        owners = set(columns.values())
        if native in {"25.35587", "25.40060"}:
            assert len(owners) == 1
        else:
            assert len(owners) == 2
            assert sum(owner.endswith("-agi-ku") for owner in owners) == 1
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # KUPENS and TOVR also occur on other native variables; those stay separate.
    assert not any(owner.startswith(("25.18384.", "25.30862.")) for owner in names)


def test_repo_rtb_contexts_preserve_native_aliases_and_distinct_roles(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "2.19": {"ARegion": "2.19.a-region", "AReg": "2.19.a-region"},
        "2.250": {
            "CivDatGRel": "2.250.civildat-tidigare-rel",
            "CivilDatGRel": "2.250.civildat-tidigare-rel",
        },
        "2.251": {
            "CivilG": "2.251.civilstand-tidigare",
            "TidCivil": "2.251.civilstand-tidigare",
        },
        "2.267": {
            "KonRel": "2.267.kon-relationsperson",
            "KonMakPart": "2.267.kon-make-maka-partner",
        },
        "2.272": {
            "VarGamCiv": "2.272.civilstand-tidigare-varaktighet",
            "AktVar": "2.272.civilstand-tidigare-varaktighet",
            "Aktvar": "2.272.civilstand-tidigare-varaktighet",
        },
        "2.293": {
            "AntDodFoddTot": "2.293.antal-dodfodda",
            "AntDodFodd": "2.293.antal-dodfodda",
        },
        "2.302": {
            "FodDat": "2.302.fodelsedatum",
            "FodelseDatumPnr": "2.302.fodelsedatum-personnummer",
        },
        "2.3195": {"HRegion": "2.3195.h-region", "HReg": "2.3195.h-region"},
        "2.40288": {
            "StorstOmr": "2.40288.storstadsomrade",
            "StorStadsOmr": "2.40288.storstadsomrade",
        },
    }
    tree = repo_tree
    rtb = next(
        register for register in tree.registers if register.register_info.slug == "rtb"
    )
    partitions = {entry.variable: entry for entry in rtb.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (2 if native in {"2.267", "2.302"} else 1)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in rtb.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # TidCivil on the current-status native variable cannot join previous status.
    assert "2.15" not in expected
    assert not any(owner.startswith("2.15.") for owner in names)
    assert snapshot["scb/2.15.tidcivil"] == "tidpunkt-civilstand"


def test_repo_hreg_source_constructs_preserve_event_anchors_and_aliases(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "47.44": {"Kon": "47.44.kon", "Kon2": "47.44.kon", "kon": "47.44.kon"},
        "47.73": {"Ar": "47.73.ar", "KAr": "47.73.ar-for-tillgodoraknande"},
        "47.326": {"Namn": "47.326.namn", "FSLNamn": "47.326.namn"},
        "47.1280": {
            "Pomf": "47.1280.poangomfattning",
            "Omfattning": "47.1280.tillgodoraknad-omfattning-forskarniva",
        },
        "47.1375": {"ExDatum": "47.1375.examensdatum", "Datum": "47.1375.examensdatum"},
        "47.1465": {
            "UtbytStud": "47.1465.utbytesstudier",
            "Typ": "47.1465.typ-av-utlandsstudier-csn",
        },
        "47.1537": {
            "ar": "47.1537.ar-for-examensbevis",
            "Kar": "47.1537.ar-for-examensbevis",
        },
        "47.1557": {
            "Ar": "47.1557.kalenderar",
            "Kar": "47.1557.kalenderar-for-examensbevis",
        },
        "47.1565": {
            "AvhoppDat": "47.1565.datum-for-studieavbrott",
            "AvbrDatum": "47.1565.datum-for-studieavbrott",
        },
        "47.38118": {
            "GenomHsKod": "47.38118.medverkande-hogskola",
            "MedverkHsKod": "47.38118.medverkande-hogskola",
        },
    }
    tree = repo_tree
    hreg = next(
        register for register in tree.registers if register.register_info.slug == "hreg"
    )
    partitions = {entry.variable: entry for entry in hreg.identity.partition}
    split_constructs = {"47.73", "47.1280", "47.1465", "47.1557"}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (2 if native in split_constructs else 1)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in hreg.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # Literal-only evidence cannot separate age anchors or establish ambiguous
    # examination/program representations. These await a different decision.
    for native in ("47.241", "47.1715", "47.29874"):
        assert native not in expected
        assert not any(owner.startswith(f"{native}.") for owner in names)


def test_repo_iot_operational_definitions_keep_slots_points_and_native_scope(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "25.21523": {
            "TKULONSF": "25.21523.vissa-ej-skattepliktiga-ersattningar",
            "IKUSF": "25.21523.vissa-ej-skattepliktiga-ersattningar",
            "TLONSF": "25.21523.vissa-ej-skattepliktiga-ersattningar-agi-ku",
        },
        "25.30856": {
            "BSAMF1": "25.30856.samfundskod-samfund-1",
            "BSAMF2": "25.30856.samfundskod-samfund-2",
        },
        "25.39924": {
            "INDTJ1": "25.39924.inkomst-av-tjanst-punkt-1",
            "INDTJ2": "25.39924.inkomst-av-tjanst-punkt-2",
        },
        "25.40427": {
            "IDMANT": "25.40427.folkbokforingsforhallande",
            "KBH": "25.40427.folkbokforingsforhallande",
        },
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partitions = {entry.variable: entry for entry in iot.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (1 if native == "25.40427" else 2)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {f"scb/{owner}": snapshot[f"scb/{owner}"] for owner in names} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # The 2010 preliminary/final name boundary stays within the historical
    # construct. Post-2019 source basis and the co-delivered slots/points differ.
    assert expected["25.21523"]["IKUSF"] == expected["25.21523"]["TKULONSF"]
    assert expected["25.21523"]["TLONSF"] != expected["25.21523"]["TKULONSF"]
    # IDMANT/KBH also describe a different native parish variable (1 November).
    assert "25.24373" not in expected
    assert all(not owner.startswith("25.24373.") for owner in names)
    assert snapshot["scb/25.24373"] == "forsamling-den-1-11"
    assert snapshot["scb/25.30856"] == "bsamf"
    assert snapshot["scb/25.39924"] == "indtj"


def test_repo_workplace_employment_keeps_exact_bas_editions_separate(
    repo_tree: CurationTree,
) -> None:
    lisa = next(r for r in repo_tree.registers if r.register_info.native_id == "34")
    selectors = [p for p in lisa.identity.column_owner if p.variable == "34.15532"]
    assert len(selectors) == 5
    assert not any(p.variable == "34.15532" for p in lisa.identity.partition)
    for selector in selectors:
        assert selector.source_editions
        if selector.owner == "34.15532.bas":
            assert set(selector.source_editions) == {"2022", "2023"}
        else:
            assert selector.owner == "34.15532.fore-bas"
            assert not {"2022", "2023"} & set(selector.source_editions)
    assert {(p.variant, p.column) for p in selectors} == {
        ("34.151", "Ast_AntalSys"),
        ("34.153", "AntalSys"),
        ("34.1335", "AntalSys"),
    }


def test_repo_rtb_roles_and_date_granularity_remain_distinct(
    repo_tree: CurationTree,
) -> None:
    rtb = next(r for r in repo_tree.registers if r.register_info.native_id == "2")
    maps = {p.variable: dict(p.columns) for p in rtb.identity.partition}
    country = maps["2.16234"]
    assert country["FlandLan"] == country["FLandLan"] == country["FLandlan"]
    assert country["FLandLanMor"] != country["FLandLan"]
    assert maps["2.332"]["SenInvAr"] != maps["2.332"]["DatInv"]
    week = maps["2.40255"]
    assert week["BearbArVecka"] == week["BearbArvecka"] == week["BeArbArVecka"]
    assert week["BearbVecka"] == week["BearbVecka1"] != week["BearbArVecka"]
    assert maps["2.339"]["FlyttGrans"] != maps["2.339"]["Posttyp"]


def test_repo_identity_splits_preserve_public_dependency_targets(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
    }
    assert names["34.17.arbetsstallenummer"] == "arbetsstallenummer"
    assert (
        names["258.44742.kommersiell-modifierad"] == "ai-forvarv-kommersiell-modifierad"
    )
    assert (
        names["258.44742.oppen-kallkod-modifierad"]
        == "ai-anvands-oppen-kallkod-modifierad"
    )
    assert names["34.667.arbetsstallekommun"] == "kommun-for-arbetsstalle"
    assert names["25.21515.inkl-kapitalvinst"] == "delkomponent-disponibel-inkomst-2004"
    assert names["25.22306.individ"] == "disponibel-inkomst-exkl-kapvinst-2004"
    assert names["25.22307.individ"] == "disponibel-inkomst-exkl-kapitalvinst"
    assert not {"34.17", "34.667", "25.21515"} & names.keys()
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    for owner in (
        "34.17.arbetsstallenummer",
        "258.44742.kommersiell-modifierad",
        "258.44742.oppen-kallkod-modifierad",
        "34.667.arbetsstallekommun",
        "25.21515.inkl-kapitalvinst",
        "25.22306.individ",
        "25.22307.individ",
    ):
        assert snapshot[f"scb/{owner}"] == names[owner]
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    group = next(group for group in iot.group if group.key == "disponibel-inkomst")
    assert {
        member.variable
        for member in group.members
        if member.delivery_column == "CDISP04"
    } == {names["25.21515.inkl-kapitalvinst"]}


def test_repo_reviewed_parallel_columns_keep_exact_wave_intersections(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    registers = {register.register_info.slug: register for register in tree.registers}
    innovation = registers["innovation-foretag"]
    hreg = registers["hreg"]
    assert len(innovation.representation.parallel) == 128
    assert (
        sum(
            e.column_metadata == "per_column"
            for e in innovation.representation.parallel
        )
        == 118
    )
    assert (
        sum(
            e.coding_metadata == "per_column"
            for e in innovation.representation.parallel
        )
        == 34
    )
    (koncern_2008,) = [
        entry
        for entry in innovation.representation.parallel
        if entry.variable == "257.4045.koncernmarkering"
        and entry.valid_from == "2008-01-01"
    ]
    assert koncern_2008.valid_to == "2008-12-31"
    assert koncern_2008.column_metadata == "per_column"
    assert koncern_2008.coding_metadata == "shared"
    assert {column.column for column in koncern_2008.columns} == {"A1", "GP"}
    assert len(hreg.representation.parallel) == 1
    for register in (innovation, hreg):
        for entry in register.representation.parallel:
            assert entry.valid_from == max(c.valid_from for c in entry.columns)
            assert entry.valid_to == min(c.valid_to for c in entry.columns)
            assert len({c.column for c in entry.columns}) == len(entry.columns)
            partitions = [
                p
                for p in register.identity.partition
                if entry.variable in p.columns.values()
            ]
            for column in entry.columns:
                owners = {
                    p.columns[column.column]
                    for p in partitions
                    if column.column in p.columns
                }
                owners.update(
                    e.owner
                    for e in register.identity.column_owner
                    if e.owner == entry.variable and e.column == column.column
                )
                assert owners == {entry.variable}
    participating = hreg.representation.parallel[0]
    assert participating.variable == "47.38118.medverkande-hogskola"
    assert (participating.valid_from, participating.valid_to) == (
        "2008-01-01",
        "2008-12-31",
    )
    assert {c.column for c in participating.columns} == {"GenomHsKod", "MedverkHsKod"}

    metadata = [
        entry
        for register in tree.registers
        for entry in register.representation.delivery_metadata
    ]
    assert len(metadata) == 38
    assert Counter(tuple(entry.fields) for entry in metadata) == {
        ("description",): 36,
        ("name", "description"): 2,
    }


def test_repo_reviewed_matrix_activations_pin_exact_evidence(
    repo_tree: CurationTree,
) -> None:
    from reg_meta_build.cis2016_matrix import load_matrix

    register = next(
        r for r in repo_tree.registers if r.register_info.slug == "innovation-foretag"
    )
    declarations = register.representation.matrix
    assert [
        (d.source_mode, d.selector.edition, d.selector.regver_id, d.selector.cvid)
        for d in declarations
    ] == [
        ("documented_blank", "2012 - 2014", 7293, 400684),
        ("named", "2014 - 2016", 11529, 469456),
    ]
    for declaration, count in zip(declarations, (45, 54), strict=True):
        matrix = load_matrix(
            _CURATION / declaration.evidence_file,
            source_mode=declaration.source_mode,
            expected_selector=declaration.selector,
        )
        assert matrix.selector.register_id == 257
        assert matrix.selector.register_variant_id == 553
        assert matrix.selector.var_id == 15662
        assert len(matrix.answers) == count
