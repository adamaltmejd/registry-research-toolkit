# D field carry-over audit

Deleted at milestone F (cutover), with the one-shot converters.

This supplements `rebuild_convert_report.md`; historical stage counts are not additive.
Only source metadata is listed, never holdings or individual records.

## Outcomes

- Delivery aliases: all 44 `enrichment.alias` conversions have been undone, including
  their 44 premise additions. They are dropped with the reason **delivery-header
  mapping; home: E holdings layer**. `set synonyms` remains available for actual search
  synonyms.
- Local sentinels: nine of 15 entries have exclusively scoped hashes and are converted
  to set-wide setters. Four Lisa AstLan claims are already applied set-wide through the
  original LKF1992/LKF1993 lists, wider than their local scope; no additional scoped
  setter is added. Two IoT claims remain deferred because their hashes are shared
  outside scope. Exact references are below.
- Classification sentinels: all 63 lists equal their legacy declarations, with 490
  code/version pairs and one allowed unobserved pair. Lists are additive on shared bound
  sets; they never add items to canonical books. A listed code that is canonical in
  another bound classification version refuses. The 22 fix-02 LKF additions are
  reverted. Meaning differences have no distinct schema home; counts and exceptional
  labels are retained below for review.
- Roles and unit types: no legacy key named `role` or `unit_type` exists. No values are
  inferred. Unit types are table-only; variable inheritance and its override were
  removed.
- Names: all 83 classification names remain preserved. All six Socialstyrelsen register
  corrections equal the source workbook title after the reader's whitespace trimming, so
  they are dropped as already satisfied. No register grammar extension is needed.
- Twelve `source_attribution` corrections remain dropped: outside these five fields,
  with no four-kind field. Three authored GDB table names lack existing table
  identities.
- Other classification presentation fields retain their named #2016 inventory-contract
  omission reason; classification aliases are not variable search synonyms.

## Delivery-header mappings

Every row below is dropped: **delivery-header mapping; home: E holdings layer**. The
previous search-synonym statements and their locks were removed. The one-shot converter
now emits the same named drop explicitly.

  | Legacy source                                                        | Previous variable target                     | Outcome                                                  |
  | -------------------------------------------------------------------- | -------------------------------------------- | -------------------------------------------------------- |
  | `reg_meta_build/curation/registers/scb/agi.toml:708`                 | `var:bostadsf-smahus-ul-ag-ag`               | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/agi.toml:713`                 | `var:bostadsf-smahus-ul-ag-ea`               | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:138`                 | `var:aktier-och-andelar`                     | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:143`                 | `var:bidrag-for-verksamheten`                | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:148`                 | `var:byggnader`                              | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:153`                 | `var:foradlingsvarde`                        | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:158`                 | `var:forandring-lager-pagaende-arbete`       | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:163`                 | `var:forbrukningsvarde`                      | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:168`                 | `var:kostnader-loner-ersattningar`           | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:173`                 | `var:kostnader-ovriga-sociala-avgifter`      | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:178`                 | `var:kundfordringar`                         | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:183`                 | `var:mark`                                   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:188`                 | `var:medelantal-anstallda`                   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:193`                 | `var:nettoomsattning`                        | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:198`                 | `var:nettoomsattning-justerad-merchanting`   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:203`                 | `var:ovriga-personalkostnader`               | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:208`                 | `var:ovriga-kortfristiga-placeringar`        | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:213`                 | `var:pensionskostnader`                      | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:218`                 | `var:produktionsvarde`                       | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:223`                 | `var:rorelseresultat`                        | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:228`                 | `var:rorelseresultat-fore-avskrivningar`     | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:233`                 | `var:skatteskulder`                          | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:238`                 | `var:sociala-kostnader-personalkostnader`    | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:243`                 | `var:summa-bruttoinvesteringar`              | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:248`                 | `var:summa-eget-kapital`                     | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:253`                 | `var:summa-kostnader-ravaror-handelsvaror`   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:258`                 | `var:summa-nettoinvesteringar`               | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:263`                 | `var:utdelning`                              | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fek.toml:268`                 | `var:varulager`                              | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fou.toml:14594`               | `var:redovisningsgrupp`                      | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fou.toml:14599`               | `var:sektor`                                 | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fou.toml:14604`               | `var:v700-1`                                 | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/fou.toml:14609`               | `var:v700-2`                                 | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/gymnasieskola-betyg.toml:47`  | `var:kurs`                                   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7794` | `var:eco-enu`                                | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7799` | `var:eco-ext`                                | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7804` | `var:eco-rea`                                | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7809` | `var:eco-rep`                                | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7814` | `var:j20c`                                   | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7819` | `var:miljoinno-fororeningar-slutkonsumtion`  | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7824` | `var:miljoinnovation-atervinning-avfall`     | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7829` | `var:miljoinnovation-fororeningar-foretaget` | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7834` | `var:miljoinnovation-materialersattning`     | dropped: delivery-header mapping; home: E holdings layer |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7839` | `var:miljoinnovation-minskad-materialatgang` | dropped: delivery-header mapping; home: E holdings layer |

## Local sentinels

Read-only audit of the retained 0.66.0 catalog: `release-0.66.0/ev/reg_meta.db.zst`.
Each original variant was matched through its source table title and current ledger;
column, source key and every original date window were retained. All matching
occurrences and direct/population value-set alternatives were examined. All target
code-label pairs exist exactly. References to each selected hash were searched across
all published occurrences, including population alternatives. End dates below are
exclusive, as in the published schema.

  | Legacy source                                          | Subject/window                                                            | Hash                                                               | Outcome                                                                                                                                                       |
  | ------------------------------------------------------ | ------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `reg_meta_build/curation/registers/scb/iot.toml:4770`  | `individer-och-dodsbon: KUSNI @1987 where key = 'v21073'`                 | `dedd3dbf1a994761529bdd58117cc6b3cd4eaac2d05310bb1b6d46acdf2460bb` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/iot.toml:4781`  | `individer-och-dodsbon: KUSNI [['1988-01-01', '1988-12-31']]`             | `5b16fe377f6c25cb14a3643284ac37a6e5a03c80878f2b79e5846d67e3ebfaba` | dropped: 13 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed |
  | `reg_meta_build/curation/registers/scb/iot.toml:4792`  | `individer-och-dodsbon: KUSNI [['1989-01-01', '1989-12-31']]`             | `57b4e746ca4d6f9a8f1400dbaa3c0e591d7e60f31b70275a37af7ca2d9d2bce8` | dropped: 2 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed  |
  | `reg_meta_build/curation/registers/scb/iot.toml:4868`  | `individer-och-dodsbon: BNGR @2003 where key = 'v2702'`                   | `26e0cd85f2342485158ab07463a9b02dd3584a34918aa9c81f5a782e7d4e2f30` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/iot.toml:5035`  | `individer-och-dodsbon: BSUN @1996 where key = 'v8189'`                   | `88a9c47ebb19655da9e2064292d4c23ece369a19879e3f2c3c4239b42718212b` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/iot.toml:5045`  | `individer-och-dodsbon: MAKEBOST @1991 where key = 'v39851'`              | `8cd2632ac496364e29131f07f6d4c2f294e0520994f8beed85eb362ac6e77701` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/iot.toml:5055`  | `individer-och-dodsbon: TNLAN @1990 where key = 'v39966'`                 | `1bc176d895af8e9195741580bb229eaed66ff48c7d705577b2481a45036fbc24` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/lisa.toml:2633` | `foretag: Org_Sni92 @1990,1991,1992,1993,1994,1995 where key = 'v19054'`  | `fba414b9b471c6191aaaa88ea747556d1f4daadd9bd91fbb4ac7f66a490ccba2` | converted: 6 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3231` | `individer-16plus: KU1AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | applied set-wide through original lkf1992/lkf1993 lists, wider than scoped claim; no added setter                                                             |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3241` | `individer-16plus: KU2AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | applied set-wide through original lkf1992/lkf1993 lists, wider than scoped claim; no added setter                                                             |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3251` | `individer-16plus: KU3AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | applied set-wide through original lkf1992/lkf1993 lists, wider than scoped claim; no added setter                                                             |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3261` | `individer-16plus: AstLan [['1990-01-01', '1990-12-31']]`                 | `52f34f09dcc93bfe814443cbe84c3d4320965d14851b32962b903e5d1c9b43a3` | applied set-wide through original lkf1992/lkf1993 lists, wider than scoped claim; no added setter                                                             |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1317`  | `folkbokforda-personer-kvartal: Forsamling @1986-12-31 where key = 'v31'` | `d9772838ee6e4543c4b1421e357045b0f0072f6cfb6d90e64fdc9270155138c5` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1327`  | `folkbokforda-personer-kvartal: Forsamling @1987-12-31 where key = 'v31'` | `67c8f982e7d06a3e3f88afa2a21df4b93718fee4ff7752d8f37ac3a0722003cf` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1337`  | `folkbokforda-personer-kvartal: Kommun @1987-12-31 where key = 'v36'`     | `e703a6ccdb4ebb315f5141a846c021daea1a35384080a375691f183384bbcedd` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |

Outside-scope references for claims without an additional scoped setter:

- `reg_meta_build/curation/registers/scb/iot.toml:4781`:
  - `scb/linda/panelurval-befolkningen`, `v21073`, `KUSNI`:
    `r16370 [1991-01-01,1992-01-01)`, `r16372 [1992-01-01,1993-01-01)`.
  - `scb/linda/panelurval-taxeringshushall`, `v21073`, `KUSNI`:
    `r16329 [1987-01-01,1988-01-01)`, `r16338 [1988-01-01,1989-01-01)`,
    `r16341 [1989-01-01,1990-01-01)`, `r16342 [1990-01-01,1991-01-01)`,
    `r16368 [1991-01-01,1992-01-01)`, `r16369 [1992-01-01,1993-01-01)`.
  - `scb/linda/tillaggsurval-utrikesfodda-taxeringshushall`, `v21073`, `KUSNI`:
    `r16345 [1988-01-01,1989-01-01)`, `r16346 [1989-01-01,1990-01-01)`,
    `r16347 [1990-01-01,1991-01-01)`, `r16396 [1991-01-01,1992-01-01)`,
    `r16399 [1992-01-01,1993-01-01)`.
- `reg_meta_build/curation/registers/scb/iot.toml:4792`:
  - `scb/fordonsreg/personbilar`, `v4027`, `SNI92`: `r15692 [1990-12-31,1991-01-01)`.
  - `scb/rams/foretag`, `v4027`, `FtgSni69`: `r1857 [1989-01-01,1990-01-01)`.
- `reg_meta_build/curation/registers/scb/lisa.toml:3231`:
  - `scb/lisa/individer-16plus`, `v1936`, `KU1AstLan`: `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31109`, `KU2AstLan`:
    `r15515 [1990-01-01,1991-01-01)`, `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31296`, `KU3AstLan`:
    `r15515 [1990-01-01,1991-01-01)`, `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.
- `reg_meta_build/curation/registers/scb/lisa.toml:3241`:
  - `scb/lisa/individer-16plus`, `v1936`, `KU1AstLan`: `r15515 [1990-01-01,1991-01-01)`,
    `r15514 [1991-01-01,1992-01-01)`, `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31109`, `KU2AstLan`:
    `r15514 [1991-01-01,1992-01-01)`, `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31296`, `KU3AstLan`:
    `r15515 [1990-01-01,1991-01-01)`, `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.
- `reg_meta_build/curation/registers/scb/lisa.toml:3251`:
  - `scb/lisa/individer-16plus`, `v1936`, `KU1AstLan`: `r15515 [1990-01-01,1991-01-01)`,
    `r15514 [1991-01-01,1992-01-01)`, `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31109`, `KU2AstLan`:
    `r15515 [1990-01-01,1991-01-01)`, `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.
  - `scb/lisa/individer-16plus`, `v31296`, `KU3AstLan`:
    `r15514 [1991-01-01,1992-01-01)`, `r15513 [1992-01-01,1993-01-01)`.
- `reg_meta_build/curation/registers/scb/lisa.toml:3261`:
  - `scb/lisa/individer-16plus`, `v473`, `AstLan`: `r15514 [1991-01-01,1992-01-01)`,
    `r15513 [1992-01-01,1993-01-01)`.

## Classification books

The legacy lists name known noncanonical codes observed in bound columns, not items of
the canonical book. `resolved_catalog.py` combines the list with scoped certificates,
and `source_classification_bindings.py::_sentinel_map` retains its meanings. All 63
inventory lists now equal those legacy declarations exactly; the 22 fix-02 additions and
their generic evidence suffixes are removed.

The read-only 0.66.0 audit included all 83 versions: 70 have bindings, referencing 1,495
distinct direct or population-alternative hashes. Of 490 declared code/version pairs,
489 occur in a bound set. The unobserved pair is `class/sektorkod/sektorkod` code `99`;
it is allowed and counted. Absence from a canonical book is expected.

Fix-03's Operator ruling makes lists additive on a shared bound hash. The original 59
version-pair differences across 13 hashes are not conflicts: the disputed codes are
noncanonical in the other bound versions. A code canonical in another version bound to
that hash is a located refusal at both bindings. No version-level list is widened merely
to align the flags of shared sets.

### Blank codes

The literal code `[BLANK]` is listed by `class/lkf/lkf2016`, `class/ssyk/ssyk1996`,
`class/sun-inriktning/sun2000-inriktning` and `class/sun-niva/sun2000-niva`. It is
marked when present in their bound sets. No inventory list or converted local setter
currently declares the actual empty code `""`. The reader preserves code text and does
not translate `[BLANK]` to `""`; an actual empty code remains a real item unless
separately curated.

### Bound-set accounting

  | Classification                            | Legacy declared pairs | Bound sets | Unobserved codes |
  | ----------------------------------------- | --------------------- | ---------- | ---------------- |
  | `class/agarkat/agarkat2000`               | 3                     | 8          | none             |
  | `class/agarkat/agarkat2020`               | 1                     | 5          | none             |
  | `class/isced/isced2011`                   | 2                     | 4          | none             |
  | `class/jurform/jurform2000`               | 2                     | 13         | none             |
  | `class/jurform/jurform2020`               | 1                     | 4          | none             |
  | `class/lkf/lkf1980`                       | 2                     | 5          | none             |
  | `class/lkf/lkf1981`                       | 2                     | 9          | none             |
  | `class/lkf/lkf1986`                       | 4                     | 17         | none             |
  | `class/lkf/lkf1987`                       | 5                     | 17         | none             |
  | `class/lkf/lkf1988`                       | 4                     | 20         | none             |
  | `class/lkf/lkf1989`                       | 4                     | 16         | none             |
  | `class/lkf/lkf1990`                       | 3                     | 15         | none             |
  | `class/lkf/lkf1991`                       | 4                     | 25         | none             |
  | `class/lkf/lkf1992`                       | 6                     | 20         | none             |
  | `class/lkf/lkf1993`                       | 6                     | 17         | none             |
  | `class/lkf/lkf1994`                       | 6                     | 20         | none             |
  | `class/lkf/lkf1995`                       | 6                     | 22         | none             |
  | `class/lkf/lkf1996`                       | 7                     | 30         | none             |
  | `class/lkf/lkf1997`                       | 7                     | 25         | none             |
  | `class/lkf/lkf1998`                       | 7                     | 31         | none             |
  | `class/lkf/lkf1999`                       | 9                     | 36         | none             |
  | `class/lkf/lkf2000`                       | 10                    | 39         | none             |
  | `class/lkf/lkf2001`                       | 11                    | 35         | none             |
  | `class/lkf/lkf2002`                       | 9                     | 34         | none             |
  | `class/lkf/lkf2003`                       | 8                     | 38         | none             |
  | `class/lkf/lkf2004`                       | 10                    | 44         | none             |
  | `class/lkf/lkf2005`                       | 11                    | 55         | none             |
  | `class/lkf/lkf2006`                       | 16                    | 62         | none             |
  | `class/lkf/lkf2007`                       | 10                    | 56         | none             |
  | `class/lkf/lkf2008`                       | 11                    | 54         | none             |
  | `class/lkf/lkf2009`                       | 11                    | 54         | none             |
  | `class/lkf/lkf2010`                       | 12                    | 61         | none             |
  | `class/lkf/lkf2011`                       | 10                    | 58         | none             |
  | `class/lkf/lkf2012`                       | 10                    | 62         | none             |
  | `class/lkf/lkf2013`                       | 11                    | 61         | none             |
  | `class/lkf/lkf2014`                       | 10                    | 56         | none             |
  | `class/lkf/lkf2015`                       | 9                     | 49         | none             |
  | `class/lkf/lkf2016`                       | 15                    | 55         | none             |
  | `class/lkf/lkf2017`                       | 10                    | 57         | none             |
  | `class/lkf/lkf2018`                       | 11                    | 58         | none             |
  | `class/lkf/lkf2019`                       | 9                     | 58         | none             |
  | `class/lkf/lkf2020`                       | 7                     | 64         | none             |
  | `class/lkf/lkf2021`                       | 10                    | 60         | none             |
  | `class/lkf/lkf2022`                       | 12                    | 61         | none             |
  | `class/lkf/lkf2023`                       | 11                    | 50         | none             |
  | `class/lkf/lkf2024`                       | 9                     | 52         | none             |
  | `class/lkf/lkf2025`                       | 5                     | 23         | none             |
  | `class/niva-old/niva-oldv1`               | 5                     | 29         | none             |
  | `class/sektor/sektor2000`                 | 1                     | 7          | none             |
  | `class/sektorkod/sektorkod`               | 2                     | 22         | `"99"`           |
  | `class/sni/sni2002`                       | 11                    | 46         | none             |
  | `class/sni/sni2007`                       | 13                    | 122        | none             |
  | `class/sni/sni1969`                       | 4                     | 48         | none             |
  | `class/sni/sni1992`                       | 5                     | 89         | none             |
  | `class/ssyk/ssyk2012`                     | 21                    | 61         | none             |
  | `class/ssyk/ssyk1996`                     | 32                    | 124        | none             |
  | `class/sun/sun1996`                       | 3                     | 19         | none             |
  | `class/sun-grupp/sun2000-grupp`           | 1                     | 2          | none             |
  | `class/sun-inriktning/sun2000-inriktning` | 9                     | 37         | none             |
  | `class/sun-niva/sun2000-niva`             | 14                    | 63         | none             |
  | `class/sun-grupp/sun2020-grupp`           | 1                     | 3          | none             |
  | `class/sun-inriktning/sun2020-inriktning` | 3                     | 21         | none             |
  | `class/sun-niva/sun2020-niva`             | 6                     | 31         | none             |

### Legacy meanings

The schema has no separate sentinel-meaning field. The earlier read-only audit found 845
differing version/code/label combinations, grouped into 357 exact code/meaning/label
combinations. Those separate meanings are dropped with that single reason; existing
labels and content hashes remain unchanged. Routine missing, unknown, abroad,
inapplicable and nonresponse wording is summarized by these counts, not repeated in a
per-delivery table.

The following non-routine labels or meanings remain for review. This shortlist records
source descriptions; it does not change flags or redefine the sentinel policy.

  | Source                                    | Code(s)                                                                                                                | Description requiring review                                                                                       |
  | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
  | `class/lkf/lkf2008`                       | `00`, `0000`                                                                                                           | Bound label `Hela riket`, rather than missing/inapplicable wording.                                                |
  | `class/lkf/lkf2008`                       | `9999`                                                                                                                 | Bound label `Riket exkl. Gotland`; legacy meaning `Utländsk adress`.                                               |
  | `class/lkf/lkf2021`                       | `0000`                                                                                                                 | Bound label `kommun eller region ägd`.                                                                             |
  | `class/lkf/lkf2011`                       | `0000`                                                                                                                 | Bound label `Kursort = skolort`.                                                                                   |
  | `class/lkf/lkf2017`, `lkf2018`, `lkf2019` | `9999`                                                                                                                 | Bound label `Samma som skolkommun`.                                                                                |
  | `class/lkf/lkf2020`–`lkf2024`             | `0000`                                                                                                                 | Bound label `Personen är inte folkbokförd`.                                                                        |
  | `class/lkf/lkf1999`                       | `99`, `9900`                                                                                                           | `99` has legacy meaning `Obefintliga` and label `Utomlands`; `9900` has label `Obefintliga`.                       |
  | `class/ssyk/ssyk1996`                     | `2140`, `2410`, `2420`, `2490`, `3110`, `4210`, `5220`, `7210`, `7230`, `7340`, `8120`, `8140`, `8250`, `8260`, `8280` | Occupation-group labels (sometimes with `4:e siffran okodad`), against legacy meaning `Bortfall på fyrsiffernivå`. |
  | `class/ssyk/ssyk2012`                     | `9993`                                                                                                                 | Bound label `Arbetslös`.                                                                                           |
  | `class/sni/sni2007`                       | `U99`                                                                                                                  | Bound label `Internationella organisationer`.                                                                      |
  | `class/sni/sni2002`                       | `XXXXX`                                                                                                                | Bound label `Samtliga branscher`.                                                                                  |
  | IoT `MAKEBOST`, local 1991 claim          | `00`, `99`                                                                                                             | Exact source descriptions `Övriga fall` and `Mantalsskrivna i samma län men ej samma fastighet`.                   |

### Interaction with local setters

All nine local setter hashes also have classification bindings. Their local codes are
additional exemptions, not replacements for the original book-list intersections. The
old builder combines book sentinels with scoped certificates, so book allowances must
not negate local source-reviewed sentinel claims. The local-source scope proof above
remains valid.

  | Local setter                                                                  | Local codes            | Bound classification intersection |
  | ----------------------------------------------------------------------------- | ---------------------- | --------------------------------- |
  | `r2: folkbokforda-personer-kvartal: Forsamling @1986-12-31 where key = 'v31'` | `"111183"`             | `class/lkf/lkf1987`: empty        |
  | `r2: folkbokforda-personer-kvartal: Forsamling @1987-12-31 where key = 'v31'` | `"048080"`, `"202084"` | `class/lkf/lkf1988`: empty        |
  | `r2: folkbokforda-personer-kvartal: Kommun @1987-12-31 where key = 'v36'`     | `"2020"`               | `class/lkf/lkf1988`: empty        |
  | `r25: individer-och-dodsbon: KUSNI @1987 where key = 'v21073'`                | `"09350"`, `"83100"`   | `class/sni/sni1969`: `"00000"`    |
  | `r25: individer-och-dodsbon: BNGR @2003 where key = 'v2702'`                  | `"72201"`              | `class/sni/sni2002`: `"00000"`    |
  | `r25: individer-och-dodsbon: BSUN @1996 where key = 'v8189'`                  | `"90000"`, `"[NULL]"`  | `class/sun/sun1996`: empty        |
  | `r25: individer-och-dodsbon: MAKEBOST @1991 where key = 'v39851'`             | `"00"`, `"99"`         | `class/lkf/lkf1991`: empty        |
  | `r25: individer-och-dodsbon: TNLAN @1990 where key = 'v39966'`                | `"00"`                 | `class/lkf/lkf1991`: empty        |
  | `r34: foretag: Org_Sni92 @1990,1991,1992,1993,1994,1995 where key = 'v19054'` | `"01250"`, `"02010"`   | `class/sni/sni1992`: `"00000"`    |

## Other unmapped fields

  | Legacy source                                                      | Field                | Outcome                                          |
  | ------------------------------------------------------------------ | -------------------- | ------------------------------------------------ |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:210` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:223` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:236` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:249` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:262` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:275` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:288` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:301` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:314` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:327` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:340` | `source_attribution` | dropped: no four-kind field; outside this change |
  | `reg_meta_build/curation/registers/scb/foretagsstatistik.toml:353` | `source_attribution` | dropped: no four-kind field; outside this change |

## Additional entity-name declarations

  | Legacy source                                       | Entity                | Outcome                                                                                                                                                                                                                                                                                                      |
  | --------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `reg_meta_build/curation/registers/scb/gdb.toml:44` | table `individer`     | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it                                                                                                                                                                                                    |
  | `reg_meta_build/curation/registers/scb/gdb.toml:53` | table `arbetsstallen` | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it                                                                                                                                                                                                    |
  | `reg_meta_build/curation/registers/scb/gdb.toml:62` | table `skolor`        | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it                                                                                                                                                                                                    |
  | `reg_meta_build/curation/registers/sos/bu.toml:5`   | register `sos/bu`     | dropped: source name already equals corrected name; source `catalog/source/Socialstyrelsen/Metadata_Insatser till barn och unga (BU)_webb.xlsx`, sheet `Metadata-Datamängd (DCAT-AP) `, cell `C2`                                                                                                            |
  | `reg_meta_build/curation/registers/sos/dors.toml:5` | register `sos/dors`   | dropped: source name already equals corrected name; source `catalog/source/Socialstyrelsen/Metadata_Dödsorsaksregistret (DORS)_webb.xlsx`, sheet `Metadata-Datamängd (DCAT-AP) `, cell `C2`                                                                                                                  |
  | `reg_meta_build/curation/registers/sos/lmed.toml:5` | register `sos/lmed`   | dropped: source name already equals corrected name; source `catalog/source/Socialstyrelsen/Metadata Läkemedelsregistret (LMED)_webb.xlsx`, sheet `Metadata-Datamängd`, cell `C2`                                                                                                                             |
  | `reg_meta_build/curation/registers/sos/lova.toml:5` | register `sos/lova`   | dropped: source name already equals corrected name; LOVA trailing space is removed by the existing reader trim; source `catalog/source/Socialstyrelsen/Metadata_Förteckning legitimerade omsorgs- och vårdyrkesgruppers arbetsmarknadsstatus (LOVA)_web_klar.xlsx`, sheet `Metadata - Datamängd `, cell `C2` |
  | `reg_meta_build/curation/registers/sos/lvm.toml:5`  | register `sos/lvm`    | dropped: source name already equals corrected name; source `catalog/source/Socialstyrelsen/Metadata_Registret över tvångsvård enligt lagen om vård av missbrukare i vissa fall (LVM)_webb.xlsx`, sheet `Metadata-Datamängd (DCAT-AP) `, cell `C2`                                                            |
  | `reg_meta_build/curation/registers/sos/par.toml:5`  | register `sos/par`    | dropped: source name already equals corrected name; source `catalog/source/Socialstyrelsen/Metadata_Patientregistret (PAR)_webb.xlsx`, sheet `Metadata-Datamängd (DCAT-AP) `, cell `C2`                                                                                                                      |

## Implementation and validation

Fix-03 applies explicit sentinel setters after `set value_set` overrides. Their premises
and flags therefore use the final published hashes, including population alternatives.
Classification lists are additive on shared hashes; only a listed code canonical in
another bound version refuses, locating both binding authors and the inventory sources.
Disagreeing local complete claims still refuse.

All 63 inventory lists again equal their legacy declarations (490 code/version pairs).
The 22 fix-02 additions and generic evidence suffixes are removed. Four Lisa AstLan
claims are already marked set-wide by the original LKF1992/LKF1993 lists; no scoped
setter is added. The actual empty code remains a real item: only the four literal
`[BLANK]` declarations listed above are currently curated.

The unwritten `variable_unit_type` table and its variable-show field and reader remain
for schema-4 release comparison and retirement verification. Nothing writes the table
since D's table-only unit types; its DDL schedules deletion with the next schema-major
change. The variable unit-type diff facet remains removed. The catalog schema stays 4
and operation contract stays 5.1.0, as on `origin/main`.

Operator-provided evidence for parent `06687d8d`: its real build and
`verify --previous 0.66.0` passed, and its diff changed only sentinel flags on 965 value
sets. Those are the Operator's results, not a new full build in this fix.

Historical checks run in fix-03 (before fix-04 restored schema 4):

- `cargo build --release -p reg-build --locked` passed.
- The saved pre-fix builder silently reproduced the defect: lock/build/verify passed
  with replacement code `0` unmarked and superseded source code `0` marked. The new
  hardest-case HTTP corpus scenario uses two lists that both contain `0`, places
  sentinel curation before the override, and checks both the published replacement and
  unchanged control. No existing case covered sentinel/override ordering. It uses the
  existing multi-request harness unchanged; source lists and independently calculated
  hashes supply the oracle.
- The fixed release binary's synthetic lock/build/verify passed. Direct publication
  checks confirmed replacement `0` marked, original/control `0` unmarked, unchanged
  value-set hashes and absence of the removed table.
- Separate synthetic classification checks passed: differing noncanonical exemption
  lists union on a shared set; making code `0` canonical in the other bound version
  produces `sentinel-conflict` at both authors.
- Cross-schema guard checks passed: schema-5 `verify --previous` against a valid
  schema-4 fixture exits 1, and `diff` exits 2 with the schema mismatch.
- `gate.py g0 rust` passed: static checks, Clippy, workspace Rust tests, dependency
  audit, 116 script tests, byte-identical synthetic builds, and 203 conformance checks
  (two skipped). Gate times: G0 65 s, Rust 16 s. Contract oracles changed only for the
  removed field and version gates; no expected semantic result was weakened.
- Independent review passed, including the regression's test-policy checks, final
  value-set premises, canonical-code guard and schema/API contract edits.
- All-register `reg-build lock` passed against input commit
  `9186fcbceeb2283a50c7f684764869e4dda5765e` and SCB archive SHA-256
  `a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`. It refreshed 1,023
  existing premises across 132 root lock files, adding or removing no keys. All 63
  legacy lists (490 pairs) match their binding premises exactly. It reported 45
  redundant-set notices and one unused declaration (`sektorkod/99`, 22 bound sets), with
  no refusals. The new regression fixture was locked separately.

No real-seed build, full-artifact verify/diff, `gate.py regen`, release publication or
PR merge ran. The Operator reruns full validation after push.

Fix-04 restores the schema-4 table/reader and contract 5.1.0 only. The variable
unit-type diff facet remains removed; all sentinel changes and locked curation from
fix-03 remain unchanged. Existing API, admission and order oracles are restored to their
pre-bump values; no new tests or weaker assertions were added.

Checks run in fix-04:

- `gate.py g0 rust` passed: static checks, workspace Rust tests, 116 script tests,
  byte-identical synthetic builds and 203 conformance checks (two skipped). Gate times:
  G0 34 s, Rust 9 s.
- Explicit synthetic build and `verify` passed. `verify --previous` against an identical
  schema-4 synthetic copy passed; `diff` against that copy exited 0 with no changes. The
  artifact carries schema 4 and the retained `variable_unit_type` table is empty.
- Independent restoration review and `git diff --check` passed.

No real-seed build, 0.66.0 artifact comparison, `gate.py regen`, publication or PR merge
ran in fix-04. The Operator runs full release verification after push.

Fix-05 validates canonical membership in every classification version bound to a shared
set, including the declaring version. Classification lists remain additive; only codes
present in the bound set are candidates for sentinel publication. A listed code absent
from every bound set still counts as unused. Refusals retain the `sentinel-conflict`
code, binding statements and inventory locations.

Before changing behavior, all 63 committed lists (490 code/version declarations) were
checked against 110,719 canonical codes in their exact inventory CSVs at input pin
`9186fcbceeb2283a50c7f684764869e4dda5765e`. Header selection and trimmed, nonblank code
semantics matched the classification reader; the canonical source files were clean.
There were **zero own-book overlaps**, so no legacy-meaning exceptions, code removals or
premise re-locks were needed.

Checks run in fix-05:

- `gate.py g0 rust` passed: static checks, workspace Rust tests, 116 script tests,
  byte-identical synthetic builds and 203 conformance checks (two skipped). Gate times:
  G0 18 s, Rust 7 s. The first attempt stopped at Markdown formatting, including two
  wraps left from fix-04; formatting was corrected before the passing run.
- Explicit standard synthetic build/verify passed. One-off CLI fixture variants
  confirmed that own-book canonical code `1` and other-bound-book canonical code `3`
  refuse with located `sentinel-conflict` errors. Differing additive lists with
  noncanonical code `99` still passed lock/build/verify and published the sentinel bit.
- No existing Rust sentinel-refusal corpus case covers this path. The legacy Python
  canonical-sentinel refusal case exercises a different builder. It was left intact; no
  duplicate permanent case or new harness was added under REBUILD.md's no-new-tests
  rule. The regression proof here is the one-off Rust CLI check, not a corpus extension.
- Independent code and test-policy review passed.

Operator-provided evidence for `9c03adf6`: real build and `verify --previous 0.66.0`
passed; the diff changed sentinel marks only. Those are the Operator's results. No
real-seed build, full-artifact verify/diff, `gate.py regen`, publication or PR merge ran
in fix-05. The Operator reruns full release verification after push.
