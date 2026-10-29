# D field carry-over audit

Deleted at milestone F (cutover), with the one-shot converters.

This supplements `rebuild_convert_report.md`; historical stage counts are not additive.
Only source metadata is listed, never holdings or individual records.

## Outcomes

- Delivery aliases: all 44 `enrichment.alias` conversions have been undone, including
  their 44 premise additions. They are dropped with the reason **delivery-header
  mapping; home: E holdings layer**. `set synonyms` remains available for actual search
  synonyms.
- Local sentinels: nine of 15 entries have exclusively scoped value-set hashes and are
  converted to set-wide `set sentinels`. Six remain dropped because the same hash is
  referenced outside the entry's scope; exact references are below.
- Classification sentinels: all 63 legacy lists apply to bound occurrence value sets,
  never to their canonical books. Their 490 original code/version pairs include one
  allowed unobserved pair. Reviewed noncanonical-code propagation resolves actual
  shared-bound-set conflicts with 22 additions in 14 LKF versions (512 pairs total).
  Differing legacy meanings are listed separately as dropped: the schema has no distinct
  sentinel-meaning field.
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
  | `reg_meta_build/curation/registers/scb/lisa.toml:3231` | `individer-16plus: KU1AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | dropped: 8 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed  |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3241` | `individer-16plus: KU2AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | dropped: 8 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed  |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3251` | `individer-16plus: KU3AstLan [['1990-01-01', '1990-12-31']]`              | `c1ffbef279ea4136a8b872671273e7ca7667a488ff8aa0031ed1cc399266af8d` | dropped: 8 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed  |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3261` | `individer-16plus: AstLan [['1990-01-01', '1990-12-31']]`                 | `52f34f09dcc93bfe814443cbe84c3d4320965d14851b32962b903e5d1c9b43a3` | dropped: 2 outside-scope occurrence references; occurrence-scoped sentinels deferred to sentinel-aware value-set identity, revisit when this entry is needed  |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1317`  | `folkbokforda-personer-kvartal: Forsamling @1986-12-31 where key = 'v31'` | `d9772838ee6e4543c4b1421e357045b0f0072f6cfb6d90e64fdc9270155138c5` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1327`  | `folkbokforda-personer-kvartal: Forsamling @1987-12-31 where key = 'v31'` | `67c8f982e7d06a3e3f88afa2a21df4b93718fee4ff7752d8f37ac3a0722003cf` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1337`  | `folkbokforda-personer-kvartal: Kommun @1987-12-31 where key = 'v36'`     | `e703a6ccdb4ebb315f5141a846c021daea1a35384080a375691f183384bbcedd` | converted: 1 scoped occurrence(s), zero outside references                                                                                                    |

Outside-scope references, by dropped entry:

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

Fix-02 corrects the fix-01 interpretation. The legacy `sentinel_codes` lists name known
noncanonical codes observed in columns bound to a classification; they do not add codes
to the canonical book. `resolved_catalog.py` checks the observed noncanonical members
against the book sentinel list and scoped certificates, and
`source_classification_bindings.py::_sentinel_map` retains their meanings.

Read-only audit of the published 0.66.0 catalog examined all direct and
population-alternative value sets of occurrences bound to each exact classification
version. All 83 versions were included: 70 have bindings and together reference 1,495
distinct value-set hashes. The 63 original lists contain 490 code/version pairs; 489
occur in at least one bound set. The remaining pair, `class/sektorkod/sektorkod` code
`99`, occurs in no bound set and is an allowed unobserved declaration. Absence from the
canonical book is expected.

The original lists disagree on present codes for 59 version pairs across 13 shared bound
hashes. Every conflicting code was checked against both versions' canonical books and is
noncanonical. A minimal per-code propagation across shared hashes resolves those
disagreements: 22 code/version additions in 14 LKF versions, giving 512 declared pairs,
511 observed pairs and the same one unobserved pair. No code was added to a value set or
canonical book; identities and supplied labels are unchanged. These are reviewed
curation amendments, not a rerun of the one-shot legacy converter. The
classification-only projection marks 1,585 hash/code pairs across 961 observed value
sets; no canonical-book value set receives a flag. Local setters contribute separately.

The earlier `lkf1987/lkf1988` pair has no present-code conflict on their shared bound
sets. The `lkf1989/lkf1990/lkf1991` group does: code `99` on
`444d84e28637fa1a0c093ef4996281f3ff9e98fd58cad21ccabf7d2dc2bc6141`, `0000`/`9999` on
`c3664d0303de3ee323d7cd067ad1e12f08da489157c7164e5984042c9b376fde`, and `0000` on
`fd1587a2798c37f19e4b8013efaa830a9d8ac8fb57aae2308486127104dcde0a`. The amendments below
resolve them and the other shared-bound-set disagreements.

### Reviewed list amendments

Each added code occurs in the listed shared bound set and is absent from the target
version's canonical book. Repeating this step to a fixed point gives zero remaining
present-code disagreements. Hashes are evidence for the addition; original conflicts can
involve more hashes.

  | Version             | Added code | Shared bound hash proving the addition                             |
  | ------------------- | ---------- | ------------------------------------------------------------------ |
  | `class/lkf/lkf1986` | `"00"`     | `26411f2393dd92e3a760913ab6418581f35054862232aa63cddfb5be6ae3c015` |
  | `class/lkf/lkf1986` | `"0000"`   | `fd1587a2798c37f19e4b8013efaa830a9d8ac8fb57aae2308486127104dcde0a` |
  | `class/lkf/lkf1987` | `"0000"`   | `c3664d0303de3ee323d7cd067ad1e12f08da489157c7164e5984042c9b376fde` |
  | `class/lkf/lkf1987` | `"9999"`   | `c3664d0303de3ee323d7cd067ad1e12f08da489157c7164e5984042c9b376fde` |
  | `class/lkf/lkf1988` | `"0000"`   | `fd1587a2798c37f19e4b8013efaa830a9d8ac8fb57aae2308486127104dcde0a` |
  | `class/lkf/lkf1989` | `"00"`     | `2be1e760559fe462059b25f08c9169ea20710d6a1487228c45ad5caf8676f4ff` |
  | `class/lkf/lkf1989` | `"0000"`   | `fd1587a2798c37f19e4b8013efaa830a9d8ac8fb57aae2308486127104dcde0a` |
  | `class/lkf/lkf1990` | `"00"`     | `26411f2393dd92e3a760913ab6418581f35054862232aa63cddfb5be6ae3c015` |
  | `class/lkf/lkf1990` | `"0000"`   | `c3664d0303de3ee323d7cd067ad1e12f08da489157c7164e5984042c9b376fde` |
  | `class/lkf/lkf1990` | `"9999"`   | `c3664d0303de3ee323d7cd067ad1e12f08da489157c7164e5984042c9b376fde` |
  | `class/lkf/lkf1991` | `"00"`     | `2be1e760559fe462059b25f08c9169ea20710d6a1487228c45ad5caf8676f4ff` |
  | `class/lkf/lkf1991` | `"99"`     | `444d84e28637fa1a0c093ef4996281f3ff9e98fd58cad21ccabf7d2dc2bc6141` |
  | `class/lkf/lkf1998` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf1999` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf1999` | `"8000"`   | `024f36bf2fc08e91b3842ac8798771ae9af3cf1692f8b3e47fd6fb028bc4c356` |
  | `class/lkf/lkf2002` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf2002` | `"8000"`   | `d273a7ec6b6621bb8355bf453b8b6b3a3433b49f91b3ff2531d2c6e5ed8ae9ef` |
  | `class/lkf/lkf2003` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf2004` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf2006` | `"[NULL]"` | `8d39e670f1c9a2cfbebd7176b57a77d44dcfb123aaa5b30c9ab8d0845ff8b40f` |
  | `class/lkf/lkf2008` | `"80"`     | `6b26d6f5af3fe7b8029072b75ed0631ecee2be5e0cbcb911e042d00e1b105300` |
  | `class/lkf/lkf2025` | `"0199"`   | `7fb631740c6891ccb67899e633d86a187485d4ab778d39df664ad18cee8189a8` |

### Bound-set accounting

  | Classification                            | Original / normalized declared pairs | Bound sets | Unobserved codes |
  | ----------------------------------------- | ------------------------------------ | ---------- | ---------------- |
  | `class/agarkat/agarkat2000`               | 3 / 3                                | 8          | none             |
  | `class/agarkat/agarkat2020`               | 1 / 1                                | 5          | none             |
  | `class/isced/isced2011`                   | 2 / 2                                | 4          | none             |
  | `class/jurform/jurform2000`               | 2 / 2                                | 13         | none             |
  | `class/jurform/jurform2020`               | 1 / 1                                | 4          | none             |
  | `class/lkf/lkf1980`                       | 2 / 2                                | 5          | none             |
  | `class/lkf/lkf1981`                       | 2 / 2                                | 9          | none             |
  | `class/lkf/lkf1986`                       | 4 / 6                                | 17         | none             |
  | `class/lkf/lkf1987`                       | 5 / 7                                | 17         | none             |
  | `class/lkf/lkf1988`                       | 4 / 5                                | 20         | none             |
  | `class/lkf/lkf1989`                       | 4 / 6                                | 16         | none             |
  | `class/lkf/lkf1990`                       | 3 / 6                                | 15         | none             |
  | `class/lkf/lkf1991`                       | 4 / 6                                | 25         | none             |
  | `class/lkf/lkf1992`                       | 6 / 6                                | 20         | none             |
  | `class/lkf/lkf1993`                       | 6 / 6                                | 17         | none             |
  | `class/lkf/lkf1994`                       | 6 / 6                                | 20         | none             |
  | `class/lkf/lkf1995`                       | 6 / 6                                | 22         | none             |
  | `class/lkf/lkf1996`                       | 7 / 7                                | 30         | none             |
  | `class/lkf/lkf1997`                       | 7 / 7                                | 25         | none             |
  | `class/lkf/lkf1998`                       | 7 / 8                                | 31         | none             |
  | `class/lkf/lkf1999`                       | 9 / 11                               | 36         | none             |
  | `class/lkf/lkf2000`                       | 10 / 10                              | 39         | none             |
  | `class/lkf/lkf2001`                       | 11 / 11                              | 35         | none             |
  | `class/lkf/lkf2002`                       | 9 / 11                               | 34         | none             |
  | `class/lkf/lkf2003`                       | 8 / 9                                | 38         | none             |
  | `class/lkf/lkf2004`                       | 10 / 11                              | 44         | none             |
  | `class/lkf/lkf2005`                       | 11 / 11                              | 55         | none             |
  | `class/lkf/lkf2006`                       | 16 / 17                              | 62         | none             |
  | `class/lkf/lkf2007`                       | 10 / 10                              | 56         | none             |
  | `class/lkf/lkf2008`                       | 11 / 12                              | 54         | none             |
  | `class/lkf/lkf2009`                       | 11 / 11                              | 54         | none             |
  | `class/lkf/lkf2010`                       | 12 / 12                              | 61         | none             |
  | `class/lkf/lkf2011`                       | 10 / 10                              | 58         | none             |
  | `class/lkf/lkf2012`                       | 10 / 10                              | 62         | none             |
  | `class/lkf/lkf2013`                       | 11 / 11                              | 61         | none             |
  | `class/lkf/lkf2014`                       | 10 / 10                              | 56         | none             |
  | `class/lkf/lkf2015`                       | 9 / 9                                | 49         | none             |
  | `class/lkf/lkf2016`                       | 15 / 15                              | 55         | none             |
  | `class/lkf/lkf2017`                       | 10 / 10                              | 57         | none             |
  | `class/lkf/lkf2018`                       | 11 / 11                              | 58         | none             |
  | `class/lkf/lkf2019`                       | 9 / 9                                | 58         | none             |
  | `class/lkf/lkf2020`                       | 7 / 7                                | 64         | none             |
  | `class/lkf/lkf2021`                       | 10 / 10                              | 60         | none             |
  | `class/lkf/lkf2022`                       | 12 / 12                              | 61         | none             |
  | `class/lkf/lkf2023`                       | 11 / 11                              | 50         | none             |
  | `class/lkf/lkf2024`                       | 9 / 9                                | 52         | none             |
  | `class/lkf/lkf2025`                       | 5 / 6                                | 23         | none             |
  | `class/niva-old/niva-oldv1`               | 5 / 5                                | 29         | none             |
  | `class/sektor/sektor2000`                 | 1 / 1                                | 7          | none             |
  | `class/sektorkod/sektorkod`               | 2 / 2                                | 22         | `"99"`           |
  | `class/sni/sni2002`                       | 11 / 11                              | 46         | none             |
  | `class/sni/sni2007`                       | 13 / 13                              | 122        | none             |
  | `class/sni/sni1969`                       | 4 / 4                                | 48         | none             |
  | `class/sni/sni1992`                       | 5 / 5                                | 89         | none             |
  | `class/ssyk/ssyk2012`                     | 21 / 21                              | 61         | none             |
  | `class/ssyk/ssyk1996`                     | 32 / 32                              | 124        | none             |
  | `class/sun/sun1996`                       | 3 / 3                                | 19         | none             |
  | `class/sun-grupp/sun2000-grupp`           | 1 / 1                                | 2          | none             |
  | `class/sun-inriktning/sun2000-inriktning` | 9 / 9                                | 37         | none             |
  | `class/sun-niva/sun2000-niva`             | 14 / 14                              | 63         | none             |
  | `class/sun-grupp/sun2020-grupp`           | 1 / 1                                | 3          | none             |
  | `class/sun-inriktning/sun2020-inriktning` | 3 / 3                                | 21         | none             |
  | `class/sun-niva/sun2020-niva`             | 6 / 6                                | 31         | none             |

### Legacy meanings

The schema has no separate home for a sentinel meaning that differs from the bound
item's supplied label. The 845 differing version/code/label combinations (357 exact
grouped rows) are listed as dropped in [the sentinel-meanings
report](rebuild_convert_sentinel_meanings_report.md). Existing code labels are retained.
Equal meanings require no separate storage.

### Interaction with local setters

All nine local setter hashes also have classification bindings. Seven local code lists
differ from the book-list intersection on that hash; two agree. The old builder combines
book sentinels with scoped certificates, so book allowances must not negate local
source-reviewed sentinel claims. The local-source scope proof above remains valid.

  | Local setter                                                                  | Local codes            | Bound classification intersection   |
  | ----------------------------------------------------------------------------- | ---------------------- | ----------------------------------- |
  | `r2: folkbokforda-personer-kvartal: Forsamling @1986-12-31 where key = 'v31'` | `"111183"`             | `class/lkf/lkf1987`: empty          |
  | `r2: folkbokforda-personer-kvartal: Forsamling @1987-12-31 where key = 'v31'` | `"048080"`, `"202084"` | `class/lkf/lkf1988`: empty          |
  | `r2: folkbokforda-personer-kvartal: Kommun @1987-12-31 where key = 'v36'`     | `"2020"`               | `class/lkf/lkf1988`: empty          |
  | `r25: individer-och-dodsbon: KUSNI @1987 where key = 'v21073'`                | `"09350"`, `"83100"`   | `class/sni/sni1969`: `"00000"`      |
  | `r25: individer-och-dodsbon: BNGR @2003 where key = 'v2702'`                  | `"72201"`              | `class/sni/sni2002`: `"00000"`      |
  | `r25: individer-och-dodsbon: BSUN @1996 where key = 'v8189'`                  | `"90000"`, `"[NULL]"`  | `class/sun/sun1996`: empty          |
  | `r25: individer-och-dodsbon: MAKEBOST @1991 where key = 'v39851'`             | `"00"`, `"99"`         | `class/lkf/lkf1991`: `"00"`, `"99"` |
  | `r25: individer-och-dodsbon: TNLAN @1990 where key = 'v39966'`                | `"00"`                 | `class/lkf/lkf1991`: `"00"`         |
  | `r34: foretag: Org_Sni92 @1990,1991,1992,1993,1994,1995 where key = 'v19054'` | `"01250"`, `"02010"`   | `class/sni/sni1992`: `"00000"`      |

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

Fix-02 corrects fix-01's interpretation: inventory sentinel lists describe known
non-canonical codes in bound columns, not membership of the canonical book. The engine
projects each list onto the direct and population-alternative sets of occurrences
curated to that exact version. The book's own set is unchanged. Unobserved declarations
are counted without refusal. Classification versions sharing a hash must agree about
codes present there; codes absent from that set do not cause a conflict.

Explicit local certificates and classification exemptions are additive, as in the old
builder. Local/local complete-list conflicts and classification/classification projected
conflicts are checked separately before emitting their union. Missing codes in an
explicit local setter still refuse. Code/label/parent identity is unchanged. Binding
premises lock inventory sentinel lists and observed value-set hashes. Those content
hashes already cover all item facts; no duplicate item snapshots are required for
classification bindings. Explicit local setters retain their item validation.

The 44 delivery-header mappings remain deferred to E; genuine `set synonyms` is still
supported. Roles share one vocabulary. Unit types remain table-only, with the existing
non-null dictionary label represented by the slug. All six register-name corrections are
already satisfied by source titles after reader trimming.

Read-only audit used input commit `9186fcbceeb2283a50c7f684764869e4dda5765e` and the
published 0.66.0 catalog. The pinned SCB archive SHA-256 is
`a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`. The binding audit
and every noncanonical-code amendment are recorded above. Distinct legacy meanings have
no published home and remain named drops in the linked report; source labels were not
overwritten.

Final implementation checks:

- `cargo build --release -p reg-build --locked` passed.
- `gate.py g0 rust` passed: static checks, Clippy, workspace Rust tests, dependency
  audit, 116 script tests, byte-identical synthetic builds, and 201 conformance checks
  (two skipped). Final gate times: G0 25 s, Rust 7 s. Existing fixture inputs and
  expected outputs were unchanged.
- The final release binary's synthetic lock/build/verify passed. Direct publication
  checks confirmed unchanged content hash, untouched canonical-book sets, observed
  direct and population-alternative flags, and additive local certificates.
- Different lists agreeing on all present codes passed. Unobserved declarations were
  counted without refusal. Present-code disagreement refused at both binding statements;
  a missing code in an explicit local setter still refused.
- Independent review passed, including premise coverage by verified content hashes and
  the separate classification/local conflict checks. No new tests were added. No
  real-seed build, full-artifact verify/diff, `gate.py regen`, release publication or PR
  merge ran. The Operator runs the full pinned build, verify and comparison against
  0.66.0 after push.
