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
- Classification sentinels: all 63 declarations are transcribed unchanged as code lists
  into the new inventory contract. Read-only validation found missing canonical codes in
  every declaration and two shared-hash disagreement groups. These are unresolved
  authoring refusals, not successful strict-build conversion; no code was silently
  filtered or added to a value set. They are retained exactly as requested in fix-01,
  pending a maintainer decision on their source-backed representation.
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

All 83 classification names are already preserved exactly. The new `sentinel_codes`
inventory list is set-wide metadata; it does not change code/label/parent hashing or
create codes. Legacy declarations include observed out-of-book values. All 63 therefore
have at least one code absent from the canonical book value set in 0.66.0. They are
listed in full below so a strict-build refusal cannot be mistaken for successful
carry-over. No absent code was filtered. Other omitted presentation fields retain the
#2016 inventory-contract reason in the earlier classification report.

  | Classification                            | Missing codes in its canonical value set                                                                                                                                                                                                           |
  | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `class/agarkat/agarkat2000`               | `00`, `9`, `NULL`                                                                                                                                                                                                                                  |
  | `class/agarkat/agarkat2020`               | `00`                                                                                                                                                                                                                                               |
  | `class/isced/isced2011`                   | `-1`, `-2`                                                                                                                                                                                                                                         |
  | `class/jurform/jurform2000`               | `0`, `00`                                                                                                                                                                                                                                          |
  | `class/jurform/jurform2020`               | `00`                                                                                                                                                                                                                                               |
  | `class/lkf/lkf1980`                       | `058313`, `999999`                                                                                                                                                                                                                                 |
  | `class/lkf/lkf1981`                       | `99`, `9999`                                                                                                                                                                                                                                       |
  | `class/lkf/lkf1986`                       | `******`, `99`, `9999`, `999999`                                                                                                                                                                                                                   |
  | `class/lkf/lkf1987`                       | `******`, `00`, `1111`, `99`, `999999`                                                                                                                                                                                                             |
  | `class/lkf/lkf1988`                       | `******`, `00`, `99`, `999999`                                                                                                                                                                                                                     |
  | `class/lkf/lkf1989`                       | `*****`, `******`, `99`, `999999`                                                                                                                                                                                                                  |
  | `class/lkf/lkf1990`                       | `******`, `99`, `999999`                                                                                                                                                                                                                           |
  | `class/lkf/lkf1991`                       | `******`, `0000`, `9999`, `999999`                                                                                                                                                                                                                 |
  | `class/lkf/lkf1992`                       | `******`, `00`, `0000`, `99`, `9999`, `999999`                                                                                                                                                                                                     |
  | `class/lkf/lkf1993`                       | `******`, `00`, `0000`, `99`, `9999`, `999999`                                                                                                                                                                                                     |
  | `class/lkf/lkf1994`                       | `******`, `00`, `0000`, `99`, `9999`, `999999`                                                                                                                                                                                                     |
  | `class/lkf/lkf1995`                       | `******`, `00`, `0000`, `99`, `9999`, `999999`                                                                                                                                                                                                     |
  | `class/lkf/lkf1996`                       | `******`, `00`, `0000`, `2088`, `99`, `9999`, `999999`                                                                                                                                                                                             |
  | `class/lkf/lkf1997`                       | `******`, `00`, `0000`, `99`, `9999`, `99999`, `999999`                                                                                                                                                                                            |
  | `class/lkf/lkf1998`                       | `******`, `00`, `0000`, `000000`, `99`, `9999`, `999999`                                                                                                                                                                                           |
  | `class/lkf/lkf1999`                       | `******`, `00`, `0000`, `000000`, `0199`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                             |
  | `class/lkf/lkf2000`                       | `00`, `0000`, `000000`, `0199`, `80`, `8000`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                         |
  | `class/lkf/lkf2001`                       | `0`, `00`, `0000`, `000000`, `0199`, `80`, `8000`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                    |
  | `class/lkf/lkf2002`                       | `0`, `00`, `0000`, `000000`, `0199`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                                  |
  | `class/lkf/lkf2003`                       | `00`, `0000`, `000000`, `0199`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                                       |
  | `class/lkf/lkf2004`                       | `00`, `0000`, `000000`, `0199`, `9108`, `9800`, `99`, `9900`, `9999`, `NULL`                                                                                                                                                                       |
  | `class/lkf/lkf2005`                       | `0`, `00`, `0000`, `000000`, `0199`, `2762`, `80`, `9800`, `99`, `9900`, `9999`                                                                                                                                                                    |
  | `class/lkf/lkf2006`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `1111`, `2762`, `9800`, `99`, `9900`, `9993`, `9994`, `9999`, `NULL`                                                                                                                          |
  | `class/lkf/lkf2007`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                          |
  | `class/lkf/lkf2008`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`, `NULL`                                                                                                                                                                  |
  | `class/lkf/lkf2009`                       | `0`, `00`, `0000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`, `NULL`, `[NULL]`                                                                                                                                                                  |
  | `class/lkf/lkf2010`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`, `NULL`, `[NULL]`                                                                                                                                                        |
  | `class/lkf/lkf2011`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                          |
  | `class/lkf/lkf2012`                       | `0`, `00`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                          |
  | `class/lkf/lkf2013`                       | `00`, `000`, `000.`, `0000`, `000000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                |
  | `class/lkf/lkf2014`                       | `0`, `00`, `0000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`, `NULL`                                                                                                                                                                            |
  | `class/lkf/lkf2015`                       | `0`, `00`, `0000`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                                    |
  | `class/lkf/lkf2016`                       | `0`, `00`, `000`, `000.`, `0000`, `0097`, `0098`, `0099`, `0141`, `0147`, `0199`, `99`, `9900`, `9999`, `[BLANK]`                                                                                                                                  |
  | `class/lkf/lkf2017`                       | `0`, `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                            |
  | `class/lkf/lkf2018`                       | `0`, `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `99`, `9900`, `9999`, `NULL`                                                                                                                                                                    |
  | `class/lkf/lkf2019`                       | `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                                 |
  | `class/lkf/lkf2020`                       | `0`, `00`, `0000`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                                                    |
  | `class/lkf/lkf2021`                       | `0`, `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `99`, `9900`, `9999`                                                                                                                                                                            |
  | `class/lkf/lkf2022`                       | `0`, `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `88`, `98`, `99`, `9900`, `9999`                                                                                                                                                                |
  | `class/lkf/lkf2023`                       | `0`, `00`, `0000`, `0097`, `0098`, `0099`, `0199`, `99`, `9900`, `9990`, `9999`                                                                                                                                                                    |
  | `class/lkf/lkf2024`                       | `0`, `00`, `0000`, `00000`, `98`, `99`, `9900`, `9999`, `NULL`                                                                                                                                                                                     |
  | `class/lkf/lkf2025`                       | `00`, `0000`, `99`, `9900`, `9999`                                                                                                                                                                                                                 |
  | `class/niva-old/niva-oldv1`               | `*`, `-`, `0`, `9`, `NULL`                                                                                                                                                                                                                         |
  | `class/sektor/sektor2000`                 | `000`                                                                                                                                                                                                                                              |
  | `class/sektorkod/sektorkod`               | `0`, `99`                                                                                                                                                                                                                                          |
  | `class/sni/sni2002`                       | `.`, `0`, `00`, `000`, `0000`, `00000`, `X`, `XX`, `XXXX`, `XXXXX`, `xxx`                                                                                                                                                                          |
  | `class/sni/sni2007`                       | `0`, `00`, `000`, `0000`, `00000`, `000000`, `G99`, `NULL`, `U99`, `XX`, `XXXX`, `XXXXX`, `xxx`                                                                                                                                                    |
  | `class/sni/sni1969`                       | `0`, `00000`, `000000`, `NULL`                                                                                                                                                                                                                     |
  | `class/sni/sni1992`                       | `00`, `000`, `0000`, `00000`, `NULL`                                                                                                                                                                                                               |
  | `class/ssyk/ssyk2012`                     | `***`, `****`, `*****`, `0000`, `88`, `8888`, `99`, `9991`, `9992`, `9993`, `9994`, `9995`, `9996`, `9997`, `9999`, `NULL`, `X`, `XX`, `XXX`, `XXXX`, `bortfall`                                                                                   |
  | `class/ssyk/ssyk1996`                     | `***`, `****`, `.`, `000`, `0000`, `041`, `0413`, `2140`, `2410`, `2420`, `2490`, `3110`, `4210`, `5220`, `7210`, `7230`, `7340`, `8120`, `8140`, `8250`, `8260`, `8280`, `88`, `888`, `8888`, `99`, `999`, `9999`, `XX`, `XXXX`, `[BLANK]`, `xxx` |
  | `class/sun/sun1996`                       | `***`, `*****`, `00000`                                                                                                                                                                                                                            |
  | `class/sun-grupp/sun2000-grupp`           | `***`                                                                                                                                                                                                                                              |
  | `class/sun-inriktning/sun2000-inriktning` | `*`, `****`, `9`, `977`, `977z`, `988`, `988z`, `998z`, `[BLANK]`                                                                                                                                                                                  |
  | `class/sun-niva/sun2000-niva`             | `*`, `***`, `400`, `405`, `88`, `888`, `9`, `97`, `977`, `988`, `99`, `M`, `NULL`, `[BLANK]`                                                                                                                                                       |
  | `class/sun-grupp/sun2020-grupp`           | `99Z`                                                                                                                                                                                                                                              |
  | `class/sun-inriktning/sun2020-inriktning` | `*`, `000`, `NULL`                                                                                                                                                                                                                                 |
  | `class/sun-niva/sun2020-niva`             | `400`, `405`, `88`, `97`, `99`, `999`                                                                                                                                                                                                              |

Shared-hash declaration disagreements:

- `ebec68b8c08f489bfcea3b7c50546f2deea00e214cd79bb0ebdc65659c72531b`:
  `class/lkf/lkf1987` = `['******', '00', '1111', '99', '999999']`; `class/lkf/lkf1988` =
  `['******', '00', '99', '999999']`.
- `20995149d8b590a71b10889315e6dc91cc13e01fd6717c9b87442a6eae86fd50`:
  `class/lkf/lkf1989` = `['*****', '******', '99', '999999']`; `class/lkf/lkf1990` =
  `['******', '99', '999999']`; `class/lkf/lkf1991` =
  `['******', '0000', '9999', '999999']`.

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

Set-wide sentinel bits use the existing `vs_item.sentinel`; hashes still cover only
code, label and parent. Missing codes and incompatible declarations for a shared hash
are located refusals. Local occurrence-scoped sentinel meanings on a reused hash remain
a named deferral to sentinel-aware value-set identity, revisited when a dropped entry
needs it. `set synonyms` still publishes genuine search synonyms to the FTS label. Roles
use one shared vocabulary and unknown values refuse. Table unit types are published
without variable inheritance; catalog diff compares role and unit-type slugs. The
existing unit dictionary requires a non-null label and has no separate label-authoring
surface, so the slug label remains necessary for the retained table field.

Read-only carry-over checks ran against source metadata at inputs commit
`9186fcbceeb2283a50c7f684764869e4dda5765e` and the retained 0.66.0 artifact, with no
real-seed build:

- Every one of the 44 legacy delivery-alias statement/lock additions was removed.
- Nine local entries have exact source code-label matches and exclusively scoped hashes;
  six have the outside references listed above.
- All 63 classification declarations were checked without filtering: all have absent
  canonical codes, and two hash groups disagree.
- All six Socialstyrelsen workbook Svenska title cells equal corrected register names
  after existing reader trimming. All 83 classification names remain preserved.

The Operator runs the full pinned-input build, verify and 0.66.0 diff after push. No
real-seed build, full-artifact verify/diff, `gate.py regen`, publication or PR merge ran
in this fix session. A version-only move of `origin/main` was merged without rebasing.
No new tests were added.

Main-session validation:

- `cargo build --release -p reg-build --locked` passed.
- `gate.py g0 rust` passed: static checks, Clippy, workspace Rust tests, dependency
  audit, 116 script tests, byte-identical synthetic builds and 201 conformance checks
  (two skipped). Gate times: G0 40 s, Rust 14 s.
- One-off synthetic lock/build/verify passed. Publication queries confirmed local and
  inventory sentinel bits, table unit type, no variable inheritance, role, table and
  release names, and a genuine search synonym.
- Synthetic lock refusals passed for missing codes, a population-alternative mismatch,
  unknown role, disagreeing setters and disagreeing classification versions. Both
  locations are reported for shared-hash conflicts.
- Separate synthetic unit-only and same-hash sentinel-only edits each made
  `reg-build diff --json` exit 1 with the intended semantic delta visible.
- Independent Rust review passed. Omitted inventory sentinel lists skip serialization,
  preserving unspecified inventory assertion identities.

The nine local statements are not locked: the 63 retained invalid inventory lists
prevent a successful whole-inventory lock. No declarations were hidden or filtered to
produce locks. Resolve these authoring refusals, then lock `scb/r2 scb/r25 scb/r34`
before the Operator's strict build. This draft does not claim a passing real catalog.
