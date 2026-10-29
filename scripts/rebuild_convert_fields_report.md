# D field carry-over audit

Deleted at milestone F (cutover), with the one-shot converters.

This supplements `rebuild_convert_report.md`; its historical drop counts are not
rewritten. Only source metadata is listed. No holdings or individual records are
included.

## Outcomes

- Synonyms: all 44 `enrichment.alias` entries converted to `set synonyms`; exact
  existing ledger variable ids are retained.
- Roles and unit types: no legacy curation key named `role` or `unit_type` exists; no
  values are inferred from names or panel metadata.
- Names: all 83 classification names already occur in `curation/classifications.toml`;
  no new statement is required. Legacy occurrence `errata.field name` is already
  translated as `label`, not entity `name`.
- Local sentinels: 15 entries remain dropped because the published schema has only
  value-set-wide `vs_item.sentinel`, without occurrence/period-scoped sentinel
  membership. Their explicit local scope cannot be widened.
- Global classification sentinels: 63 of the 83 books declare `sentinel_codes`. They
  remain dropped because the classification inventory and address grammar have no
  sentinel assignment surface; no new schema or grammar is invented.
- Other legacy fields: 12 `source_attribution` corrections remain dropped; this field is
  outside the five fields and has no four-kind field.
- Classification presentation: translated inventory retains the existing D-9 contract;
  omitted English names, URLs, aliases, family aliases, label sources and descriptions
  keep the named #2016 inventory-contract reason. Classification aliases cannot be
  variable synonyms: they name classification entities, not variables.

## Synonyms

  | Legacy source                                                        | Destination                                                                                | Outcome   |
  | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | --------- |
  | `reg_meta_build/curation/registers/scb/agi.toml:708`                 | `curation/registers/scb/r392/curation.toml` / `var:bostadsf-smahus-ul-ag-ag`               | converted |
  | `reg_meta_build/curation/registers/scb/agi.toml:713`                 | `curation/registers/scb/r392/curation.toml` / `var:bostadsf-smahus-ul-ag-ea`               | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:138`                 | `curation/registers/scb/r168/curation.toml` / `var:aktier-och-andelar`                     | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:143`                 | `curation/registers/scb/r168/curation.toml` / `var:bidrag-for-verksamheten`                | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:148`                 | `curation/registers/scb/r168/curation.toml` / `var:byggnader`                              | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:153`                 | `curation/registers/scb/r168/curation.toml` / `var:foradlingsvarde`                        | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:158`                 | `curation/registers/scb/r168/curation.toml` / `var:forandring-lager-pagaende-arbete`       | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:163`                 | `curation/registers/scb/r168/curation.toml` / `var:forbrukningsvarde`                      | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:168`                 | `curation/registers/scb/r168/curation.toml` / `var:kostnader-loner-ersattningar`           | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:173`                 | `curation/registers/scb/r168/curation.toml` / `var:kostnader-ovriga-sociala-avgifter`      | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:178`                 | `curation/registers/scb/r168/curation.toml` / `var:kundfordringar`                         | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:183`                 | `curation/registers/scb/r168/curation.toml` / `var:mark`                                   | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:188`                 | `curation/registers/scb/r168/curation.toml` / `var:medelantal-anstallda`                   | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:193`                 | `curation/registers/scb/r168/curation.toml` / `var:nettoomsattning`                        | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:198`                 | `curation/registers/scb/r168/curation.toml` / `var:nettoomsattning-justerad-merchanting`   | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:203`                 | `curation/registers/scb/r168/curation.toml` / `var:ovriga-personalkostnader`               | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:208`                 | `curation/registers/scb/r168/curation.toml` / `var:ovriga-kortfristiga-placeringar`        | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:213`                 | `curation/registers/scb/r168/curation.toml` / `var:pensionskostnader`                      | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:218`                 | `curation/registers/scb/r168/curation.toml` / `var:produktionsvarde`                       | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:223`                 | `curation/registers/scb/r168/curation.toml` / `var:rorelseresultat`                        | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:228`                 | `curation/registers/scb/r168/curation.toml` / `var:rorelseresultat-fore-avskrivningar`     | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:233`                 | `curation/registers/scb/r168/curation.toml` / `var:skatteskulder`                          | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:238`                 | `curation/registers/scb/r168/curation.toml` / `var:sociala-kostnader-personalkostnader`    | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:243`                 | `curation/registers/scb/r168/curation.toml` / `var:summa-bruttoinvesteringar`              | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:248`                 | `curation/registers/scb/r168/curation.toml` / `var:summa-eget-kapital`                     | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:253`                 | `curation/registers/scb/r168/curation.toml` / `var:summa-kostnader-ravaror-handelsvaror`   | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:258`                 | `curation/registers/scb/r168/curation.toml` / `var:summa-nettoinvesteringar`               | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:263`                 | `curation/registers/scb/r168/curation.toml` / `var:utdelning`                              | converted |
  | `reg_meta_build/curation/registers/scb/fek.toml:268`                 | `curation/registers/scb/r168/curation.toml` / `var:varulager`                              | converted |
  | `reg_meta_build/curation/registers/scb/fou.toml:14594`               | `curation/registers/scb/r182/curation.toml` / `var:redovisningsgrupp`                      | converted |
  | `reg_meta_build/curation/registers/scb/fou.toml:14599`               | `curation/registers/scb/r182/curation.toml` / `var:sektor`                                 | converted |
  | `reg_meta_build/curation/registers/scb/fou.toml:14604`               | `curation/registers/scb/r182/curation.toml` / `var:v700-1`                                 | converted |
  | `reg_meta_build/curation/registers/scb/fou.toml:14609`               | `curation/registers/scb/r182/curation.toml` / `var:v700-2`                                 | converted |
  | `reg_meta_build/curation/registers/scb/gymnasieskola-betyg.toml:47`  | `curation/registers/scb/r102/curation.toml` / `var:kurs`                                   | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7794` | `curation/registers/scb/r257/curation.toml` / `var:eco-enu`                                | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7799` | `curation/registers/scb/r257/curation.toml` / `var:eco-ext`                                | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7804` | `curation/registers/scb/r257/curation.toml` / `var:eco-rea`                                | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7809` | `curation/registers/scb/r257/curation.toml` / `var:eco-rep`                                | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7814` | `curation/registers/scb/r257/curation.toml` / `var:j20c`                                   | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7819` | `curation/registers/scb/r257/curation.toml` / `var:miljoinno-fororeningar-slutkonsumtion`  | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7824` | `curation/registers/scb/r257/curation.toml` / `var:miljoinnovation-atervinning-avfall`     | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7829` | `curation/registers/scb/r257/curation.toml` / `var:miljoinnovation-fororeningar-foretaget` | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7834` | `curation/registers/scb/r257/curation.toml` / `var:miljoinnovation-materialersattning`     | converted |
  | `reg_meta_build/curation/registers/scb/innovation-foretag.toml:7839` | `curation/registers/scb/r257/curation.toml` / `var:miljoinnovation-minskad-materialatgang` | converted |

## Local sentinels

Each entry preserves literal code labels and a finite occurrence window in the old tree.
Reason for every row: no occurrence/period-scoped sentinel home; global assignment would
widen the curated claim.

  | Legacy source                                          | Column       | Outcome                                             |
  | ------------------------------------------------------ | ------------ | --------------------------------------------------- |
  | `reg_meta_build/curation/registers/scb/iot.toml:4770`  | `KUSNI`      | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:4781`  | `KUSNI`      | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:4792`  | `KUSNI`      | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:4868`  | `BNGR`       | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:5035`  | `BSUN`       | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:5045`  | `MAKEBOST`   | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/iot.toml:5055`  | `TNLAN`      | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/lisa.toml:2633` | `Org_Sni92`  | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3231` | `KU1AstLan`  | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3241` | `KU2AstLan`  | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3251` | `KU3AstLan`  | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/lisa.toml:3261` | `AstLan`     | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1317`  | `Forsamling` | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1327`  | `Forsamling` | dropped: local sentinel scope has no published home |
  | `reg_meta_build/curation/registers/scb/rtb.toml:1337`  | `Kommun`     | dropped: local sentinel scope has no published home |

## Classification books

All 83 names are already converted. Global sentinel declarations and presentation
omissions are listed separately in the same row.

  | Legacy source                                                       | Name destination                                              | Sentinel outcome                                  | Other omitted fields                                                     |
  | ------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------ |
  | `reg_meta_build/curation/classifications/AGARKAT2000.toml:1`        | `class/agarkat/agarkat2000` (already converted)               | dropped: no inventory/address sentinel assignment | `name_en` (#2016 inventory contract)                                     |
  | `reg_meta_build/curation/classifications/AGARKAT2020.toml:1`        | `class/agarkat/agarkat2020` (already converted)               | dropped: no inventory/address sentinel assignment | `name_en` (#2016 inventory contract)                                     |
  | `reg_meta_build/curation/classifications/ATC.toml:1`                | `class/atc/atc` (already converted)                           | no sentinel declaration                           | `aliases`, `name_en`, `url` (#2016 inventory contract)                   |
  | `reg_meta_build/curation/classifications/DISTR2016.toml:1`          | `class/distrikt/distrikt2016` (already converted)             | no sentinel declaration                           | `aliases`, `description`, `name_en`, `url` (#2016 inventory contract)    |
  | `reg_meta_build/curation/classifications/DRG.toml:1`                | `class/drg/drg` (already converted)                           | no sentinel declaration                           | `aliases`, `name_en`, `url` (#2016 inventory contract)                   |
  | `reg_meta_build/curation/classifications/ICD-10-SE.toml:1`          | `class/icd/icd-10-se` (already converted)                     | no sentinel declaration                           | `aliases`, `family_aliases`, `name_en`, `url` (#2016 inventory contract) |
  | `reg_meta_build/curation/classifications/ICD-11-SE.toml:1`          | `class/icd/icd-11-se` (already converted)                     | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/ICD-8-KS68.toml:1`         | `class/icd/icd-8-ks68` (already converted)                    | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/ICD-9-KS87.toml:1`         | `class/icd/icd-9-ks87` (already converted)                    | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/ICF.toml:1`                | `class/icf/icf` (already converted)                           | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/ISCED-F2013.toml:1`        | `class/isced-f/isced-f2013` (already converted)               | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/ISCED2011.toml:1`          | `class/isced/isced2011` (already converted)                   | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/JURFORM2000.toml:1`        | `class/jurform/jurform2000` (already converted)               | dropped: no inventory/address sentinel assignment | `name_en` (#2016 inventory contract)                                     |
  | `reg_meta_build/curation/classifications/JURFORM2020.toml:1`        | `class/jurform/jurform2020` (already converted)               | dropped: no inventory/address sentinel assignment | `name_en` (#2016 inventory contract)                                     |
  | `reg_meta_build/curation/classifications/KS87-P.toml:1`             | `class/ks87-p/ks87-p` (already converted)                     | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/KSI.toml:1`                | `class/ksi/ksi` (already converted)                           | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/KVA.toml:12`               | `class/kva/kva` (already converted)                           | no sentinel declaration                           | `aliases`, `name_en`, `url` (#2016 inventory contract)                   |
  | `reg_meta_build/curation/classifications/LKF1980.toml:1`            | `class/lkf/lkf1980` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1981.toml:1`            | `class/lkf/lkf1981` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1982.toml:1`            | `class/lkf/lkf1982` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1983.toml:1`            | `class/lkf/lkf1983` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1984.toml:1`            | `class/lkf/lkf1984` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1985.toml:1`            | `class/lkf/lkf1985` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1986.toml:1`            | `class/lkf/lkf1986` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1987.toml:1`            | `class/lkf/lkf1987` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1988.toml:1`            | `class/lkf/lkf1988` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1989.toml:1`            | `class/lkf/lkf1989` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1990.toml:1`            | `class/lkf/lkf1990` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1991.toml:1`            | `class/lkf/lkf1991` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1992.toml:1`            | `class/lkf/lkf1992` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1993.toml:1`            | `class/lkf/lkf1993` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1994.toml:1`            | `class/lkf/lkf1994` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1995.toml:1`            | `class/lkf/lkf1995` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1996.toml:1`            | `class/lkf/lkf1996` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1997.toml:1`            | `class/lkf/lkf1997` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1998.toml:1`            | `class/lkf/lkf1998` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF1999.toml:1`            | `class/lkf/lkf1999` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2000.toml:1`            | `class/lkf/lkf2000` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2001.toml:1`            | `class/lkf/lkf2001` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2002.toml:1`            | `class/lkf/lkf2002` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2003.toml:1`            | `class/lkf/lkf2003` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2004.toml:1`            | `class/lkf/lkf2004` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2005.toml:1`            | `class/lkf/lkf2005` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2006.toml:1`            | `class/lkf/lkf2006` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2007.toml:1`            | `class/lkf/lkf2007` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2008.toml:1`            | `class/lkf/lkf2008` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2009.toml:1`            | `class/lkf/lkf2009` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2010.toml:1`            | `class/lkf/lkf2010` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2011.toml:1`            | `class/lkf/lkf2011` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2012.toml:1`            | `class/lkf/lkf2012` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2013.toml:1`            | `class/lkf/lkf2013` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2014.toml:1`            | `class/lkf/lkf2014` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2015.toml:1`            | `class/lkf/lkf2015` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2016.toml:1`            | `class/lkf/lkf2016` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2017.toml:1`            | `class/lkf/lkf2017` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2018.toml:1`            | `class/lkf/lkf2018` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2019.toml:1`            | `class/lkf/lkf2019` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2020.toml:1`            | `class/lkf/lkf2020` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2021.toml:1`            | `class/lkf/lkf2021` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2022.toml:1`            | `class/lkf/lkf2022` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2023.toml:1`            | `class/lkf/lkf2023` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2024.toml:1`            | `class/lkf/lkf2024` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2025.toml:1`            | `class/lkf/lkf2025` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/LKF2026.toml:1`            | `class/lkf/lkf2026` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/MDC.toml:1`                | `class/mdc/mdc` (already converted)                           | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/NIVA-GROV.toml:15`         | `class/niva-grov/niva-grovv1` (already converted)             | no sentinel declaration                           | `description`, `name_en` (#2016 inventory contract)                      |
  | `reg_meta_build/curation/classifications/NIVA-OLD.toml:15`          | `class/niva-old/niva-oldv1` (already converted)               | dropped: no inventory/address sentinel assignment | `description`, `name_en` (#2016 inventory contract)                      |
  | `reg_meta_build/curation/classifications/SEKTOR2000.toml:6`         | `class/sektor/sektor2000` (already converted)                 | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SEKTORKOD.toml:17`         | `class/sektorkod/sektorkod` (already converted)               | dropped: no inventory/address sentinel assignment | `name_en` (#2016 inventory contract)                                     |
  | `reg_meta_build/curation/classifications/SNI2002.toml:1`            | `class/sni/sni2002` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SNI2007.toml:1`            | `class/sni/sni2007` (already converted)                       | dropped: no inventory/address sentinel assignment | `aliases`, `name_en`, `url` (#2016 inventory contract)                   |
  | `reg_meta_build/curation/classifications/SNI2025.toml:1`            | `class/sni/sni2025` (already converted)                       | no sentinel declaration                           | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SNI69.toml:1`              | `class/sni/sni1969` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SNI92.toml:1`              | `class/sni/sni1992` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SSYK2012.toml:1`           | `class/ssyk/ssyk2012` (already converted)                     | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SSYK96.toml:1`             | `class/ssyk/ssyk1996` (already converted)                     | dropped: no inventory/address sentinel assignment | `family_aliases`, `name_en`, `url` (#2016 inventory contract)            |
  | `reg_meta_build/curation/classifications/SUN1996.toml:1`            | `class/sun/sun1996` (already converted)                       | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SUN2000-GRUPP.toml:1`      | `class/sun-grupp/sun2000-grupp` (already converted)           | dropped: no inventory/address sentinel assignment | `description`, `name_en`, `url` (#2016 inventory contract)               |
  | `reg_meta_build/curation/classifications/SUN2000-INRIKTNING.toml:1` | `class/sun-inriktning/sun2000-inriktning` (already converted) | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SUN2000-NIVA.toml:16`      | `class/sun-niva/sun2000-niva` (already converted)             | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SUN2020-GRUPP.toml:1`      | `class/sun-grupp/sun2020-grupp` (already converted)           | dropped: no inventory/address sentinel assignment | `description`, `name_en`, `url` (#2016 inventory contract)               |
  | `reg_meta_build/curation/classifications/SUN2020-INRIKTNING.toml:1` | `class/sun-inriktning/sun2020-inriktning` (already converted) | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |
  | `reg_meta_build/curation/classifications/SUN2020-NIVA.toml:1`       | `class/sun-niva/sun2020-niva` (already converted)             | dropped: no inventory/address sentinel assignment | `name_en`, `url` (#2016 inventory contract)                              |

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

These scalar declarations are outside the historical array-entry drop census. They are
not inferred to be variable labels.

  | Legacy source                                       | Entity                | Outcome                                                                                                   |
  | --------------------------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------- |
  | `reg_meta_build/curation/registers/scb/gdb.toml:44` | table `individer`     | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it |
  | `reg_meta_build/curation/registers/scb/gdb.toml:53` | table `arbetsstallen` | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it |
  | `reg_meta_build/curation/registers/scb/gdb.toml:62` | table `skolor`        | dropped: authored table has no existing ledger entry or curation declaration; name alone cannot assert it |
  | `reg_meta_build/curation/registers/sos/bu.toml:5`   | register              | dropped: no register selector in the four-kind address grammar                                            |
  | `reg_meta_build/curation/registers/sos/dors.toml:5` | register              | dropped: no register selector in the four-kind address grammar                                            |
  | `reg_meta_build/curation/registers/sos/lmed.toml:5` | register              | dropped: no register selector in the four-kind address grammar                                            |
  | `reg_meta_build/curation/registers/sos/lova.toml:5` | register              | dropped: no register selector in the four-kind address grammar                                            |
  | `reg_meta_build/curation/registers/sos/lvm.toml:5`  | register              | dropped: no register selector in the four-kind address grammar                                            |
  | `reg_meta_build/curation/registers/sos/par.toml:5`  | register              | dropped: no register selector in the four-kind address grammar                                            |

## Implementation and validation

The existing published homes are used without a schema change: table roles in
`tbl.role`, occurrence and segment roles in their `role` columns, unit types in
`unit_type`, `tbl.unit_type_id` and `variable_unit_type`, table/release names in
`name_text`, and variable synonyms in the FTS `label` column. Synonyms do not add
delivery aliases. Whole-variable metadata setters share the existing sets/tags ownership
and conflict rules. Explicit variable unit types replace inherited table unit types.
Table setters require stated/asserted tables; synthesized implicit tables remain
refused.

No new tests were added or existing assertions changed. Independent review found and
corrected loss of a selected role among overlapping occurrences and unit-type
ownership/provenance before verification.

Passed on the final implementation:

- `cargo build --release -p reg-build --locked`.
- `gate.py g0 rust`: lint, formatting, type checks, Clippy, all workspace Rust tests,
  dependency audit, 116 script tests, byte-identical synthetic rebuilds, and 201
  conformance checks (two skipped). Final gate times: G0 26 s, Rust 5 s.
- One-off synthetic lock/build with the release binary, followed by `verify`. Direct
  publication queries confirmed table name/role/unit type, segment role, explicit
  variable unit type, release name and synonym FTS membership. A sentinel setter refused
  at its statement file and line with the missing-scope reason.
- All 44 synonym destinations exist; all 83 legacy classification names match the
  existing inventory exactly.
- `reg-build lock scb/r102 scb/r168 scb/r182 scb/r257 scb/r392` passed against clean
  inputs commit `9186fcbceeb2283a50c7f684764869e4dda5765e` and the pinned SCB archive
  SHA-256 `a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`. Exactly 44
  premises were added (1, 27, 4, 10, 2 respectively); every existing premise is
  unchanged. There were 45 redundant-set notices and no refusals.

The requested full pinned-input release build, verification of that full artifact, and
`reg-build diff` against 0.66.0 were not run. The task simultaneously prohibits
real-seed builds and requests this build; clarification was requested before execution.
In addition, the sandbox denies both `pgrep` and `ps`, preventing the required
idle-build check. The 0.66.0 release asset became available during this session and was
downloaded once; no artifact-content comparison is claimed. Consequently the per-field
real-catalog diff remains unverified, including synonyms. No `gate.py regen`, legacy
real-seed build, release publication or merge ran.
