---
display_name: "Förändringar i LISA 2019"
tags:
  - type/changelog
source: "lisa-2019---forandringar.md"
---

2021-04-22

# **Förändringar i LISA 2019**

# **Utbildningsvariabler**

Våren 2019 fastställde SCB den nya versionen av SUN, benämnd SUN 2020. I Utbildningsregistret implementeras SUN 2020 för läsåret 2018/19 med publicering våren 2020. Utöver en uppdaterad struktur för inriktningar har innehållsbeskrivningar för såväl nivåer som inriktningar förbättrats och förtydligats med fler exempel på utbildningar som ingår eller inte ingår i respektive kod. Utbildningsinriktning och utbildningsgrupp redovisas för både SUN 2000 och SUN 2020 då inriktningsstrukturen har uppdaterats. Utbildningsnivå dubbelredovisas dock inte då nivåstrukturen är oförändrad.

# [SUN 2020 \(scb.se\)](https://scb.se/contentassets/aeeedec0e28c465aa524429407dcd5ba/sun_2020_version_1.1.pdf)

| Klartext                                                                | 2018            | 2019            |
|-------------------------------------------------------------------------|-----------------|-----------------|
| Utbildningsnivå, högsta, aggregerad till 7<br>'svenska' nivåer, SUN2020 |                 | SUN2020Niva_Old |
| Utbildningsnivå, högsta avslutade, SUN2020                              |                 | SUN2020Niva     |
| Utbildningsinriktning, högsta avslutade,<br>SUN2020                     |                 | SUN2020Inr      |
| Utbildningsgrupp, högsta avslutade,<br>SUN2020                          |                 | SUN2020Grp      |
| Utbildningsnivå, högsta, aggregerad till 7<br>'svenska' nivåer, SUN2000 | SUN2000Niva_Old |                 |
| Utbildningsnivå, högsta avslutade, SUN2000                              | SUN2000Niva     |                 |
| Utbildningsinriktning, högsta avslutade,<br>SUN2000                     | SUN2000Inr      | SUN2000Inr      |
| Utbildningsgrupp, högsta avslutade,<br>SUN2000                          | SUN2000Grp      | SUN2000Grp      |

# **Sysselsättningsvariabler**

## **Förvärvsarbetande**

Från och med referensår 2019 används en ny datakälla och metod för att klassificera förvärvsarbetande i RAMS. De månatliga arbetsgivardeklarationerna på individnivå (AGI) ersätter de årliga

Image /page/0/Picture/10 description: This image features a stylized logo consisting of the letters 'SCB' in a bold, black, sans-serif font against a white background. The letters are arranged horizontally, with the central 'C' being significantly taller than the 'S' on its left and the 'B' on its right, giving the logo a vertically elongated appearance.

kontrolluppgifterna (KU) och förvärvsarbetande klassificeras numera efter utbetald lön i november.

#### **Tillfälligt frånvarande**

Även vissa personer som tillfälligt inte är i arbete klassificeras som förvärvsarbetande. Det gäller personer som fått sjuk- eller föräldrapenning under november och som fått en löneutbetalning enligt AGI i anslutning till frånvaron.

#### **Arbetsställe- och företagsinformation**

Ett huvudsakligt arbetsställe bestäms för varje person som fått AGI eller haft inkomst av näringsverksamhet. Detta för att kunna koppla arbetsställe- och företagsinformation så som bransch och sektor till personen. Arbetsställe-/företagsvariabler redovisas även för personer som inte klassats som förvärvsarbetande men som ändå fått AGI från arbetsgivare eller deklarerat som egen företagare under året.

[Registerbaserad arbetsmarknadsstatistik \(RAMS\) \(scb.se\)](https://www.scb.se/hitta-statistik/statistik-efter-amne/arbetsmarknad/sysselsattning-forvarvsarbete-och-arbetstider/registerbaserad-arbetsmarknadsstatistik-rams/)

**Huvudsakligt arbete**

| Klartext                                                                           | Variabel     |
|------------------------------------------------------------------------------------|--------------|
| Sysselsättningsstatus enligt justerad metod 2019                                   | SyssStat19   |
| Antal AGI som individen erhållit under året                                        | AntalAGI     |
| Antal aktiva/passiva företag som individen har                                     | AntalEgf     |
| Antal förvärvskällor, samtliga                                                     | AntalJOBB    |
| Tillfälligt frånvarande                                                            | TFranv       |
| Antal månader man varit tillfälligt frånvarande under året                         | AntManTFranv |
| Erhållen lönegaranti, förekomst av                                                 | LonGarant    |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - januari  | LonFInkJan   |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - februari | LonFInkFeb   |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - mars     | LonFInkMars  |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - april    | LonFInkApril |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - maj      | LonFInkMaj   |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - juni     | LonFInkJuni  |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - juli     | LonFInkJuli  |

| Klartext                                                                            | Variabel   |
|-------------------------------------------------------------------------------------|------------|
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - augusti   | LonFInkAug |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - september | LonFInkSep |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - oktober   | LonFInkOkt |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - november  | LonFInkNov |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - december  | LonFInkDec |

För personer som har haft flera jobb och/eller näringsverksamheter under året läggs information om jobb under "*Största, näst största och tredje största förvärvskälla"* i LISA.

**Största (AGI1), näst största (AGI2) och tredje största förvärvskälla (AGI3)**

| Klartext                                                                           | Variabel                                                 |
|------------------------------------------------------------------------------------|----------------------------------------------------------|
| Erhållen lönegaranti, förekomst av                                                 | AGI1LonGarant<br>AGI2LonGarant<br>AGI3LonGarant          |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - januari  | AGI1LonFInkJan<br>AGI2LonFInkJan<br>AGI3LonFInkJan       |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - februari | AGI1LonFInkFeb<br>AGI2LonFInkFeb<br>AGI3LonFInkFeb       |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - mars     | AGI1LonFInkMars<br>AGI2LonFInkMars<br>AGI3LonFInkMars    |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - april    | AGI1LonFInkApril<br>AGI2LonFInkApril<br>AGI3LonFInkApril |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - maj      | AGI1LonFInkMaj<br>AGI2LonFInkMaj<br>AGI3LonFInkMaj       |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - juni     | AGI1LonFInkJuni<br>AGI2LonFInkJuni<br>AGI3LonFInkJuni    |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - juli     | AGI1LonFInkJuli<br>AGI2LonFInkJuli<br>AGI3LonFInkJuli    |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - augusti  | AGI1LonFInkAug<br>AGI2LonFInkAug<br>AGI3LonFInkAug       |

| Klartext                                                                            | Variabel                                           |
|-------------------------------------------------------------------------------------|----------------------------------------------------|
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - september | AGI1LonFInkSep<br>AGI2LonFInkSep<br>AGI3LonFInkSep |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - oktober   | AGI1LonFInkOkt<br>AGI2LonFInkOkt<br>AGI3LonFInkOkt |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - november  | AGI1LonFInkNov<br>AGI2LonFInkNov<br>AGI3LonFInkNov |
| Kontant bruttolön för anställning eller inkomst av<br>näringsverksamhet - december  | AGI1LonFInkDec<br>AGI2LonFInkDec<br>AGI3LonFInkDec |

# **Tjänstepension**

Från och med 2019 försvinner uppgifterna från Statens pensionsverk och variabler försvinner från kontrolluppgifterna i samband med övergången till AGI. Uppdelningen av olika typer av tjänstepension har försvunnit, från 2019 finns den en variabel för tjänstepension från utbetalare av tjänstepension.

| Klartext                                                                                          | 2018   | 2019    |
|---------------------------------------------------------------------------------------------------|--------|---------|
| [[ITP]] – industrins och handelns tjänste- och<br>tilläggspension                                     | [[ITP]]    |         |
| Kommunal- och landstingsanställdas<br>tjänstepension                                              | [[KTjP]]   |         |
| Statlig tjänstepension                                                                            | [[STjP]]   |         |
| Tjänstepension från statligt bolag                                                                | [[SBTjP]]  |         |
| Särskild tjänste- och tilläggspension för<br>privatanställda LO-medlemmar                         | [[STP]]    |         |
| Övrig tjänste- och tilläggspension                                                                | [[OvrTjp]] |         |
| Tjänstepension, från utbetalaren av tjänstepension                                                |        | PTjP    |
| Tjänstepension enligt individuellt tjänstepensions-<br>avtal mellan arbetsgivare och arbetstagare | [[KUPens]] | AGIPens |
| Summa tjänstepension                                                                              | SumTjp | SumTjp  |

#### **Vårdbidrag**

Vårdbidrag är ett stöd för föräldrar som vårdar ett barn med funktionsnedsättning eller långvarig sjukdom.

Det går inte längre att ansöka om vårdbidrag. Från 2019 ansöker man om omvårdnadsbidrag och merkostnadsersättning för barnet. Vårdbidraget betalas fortfarande ut till de som har ett tidigare beslut.

| Klartext                                 | 2018     | 2019       |
|------------------------------------------|----------|------------|
| Vårdbidrag                               | [[VardBidr]] | [[VardBidr]]   |
| Omvårdnadsbidrag + ferieomvårdnadsbidrag |          | OmVardBidr |
| Merkostnadsersättning barn               |          | MerkErsB   |

### **Handikappersättning**

Handikappersättning är en ersättning som betalas ut till personer som har en funktionsnedsättning eller sjukdom som gör att de behöver hjälp i vardagen eller har merkostnader.

Det går inte längre att ansöka om handikappersättning. Från 2019 ansöker man om merkostnadsersättning. Handikappersättning betalas fortfarande ut till de som har ett tidigare beslut.

| Klartext                    | 2018    | 2019     |
|-----------------------------|---------|----------|
| Handikappersättning         | [[HKapErs]] | [[HKapErs]]  |
| Merkostnadsersättning vuxen |         | MerkErsV |
