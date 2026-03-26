---
display_name: "Förändringar i LISA 2011"
tags:
  - topic/changelog
  - topic/changelog/2011
source: "hushallsinformation-i-lisa-2011-.md"
---

2021-06-18
## För att koppla personer till varandra i en familj tittar man på vilka som har en relation till varandra. Personerna ska vara folkbokförda på
samma fastighet och relationerna omfattar make/maka, registrerad partner, sambo som har/har haft gemensamma barn (biologiskt/ adoptiv), biologisk förälder, adoptivförälder, vårdnadshavare (för barn under 18 år) samt annan förälder än vårdnadshavare (fosterförälder).
Tidigare var en person folkbokförd på en fastighet, vilket innebar att sambos utan gemensamma barn som var folkbokförda på en fastighet med flera lägenheter inte kunde kopplas ihop.
Sedan 2010 folkbokför Skatteverket personer boende i flerbostadshus på lägenhet. Denna komplettering av folkbokföringen medför att alla personer kan klassificeras in i hushåll, även sambos utan gemensamma barn.
Variabeln som visar hushållsidentitet är unik för varje hushåll och avser det bostadshushåll som individen tillhör. Hushållet får ett nytt löpnummer varje år.
För att bilda sambopar av personer utan gemensamma barn används en
- modell utifrån nedanstående kriterier:
  - Personerna är folkbokförda på samma fastighet och lägenhet
  - Personerna är minst 18 år
  - Personerna är av olika kön • Åldersskillnaden mellan personerna är mindre än 15 år
  - Personerna är inte nära släkt
Endast ett möjligt sambopar kan bildas inom hushållet.
Av " [Hushållsdokumentation \(scb.se\)"](https://www.scb.se/contentassets/0168b57e7f1d4220983e5deed2f3f915/registerbaserad-hushallsstatistik.pdf) framgår mer information om hushåll.
## **Hushållsinformation i LISA**
Sedan juni 2021 finns hushållsinformation inlagt i LISA. Nedan variabler har lagts till för år 2011-.
## **Hushållsinformation i LISA**
| Klartext                     | Variabel       | År        |
|------------------------------|----------------|-----------|
| Hushålls-ID                  | HushallsID     | 2011-2019 |
| Hushållsställning enligt RTB | HushallsSt_RTB | 2011-2019 |
Image /page/0/Picture/18 description: This image features a stylized logo consisting of the letters 'SCB' in a bold, black, sans-serif font against a white background. The letters are arranged in a compact, vertically elongated manner, with the central letter 'C' being taller than the flanking 'S' and 'B'. The characters are closely spaced, creating a unified graphic mark.
| Klartext                                                 | Variabel        | År        |
|----------------------------------------------------------|-----------------|-----------|
| Hushållstyp enligt RTB                                   | Hushallstyp_RTB | 2011-2019 |
| Hushållsställning enligt IoT                             | HushallsSt_IoT  | 2011-2019 |
| Hushållstyp enligt IoT                                   | Hushallstyp_IoT | 2011-2019 |
| Konsumtionsvikt för hushållet                            | KonsviktHB04    | 2011-2019 |
| Ekonomiskt bistånd för hushållet                         | SocBidrHB       | 2011-2019 |
| Bostadsbidrag för hushållet                              | BostBidrHB      | 2011-2019 |
| Bostadstillägg för hushållet                             | BostTillHB      | 2011-2019 |
| Disponibel inkomst per konsumtionsenhet för<br>hushållet | DispInkKEHB04   | 2011-2019 |
| Disponibel inkomst för hushållet                         | DispInkHB04     | 2011-2019 |
Indelningarna i hushållsställning och hushållstyp skiljer sig åt mellan Inkomst- och taxeringsregistret (IoT) och Registret över totalbefolkningen (RTB), detta beror på att IoT är anpassad till den redovisning som används i den officiella inkomststatistiken. I LISA har båda indelningarna lagts till.
**Hushållsställning – skillnad mellan IoT och RTB**
| Hushållsställning IoT                     | Hushållsställning RTB                     |
|-------------------------------------------|-------------------------------------------|
| A1 - Person i gift par                    | 11 - Person i gift par                    |
| A2 - Person i ett registrerat partnerskap | 12 - Person i ett registrerat partnerskap |
| A3 - Person i samboförhållande            | 13 - Person i samboförhållande            |
| B1 - Ensamstående förälder                | 21 - Ensamstående förälder                |
|                                           | 31 - Barn 0-17 år                         |
|                                           | 32 - Barn 18-24 år                        |
|                                           | 33 - Barn över 24 år                      |
| C1 - Barn 0-19 år                         |                                           |
| C2 - Barn 20-29 år                        |                                           |
| C3 - Barn över 29 år                      |                                           |
| D1 - Ensamboende                          | 40 - Ensamboende                          |
| E1 - Övrig person                         |                                           |
|                                           | 51 - Övrig person 0-17 år                 |
|                                           | 52 - Övrig person 18-24 år                |
|                                           | 53 - Övrig person över 24 år              |
| Hushållsställning IoT | Hushållsställning RTB                                                    |
|-----------------------|--------------------------------------------------------------------------|
|                       | 99 - Uppgift saknas (inkl. På kommunen<br>skriven och Utan känt hemvist) |
## **Hushållstyp – skillnad mellan IoT och RTB**
| Hushållstyp IoT                                 | Hushållstyp RTB                              |
|-------------------------------------------------|----------------------------------------------|
| A1 - Ensamstående utan barn                     | 1.1 - Ensamstående utan barn                 |
|                                                 | 1.2 - Ensamstående med barn under 25 år      |
|                                                 | 1.3 - Ensamstående med barn över 24 år       |
| A2 - Ensamstående med barn under 20 år          |                                              |
| A3 - Ensamstående med barn 20-29 år             |                                              |
| A4 - Ensamstående med barn 30 år och<br>äldre   |                                              |
| B1 - Sammanboende utan barn                     | 2.1 - Sammanboende utan barn                 |
|                                                 | 2.2 - Sammanboende med barn under 25<br>år   |
|                                                 | 2.3 - Sammanboende med barn över 24<br>år    |
| B2 - Sammanboende med barn under 20 år          |                                              |
| B3 - Sammanboende med barn 20-29 år             |                                              |
| B4 - Sammanboende med barn 30 år och<br>äldre   |                                              |
| C1 - Övriga hushåll utan barn                   | 3.1 - Övriga hushåll utan barn               |
|                                                 | 3.2 - Övriga hushåll med barn under 25<br>år |
|                                                 | 3.3 - Övriga hushåll med barn över 24 år     |
| C2 - Övriga hushåll med barn under 20 år        |                                              |
| C3 - Övriga hushåll med barn 20-29 år           |                                              |
| C4 - Övriga hushåll med barn 30 år och<br>äldre |                                              |
