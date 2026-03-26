---
display_name: "Förändringar i LISA 2020"
tags:
  - type/changelog
source: lisa_2020-forandringar.md
---


# 2022-02-28
# **Förändringar i LISA 2020 och kompletteringar i tidigare årgångar föranlett av dessa**
# **Coronaersättningar**
Under 2020 införde regeringen åtgärder på socialförsäkringsområdet till följd av coronapandemin. Åtgärderna syftar till att minska smittspridningen, minska belastningen på sjukvården, stärka den ekonomiska tryggheten samt lindra konsekvenserna för arbetsgivare.
## **Ersättning för karensavdrag (karensdag)**
Karensavdraget ersätts tillfälligt för att underlätta för människor att stanna hemma vid sjukdom.
Den anställde får karensavdrag på sin sjuklön som vanligt sedan ansöker den anställde om ersättning hos Försäkringskassan. Ersättningen är 810 kr per dag. Arbetsgivaren gör inte något nytt karensavdrag om den anställde blir sjuk igen inom 5 kalenderdagar. Om den anställde har flera arbetsgivare som gör karensavdrag inom 5 kalenderdagar har den anställde endast rätt till ersättning för det första karensavdraget.
Egenföretagare med karensdagar i sjukperiodens början ansöker om ersättning för karensdagarna när dagarna med karensavdrag har passerat. Som egen företagare kan man välja hur många karensdagar man vill ha. Om inget val görs är det automatiskt 7 karensdagar. Ju fler karensdagar desto lägre blir sjukförsäkringsavgiften.
Karensersättningen till anställd ges en dag per sjuktillfälle, ersättningarna går att söka retroaktivt.
#### **Ersättning till riskgrupper**
De personer som tillhör en definierad riskgrupp och som inte har möjlighet att arbeta hemifrån eller måste avstå från arbete för att undvika att smittas av covid-19, har rätt till ersättning med högst 810 kronor per dag. Ersättning ges för arbetstid på 25 procent, 50 procent, 75 procent eller 100 procent där 100 procent motsvarar ett heltidsarbete
Även vissa anhöriga till riskgrupper ges rätt till ersättning.
Image /page/0/Picture/13 description: This image shows a black and white logo consisting of the letters 'SCB' in a bold, stylized, sans-serif font. The letters are tall and narrow, with the central 'C' being slightly taller than the 'S' on its left and the 'B' on its right. The letters are closely spaced, with the 'C' and 'B' sharing a vertical stroke.
Ersättningarna går att söka retroaktivt.
#### **Viss tillfällig föräldrapenning när skola och förskola stänger**
Åtgärden innebär att en förälder kan få tillfällig föräldrapenning om föräldern behöver avstå från förvärvsarbete för att vårda ett barn när till exempel en förskola eller skola är stängd i vissa situationer som är kopplade till sjukdomen covid-19.
Ersättningen ingår i ordinarie tillfällig föräldrapenning. Den ordinarie tillfälliga föräldrapenningen har ökat men det är få som sökt den tillfälliga föräldrapenningen när skola och förskola stänger.
## **Förebyggande ersättning för vab för barn som varit allvarligt sjuka**
Föräldrar till barn som nyligen har varit allvarligt sjuka och som behöver skyddas så de inte insjuknar i covid-19, kan få en förebyggande ersättning för vab om de behöver avstå från arbete.
Ersättningen ingår i ordinarie tillfällig föräldrapenning.
#### **Graviditetspenning till gravida vid risker i arbetsmiljön**
Gravida kan ha rätt till graviditetspenning om det finns risker i arbetsmiljön. Socialstyrelsen har den 26 februari 2021 informerat om en särskild risk för gravida i vecka 20–36 med anledning av covid-19. Denna riskfaktor ingår därmed vid bedömning av rätt till graviditetspenning. Det är arbetsgivaren som ansvarar för arbetsmiljön och som bedömer om en gravid ska stängas av från arbetet.
Ersättningen ingår i ordinarie graviditetspenning.
#### **Förstärkning av bostadsbidraget för barnfamiljer**
Ett tillfälligt tilläggsbidrag till barnfamiljer som har rätt till bostadsbidrag. Bidraget ska betalas ut med ett belopp som motsvarar 25 procent av storleken på det preliminära bostadsbidraget.
Ersättningen ingår i ordinarie bostadsbidrag.
### **Smittbärarpenning**
Smittbärarpenning ges till personer som har fått förhållningsregler från hälso- och sjukvården om att inte gå till arbetsplatsen eftersom personen är eller kan vara smittad av covid-19. Ansökan om smittbärarpenning ska inte göras om personen är så sjuk att den inte kan arbeta. Då gäller samma sak som när personen blir sjuk i vanliga fall.
Smittbärarpenning är inte en tillfällig ersättning med anledning av coronaviruset men under 2020 har antal personer med smittbärarpenning ökat kraftigt.
#### **Nya variabler i LISA**
| Klartext                                                                                                              | 2020                  | Källa |
|-----------------------------------------------------------------------------------------------------------------------|-----------------------|-------|
| Ersättning för karens, beloppet som utbetalats under<br>året oavsett när händelsen skett                              | KarensErs             | IoT   |
| Ersättning för karens, beloppet som utbetalats under<br>året kopplat till händelse under året                         | KarensErs_Belopp      | FK    |
| Ersättning för karens, antal bruttodagar                                                                              | KarensErs_Bdag        | FK    |
| Ersättning för karens till anställd, beloppet som<br>utbetalats under året kopplat till händelse under året           | KarensErs_Anst_Belopp | FK    |
| Ersättning för karens till anställd, antal bruttodagar                                                                | KarensErs_Anst_Bdag   | FK    |
| Ersättning för karens till egen företagare, beloppet<br>som utbetalats under året kopplat till händelse under<br>året | KarensErs_Egf_Belopp  | FK    |
| Ersättning för karens till egen företagare, antal<br>bruttodagar                                                      | KarensErs_Egf_Bdag    | FK    |
| Ersättning till riskgrupp, beloppet som utbetalats<br>under året oavsett när händelsen skett                          | RiskgruppErs          | IoT   |
| Ersättning till riskgrupp, beloppet som utbetalats<br>under året kopplat till händelse under året                     | RiskgruppErs_Belopp   | FK    |
| Ersättning till riskgrupp, antal bruttodagar                                                                          | Riskgrupp_Bdag        | FK    |
| Ersättning till riskgrupp, antal nettodagar                                                                           | Riskgrupp_Ndag        | FK    |
## **Sammansatta variabler**
Variabeln *SocInk* - innehåller inkomster som avser annan sysselsättning än aktivt arbetslivsdeltagande, inkomster som dessutom i princip utesluter möjligheten att arbeta heltid samtidigt som inkomsten erhålls. 2020 ingår även ersättning för karens och ersättning för riskgruppen.
## **Inkomster intjänade i annat nordiskt land**
Inkomst- och taxeringsregistret (IoT) och den officiella inkomststatistiken omfattar i huvudsak inkomster från arbete och olika transfereringar i Sverige. Inkomst som svenskar får från utlandet saknas därför i stor utsträckning i statistiken. Denna täckningsbrist påverkar inkomststatistiken i stort, men i synnerhet statistiken för kommuner som ligger nära grannländerna, där en förhållandevis stor andel av befolkningen pendlar till arbete över en riksgräns – det vill säga bor i Sverige men arbetar i grannlandet.
IoT har nu kompletterats med nordiska inkomster som inhämtats från Skatteverket.
Påverkan på inkomststatistiken blir sammanfattningsvis störst på regional nivå, där de tre kommunerna Årjäng, Strömstad och Eda, vilka samtliga gränsar mot Norge, sticker ut med störst inkomstökningar. Vid gränsen mot Danmark och Finland är det Malmö respektive Haparanda som ser sina inkomster öka mest.
Inkomsterna ökar generellt mer för män än för kvinnor samt för personer födda i något av de övriga nordiska länderna.
Inkomstspridningen för disponibel inkomst per konsumtionsenhet (k.e.) blir något lägre, främst för att inkomsterna ökar mer i den nedre halvan av inkomstfördelningen. Andelen med låg ekonomisk standard minskar något, samtidigt som det även finns en viss ökning av andelen med hög ekonomisk standard.
Mer information om påverkan på inkomststatistiken finns på SCB:s hemsida (Inkomster och skatter) i PM, Nordiska inkomster i IoT och inkomststatistiken 2011 – 2019 (pdf). [nordiska-inkomster-i](https://scb.se/contentassets/9e9d096d69934f5fa67503e0b56fc00b/nordiska-inkomster-i-inkomststatistiken-2011-2019.pdf)[inkomststatistiken-2011-2019.pdf \(scb.se\)](https://scb.se/contentassets/9e9d096d69934f5fa67503e0b56fc00b/nordiska-inkomster-i-inkomststatistiken-2011-2019.pdf)
Detta dokument beskriver kvaliteten på indata samt uppdateringen av IoT med dessa inkomster.
**Nya variabler och befintliga variabler i LISA som påverkas**
|    | Klartext                                                                                                   | Variabel           | År        |
|----|------------------------------------------------------------------------------------------------------------|--------------------|-----------|
| NY | Deklarerad lön – inkl. lön intjänat i annat nordiskt land                                                  | DekLon_INKLGP      | 2011–2019 |
|    | Deklarerad lön – från 2020 ingår lön intjänat i annat nordiskt land                                        | DekLon             | 1991–2020 |
| NY | Lön, utländsk beskattning (annat nordiskt land)                                                            | TTJLONU            | 2011–2020 |
| NY | Sammanräknad deklarerad förvärvsinkomst – inkl. lön intjänas i<br>annat nordiskt land                      | CSFVI_INKLGP       | 2011–2019 |
| NY | Sammanräknad deklarerad förvärvsinkomst –från 2020 ingår lön<br>intjänas i annat nordiskt land             | CSFVI              | 1990–2020 |
| NY | Pensioner, utländsk beskattning (annat nordiskt land)                                                      | TPENSAU            | 2011–2020 |
| NY | Summa inkomst av pensioner – inkl. pension från annat nordiskt<br>land                                     | AldPens_INKLGP     | 2011–2019 |
|    | Summa inkomst av pensioner - från 2020 ingår pension från annat<br>nordiskt land                           | AldPens            | 1990–2020 |
| NY | Disponibel inkomst per konsumtionsenhet för familj - inkl. lön<br>intjänat i annat nordiskt land           | DispInkKE_INKLGP   | 2011–2019 |
|    | Disponibel inkomst per konsumtionsenhet för familj - från 2020<br>ingår lön intjänat i annat nordiskt land | DispInkKE          | 1998–2020 |
| NY | Disponibel inkomst per konsumtionsenhet för familj - inkl. lön<br>intjänat i annat nordiskt land           | DispInkKE04_INKLGP | 2011–2019 |
|    | Disponibel inkomst per konsumtionsenhet för familj - från 2020<br>ingår lön intjänat i annat nordiskt land | DispInkKE04        | 2005–2020 |
|    | Klartext                                                                                                      | Variabel             | År        |
|----|---------------------------------------------------------------------------------------------------------------|----------------------|-----------|
| NY | Disponibel inkomst per konsumtionsenhet för hushållet – inkl. lön<br>intjänat i annat nordiskt land           | DispInkKEHB04_INKLGP | 2011–2019 |
|    | Disponibel inkomst per konsumtionsenhet för hushållet - från 2020<br>ingår lön intjänat i annat nordiskt land | DispInkKEHB04        | 2011–2020 |
| NY | Disponibel inkomst (individens delkomponent) - inkl. lön intjänat i<br>annat nordiskt land                    | DispInk04_INKLGP     | 2011–2019 |
|    | Disponibel inkomst (individens delkomponent) - från 2020 ingår lön<br>intjänat i annat nordiskt land          | DispInk04            | 2004–2020 |
| NY | Disponibel inkomst för familj - inkl. lön intjänat i annat nordiskt<br>land                                   | DispInkFam04_INKLGP  | 2011–2019 |
|    | Disponibel inkomst för familj - från 2020 ingår lön intjänat i annat<br>nordiskt land                         | DispInkFam04         | 2004–2020 |
| NY | Disponibel inkomst för hushållet- inkl. lön intjänat i annat nordiskt<br>land                                 | DispInkHB04_INKLGP   | 2011–2019 |
|    | Disponibel inkomst för hushållet - från 2020 ingår lön intjänat i<br>annat nordiskt land                      | DispInkHB04          | 2011–2020 |
# **Övrigt**
#### **Landindelning**
Den 31 januari 2020 genomfördes brexit, som innebar att Storbritannien lämnade EU-samarbetet. Antal EU-länder är nu 27 och därmed finns en ny grupperad landsvariabel.
| Klartext                                     | Variabel     |
|----------------------------------------------|--------------|
| Eget födelseland (grupperad EU27_2020)       | FodGrEg5     |
| Moderns födelseland (grupperad EU27_2020)    | FodGrMor5    |
| Fadern födelseland (grupperad EU27_2020)     | FodGrFar5    |
| Land vid in/utvandring (grupperad EU27_2020) | Inv_UtvGrEg5 |
| Klartext                            | Variabel  | År        |
|-------------------------------------|-----------|-----------|
| Medborgarskap (grupperad EU27_2020) | MedbGrEg5 | 1990–2020 |
#### **Familjeidentitet inom hushållet**
Familj identifieras med hjälp av familjeidentitet. Familjeidentitet (RTBfamilj) utgörs av personnumret för den äldste personen av maximalt två generationer som har relationer med varandra, som är folkbokförda på samma fastighet. Då fler än två generationer bor tillsammans bildas
RTB-familjen med utgångspunkt från den yngsta generationen, om den är ogift. En person kan endast ingå i en RTB-familj.
Sedan 2010 folkbokför Skatteverket personer boende i flerbostadshus på lägenhet. Denna komplettering av folkbokföringen medför att alla personer kan klassificeras in i hushåll, även sambos utan gemensamma barn. Variabeln som visar hushållsidentitet är unik för varje hushåll och avser det bostadshushåll som individen tillhör.
Flera familjer kan ingå i ett och samma hushåll, för att identifiera familjer inom ett hushåll finns ytterligare en familjeidentitet för detta.
| Klartext                        | Variabel | År        |
|---------------------------------|----------|-----------|
| Familjeidentitet inom hushållet | FamId2   | 2011–2020 |
#### **Stipendium**
Konstnärsnämnden fördelar ett brett spektrum av stipendier och bidrag till yrkesverksamma konstnärer inom bild, form, musik, teater, dans och film. Stödet riktar sig främst till de som är enskilda frilansande konstnärer, bosatt i Sverige och/eller med konstnärliga verksamhet här. Dramatiker får stöd av [Sveriges författarfond.](http://www.svff.se/)
| Klartext                                                                          | 2020   |
|-----------------------------------------------------------------------------------|--------|
| Ordinarie stipendium utbetalat av konstnärsnämnden och<br>Sveriges författarfond) | ISTIPO |
Krisstipendium är en del av det krisstöd från regeringen som Konstnärsnämnden fått i uppdrag att fördela ut. Syftet med stipendiet är att kompensera för yrkesverksamma konstnärers intäktsbortfall till följd av utebliven ersättning i samband med inställda konstnärliga uppdrag pga. covid-19.
Författarfonden tilldelades ett statligt krispaket för kulturen, för snabbast möjliga fördelning av stipendier till författare och andra litterära upphovsmän med anledning av de ekonomiska konsekvenser covid-19 medfört.
| Klartext                                                                    | 2020   |
|-----------------------------------------------------------------------------|--------|
| Krisstipendium utbetalat av konstnärsnämnden och<br>Sveriges författarfond) | ISTIPK |
## **Yrkesuppgifter**
I enlighet med gällande säkerhetsskyddslagstiftning har SCB genomfört en säkerhetsskyddsanalys som kartlagt om det vid myndigheten finns
information som måste skyddas i enlighet med lagstiftningen. Analysen har bland annat slagit fast att tillgången till information gällande vilka personer som arbetar som poliser och militärer enligt standarden för yrkesklassificering (SSYK 2012) måste begränsas. Det innebär att personer med yrkeskoderna 01, 02, 03 (militärer) och 3360 (poliser) inkluderas i ett mikrodatautlämnande men omkodning har gjorts så att dessa personer istället ingår i gruppen med okänt yrke. Detta gäller från och med årgång 2020 av Yrkesregistret, övriga register som hämtar uppgifterna från Yrkesregistret påverkas också.
