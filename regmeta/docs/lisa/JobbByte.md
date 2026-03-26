---
variable: JobbByte
display_name: "Bytt arbetsgivare"
tags:
  - type/variable
  - topic/employment
source: lisa-bakgrundsfakta-1990-2017
---


**Bytt arbetsgivare JobbByte**

(2002-)

Variabeln anger om en person, som har varit förvärvsarbetade bägge åren i ett årpar, har bytt arbete.

Uppgifter om arbetsgivarbyte baseras på Registerbaserad arbetsmarknadsstatistik (RAMS) och Företags- och arbetsställens dynamik (FAD). I RAMS finns kopplingen personnummer, organisationsnummer (företagsidentitet) och CFARnr (arbetsställets identitet) och i FAD finns kopplingen personnummer och FAD_F_ID (företagets identitet i FAD) och personnummer och FAD_A_ID (arbetsställets identitet i FAD).

FAD är en metod för att bättre följa företag och arbetsställen över tiden.

Om till exempel ett företag byter organisationsnummer vid ett ägarbyte så behåller företaget samma FAD_F_ID och om ett arbetsställe, av någon anledning får ett nytt CFARnr, så behåller arbetsstället samma FAD_A_ID i FAD.

De personer som inte bytt varken organisationsnummer, CFARnr, FAD_F_ID eller FAD_A_ID klassificeras som ej jobbytare och där alla identiteterna är olika mellan åren klassificeras personerna som jobbytare.

Personer som byter arbetsuppgifter inom ett och samma företag eller arbetsställe klassificeras som ej jobbytare.

#### Kod:

- 1 = Arbetsgivarbyte
- 2 = Utflöde (har gått från förvärvsarbetande till ej förvärvsarbetande)
- 3 = Inflöde (har gått från ej förvärvsarbetande till förvärvsarbetande)
- 4 = Inget arbetsgivarbyte

#### **Yrke**

Uppgifter i yrkesregistret ligger till grund för yrkesstatistiken vars syfte är att beskriva yrkesutövanden bland förvärvsarbetande inom olika branscher och samhällssektorer, samt följa yrkesutvecklingen över tid. Statistiken redovisar t.ex. antalet anställda med ett visst yrke, fördelat på yrke, ålder, kön och utbildning. Statistiken innehåller även fördelning av utövande yrken på bransch, sektor, län och kommun mm.

Yrkesregistret bygger på uppgifter från den registerbaserade arbetsmarknadsstatistiken (RAMS) och kompletteras med uppgifter om yrken från en mängd olika källor, som Arbetsgivarverket,

Lönsestrukturstatistikens undersökningar för privat och offentlig sektor och yrkesregistrets egen enkät till mindre företag.

Inom den offentliga sektorn genomförs separata undersökningar som omfattar alla anställda i kommun, stat och landsting.

Inom den privata sektorn genomförs två urvalsundersökningar, strukturlönestatistiken (SLP) och yrkesregistrets (Yreg) egna enkät.

Strukturlönestatistiken avseende den privata sektorn (SLP) är uppbyggd kring en avtalsstatistik producerad av olika arbetsgivarorganisationer, samt en kompletterande undersökning gjord av SCB. Vid uppgiftsinsamlingen samarbetar SCB med Svenskt Näringsliv (f.d. SAF) och sex andra arbetsgivarorganisationer. Undersökningen är i huvudsak urvalsbaserad. Samtliga företag med fler än 500 anställda undersöks varje år, medan bland företag med färre än 500 anställda görs ett sannolikhetsurval. Urvalet uppgår till drygt 11 000 företag.

Yrkesuppgifter från företag utanför lönestatistiken, i huvudsak mindre företag och organisationer med 1-19 anställda, samlas in av SCB med hjälp av en särskild yrkesenkät under vår och höst. Cirka 47 000 företag undersöks årligen enligt ett rullande schema, vilket innebär att samtliga företag i yrkesregistrets företagsram undersöks under fyra till fem års period. Under 2017 omfattades ca 240 000 individer av yrkesenkäten.

Yrke är ett svårdefinierat och mångsidigt begrepp. För att tillgodose olika behov har flera klassifikationssystem växt fram, utformade efter skilda kriterier. Inom lönestatistiken finns till exempel ett antal befattningsnomenklaturer, som endast berör vissa yrkesområden och som har mycket snäva och specifika tillämpningar. För att uppgifterna om yrke och befattning i de olika källorna ska bli jämförbara, måste uppgifterna som inte är klassificerade enligt SSYK översättas med hjälp av olika översättningsnycklar. Beroende på bl.a. den ursprungliga nomenklaturens detaljeringsgrad och likheter med SSYK, kan en sådan översättning bli mer eller mindre fullständig.

Yrkesuppgifterna från alla tillgängliga källor läggs ihop i ett s.k. bruttoregister. En person kan förekomma i ett som samma bruttoregister en eller flera gånger, beroende på hur många arbetsgivare som personen har haft inkomster ifrån under det aktuella året och i vilken utsträckning dessa arbetsgivare har lämnat in uppgift om personens yrke. Bruttoregistret innehåller även uppgifter om arbetsgivarens organisationsnummer.

För alla individer i individramen kopplas upp till fem yrken per individ. För att uppnå ett heltäckande yrkesregister nyttjas uppgifter från tidigare år. Till sin hjälp har man de årsvisa bruttoregistren, vilka innehåller individernas "alla" yrken insamlade av de olika källorna. Yrkesregistret avseende det aktuella referensåret skapas därefter i ett antal steg för att fastställa varje individs huvudsakliga yrkesuppgift.

När alla individer i bruttoregistret har fått en yrkesuppgift eller när alla tillgängliga yrkesuppgifter är genomgångna, skapas yrkesregister. Målsättningen är att alla förvärvsarbetande ska ha en yrkesuppgift. Variabeln StatusSsyk anger vilken yrkesuppgift som har använts vid tabellframställningen. StatusSsyk är en kvalitetsvariabel och visar hur stark koppling individen har till det redovisade yrket. I första hand används s.k. första yrket för respektive redovisningsgrupp. Det vill säga om personen är klassad som förvärvsarbetande under det aktuella året och det i bruttoregistret finns ett yrke som angivits av den aktuella arbetsgivaren, används denna uppgift. Detta ger StatusSsyk = 1.
