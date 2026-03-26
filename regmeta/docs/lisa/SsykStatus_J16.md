---
variable: SsykStatus_J16
display_name: "Yrkets överensstämmelse med det jobb som valts som huvudsaklig verksamhet under november, inkl. härledda yrken för företagare"
tags:
  - type/variable
  - topic/employment
source: lisa-bakgrundsfakta-1990-2017
---


**Yrkets överensstämmelse med det jobb som valts som huvudsaklig verksamhet under november, inkl. härledda yrken för företagare SsykStatus_J16**

(2010-)

Från år 2016 ingår även härledda yrken för företagare. Denna metod har tillämpats bakåt i tiden så uppgift om yrket stämmer med det jobb som valts som huvudsaklig verksamhet under november inklusive de härledda yrkena för företagare finns sedan 2010.

För att undvika att företagare (oavsett om de är enskilda näringsidkare eller ägare av egna AB) får en gammal yrkeskod från en tidigare anställning eller att de helt saknar yrkeskod har man med hjälp av utbildning, licens, bransch och företagsnamn härlett ett yrke. Detta är en kvalitetsförbättring som ger fler företagare uppgift om yrke.

Variabeln SsykStatus_J16 är en så kallad kvalitetsvariabel som anger om yrkesuppgiften hör samman med det företag som personen fått sin förvärvsinkomst från. Om man inte hittar en uppgift för aktuellt år används eventuell uppgift om yrke från tidigare år om personen fortfarande finns på samma företag. Matchar SSYK-koden med företaget får personen SsykStatus_J16=1, oavsett vilket år som uppgiften är hämtad från. Om ingen uppgift återfinns som anger yrke för personen vid aktuellt företag används SSYK-koden från annan inkomst personen haft under mätåret. I dessa fall blir värdet 2–4. Värdet 5 visar den mest aktuella SSYK-koden som finns för personen oavsett företag om inget yrke funnits i någon av kategorierna 1–4.

Kod 11-15 gäller endast företagare där yrkeskoden har blivit härledd.

#### Kod 2010-:

- 1 = Yrke från det jobb som valts som huvudsaklig under november
- 2 = Yrke från största förvärvskälla under år (>1 prisbasbelopp)
- 3 = Yrke från jobb 2 under november
- 4 = Yrke från näst största förvärvskälla under år (>1 prisbasbelopp)
- 5 = Yrke från övriga
- 6 = Saknar uppgift
- 13 = Härlett yrke med koppling till utbildning eller licens och bransch
- 14 = Härlett yrke med koppling till företagsnamn och bransch
- 15 = Härlett yrke med koppling till bransch

#### **Kvalitet på yrkesuppgift 2017, SSYK 2012**

| SsykStatus_J16 | Förvärvsarbetande, % | Anställda 16 år och | äldre, % Anställda 16-64 år, % |
|----------------|----------------------|---------------------|--------------------------------|
| 1              | 78,8                 | 82,9                | 83,8                           |
| 2              | 1,7                  | 1,7                 | 1,8                            |
| 3              | 0,8                  | 0,8                 | 0,8                            |
| 4              | 0,9                  | 0,9                 | 0,9                            |
| 5              | 3,3                  | 3,2                 | 3,2                            |
| 6              | 5,5                  | 4,8                 | 4,5                            |
| 13*            | 1,1                  | 0,3                 | 0,3                            |
| 14*            | 1,6                  | 0,4                 | 0,4                            |
| 15*            | 0,8                  | 0,0                 | 0,0                            |

\*) Avser endast företagare med ett härlett yrke
