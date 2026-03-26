---
variable: Raks_Huvudanknytning
display_name: "Individens huvudanknytning till arbetsmarknad"
tags:
  - variable/activity-status
source: "lisa-bakgrundsfakta-1990-2017"
---

---

**Individens huvudanknytning till arbetsmarknad RAKS_Huvudanknytning**

**RAKS_Huvudanknytning**

(2003–)

Alla arbetsgivare lämnar in kontrolluppgifter (KU) till Skatteverket för sina anställda, vilket innebär att en konsoliderad tidsmarkering (KTM) kan beräknas. Konsolideringen innebär att en persons kontrolluppgifter från alla arbetsgivare och utbetalare av sociala ersättningar läggs samman. KTM anger för varje månad under året om individen varit anställd eller inte.

KTM anger om en person är: *helårsanställd, nyanställd, avgången* eller *delårsanställd*.

Dessutom finns information om huruvida personerna har deklarerat för näringsverksamhet. Personer som är anställda och bedriver näringsverksamhet parallellt klassificeras som *kombinatörer*, medan de som enbart bedriver näringsverksamhet kallas *företagare*. Det finns två slags företagare i statistiken: i) företagare i

fåmansaktiebolag (eget AB) och ii) egna företagare, dvs. personer som har enskild firma eller handelsbolag.

Den sista huvudaktiviteten är *utan arbete* om varken uppgift om näringsverksamhet eller kontrolluppgift från Skatteverket finns. Definitionerna görs med hjälp av individens KTM och uppgiften om huruvida individen har varit företagare under året (F=1) eller inte (F=0).

| Kod | Benämning      | Definition                               |
|-----|----------------|------------------------------------------|
| 1   | Helårsanställd | F=0 och KTM=jan-dec                      |
| 2   | Nyanställd     | F=0 och KTM=x-dec, x>jan                 |
| 3   | Avgången       | F=0 och KTM=jan-x, x <jan< td=""></jan<> |
| 4   | Delårsanställd | F=0 och KTM=övrigt                       |
| 5   | Kombinatör     | F=1 med KUA                              |
| 6   | Företagare     | F=1 utan KUA                             |
| 7   | Utan arbete    |                                          |

*För mer detaljerad information om huvudanknytningarna se SCB, Bakgrundsfakta, Arbetsmarknads- och utbildningsstatistik 2007:2, Registerbaserad aktivitetsstatistik Individens etablering till arbetsmarknad.*
