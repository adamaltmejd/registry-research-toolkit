---
variable: SjukSum_Bdag_MiDAS
display_name: "Summerar bruttodagar för ersättningsslagen sjuk-,rehabiliterings- och arbetsskadesjukpenning"
tags:
  - topic/employment
  - topic/social-insurance
  - type/variable
source: "lisa-bakgrundsfakta-1990-2017"
---

Det är viktigt att notera att antal fall inte på ett enkelt sätt kan summeras över år, men naturligtvis kan summationer ske inom år.

Det finns ett antal fall där det finns belopp registrerade men där SjukSum_Bdag_MiDAS=0. Anledningen till detta är att där belopp per nettodag < 101 kronor så anses det vara en tilläggsutbetalning vilket inte ska generera några nya dagar.

### **Bilaga 6**

#### **Ursprunglig variabelkälla**

Uppgift om ursprunglig variabelkälla anges för flertalet av de variabler som ej utgör aggregat av andra i databasen förekommande variabler.

| DEMOGRAFISKA VARIABLER                                                          |                                                                                                                                                                                         |
|---------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Ålder                                                                           | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Födelseår                                                                       | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Kön                                                                             | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Bostadslän                                                                      | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Bostadskommun                                                                   | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Bostadsförsamling                                                               | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| [[Distriktskod]]                                                                    | Folkbokföringsuppgift,<br>härleds utifrån<br>personnummer                                                                                                                               |
| Fastighetens löpnummer                                                          | SCB-löpnummer utifrån<br>folkbokföringsuppgift, ändras<br>efter egen flyttningsanmälan                                                                                                  |
| Fastighetsbeteckning                                                            | Folkbokföringsuppgift,<br>ändras efter egen<br>flyttningsanmälan                                                                                                                        |
| Antal flyttningar                                                               | Folkbokföringsuppgift,<br>ändras efter egen<br>flyttningsanmälan                                                                                                                        |
| Civilstånd                                                                      | Folkbokföringsuppgift,<br>ändras efter uppgift från<br>domstol, vigselförrättare.<br>Ändring vid änka/änkling<br>genomförs av<br>folkbokföringen                                        |
| Familjeställning                                                                | Härledd utifrån<br>folkbokföringsuppgifter                                                                                                                                              |
| Senaste invandringsår                                                           | Folkbokföringsuppgift,<br>registreras utifrån egen<br>flyttningsanmälan                                                                                                                 |
| Land vid invandring                                                             | Folkbokföringsuppgift,<br>registreras utifrån egen<br>flyttningsanmälan                                                                                                                 |
| Land vid utvandring                                                             | Folkbokföringsuppgift,<br>registreras utifrån egen<br>flyttningsanmälan                                                                                                                 |
| År och månad vid invandring                                                     | Folkbokföringsuppgift,<br>registreras utifrån egen<br>flyttningsanmälan                                                                                                                 |
| År och månad vid utvandring                                                     | Folkbokföringsuppgift,<br>registreras utifrån egen<br>flyttningsanmälan                                                                                                                 |
| Eget födelselän                                                                 | Folkbokföringsuppgift<br>(moderns folkbokföringslän)                                                                                                                                    |
| Eget födelseland                                                                | Folkbokföringsuppgift                                                                                                                                                                   |
| Moderns födelseland                                                             | Folkbokföringsuppgift                                                                                                                                                                   |
