---
display_name: "Bilaga 5 — Hur FK-data tolkas"
tags:
  - type/appendix
  - topic/social-insurance
source: lisa-bakgrundsfakta-1990-2017
---


#### **Hur FK-data tolkas**

Om antal personer med ett visst ersättningsslag ska beräknas ska den variabel som anger antal bruttodagar/nettodagar användas. Beloppsvariablerna ska inte användas för detta ändamål.

**Sjukfall – antal fall/gånger som en person har haft sjukpenningförmåner under året (sjukpenning, arbetsskadesjukpenning, rehabiliteringspenning).**

*SjukFall_Startade_MiDAS* – antal startade fall med sjukförmåner under året

*SjukFall_Avslutade_MiDAS* – antal avslutade fall med sjukförmåner under året

*SjukFall_Antal_MiDAS* – summerar antal sjukfall under året med sjukförmåner under året

*SjukFall_Antal_MiDAS = SjukFall_Startade_MiDAS* = "Nya sjukfall för året"

*SjukFall_Antal_MiDAS > SjukFall_Startade_MiDAS* = "Det första sjukfallet för året är pågående sedan föregående år"

*SjukFall_Antal_MiDAS > SjukFall_Avslutade_MiDAS* = "Det sista sjukfallet för året följer med till året efter"

Nedan finns exempel på verkliga fall och hur de ska tolkas.

| Exempel | Antal Sjukfall | Antal startade sjukfall | Antal avslutade sjukfall |
|---------|----------------|-------------------------|--------------------------|
| 1       | 5              | 5                       | 5                        |
| 2       | 5              | 4                       | 5                        |
| 3       | 3              | 3                       | 2                        |

*Exempel 1*: En person som har 5 pågående sjukfall, alla 5 har startat och avslutat under året. Personen har inget pågående sjukfall från året innan och inget följer med till året efter.

*Exempel 2*: En person som har 5 pågående sjukfall, 4 har startat och 5 har avslutat under året. Personen gick då in med ett sjukfall från året innan men inget följer med till året efter.

*Exempel 3*: En person som har 3 pågående sjukfall, 3 har startat och 2 har avslutat under året. Personen har inget pågående sjukfall från året innan men ett följer med till året efter.

| Exempel | Antal Sjukfall | Antal startade sjukfall | Antal avslutade sjukfall |
|---------|----------------|-------------------------|--------------------------|
| 4       | 5              | 4                       | 4                        |
| 5       | 1              | 0                       | 0                        |

I *exempel 4 och 5* har båda sjukfall som gick över årsskiftet både vid årets start och slut.

I *exempel 5* kan man se att det skulle kunna vara 0 i både startade och avslutade även fast man har ett sjukfall, i det här fallet ett långt sjukfall som sträcker sig över två årsskiften.

Det är viktigt att notera att antal fall inte på ett enkelt sätt kan summeras över år, men naturligtvis kan summationer ske inom år.

Det finns ett antal fall där det finns belopp registrerade men där SjukSum_Bdag_MiDAS=0. Anledningen till detta är att där belopp per nettodag < 101 kronor så anses det vara en tilläggsutbetalning vilket inte ska generera några nya dagar.
