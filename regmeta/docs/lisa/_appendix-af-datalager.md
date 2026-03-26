---
display_name: "Bilaga 2 — Bearbetning av data från Arbetsförmedlingens Datalager"
tags:
  - type/appendix
  - topic/employment
source: lisa-bakgrundsfakta-1990-2017
---


### **Bearbetning av data från Arbetsförmedlingens Datalager**

De variabler i LISA-databasen som helt eller delvis har sin källa i Arbetsförmedlingens Datalager är170:

- Antal dagar i arbetslöshet
- Antal dagar som "Övrig inskriven vid arbetsförmedling"
- Antal dagar i deltidsarbetslöshet
- Antal dagar som arbetssökande tillfälligt timanställd
- Antal dagar i åtgärdsstudier
- Antal dagar i åtgärdssysselsättning
- Förekomst av åtgärdssysselsättning
- Arbetslöshetskod
- Åtgärdskod

De variabler i Datalagret som primärt använts för att härleda ovanstående variabler är startdatum för sökandekategoriperiod (inskadm), slutdatum för sökandekategoriperiod (utskadm), sökandekategori (skat) samt kod för arbetshandikapp (ahkp). Vid beräkningsarbetet har även sökandekategoriperiodens löpnummer samt antal dagar i sökandekategori använts.

Datalagret omfattar samtliga händelser (= arbetssökandeperioder) från och med augusti 1991. I LISA har informationen använts från och med 1992.

Dataregistreringen fungerar på så sätt att arbetsförmedlaren skriver in startdatum för en sökandekategoriperiod. Att en sökandekategori avslutats registreras inte manuellt. När en sökande byter sökandekategori registreras en sökandekategori till. I Datalagret sätts samtidigt automatiskt ett slutdatum för den närmast föregående kategorin.

När en arbetssökande avaktualiseras från Arbetsförmedlingen sätts slutdatumet för den sista sökandekategoriperioden i inskrivningsperioden automatiskt till samma datum som avaktualiseringsdatumet. Tidigare inmatade sökandekategoriperioder

170 I LISA finns även uppgifter hämtade från Arbetsförmedlingens månadsfiler (PRESO) för november månad 1998–2002. Se vidare under *Arbetssökande (november),* [[ArbSokNov]].

kan inte tas bort eller ändras. Man går således aldrig in och korrigerar redan inmatade uppgifter.

Att tidigare inmatade sökandekategoriperioder inte kan tas bort eller ändras medför att de data som finns i Datalagret omfattar överlappande perioder (att flera perioder har samma startdatum, alternativt att en period har ett startdatum som ligger mellan start- och slutdatum för tidigare period). Det medför även att negativa sökandekategoriperioder förekommer (periodens slutdatum ligger tidigare än dess startdatum).

Skälen till ovanstående problematik torde vara att man antingen korrigerat tidigare inmatade uppgifter eller att man matat in fel datum.

Data har bearbetats innan de lagts in i LISA.
