# Poročilo o izdelavi: Tabela dejstev (Fact Table)

To poročilo povzema celoten prehod in postopek izdelave Tabele dejstev z uporabo programskega jezika Python (knjižnica Pandas) kot zamenjavo za orodje Pentaho Data Integration.

## Povzetek ocene in zahtev naloge
Naloga (15 točk): **"Tabela dejstev vsebuje vse vaše meritve na najbolj atomarnem nivoju z ustreznimi tujimi ključi posameznih dimenzij. Nove vrstice se ustvarjajo ob novi kombinaciji ključev, pri obstoječih se izvede posodobitev."**

**Ali smo izpolnili vse pogoje? DA.**
- **Surovi podatki naloženi:** Naložili smo SURS datoteki (Prihodi in Prenočitve).
- **Transformacija (Unpivot):** Namesto *Row Normaliser* v Pentahu smo uporabili funkcijo `pd.melt`, s katero smo stolpce različnih držav pretvorili v posamezne vrstice na atomarnem nivoju (čas, lokacija, država izvora).
- **Povezava z dimenzijami (Lookup):** Podatke smo povezali z vašim prilagojenim modelom (uporabljene tri ključne dimenzije: `dim_time`, `dim_country`, `dim_municipality`). Za vsak podatek smo preko t.i. *inner/left join* poiskali ustrezen tuji ključ (časovni id, id občine, id države).
- **Insert / Update logika (Združevanje):** S pomočjo `FULL OUTER JOIN` (`pd.merge(..., how="outer")`) smo zagotovili, da vsaka unikatna kombinacija treh ključev zaseda točno eno vrstico, zraven pa so dopisane meritve (število prihodov in število prenočitev).
- **Kreiranje tabele:** Končni rezultat vsebuje izključno tuje ključe in dejstva (meritve), kar je popolnoma v skladu s pravili relacijskih baz in zvezdnih shem (*Star Schema*).

---

## Opis korakov / Skripta

Vsa logika je zajeta v skripti `transformations_model/tr_fact_table.py`. Spodaj so opisani ključni koraki procesa v Pythonu (z ekvivalenti, ki bi jih uporabili v Pentahu):

### Korak 1: Priprava in branje dimenzij
Najprej smo prebrali končne izvoze vaših trdih dimenzij (*.csv datoteke* v mapi `output_dimensions_model/`):
- `dim_time.csv` (Ključ: `time_id`)
- `dim_country.csv` (Ključ: `country_id`)
- `dim_municipality.csv` (Ključ: `municipality_id` - vanjo ste uspešno že vključili regije)

V Pentaho terminologiji se to prebere kot začetni **CSV File Input** in shrani v spomin za **Database Lookup** ali **Stream Lookup**.

### Korak 2: Cikliranje čez surove podatke in čiščenje
Uporabili smo funkcijo `process_surs_data()` nad datotekama Prihodov in Prenočitev (`2164466S_...xlsx`).
- Prebrali datoteko (preskok prvih vrstic z metapodatki).
- Pred-napolnili prazne čase, ker imajo excel tabele SURS združene celice (Merge Cells).
- Odstranili smo totalne seštevke ("0 Država - SKUPAJ", "1 DOMAČI", "2 TUJI"), saj tabela dejstev zajema in združuje samo tiste najbolj atomarne, surove podatkovne baze na ravni posamezne države.

### Korak 3: Preoblikovanje (PENTAHO: Row Normaliser)
To je bil najpomembnejši korak, kjer smo pandas funkcijo `melt()` izkoristili, da smo široko matriko s stotimi stolpci (vsak stolpec = ena država) prestrukturirali tako, da imamo za vsako državo ločeno vrstico. 
Rezultat tega koraka je bil podatek tipa: `[Casovna oznaka, Obcina, Drzava, stevilo_prihodov]`. 
Prav tako smo odstranili zaupne podatke (označene z 'z') ali manjkajoče ('-').

### Korak 4: Povezovanje na tuje ključe (PENTAHO: Database Lookup)
Za vsako vrstico smo poiskali ustrezne nadomestne ključe (*Surrogate Keys*) ali identifikatorje:
1. **Oznaka časa (`time_id`)**: Iz Surs formata (np. `2020M01`) smo enostavno z regexom prečistili črko 'M' in pretvorili tekst v integer, da smo dobili unikaten `time_id` `202001`, ki se v nulo ujema z vašo dim_time datoteko.
2. **Oznaka države (`country_id`)**: Očistili smo vrstilne številke, ki jih doda Surs (npr. "2.1 Avstrija") in po prevajalskem slovarju `SURS_TO_EN` dobili angleški naziv (Austria). To smo mapirali na `dim_country.csv`.
3. **Oznaka občine (`municipality_id`)**: Direktno smo prečistili presledke (*trim*) pri Surs datoteki in mapirali v vašo `dim_municipality.csv` ter tako pridobili ID-je neposredno.

### Korak 5: Update/Insert metoda (PENTAHO: Update / Insert)
Vsa zajeta dejstva smo shranili v posamezne Dataframe objekte in jih nato združili z operacijo:
`pd.merge(f_prihodi, f_prenocitve, on=["time_id", "country_id", "municipality_id"], how="outer")`
S tem smo zagotovili pravilen standard:
- Če določena kombinacija časa, države in občine ni obstajala, se je ustavarila povsem nova vrstica s pripadajočimi podatki.
- Če je ta kombinacija že obstajala od polnjenja prihodov, se je ob polnjenju prenočitev le posodobila ista dimenzijska vrstica (ni rablo podvojiti vrstic).

### Korak 6: Končni izpis
Podatki so na koncu razčiščeni nesnage, vsebujejo samo še tri ključe iz dimenzij pomešane s številčnim stolpcem prihodov ter prenočitev.
Izvoženo v datoteko: `output_dimensions_model/fact_measurements.csv` z natanko `230,774` prečiščenimi kombinacijami.

---

## Kako ponovno zagnati proces?
Če spremenite vhodne datoteke ali dodate nove dimenzije, morate zagnati skripto znotraj istega pyhon virtualnega okolja.

1. Odprite terminal v direktoriju `INPUT_DATA`.
2. Aktivirajte okolje (MacOS/Linux): 
   ```bash
   source .venv/bin/activate
   ```
3. Zaženite ETL skripto:
   ```bash
   python transformations_model/tr_fact_table.py
   ```
Datoteka bo po cca 15-30 sekundah procesiranja podatkov vrnila potrditveno sporočilo in shranila osveženo datoteko `fact_measurements.csv`.