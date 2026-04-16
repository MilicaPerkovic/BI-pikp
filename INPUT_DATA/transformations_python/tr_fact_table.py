import pandas as pd
from pathlib import Path
import re

# Nastavitve poti
BASE_DIR = Path(__file__).resolve().parent.parent
DIMENSIONS_DIR = BASE_DIR / "output_dimensions"
OUTPUT_FILE = DIMENSIONS_DIR / "fact_tabela_dejstev.csv"

def process_surs_data(file_path, metric_name, dim_cas, dim_drzava, dim_lokacija):
    """Procesiranje in razporkanje (Melt) SURS podatkov (Prihodi ali Prenočitve)."""
    df = pd.read_excel(file_path, skiprows=2)
    # Preimenovanje stolpcev za jasnost
    df.rename(columns={"Unnamed: 0": "oznaka_casa", "Unnamed: 1": "lokacija_naziv"}, inplace=True)
    
    # Fill forwarded time since SURS has merged cells for Time
    df["oznaka_casa"] = df["oznaka_casa"].ffill()
    df = df.dropna(subset=["lokacija_naziv"])
    
    # Kateri stolpci nas zanimajo? Vse države (brez Skupaj in agregatov Domači/Tuji, ker nas zanima atomarni nivo)
    exclude_cols = ["oznaka_casa", "lokacija_naziv", "0 Država - SKUPAJ", "1 DOMAČI", "2 TUJI"]
    value_vars = [col for col in df.columns if col not in exclude_cols]
    
    # UNPIVOT / MELT (točno to kar prenaša stolpce v vrstice kot 'Row Normaliser' iz Pentaho!)
    fact = pd.melt(df, id_vars=["oznaka_casa", "lokacija_naziv"], value_vars=value_vars, 
                   var_name="drzava_surs", value_name=metric_name)
    
    # Čiščenje vrednosti
    fact[metric_name] = pd.to_numeric(fact[metric_name].replace({'z': pd.NA, '-': pd.NA}), errors='coerce')
    fact = fact.dropna(subset=[metric_name])
    
    # --- 1) POIŠČI ID: ČAS ---
    # Naredi JOIN z dimenzijo čas (dim_cas.csv -> dim_cas_sk)
    fact = fact.merge(dim_cas[["oznaka_casa", "dim_cas_sk"]], on="oznaka_casa", how="left")
    
    # --- 2) POIŠČI ID: LOKACIJA ---
    # Očisti ime lokacije za zanesljiv JOIN (npr. strip presledkov)
    fact["lok_clean"] = fact["lokacija_naziv"].str.strip()
    dim_lokacija_clean = dim_lokacija[["lokacija_naziv", "dim_lokacija_sk"]].copy()
    dim_lokacija_clean["lokacija_naziv"] = dim_lokacija_clean["lokacija_naziv"].str.strip()
    fact = fact.merge(dim_lokacija_clean, left_on="lok_clean", right_on="lokacija_naziv", how="left")
    
    # --- 3) POIŠČI ID: DRŽAVA IZVORA ---
    # Odstranimo zaporedno številko iz stolpca (npr. "2.1 Avstrija" -> "Avstrija")
    fact["drz_clean"] = fact["drzava_surs"].apply(lambda x: re.sub(r'^\d+\.\d+\s+', '', str(x)).strip())
    
    # Match na slovensko ime
    dim_drzava_clean = dim_drzava[["drzava_naziv_sl", "dim_drzava_sk"]].dropna()
    fact = fact.merge(dim_drzava_clean, left_on="drz_clean", right_on="drzava_naziv_sl", how="left")
    
    # Obdržimo le ključe in meritev
    fact = fact[["dim_cas_sk", "dim_lokacija_sk", "dim_drzava_sk", metric_name]]
    return fact

def process_pps_data(file_path, dim_cas, dim_drzava):
    """Procesira Eurostat BDP PPS tabelo in nastavi irelevantne ključe na NULL (NaN)."""
    df = pd.read_excel(file_path, sheet_name='Sheet 1', skiprows=7)
    df.rename(columns={"TIME": "drzava_eurostat"}, inplace=True) 
    # v vrstici 8 je dejanski header "TIME" prvo polje in nato leta (ki so v vrstici 7 prazna, ampak v 8 so headerji)
    
    # Ker je datoteka specifična (Eurostat), jo preberimo pravilno
    df_praw = pd.read_excel(file_path, sheet_name='Sheet 1', header=8) 
    # Zgornja vrstica ima imena "Unnamed" razen let, poskusimo drugačen pristop, enostaven:
    df_praw.rename(columns={df_praw.columns[0]: "TIME_HEADER", df_praw.columns[1]: "GEO_Labels"}, inplace=True)
    df_praw = df_praw.dropna(subset=["GEO_Labels"])
    
    years = [col for col in df_praw.columns if str(col).isdigit() and len(str(col)) == 4]
    
    fact = pd.melt(df_praw, id_vars=["GEO_Labels"], value_vars=years, var_name="leto", value_name="pps")
    fact["pps"] = pd.to_numeric(fact["pps"].replace({':': pd.NA, 'NaN': pd.NA}), errors='coerce')
    fact = fact.dropna(subset=["pps"])
    fact["leto"] = fact["leto"].astype(int)
    
    # 1) ČAS: Poveži po Letu (vzamemo unikatni cas_sk na nivoju LESA)
    # Zasilno: Če imate agregiran čas, super. Če ne bomo izbrali prvo pojavitev meseca in vzeli leto
    dim_cas_leto = dim_cas.drop_duplicates("leto", keep="first").copy()
    fact = fact.merge(dim_cas_leto[["leto", "dim_cas_sk"]], on="leto", how="left")
    
    # 2) DRŽAVA: Poveži po angleškem kanoničnem imenu (npr. "Austria", "Germany")
    fact["geo_clean"] = fact["GEO_Labels"].astype(str).str.strip()
    dim_drzava_en = dim_drzava[["drzava_kanonicna", "dim_drzava_sk"]].dropna()
    fact = fact.merge(dim_drzava_en, left_on="geo_clean", right_on="drzava_kanonicna", how="left")
    
    # 3) LOKACIJA V SLOVENIJI in ostale dimenzije SO NULL
    fact["dim_lokacija_sk"] = pd.NA
    
    # Obdržimo ključe in meritev
    return fact[["dim_cas_sk", "dim_lokacija_sk", "dim_drzava_sk", "pps"]]

def main():
    print("1. Nalagamo dimenzije...")
    dim_cas = pd.read_csv(DIMENSIONS_DIR / "dim_cas.csv")
    dim_drzava = pd.read_csv(DIMENSIONS_DIR / "dim_drzava_izvora.csv")
    dim_lokacija = pd.read_csv(DIMENSIONS_DIR / "dim_lokacija_slo.csv")
    
    surs_prihodi_path = BASE_DIR / "2164466S_20260315-191917.xlsx"
    surs_prenoc_path = BASE_DIR / "2164466S_20260315-191956.xlsx"
    pps_path = BASE_DIR / "tec00114_page_spreadsheet.xlsx"
    
    print("2. Procesiram Arrivals (Prihode)...")
    f_prihodi = process_surs_data(surs_prihodi_path, "prihodi", dim_cas, dim_drzava, dim_lokacija)
    
    print("3. Procesiram Overnight Stays (Prenočitve)...")
    f_prenocitve = process_surs_data(surs_prenoc_path, "prenocitve", dim_cas, dim_drzava, dim_lokacija)
    
    print("4. Procesiram PPS (Eurostat)...")
    try:
        f_pps = process_pps_data(pps_path, dim_cas, dim_drzava)
    except Exception as e:
        print("Napaka pri branju Eurostat Excela (če ni na voljo list Sheet 1):", e)
        f_pps = pd.DataFrame(columns=["dim_cas_sk", "dim_lokacija_sk", "dim_drzava_sk", "pps"])
    
    print("5. Združevanje in generiranje celovite tabele dejstev...")
    # Združimo (FULL OUTER JOIN) vse tabele v eno veliko tabelo na podlagi ključev
    keys = ["dim_cas_sk", "dim_lokacija_sk", "dim_drzava_sk"]
    
    for df in [f_prihodi, f_prenocitve, f_pps]:
        for k in keys:
            df[k] = pd.to_numeric(df[k], errors="coerce").astype("Int64")
    
    # Da vsaka kombinacija ključev dobi svojo MERITEV
    fact = pd.merge(f_prihodi, f_prenocitve, on=keys, how="outer")
    fact = pd.merge(fact, f_pps, on=keys, how="outer")
    
    for k in keys:
        fact[k] = pd.to_numeric(fact[k], errors="coerce").astype("Int64")
        
    print("Vrstic v ustvarjeni tabeli:", len(fact))
    print(fact.head(10))
    fact.to_csv(OUTPUT_FILE, index=False)
    print(f"6. Končano. Shranjeno v: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
