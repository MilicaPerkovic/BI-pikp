import pandas as pd
from pathlib import Path
import re

# Nastavitve poti
BASE_DIR = Path(__file__).resolve().parent.parent
DIMENSIONS_DIR = BASE_DIR / "output_dimensions_model"
OUTPUT_FILE = DIMENSIONS_DIR / "fact_measurements.csv"

# Prevod slo-ang (enak kot v vaših drugih skriptah)
SURS_TO_EN = {
    "Avstrija": "Austria", "Belgija": "Belgium", "Bolgarija": "Bulgaria",
    "Bosna in Hercegovina": "Bosnia and Herzegovina", "Ciper": "Cyprus",
    "Češka republika": "Czechia", "Črna gora": "Montenegro", "Danska": "Denmark",
    "Estonija": "Estonia", "Finska": "Finland", "Francija": "France",
    "Grčija": "Greece", "Hrvaška": "Croatia", "Irska": "Ireland",
    "Islandija": "Iceland", "Italija": "Italy", "Latvija": "Latvia",
    "Litva": "Lithuania", "Luksemburg": "Luxembourg", "Madžarska": "Hungary",
    "Makedonija": "North Macedonia", "Malta": "Malta", "Nemčija": "Germany",
    "Nizozemska": "Netherlands", "Norveška": "Norway", "Poljska": "Poland",
    "Portugalska": "Portugal", "Romunija": "Romania", "Ruska federacija": "Russia",
    "Slovaška": "Slovakia", "Srbija": "Serbia", "Španija": "Spain",
    "Švedska": "Sweden", "Švica": "Switzerland", "Turčija": "Turkey",
    "Ukrajina": "Ukraine", "Združeno kraljestvo": "United Kingdom",
    "Južna Afrika": "South Africa", "Druge afriške države": "Other African countries",
    "Avstralija": "Australia", "Nova Zelandija": "New Zealand",
    "Druge države in ozemlja Oceanije": "Other Oceania countries and territories",
    "Izrael": "Israel", "Japonska": "Japan", "Kitajska (Ljudska republika)": "China",
    "Koreja (Republika)": "Republic of Korea", "Druge azijske države": "Other Asian countries",
    "Brazilija": "Brazil", "Druge države Južne in Srednje Amerike": "Other South and Central American countries",
    "Kanada": "Canada", "Združene države (ZDA)": "United States",
    "Druge države in ozemlja Severne Amerike": "Other North American countries and territories",
    "Druge evropske države": "Other European countries",
}

def process_surs_data(file_path, metric_name, dim_time, dim_country, dim_municipality):
    df = pd.read_excel(file_path, skiprows=2)
    df.rename(columns={"Unnamed: 0": "oznaka_casa", "Unnamed: 1": "obcina"}, inplace=True)
    
    # Forward-fill časovne oznake in odstrani vrstice brez občine
    df["oznaka_casa"] = df["oznaka_casa"].ffill()
    df = df.dropna(subset=["obcina"])
    
    # Row Normaliser (Melt): Izključi seštevke, obdrži samo dejanske posamezne države
    exclude_cols = ["oznaka_casa", "obcina", "0 Država - SKUPAJ", "1 DOMAČI", "2 TUJI"]
    value_vars = [col for col in df.columns if col not in exclude_cols]
    
    fact = pd.melt(df, id_vars=["oznaka_casa", "obcina"], value_vars=value_vars, 
                   var_name="drzava_slo", value_name=metric_name)
                   
    # Pretvori meritev v številke in odstrani prazne z(zaupne)/- vrstice
    fact[metric_name] = pd.to_numeric(fact[metric_name].replace({'z': pd.NA, '-': pd.NA}), errors='coerce')
    fact = fact.dropna(subset=[metric_name])
    
    # 1. Pripravi in posodobi ČASOVNI KLJUČ (SURS '2020M01' -> int 202001)
    fact["time_id"] = fact["oznaka_casa"].str.replace('M', '').astype(int)
    # Filter only matching times (to avoid duplicate rows due to unmatched merges)
    valid_times = dim_time["time_id"].unique()
    fact = fact[fact["time_id"].isin(valid_times)]
    
    # 2. Pripravi in posodobi OBČINSKI KLJUČ
    # Očistimo string preden naredimo join
    fact["obcina_clean"] = fact["obcina"].str.strip()
    dim_municipality_clean = dim_municipality[["obcina", "municipality_id"]].copy()
    dim_municipality_clean["obcina"] = dim_municipality_clean["obcina"].str.strip()
    
    fact = pd.merge(fact, dim_municipality_clean, left_on="obcina_clean", right_on="obcina", how="inner")
    
    # 3. Pripravi in posodobi DRŽAVNI KLJUČ (izbriši prefix 2.1 in prevedi v ANG)
    fact["drz_slo_clean"] = fact["drzava_slo"].apply(lambda x: re.sub(r'^\d+\.\d+\s+', '', str(x)).strip())
    fact["drz_ang"] = fact["drz_slo_clean"].map(SURS_TO_EN)
    
    dim_country_clean = dim_country[["drzava", "country_id"]].dropna()
    fact = pd.merge(fact, dim_country_clean, left_on="drz_ang", right_on="drzava", how="inner")
    
    # Vrni samo tiste 3 ključe + meritev
    return fact[["time_id", "country_id", "municipality_id", metric_name]]

def main():
    print("1. Nalagamo VASE DEANSKE dimenzije (time, country, municipality)...")
    dim_time = pd.read_csv(DIMENSIONS_DIR / "dim_time.csv")
    dim_country = pd.read_csv(DIMENSIONS_DIR / "dim_country.csv")
    dim_municipality = pd.read_csv(DIMENSIONS_DIR / "dim_municipality.csv")
    
    surs_prihodi_path = BASE_DIR / "2164466S_20260315-191917.xlsx"
    surs_prenoc_path = BASE_DIR / "2164466S_20260315-191956.xlsx"
    
    print("2. Procesiram Prihode...")
    f_prihodi = process_surs_data(surs_prihodi_path, "prihodi", dim_time, dim_country, dim_municipality)
    
    print("3. Procesiram Prenočitve...")
    f_prenocitve = process_surs_data(surs_prenoc_path, "prenocitve", dim_time, dim_country, dim_municipality)
    
    print("4. Združevanje in generiranje celovite tabele dejstev...")
    keys = ["time_id", "country_id", "municipality_id"]
    
    fact = pd.merge(f_prihodi, f_prenocitve, on=keys, how="outer")
    
    print("Vrstic v ustvarjeni tabelji:", len(fact))
    print(fact.head(5))
    
    fact.to_csv(OUTPUT_FILE, index=False)
    print(f"5. Končano. Tabela shranjena v: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
