from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILES_SURS = [
    BASE_DIR / "2164466S_20260315-191917.xlsx",  # Prihodi
    BASE_DIR / "2164466S_20260315-191956.xlsx",  # Prenocitve
]
INPUT_FILE_PPS = BASE_DIR / "tec00114_page_spreadsheet.xlsx"
OUTPUT_DIR = BASE_DIR / "output_dimensions"

MESECI = {
    1: "Januar",
    2: "Februar",
    3: "Marec",
    4: "April",
    5: "Maj",
    6: "Junij",
    7: "Julij",
    8: "Avgust",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "December",
}

SURS_TO_EN = {
    "Avstrija": "Austria",
    "Belgija": "Belgium",
    "Bolgarija": "Bulgaria",
    "Bosna in Hercegovina": "Bosnia and Herzegovina",
    "Ciper": "Cyprus",
    "Češka republika": "Czechia",
    "Črna gora": "Montenegro",
    "Danska": "Denmark",
    "Estonija": "Estonia",
    "Finska": "Finland",
    "Francija": "France",
    "Grčija": "Greece",
    "Hrvaška": "Croatia",
    "Irska": "Ireland",
    "Islandija": "Iceland",
    "Italija": "Italy",
    "Latvija": "Latvia",
    "Litva": "Lithuania",
    "Luksemburg": "Luxembourg",
    "Madžarska": "Hungary",
    "Makedonija": "North Macedonia",
    "Malta": "Malta",
    "Nemčija": "Germany",
    "Nizozemska": "Netherlands",
    "Norveška": "Norway",
    "Poljska": "Poland",
    "Portugalska": "Portugal",
    "Romunija": "Romania",
    "Ruska federacija": "Russia",
    "Slovaška": "Slovakia",
    "Srbija": "Serbia",
    "Španija": "Spain",
    "Švedska": "Sweden",
    "Švica": "Switzerland",
    "Turčija": "Turkey",
    "Ukrajina": "Ukraine",
    "Združeno kraljestvo": "United Kingdom",
    "Južna Afrika": "South Africa",
    "Druge afriške države": "Other African countries",
    "Avstralija": "Australia",
    "Nova Zelandija": "New Zealand",
    "Druge države in ozemlja Oceanije": "Other Oceania countries and territories",
    "Izrael": "Israel",
    "Japonska": "Japan",
    "Kitajska (Ljudska republika)": "China",
    "Koreja (Republika)": "Republic of Korea",
    "Druge azijske države": "Other Asian countries",
    "Brazilija": "Brazil",
    "Druge države Južne in Srednje Amerike": "Other South and Central American countries",
    "Kanada": "Canada",
    "Združene države (ZDA)": "United States",
    "Druge države in ozemlja Severne Amerike": "Other North American countries and territories",
    "Druge evropske države": "Other European countries",
}

EU27 = {
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
}

ALIAS_TO_CANONICAL = {
    "czech republic": "Czechia",
    "north macedonia": "North Macedonia",
    "macedonia": "North Macedonia",
    "russian federation": "Russia",
    "korea republic of": "Republic of Korea",
    "united states (zda)": "United States",
    "united states of america": "United States",
    "turkiye": "Turkey",
    "bosnia-herzegovina": "Bosnia and Herzegovina",
}

ISO_MAP = {
    "Austria": ("AT", "AUT"),
    "Belgium": ("BE", "BEL"),
    "Bulgaria": ("BG", "BGR"),
    "Bosnia and Herzegovina": ("BA", "BIH"),
    "Cyprus": ("CY", "CYP"),
    "Czechia": ("CZ", "CZE"),
    "Montenegro": ("ME", "MNE"),
    "Denmark": ("DK", "DNK"),
    "Estonia": ("EE", "EST"),
    "Finland": ("FI", "FIN"),
    "France": ("FR", "FRA"),
    "Greece": ("GR", "GRC"),
    "Croatia": ("HR", "HRV"),
    "Ireland": ("IE", "IRL"),
    "Iceland": ("IS", "ISL"),
    "Italy": ("IT", "ITA"),
    "Latvia": ("LV", "LVA"),
    "Lithuania": ("LT", "LTU"),
    "Luxembourg": ("LU", "LUX"),
    "Hungary": ("HU", "HUN"),
    "North Macedonia": ("MK", "MKD"),
    "Malta": ("MT", "MLT"),
    "Germany": ("DE", "DEU"),
    "Netherlands": ("NL", "NLD"),
    "Norway": ("NO", "NOR"),
    "Poland": ("PL", "POL"),
    "Portugal": ("PT", "PRT"),
    "Romania": ("RO", "ROU"),
    "Russia": ("RU", "RUS"),
    "Slovakia": ("SK", "SVK"),
    "Serbia": ("RS", "SRB"),
    "Spain": ("ES", "ESP"),
    "Sweden": ("SE", "SWE"),
    "Switzerland": ("CH", "CHE"),
    "Turkey": ("TR", "TUR"),
    "Ukraine": ("UA", "UKR"),
    "United Kingdom": ("GB", "GBR"),
    "South Africa": ("ZA", "ZAF"),
    "Australia": ("AU", "AUS"),
    "New Zealand": ("NZ", "NZL"),
    "Israel": ("IL", "ISR"),
    "Japan": ("JP", "JPN"),
    "China": ("CN", "CHN"),
    "Republic of Korea": ("KR", "KOR"),
    "Brazil": ("BR", "BRA"),
    "Canada": ("CA", "CAN"),
    "United States": ("US", "USA"),
    "Slovenia": ("SI", "SVN"),
}


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def normalize_key(text: str) -> str:
    t = strip_accents(str(text)).lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return t


def extract_country_name(header_text: str) -> str:
    if not isinstance(header_text, str):
        return ""
    cleaned = re.sub(r"^\d+(?:\.\d+)?\s+", "", header_text).strip()
    return cleaned


def load_surs_raw(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name="2164466S", header=None)


def get_surs_country_columns(raw: pd.DataFrame) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for col in range(5, raw.shape[1]):
        name = extract_country_name(raw.iat[2, col])
        if name:
            out.append((col, name))
    return out


def get_all_locations() -> pd.Series:
    all_locations = []
    for f in INPUT_FILES_SURS:
        raw = load_surs_raw(f)
        data = raw.iloc[4:, [0, 1]].copy()
        data.columns = ["time_code", "lokacija"]
        data["time_code"] = data["time_code"].ffill()
        all_locations.append(data["lokacija"])
    s = pd.concat(all_locations, ignore_index=True)
    return s.dropna().astype(str).str.strip().replace("", pd.NA).dropna().drop_duplicates().sort_values()


def get_surs_month_codes() -> List[str]:
    codes = set()
    for f in INPUT_FILES_SURS:
        raw = load_surs_raw(f)
        col = raw.iloc[4:, 0].ffill().dropna().astype(str).str.strip()
        for c in col:
            if re.match(r"^\d{4}M\d{2}$", c):
                codes.add(c)
    return sorted(codes)


def parse_pps_sheet() -> Tuple[pd.DataFrame, List[int]]:
    raw = pd.read_excel(INPUT_FILE_PPS, sheet_name="Sheet 1", header=None)

    # Row 8 contains year headers in every second column.
    year_cols = []
    years = []
    for col in range(raw.shape[1]):
        val = raw.iat[8, col]
        if pd.notna(val):
            sval = str(val).strip()
            if re.fullmatch(r"\d{4}", sval):
                year_cols.append(col)
                years.append(int(sval))

    rows = []
    for r in range(10, raw.shape[0]):
        geo = raw.iat[r, 0]
        if pd.isna(geo):
            continue
        geo = str(geo).strip()
        if not geo:
            continue
        rec = {"geo": geo}
        for c, y in zip(year_cols, years):
            rec[f"pps_{y}"] = raw.iat[r, c]
        rows.append(rec)

    return pd.DataFrame(rows), years


def is_aggregate(name: str) -> bool:
    n = normalize_key(name)
    return (
        "other" in n
        or "druge" in n
        or "euro area" in n
        or "european union" in n
        or n.startswith("country total")
        or n.startswith("drzava skupaj")
    )


def canonical_country_name(name: str, source: str) -> str:
    if source == "surs":
        name = SURS_TO_EN.get(name, name)
    key = normalize_key(name)
    if key in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[key]
    return str(name).strip()


def country_region(name_en: str) -> str:
    n = normalize_key(name_en)
    if "afric" in n:
        return "Afrika"
    if "oceania" in n or n in {"australia", "new zealand"}:
        return "Oceanija"
    if "asia" in n or n in {"israel", "japan", "china", "republic of korea", "turkey"}:
        return "Azija"
    if "america" in n or n in {"brazil", "canada", "united states"}:
        return "Amerika"
    if "euro" in n or n in {
        "austria",
        "belgium",
        "bosnia and herzegovina",
        "bulgaria",
        "croatia",
        "cyprus",
        "czechia",
        "denmark",
        "estonia",
        "finland",
        "france",
        "germany",
        "greece",
        "hungary",
        "iceland",
        "ireland",
        "italy",
        "latvia",
        "lithuania",
        "luxembourg",
        "malta",
        "netherlands",
        "north macedonia",
        "norway",
        "poland",
        "portugal",
        "romania",
        "russia",
        "serbia",
        "slovakia",
        "spain",
        "sweden",
        "switzerland",
        "ukraine",
        "united kingdom",
        "montenegro",
    }:
        return "Evropa"
    return "Neznano"


def get_iso_codes(name_en: str) -> Tuple[str | None, str | None]:
    return ISO_MAP.get(name_en, (None, None))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
