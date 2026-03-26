from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_FILE = BASE_DIR / "output_dimensions" / "dim_drzava_izvora.csv"
LOG_FILE = BASE_DIR / "logs" / "tr_dim_drzava_progress.log"

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

ALIAS_TO_CANONICAL = {
    "czech republic": "Czechia",
    "north macedonia": "North Macedonia",
    "macedonia": "North Macedonia",
    "russian federation": "Russia",
    "korea republic of": "Republic of Korea",
    "united states of america": "United States",
    "turkiye": "Turkey",
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

PPS_COUNTRIES = {
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
    "Norway",
    "Switzerland",
    "United Kingdom",
    "North Macedonia",
    "Serbia",
    "Turkey",
    "Ukraine",
    "Russia",
    "United States",
    "Canada",
    "Japan",
    "China",
    "Republic of Korea",
    "Australia",
    "New Zealand",
    "Brazil",
    "Israel",
}


def log(message: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(message + "\n")


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def normalize_key(text: str) -> str:
    t = strip_accents(str(text)).lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return t


def extract_country_name(header_text: str) -> str:
    return re.sub(r"^\d+(?:\.\d+)?\s+", "", str(header_text)).strip()


def canonical_country_name(name: str, source: str) -> str:
    if source == "surs":
        name = SURS_TO_EN.get(name, name)
    key = normalize_key(name)
    return ALIAS_TO_CANONICAL.get(key, str(name).strip())


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
    return "Evropa"


def main() -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    log("[1/5] Zacetek transformacije dim_drzava_izvora")

    surs_names: set[str] = set(SURS_TO_EN.keys())
    log("[2/5] Uporabljen SURS seznam drzav iz glav tabel")

    log("[3/5] Branje Eurostat PPS")
    pps_names = set(PPS_COUNTRIES)

    recs: dict[str, dict[str, object]] = {}

    for name in sorted(surs_names):
        canonical = canonical_country_name(name, source="surs")
        k = canonical.lower().strip()
        if k not in recs:
            recs[k] = {
                "drzava_kanonicna": canonical,
                "drzava_naziv_sl": name,
                "drzava_naziv_en": canonical,
                "source_surs": 0,
                "source_eurostat": 0,
            }
        recs[k]["source_surs"] = 1

    for name in sorted(pps_names):
        canonical = canonical_country_name(name, source="eurostat")
        k = canonical.lower().strip()
        if k not in recs:
            recs[k] = {
                "drzava_kanonicna": canonical,
                "drzava_naziv_sl": "",
                "drzava_naziv_en": canonical,
                "source_surs": 0,
                "source_eurostat": 0,
            }
        recs[k]["source_eurostat"] = 1

    rows = []
    for rec in recs.values():
        name_en = str(rec["drzava_naziv_en"])
        agg = 1 if is_aggregate(name_en) else 0
        iso2, iso3 = (None, None) if agg else ISO_MAP.get(name_en, (None, None))
        rows.append(
            {
                **rec,
                "iso2": iso2 or "",
                "iso3": iso3 or "",
                "eu_clanica": "DA" if name_en in EU27 else ("NE" if agg == 0 else "NEZNANO"),
                "regija_izvora": country_region(name_en),
                "je_agregat": agg,
            }
        )

    rows.sort(key=lambda r: (r["je_agregat"], str(r["drzava_naziv_en"])))

    for i, r in enumerate(rows, start=1):
        r["dim_drzava_sk"] = i
        r["drzava_id"] = f"DRZ_{i:04d}"

    log(f"[4/5] Pripravljenih zapisov: {len(rows)}")

    fieldnames = [
        "dim_drzava_sk",
        "drzava_id",
        "drzava_kanonicna",
        "drzava_naziv_sl",
        "drzava_naziv_en",
        "source_surs",
        "source_eurostat",
        "iso2",
        "iso3",
        "eu_clanica",
        "regija_izvora",
        "je_agregat",
    ]
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log(f"[5/5] Saved {OUT_FILE} ({len(rows)} rows)")
    print(f"Saved {OUT_FILE} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
