from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

BASE_DIR = Path(__file__).resolve().parent.parent
IN_FILE = BASE_DIR / "output_dimensions" / "dim_drzava_izvora.csv"
OUT_FILE = BASE_DIR / "output_dimensions_model" / "dim_country.csv"
PPS_FILE = BASE_DIR / "tec00114_page_spreadsheet.xlsx"

XML_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

COUNTRY_ALIASES = {
    "czech republic": "Czechia",
    "north macedonia": "North Macedonia",
    "macedonia": "North Macedonia",
    "russian federation": "Russia",
    "turkiye": "Turkey",
    "republic of korea": "Republic of Korea",
    "korea republic": "Republic of Korea",
    "united states of america": "United States",
    "bosnia-herzegovina": "Bosnia and Herzegovina",
}


def normalize_country(text: str) -> str:
    key = re.sub(r"\s+", " ", str(text).strip().lower())
    return COUNTRY_ALIASES.get(key, str(text).strip())


def col_index_from_ref(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - ord("A") + 1)
    return n


def parse_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    path = "xl/sharedStrings.xml"
    if path not in zf.namelist():
        return []

    root = ET.fromstring(zf.read(path))
    out = []
    for si in root.findall(f"{XML_NS}si"):
        parts = []
        for t in si.findall(f".//{XML_NS}t"):
            parts.append(t.text or "")
        out.append("".join(parts))
    return out


def read_cell_value(c_elem: ET.Element, shared_strings: list[str]) -> str | None:
    t = c_elem.attrib.get("t")

    if t == "inlineStr":
        t_node = c_elem.find(f"{XML_NS}is/{XML_NS}t")
        return None if t_node is None else (t_node.text or "")

    v_node = c_elem.find(f"{XML_NS}v")
    if v_node is None or v_node.text is None:
        return None

    if t == "s":
        try:
            return shared_strings[int(v_node.text)]
        except Exception:
            return None

    return v_node.text


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s == ":":
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def load_latest_pps_by_country() -> dict[str, float]:
    out: dict[str, float] = {}
    with zipfile.ZipFile(PPS_FILE) as zf:
        shared = parse_shared_strings(zf)
        sheet_path = "xl/worksheets/sheet2.xml"
        if sheet_path not in zf.namelist():
            return out

        root = ET.fromstring(zf.read(sheet_path))
        sheet_data = root.find(f"{XML_NS}sheetData")
        if sheet_data is None:
            return out

        cells: dict[tuple[int, int], str] = {}
        max_row = 0
        for row in sheet_data.findall(f"{XML_NS}row"):
            r = int(row.attrib.get("r", "0"))
            max_row = max(max_row, r)
            for c in row.findall(f"{XML_NS}c"):
                ref = c.attrib.get("r", "")
                if not ref:
                    continue
                col = col_index_from_ref(ref)
                val = read_cell_value(c, shared)
                if val is not None:
                    cells[(r, col)] = val

        # In Eurostat export, row 9 contains TIME and year headers.
        year_cols: list[tuple[int, int]] = []
        for col in range(1, 200):
            val = cells.get((9, col))
            if val and re.fullmatch(r"\d{4}", str(val).strip()):
                year_cols.append((col, int(val)))
        if not year_cols:
            return out

        year_cols.sort(key=lambda x: x[1])

        for r in range(11, max_row + 1):
            country_raw = cells.get((r, 1))
            if not country_raw:
                continue
            country = normalize_country(country_raw)

            latest = None
            for col, _year in year_cols:
                num = parse_number(cells.get((r, col)))
                if num is not None:
                    latest = num
            if latest is not None:
                out[country] = latest

    return out


def region_europe_label(region_src: str) -> str:
    # Keep a single region column as required by model.
    if region_src == "Evropa":
        return "Evropa"
    return "Izven Evrope"


def main() -> None:
    pps_by_country = load_latest_pps_by_country()

    rows = []
    with IN_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get("je_agregat") == "1":
                continue
            if r.get("source_surs") != "1":
                continue
            country = normalize_country(r["drzava_naziv_en"])
            if country in {"", ":", "nan", "NaN"}:
                continue
            pps_val = pps_by_country.get(country)
            rows.append(
                {
                    "drzava": country,
                    "regija_evrope": region_europe_label(r.get("regija_izvora", "")),
                    "pps": "" if pps_val is None else f"{pps_val:.2f}",
                    # Source dataset is GDP per capita in PPS index, so we use the same metric here.
                    "gdp_per_capita": "" if pps_val is None else f"{pps_val:.2f}",
                }
            )

    # Deduplicate by country name.
    uniq = {}
    for r in rows:
        uniq[r["drzava"]] = r

    rows_out = []
    for i, name in enumerate(sorted(uniq.keys()), start=1):
        rec = uniq[name]
        rows_out.append(
            {
                "country_id": i,
                "drzava": rec["drzava"],
                "regija_evrope": rec["regija_evrope"],
                "pps": rec["pps"],
                "gdp_per_capita": rec["gdp_per_capita"],
            }
        )

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["country_id", "drzava", "regija_evrope", "pps", "gdp_per_capita"],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Saved {OUT_FILE} ({len(rows_out)} rows)")


if __name__ == "__main__":
    main()
