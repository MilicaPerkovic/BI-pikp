from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_FILE = BASE_DIR / "output_dimensions" / "dim_lokacija_slo.csv"
LOG_FILE = BASE_DIR / "logs" / "tr_dim_lokacija_progress.log"

SURS_FILES = [
    BASE_DIR / "2164466S_20260315-191917.xlsx",
    BASE_DIR / "2164466S_20260315-191956.xlsx",
]

XML_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def log_progress(message: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(message + "\n")
        f.flush()


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
        if t_node is None:
            return None
        return t_node.text or ""

    v_node = c_elem.find(f"{XML_NS}v")
    if v_node is None or v_node.text is None:
        return None

    if t == "s":
        try:
            idx = int(v_node.text)
            return shared_strings[idx]
        except Exception:
            return None

    return v_node.text


def read_locations_from_xlsx(path: Path) -> set[str]:
    locations: set[str] = set()

    with zipfile.ZipFile(path) as zf:
        shared = parse_shared_strings(zf)

        # The exports contain one sheet where column B stores municipality/location.
        sheet_path = "xl/worksheets/sheet1.xml"
        if sheet_path not in zf.namelist():
            return locations

        root = ET.fromstring(zf.read(sheet_path))
        sheet_data = root.find(f"{XML_NS}sheetData")
        if sheet_data is None:
            return locations

        for row in sheet_data.findall(f"{XML_NS}row"):
            r_attr = row.attrib.get("r", "0")
            try:
                row_num = int(r_attr)
            except ValueError:
                row_num = 0

            # Skip report header rows.
            if row_num < 5:
                continue

            for c in row.findall(f"{XML_NS}c"):
                ref = c.attrib.get("r", "")
                if not ref:
                    continue
                col_idx = col_index_from_ref(ref)

                # Column B only.
                if col_idx != 2:
                    continue

                value = read_cell_value(c, shared)
                if value is None:
                    continue
                s = str(value).strip()
                if s:
                    locations.add(s)

    return locations


def classify_tip_destinacije(name: str) -> str:
    n = name.lower()
    if name == "SLOVENIJA":
        return "NACIONALNO"
    if any(k in n for k in ["bled", "bohinj", "kranjska gora", "bovec", "kobarid"]):
        return "GORSKA"
    if any(k in n for k in ["piran", "izola", "koper", "ankaran"]):
        return "OBMORSKA"
    if any(k in n for k in ["ljubljana", "maribor", "celje", "novo mesto", "kranj"]):
        return "MESTNA"
    return "OSTALO"


def main() -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    log_progress("[1/5] Zacetek transformacije dim_lokacija_slo")

    all_locations: set[str] = set()
    for f in SURS_FILES:
        log_progress(f"[2/5] Branje lokacij iz datoteke: {f.name}")
        all_locations.update(read_locations_from_xlsx(f))

    # Keep deterministic order.
    locations = sorted(all_locations)
    log_progress(f"[3/5] Najdenih lokacij: {len(locations)}")

    rows = []
    for i, name in enumerate(locations, start=1):
        rows.append(
            {
                "dim_lokacija_sk": i,
                "lokacija_id": f"LOK_{i:05d}",
                "lokacija_naziv": name,
                "nivo": "DRZAVA" if name == "SLOVENIJA" else "OBCINA",
                "drzava": "Slovenija",
                "statisticna_regija": "",
                "tip_destinacije": classify_tip_destinacije(name),
            }
        )

    log_progress(f"[4/5] Pripravljenih zapisov: {len(rows)}")

    fieldnames = [
        "dim_lokacija_sk",
        "lokacija_id",
        "lokacija_naziv",
        "nivo",
        "drzava",
        "statisticna_regija",
        "tip_destinacije",
    ]
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log_progress(f"[5/5] Saved {OUT_FILE} ({len(rows)} rows)")
    print(f"Saved {OUT_FILE} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
