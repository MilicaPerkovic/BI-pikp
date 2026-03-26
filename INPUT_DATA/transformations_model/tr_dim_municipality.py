from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
IN_FILE = BASE_DIR / "output_dimensions" / "dim_lokacija_slo.csv"
REGION_FILE = BASE_DIR / "output_dimensions_model" / "dim_region_slovenia.csv"
OUT_FILE = BASE_DIR / "output_dimensions_model" / "dim_municipality.csv"


def main() -> None:
    region_by_tip = {}
    with REGION_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            region_by_tip[r["tip_destinacije"]] = int(r["region_id"])

    rows_out = []
    with IN_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            obcina = (r.get("lokacija_naziv") or "").strip()
            if not obcina or obcina == "SLOVENIJA":
                continue
            tip = (r.get("tip_destinacije") or "").strip()
            rows_out.append(
                {
                    "obcina": obcina,
                    "region_id": region_by_tip.get(tip, ""),
                }
            )

    # Deduplicate by municipality.
    uniq = {}
    for r in rows_out:
        uniq[r["obcina"]] = r

    out_sorted = []
    for i, obcina in enumerate(sorted(uniq.keys()), start=1):
        rec = uniq[obcina]
        out_sorted.append(
            {
                "municipality_id": i,
                "obcina": obcina,
                "region_id": rec["region_id"],
            }
        )

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["municipality_id", "obcina", "region_id"])
        writer.writeheader()
        writer.writerows(out_sorted)

    print(f"Saved {OUT_FILE} ({len(out_sorted)} rows)")


if __name__ == "__main__":
    main()
