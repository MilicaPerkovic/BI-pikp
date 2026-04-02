from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
IN_FILE = BASE_DIR / "output_dimensions" / "dim_lokacija_slo.csv"
OUT_FILE = BASE_DIR / "output_dimensions_model" / "dim_location.csv"


def main() -> None:
    rows_out = []
    
    with IN_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            obcina = (r.get("lokacija_naziv") or "").strip()
            if not obcina or obcina == "SLOVENIJA":
                continue
                
            tip = (r.get("tip_destinacije") or "").strip()
            
            rows_out.append({
                "obcina": obcina,
                "regija": tip,
                "tip_destinacije": tip,
            })

    # Deduplicate by municipality and region.
    uniq = {}
    for r in rows_out:
        # A municipality might have a composite key naturally, but deduping by obcina + tip 
        # normally ensures uniqueness of location properties. We'll use obcina as primary deduplication.
        obcina = r["obcina"]
        if obcina not in uniq:
            uniq[obcina] = r

    out_sorted = []
    for i, obcina in enumerate(sorted(uniq.keys()), start=1):
        rec = uniq[obcina]
        out_sorted.append(
            {
                "location_id": i,
                "obcina": obcina,
                "regija": rec["regija"],
                "tip_destinacije": rec["tip_destinacije"],
            }
        )

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["location_id", "obcina", "regija", "tip_destinacije"])
        writer.writeheader()
        writer.writerows(out_sorted)

    print(f"Saved {OUT_FILE} ({len(out_sorted)} rows)")


if __name__ == "__main__":
    main()