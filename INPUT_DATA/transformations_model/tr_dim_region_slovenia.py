from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
IN_FILE = BASE_DIR / "output_dimensions" / "dim_lokacija_slo.csv"
OUT_FILE = BASE_DIR / "output_dimensions_model" / "dim_region_slovenia.csv"


def main() -> None:
    tips = set()
    with IN_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            t = (r.get("tip_destinacije") or "").strip()
            if t:
                tips.add(t)

    rows_out = []
    for i, tip in enumerate(sorted(tips), start=1):
        rows_out.append(
            {
                "region_id": i,
                "regija": tip,
                "tip_destinacije": tip,
            }
        )

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["region_id", "regija", "tip_destinacije"])
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Saved {OUT_FILE} ({len(rows_out)} rows)")


if __name__ == "__main__":
    main()
