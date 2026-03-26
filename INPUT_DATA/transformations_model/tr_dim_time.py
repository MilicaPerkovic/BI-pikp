from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
IN_FILE = BASE_DIR / "output_dimensions" / "dim_cas.csv"
OUT_FILE = BASE_DIR / "output_dimensions_model" / "dim_time.csv"


def main() -> None:
    rows_out = []
    with IN_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Fact table is monthly, so keep monthly grain.
            if r.get("granularnost") != "MESEC":
                continue
            rows_out.append(
                {
                    "time_id": int(r["cas_id"]),
                    "leto": int(r["leto"]),
                    "mesec": int(r["mesec"]),
                    "kvartal": int(r["kvartal"]),
                    "sezona": r["sezona"],
                }
            )

    rows_out.sort(key=lambda x: x["time_id"])
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["time_id", "leto", "mesec", "kvartal", "sezona"])
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Saved {OUT_FILE} ({len(rows_out)} rows)")


if __name__ == "__main__":
    main()
