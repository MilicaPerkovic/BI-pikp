from __future__ import annotations

import re

import pandas as pd

from etl_common import MESECI, OUTPUT_DIR, ensure_output_dir, get_surs_month_codes, parse_pps_sheet


def sezona(mesec: int | None) -> str | None:
    if mesec is None:
        return None
    if mesec in (12, 1, 2):
        return "Zima"
    if mesec in (3, 4, 5):
        return "Pomlad"
    if mesec in (6, 7, 8):
        return "Poletje"
    return "Jesen"


def main() -> None:
    ensure_output_dir()

    month_codes = get_surs_month_codes()
    _, pps_years = parse_pps_sheet()

    month_rows = []
    years_with_months = set()
    for code in month_codes:
        m = re.match(r"^(\d{4})M(\d{2})$", code)
        if not m:
            continue
        year = int(m.group(1))
        month = int(m.group(2))
        years_with_months.add(year)
        month_rows.append(
            {
                "cas_id": year * 100 + month,
                "oznaka_casa": code,
                "leto": year,
                "mesec": month,
                "naziv_meseca": MESECI.get(month),
                "kvartal": (month - 1) // 3 + 1,
                "sezona": sezona(month),
                "granularnost": "MESEC",
            }
        )

    # Ensure complete year-month hierarchy for all years where monthly data exists.
    if years_with_months:
        y_min, y_max = min(years_with_months), max(years_with_months)
        existing = {(r["leto"], r["mesec"]) for r in month_rows}
        for y in range(y_min, y_max + 1):
            for m in range(1, 13):
                if (y, m) in existing:
                    continue
                month_rows.append(
                    {
                        "cas_id": y * 100 + m,
                        "oznaka_casa": f"{y}M{m:02d}",
                        "leto": y,
                        "mesec": m,
                        "naziv_meseca": MESECI.get(m),
                        "kvartal": (m - 1) // 3 + 1,
                        "sezona": sezona(m),
                        "granularnost": "MESEC",
                    }
                )

    year_rows = []
    all_years = sorted(set(pps_years).union(years_with_months))
    for y in all_years:
        year_rows.append(
            {
                "cas_id": y * 100,
                "oznaka_casa": str(y),
                "leto": y,
                "mesec": pd.NA,
                "naziv_meseca": pd.NA,
                "kvartal": pd.NA,
                "sezona": pd.NA,
                "granularnost": "LETO",
            }
        )

    df = pd.DataFrame(year_rows + month_rows)
    df = df.drop_duplicates(subset=["cas_id"]).sort_values(["leto", "granularnost", "mesec"], na_position="first")
    df.insert(0, "dim_cas_sk", range(1, len(df) + 1))

    out = OUTPUT_DIR / "dim_cas.csv"
    df.to_csv(out, index=False)
    print(f"Saved {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
