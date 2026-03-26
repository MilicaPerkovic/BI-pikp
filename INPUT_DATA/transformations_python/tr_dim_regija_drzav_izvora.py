from __future__ import annotations

import pandas as pd

from etl_common import OUTPUT_DIR, ensure_output_dir


def main() -> None:
    ensure_output_dir()

    p = OUTPUT_DIR / "dim_drzava_izvora.csv"
    drz = pd.read_csv(p)

    vals = sorted(drz["regija_izvora"].dropna().astype(str).unique())
    df = pd.DataFrame({"regija_izvora": vals})
    df.insert(0, "dim_regija_sk", range(1, len(df) + 1))
    df.insert(1, "regija_id", "REG_" + df["dim_regija_sk"].astype(str).str.zfill(3))

    out = OUTPUT_DIR / "dim_regija_drzav_izvora.csv"
    df.to_csv(out, index=False)
    print(f"Saved {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
