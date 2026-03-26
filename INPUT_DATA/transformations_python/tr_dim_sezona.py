from __future__ import annotations

import pandas as pd

from etl_common import OUTPUT_DIR, ensure_output_dir


def main() -> None:
    ensure_output_dir()

    data = [
        {"sezona_id": "SEZ_01", "sezona": "Zima", "meseci": "12,1,2"},
        {"sezona_id": "SEZ_02", "sezona": "Pomlad", "meseci": "3,4,5"},
        {"sezona_id": "SEZ_03", "sezona": "Poletje", "meseci": "6,7,8"},
        {"sezona_id": "SEZ_04", "sezona": "Jesen", "meseci": "9,10,11"},
    ]

    df = pd.DataFrame(data)
    df.insert(0, "dim_sezona_sk", range(1, len(df) + 1))

    out = OUTPUT_DIR / "dim_sezona.csv"
    df.to_csv(out, index=False)
    print(f"Saved {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
