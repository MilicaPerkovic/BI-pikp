from tr_dim_time import main as run_dim_time
from tr_dim_country import main as run_dim_country
from tr_dim_location import main as run_dim_location
import csv
from pathlib import Path


def print_preview(csv_path: Path) -> None:
    print(f"\n=== Preview: {csv_path.name} ===")
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print("df.head()")
    for row in rows[:5]:
        print(row)

    print("df.info()")
    print(f"rows: {len(rows)}")
    print(f"columns: {reader.fieldnames}")
    if rows:
        non_null = {c: sum(1 for r in rows if (r.get(c) or "") != "") for c in reader.fieldnames}
        print(f"non-null counts: {non_null}")


if __name__ == "__main__":
    run_dim_time()
    run_dim_country()
    run_dim_location()

    out_dir = Path(__file__).resolve().parent.parent / "output_dimensions_model"
    print_preview(out_dir / "dim_time.csv")
    print_preview(out_dir / "dim_country.csv")
    print_preview(out_dir / "dim_location.csv")
