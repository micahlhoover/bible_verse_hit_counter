# inspect_suspicious_rows.py

import sys
import csv
import os
from typing import Dict

# Import your existing logic (no execution side-effects)
import verse_indexer_seo2 as indexer

#run it like this:
#   python .\repair_suspicious_rows.py proverbs_total_hits_dataforseo.csv


def infer_book_from_filename(path: str) -> str:
    name = os.path.basename(path).lower()
    if "proverbs" in name:
        return "Proverbs"
    return "Unknown"


def is_suspicious(row: Dict[str, str]) -> bool:
    retry = row["retry_applied"].lower() == "true"

    try:
        final_count = int(row["raw_hit_count_final"])
    except ValueError:
        final_count = 0

    has_result = row["has_result_final"].lower() == "true"

    return (
        retry
        or final_count < 1000
        or not has_result
    )


def main(csv_path: str):
    book = infer_book_from_filename(csv_path)

    print(f"\nInspecting suspicious rows in: {csv_path}")
    print(f"Inferred book: {book}")
    print("-" * 80)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        count = 0
        for row in reader:
            if is_suspicious(row):
                count += 1
                ch = row["chapter"]
                v = row["verse"]

                print(
                    f"{book} {ch}:{v} | "
                    f"retry={row['retry_applied']} | "
                    f"initial={row['raw_hit_count_initial']} | "
                    f"final={row['raw_hit_count_final']} | "
                    f"has_result={row['has_result_final']}"
                )

    print("-" * 80)
    print(f"Total suspicious rows: {count}\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_suspicious_rows.py <csv_file>")
        sys.exit(1)

    main(sys.argv[1])
