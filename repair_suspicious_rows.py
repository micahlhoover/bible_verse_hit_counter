# repair_suspicious_rows.py

import sys
import csv
import os
from typing import Dict, List

import verse_indexer_seo2 as indexer

#run it like this:
#   python repair_suspicious_rows.py proverbs_total_hits_dataforseo.csv

def infer_book_from_filename(path: str) -> str:
    name = os.path.basename(path).lower()
    if "proverbs" in name:
        return "Proverbs"
    return "Unknown"

# def infer_book_from_filename(path: str) -> str:
#     print(f"Inferring from os.path.basename(path) : {os.path.basename(path)}")
#     #name = os.path.basename(path).lower()
#     name = os.path.basename(path).split("_")[0]
#     print(f"Inferred name : {name}")
#     #if "proverbs" in name:
#     #    return "Proverbs"
#     #return "Unknown"
#     return name


def is_suspicious(row: Dict[str, str]) -> bool:
    retry = row["retry_applied"].lower() == "true"

    try:
        final_count = int(row["raw_hit_count_final"])
    except ValueError:
        final_count = 0

    has_result = row["has_result_final"].lower() == "true"

    return retry or final_count < 1000 or not has_result


def repair_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Re-runs the DataForSEO query using existing logic
    and returns a CSV-compatible dict row.
    """
    query = row["query"]

    # recent_nonzero not meaningful here; pass empty list
    print(f"sending this query : '{query}'")
    result = indexer.fetch_count_with_selective_retry(query, [])

    repaired = dict(row)

    repaired["retry_applied"] = str(result.get("retry_applied", False))

    repaired["raw_hit_count_initial"] = str(
        result.get("raw_hit_count_initial", 0)
    )
    repaired["raw_hit_count_final"] = str(
        result.get("raw_hit_count_final", 0)
    )

    repaired["has_result_initial"] = str(
        result.get("has_result_initial", False)
    )
    repaired["has_result_final"] = str(
        result.get("has_result_final", False)
    )

    repaired["items_count_initial"] = str(
        result.get("items_count_initial", 0)
    )
    repaired["items_count_final"] = str(
        result.get("items_count_final", 0)
    )

    repaired["organic_items_count_initial"] = str(
        result.get("organic_items_count_initial", 0)
    )
    repaired["organic_items_count_final"] = str(
        result.get("organic_items_count_final", 0)
    )

    repaired["log10_raw_hit_count"] = str(
        result.get("log10_raw_hit_count", 0.0)
    )
    repaired["z_log10_raw_hit_count"] = str(
        result.get("z_log10_raw_hit_count", 0.0)
    )
    repaired["minmax_log10_raw_hit_count"] = str(
        result.get("minmax_log10_raw_hit_count", 0.0)
    )
    repaired["fusion_score"] = str(
        result.get("fusion_score", 0.0)
    )

    return repaired


def main(csv_path: str):
    book = infer_book_from_filename(csv_path)
    repaired_path = csv_path.replace(".csv", "_repaired.csv")

    suspicious_found = 0
    suspicious_after_repair = 0
    repaired_count = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    with open(repaired_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            if is_suspicious(row):
                suspicious_found += 1

                repaired_row = repair_row(row)
                repaired_count += 1

                if is_suspicious(repaired_row):
                    suspicious_after_repair += 1

                writer.writerow(repaired_row)
            else:
                writer.writerow(row)

    print("\nRepair summary")
    print("-" * 60)
    print(f"Book inferred              : {book}")
    print(f"Source file                : {csv_path}")
    print(f"Output file                : {repaired_path}")
    print(f"Suspicious rows found      : {suspicious_found}")
    print(f"Rows re-pulled             : {repaired_count}")
    print(f"Still suspicious after run : {suspicious_after_repair}")
    print("-" * 60)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python repair_suspicious_rows.py <csv_file>")
        sys.exit(1)

    main(sys.argv[1])
