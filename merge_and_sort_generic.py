import csv

BOOK_NAME = "Mark"
KJV_FILE  = f"verse_content/{BOOK_NAME}_kjv.csv"
HITS_FILE = f"{BOOK_NAME}_total_hits_dataforseo_repaired.csv"
OUT_FILE  = f"{BOOK_NAME}_sorted_by_unpopularity2.csv"

# call it like this :
# python .\merge_and_sort_generic.py

def load_kjv(filepath):
    """
    Returns a dict keyed by (book, chapter, verse) -> verse_text
    """
    kjv = {}
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row["book"],
                int(row["chapter"]),
                int(row["verse"]),
            )
            kjv[key] = row["verse_text"]
    return kjv


def load_hits(filepath):
    """
    Returns a list of dicts with parsed numeric hit counts
    """
    hits = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                raw_hits = int(row["raw_hit_count_initial"])
            except ValueError:
                raw_hits = 0

            hits.append({
                "book": row["book"],
                "chapter": int(row["chapter"]),
                "verse": int(row["verse"]),
                "raw_hit_count": raw_hits,
            })
    return hits


def main():
    kjv_map = load_kjv(KJV_FILE)
    hits = load_hits(HITS_FILE)

    merged = []

    for row in hits:
        key = (row["book"], row["chapter"], row["verse"])
        verse_text = kjv_map.get(key)

        if verse_text is None:
            # Skip silently; could also log if desired
            continue

        merged.append({
            "book": row["book"],
            "chapter": row["chapter"],
            "verse": row["verse"],
            "raw_hit_count": row["raw_hit_count"],
            "verse_text": verse_text,
        })

    # Sort by ascending raw hit count (unpopularity)
    merged.sort(key=lambda r: r["raw_hit_count"])

    # Assign ordinal ranks
    for idx, row in enumerate(merged, start=1):
        row["ordinal_rank"] = idx

    # Write output
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "book",
            "chapter",
            "verse",
            "raw_hit_count",
            "ordinal_rank",
            "verse_text",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged:
            writer.writerow(row)

    print(f"Wrote {len(merged)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()