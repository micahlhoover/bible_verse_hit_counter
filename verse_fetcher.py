import csv
import time
import random
import urllib.parse
from typing import List, Tuple, Dict, Any
import re

import requests

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# run :
#   python ./verse_fetcher.py


BOOK_NAME = "Genesis"

# PROVERBS_VERSES_PER_CHAPTER: List[int] = [
#     33, 22, 35, 27, 23, 35, 27, 36, 18, 32,
#     31, 28, 25, 35, 33, 33, 28, 24, 29, 30,
#     31, 29, 35, 34, 28, 28, 27, 28, 27, 33, 31
# ]

#MATTHEW_VERSES_PER_CHAPTER: List[int] = [25, 23, 17, 25, 48, 34, 29, 34, 38, 42, 30, 50, 58, 36, 39, 28, 27, 35, 30, 34, 46, 46, 39, 51, 46, 75, 66, 20]

from typing import List

VERSES_PER_CHAPTER: List[int] = [
    31,  # Chapter 1
    25,  # Chapter 2
    24,  # Chapter 3
    26,  # Chapter 4
    32,  # Chapter 5
    22,  # Chapter 6
    24,  # Chapter 7
    22,  # Chapter 8
    29,  # Chapter 9
    32,  # Chapter 10
    32,  # Chapter 11
    20,  # Chapter 12
    18,  # Chapter 13
    24,  # Chapter 14
    21,  # Chapter 15
    16,  # Chapter 16
    27,  # Chapter 17
    33,  # Chapter 18
    38,  # Chapter 19
    18,  # Chapter 20
    34,  # Chapter 21
    24,  # Chapter 22
    20,  # Chapter 23
    67,  # Chapter 24
    34,  # Chapter 25
    35,  # Chapter 26
    46,  # Chapter 27
    22,  # Chapter 28
    35,  # Chapter 29
    43,  # Chapter 30
    55,  # Chapter 31
    32,  # Chapter 32
    20,  # Chapter 33
    31,  # Chapter 34
    29,  # Chapter 35
    43,  # Chapter 36
    36,  # Chapter 37
    30,  # Chapter 38
    23,  # Chapter 39
    23,  # Chapter 40
    57,  # Chapter 41
    38,  # Chapter 42
    34,  # Chapter 43
    34,  # Chapter 44
    28,  # Chapter 45
    34,  # Chapter 46
    31,  # Chapter 47
    22,  # Chapter 48
    33,  # Chapter 49
    26   # Chapter 50
]

# bible-api.com user-input endpoint; translation parameter supported (e.g., ?translation=kjv)
BASE_URL = "https://bible-api.com"  # [5](https://bible-api.com/)


def iter_refs() -> List[Tuple[int, int]]:
    refs = []
    for ch, vc in enumerate(VERSES_PER_CHAPTER, start=1):
        for v in range(1, vc + 1):
            refs.append((ch, v))
    return refs


def fetch_kjv(chapter: int, verse: int, timeout_s: int = 20) -> Dict[str, Any]:
    ref = f"{BOOK_NAME} {chapter}:{verse}"
    encoded = urllib.parse.quote(ref)
    url = f"{BASE_URL}/{encoded}?translation=kjv"  # [5](https://bible-api.com/)

    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()

def normalize_newlines(text: str) -> str:
    text = text.strip()
    # Replace one or more newlines (with optional spaces) with a single space
    text = re.sub(r'\s*\n+\s*', ' ', text)
    return text

def robust_fetch_kjv(chapter: int, verse: int, max_retries: int = 5) -> str:
    base_sleep = 0.75
    last_exc = None

    for attempt in range(max_retries):
        try:
            data = fetch_kjv(chapter, verse)
            # bible-api.com returns top-level "text" for the passage (often includes trailing whitespace/newlines)

            raw = (data.get("text") or "")
            clean = normalize_newlines(raw)

            return clean
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_exc = e
            sleep_s = base_sleep * (2 ** attempt) + random.uniform(0, 0.25)
            time.sleep(sleep_s)

    # If it truly fails, return empty string so the CSV remains complete
    return ""


def main(out_csv: str = f"verse_content/{BOOK_NAME}_kjv.csv", polite_delay_s: float = 0.05):
    refs = iter_refs()
    iterator = refs
    if tqdm is not None:
        iterator = tqdm(refs, desc=f"Fetching KJV text for {BOOK_NAME}")

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["book", "chapter", "verse", "verse_text"])
        writer.writeheader()

        for ch, v in iterator:
            verse_text = robust_fetch_kjv(ch, v)
            writer.writerow({
                "book": BOOK_NAME,
                "chapter": ch,
                "verse": v,
                "verse_text": verse_text
            })
            if polite_delay_s:
                time.sleep(polite_delay_s)

    print(f"Done. Wrote {out_csv}.")


if __name__ == "__main__":
    main()