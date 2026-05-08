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

from typing import List


# run :
#   python ./verse_fetcher.py


BOOK_NAME = "Deuteronomy"

VERSES_PER_CHAPTER: List[int] = [
    46,  # Chapter 1
    37,  # Chapter 2
    29,  # Chapter 3
    49,  # Chapter 4
    33,  # Chapter 5
    25,  # Chapter 6
    26,  # Chapter 7
    20,  # Chapter 8
    29,  # Chapter 9
    22,  # Chapter 10
    32,  # Chapter 11
    32,  # Chapter 12
    18,  # Chapter 13
    29,  # Chapter 14
    23,  # Chapter 15
    22,  # Chapter 16
    20,  # Chapter 17
    22,  # Chapter 18
    21,  # Chapter 19
    20,  # Chapter 20
    23,  # Chapter 21
    30,  # Chapter 22
    25,  # Chapter 23
    22,  # Chapter 24
    19,  # Chapter 25
    19,  # Chapter 26
    26,  # Chapter 27
    69,  # Chapter 28
    28,  # Chapter 29
    20,  # Chapter 30
    30,  # Chapter 31
    52,  # Chapter 32
    29,  # Chapter 33
    12   # Chapter 34
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