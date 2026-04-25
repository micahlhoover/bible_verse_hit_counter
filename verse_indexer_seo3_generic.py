import os
import csv
import time
import math
import statistics
from typing import Dict, List, Tuple, Any, Optional

import requests

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# python ./verse_indexer_seo3_generic.py

# ============================================================
# ENV + CONSTANTS
# ============================================================

DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN", "").strip()
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "").strip()

if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
    raise RuntimeError("Missing DATAFORSEO_LOGIN or DATAFORSEO_PASSWORD env vars")

AUTH = (DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)

BASE_URL = "https://api.dataforseo.com/v3"
POST_TASK_URL = f"{BASE_URL}/serp/google/organic/task_post"
#GET_TASK_URL = f"{BASE_URL}/serp/google/organic/task_get"
GET_TASK_URL = f"{BASE_URL}/serp/google/organic/task_get/regular"

BOOK_NAME = "1 Corinthians"
OUT_CSV = f"{BOOK_NAME}_total_hits_dataforseo.csv"

# Polling / politeness
POLL_INTERVAL_SECONDS = 2
POLITE_DELAY_BETWEEN_VERSES = 0.05
MAX_POLL_SECONDS = 120  # safety timeout per task

# Selective retry thresholds (same logic we used successfully with SearchAPI.io)
ABSOLUTE_FLOOR = 10_000
RELATIVE_DIVISOR = 50


# ============================================================
# Book CHAPTER -> VERSE COUNT
# ============================================================

# VERSES_PER_CHAPTER: Dict[int, int] = {
#     1: 2
# }
    # Prompt for each book :
    # I'm trying to get the web count totals for each verse in the book of Matthew using a search API.The python struct I used to keep track of how far to go into each chapter of Proverbs looked like this :VERSES_PER_CHAPTER: Dict[int, int] = {
    #     1: 33,  2: 22,  3: 35,  4: 27,  5: 23,  6: 35,  7: 27,  8: 36,
    #     9: 18, 10: 32, 11: 31, 12: 28, 13: 25, 14: 35, 15: 33, 16: 33,
    #     17: 28, 18: 24, 19: 29, 20: 30, 21: 31, 22: 29, 23: 35, 24: 34,
    #     25: 28, 26: 28, 27: 27, 28: 28, 29: 27, 30: 33, 31: 31
    # }Where the first instance in the python object, 1, has 33 because that's how many verses there are, and so on.
    # Can you create a similar python dict for Matthew and show me the code ?

# 1 Corinthians
VERSES_PER_CHAPTER: Dict[int, int] = {
    1: 31,   # Chapter 1
    2: 16,   # Chapter 2
    3: 23,   # Chapter 3
    4: 21,   # Chapter 4
    5: 13,   # Chapter 5
    6: 20,   # Chapter 6
    7: 40,   # Chapter 7
    8: 13,   # Chapter 8
    9: 27,   # Chapter 9
    10: 33,  # Chapter 10
    11: 34,  # Chapter 11
    12: 31,  # Chapter 12
    13: 13,  # Chapter 13 (the "love" chapter)
    14: 40,  # Chapter 14
    15: 58,  # Chapter 15 (resurrection chapter)
    16: 24   # Chapter 16
}


# Philippians
# VERSES_PER_CHAPTER: Dict[int, int] = {
#     1: 30, 2: 30, 3: 21, 4: 23
# }

def iter_refs() -> List[Tuple[int, int]]:
    refs: List[Tuple[int, int]] = []
    for ch, count in VERSES_PER_CHAPTER.items():
        for v in range(1, count + 1):
            refs.append((ch, v))
    return refs


# ============================================================
# DATAFORSEO ASYNC FLOW
#   1) POST task_post -> returns task id
#   2) Poll task_get/{id} until task status_code == 20000 with a result
# ============================================================

def post_task(query: str) -> str:
    payload = [{
        "keyword": query,
        "location_code": 2840,     # United States
        "language_code": "en",
        "device": "desktop",
        "os": "windows"
    }]

    r = requests.post(
        POST_TASK_URL,
        json=payload,
        auth=AUTH,
        headers={"Content-Type": "application/json"}
    )
    r.raise_for_status()

    data = r.json()
    # task_post returns tasks[0].id
    return data["tasks"][0]["id"]


def get_task_result(task_id: str) -> Dict[str, Any]:
    start = time.time()

    while True:
        r = requests.get(
            f"{GET_TASK_URL}/{task_id}",
            auth=AUTH
        )
        r.raise_for_status()
        data = r.json()

        task = data["tasks"][0]

        # When task is ready, task["status_code"] should be 20000 and task["result"] is present
        if task.get("status_code") == 20000 and task.get("result"):
            return task

        if (time.time() - start) > MAX_POLL_SECONDS:
            # Return task as-is so caller can record it as missing/timeout
            return task

        time.sleep(POLL_INTERVAL_SECONDS)


# ============================================================
# EXTRACTION + SANITY + RETRY
# ============================================================

def extract_totals(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull out the key totals we care about. DataForSEO result payloads can vary,
    so we defensively access fields.
    """
    out = {
        "has_result": False,
        "se_results_count": 0,
        "items_count": 0,
        "organic_items_count": 0,
    }

    result = None
    try:
        result = task.get("result", [None])[0]
    except Exception:
        result = None

    if not isinstance(result, dict):
        return out

    out["has_result"] = True

    # Common count field in DataForSEO SERP results
    out["se_results_count"] = int(result.get("se_results_count", 0) or 0)

    # Total items returned by API
    items = result.get("items") or []
    out["items_count"] = len(items)

    # Count organic items within items
    organic_count = 0
    for it in items:
        if isinstance(it, dict) and it.get("type") == "organic":
            organic_count += 1
    out["organic_items_count"] = organic_count

    return out


def needs_retry(value: int, recent_nonzero: List[int]) -> bool:
    if value < ABSOLUTE_FLOOR:
        return True
    if len(recent_nonzero) >= 3:
        median_recent = statistics.median(recent_nonzero[-3:])
        if value < (median_recent / RELATIVE_DIVISOR):
            return True
    return False


def fetch_count_with_selective_retry(query: str, recent_nonzero: List[int]) -> Dict[str, Any]:
    """
    Runs one task, extracts totals. Retries once if value looks implausible.
    Returns both initial + final counts and retry flag.
    """
    # initial
    task_id = post_task(query)
    task = get_task_result(task_id)
    totals_initial = extract_totals(task)
    initial_val = totals_initial["se_results_count"]

    retry_applied = False
    totals_final = totals_initial
    final_val = initial_val

    # We observed rare one-off SERP metadata flukes (tiny counts vs millions), so retry once only when implausible.
    if needs_retry(initial_val, recent_nonzero):
        retry_applied = True
        task_id2 = post_task(query)
        task2 = get_task_result(task_id2)
        totals_retry = extract_totals(task2)
        retry_val = totals_retry["se_results_count"]

        if retry_val >= ABSOLUTE_FLOOR:
            totals_final = totals_retry
            final_val = retry_val
        else:
            # treat as missing if both are implausible
            totals_final = totals_retry
            final_val = 0

    return {
        "retry_applied": retry_applied,
        "initial": totals_initial,
        "final": totals_final,
        "raw_hit_count_initial": initial_val,
        "raw_hit_count_final": final_val
    }


# ============================================================
# NORMALIZATION + FUSION SCORE
# ============================================================

def compute_normalizations(values: List[int]) -> Tuple[List[float], List[float], List[float]]:
    log10_vals = [math.log10(v + 1) for v in values]
    if not log10_vals:
        return [], [], []

    mean = statistics.mean(log10_vals)
    stdev = statistics.pstdev(log10_vals) or 1.0
    z_vals = [(x - mean) / stdev for x in log10_vals]

    mn = min(log10_vals)
    mx = max(log10_vals)
    denom = (mx - mn) if (mx - mn) != 0 else 1.0
    mm_vals = [(x - mn) / denom for x in log10_vals]

    return log10_vals, z_vals, mm_vals


def fusion_score(minmax_val: float, z_val: float) -> float:
    # z is unbounded; map roughly [-3..+3] into [0..1] and blend with minmax
    z01 = max(0.0, min(1.0, (z_val + 3.0) / 6.0))
    return round(0.6 * minmax_val + 0.4 * z01, 6)


# ============================================================
# MAIN
# ============================================================

def main():
    refs = iter_refs()
    iterator = refs if tqdm is None else tqdm(refs, desc=f"DataForSEO totals ({BOOK_NAME})")

    rows: List[Dict[str, Any]] = []
    final_counts: List[int] = []
    recent_nonzero: List[int] = []

    for ch, v in iterator:
        query = f"{BOOK_NAME} {ch}:{v}"

        result = fetch_count_with_selective_retry(query, recent_nonzero)

        raw_initial = result["raw_hit_count_initial"]
        raw_final = result["raw_hit_count_final"]

        if raw_final > 0:
            recent_nonzero.append(raw_final)

        final_counts.append(raw_final)

        rows.append({
            "book": BOOK_NAME,
            "chapter": ch,
            "verse": v,
            "query": query,
            "retry_applied": result["retry_applied"],

            "raw_hit_count_initial": raw_initial,
            "raw_hit_count_final": raw_final,

            "has_result_initial": result["initial"]["has_result"],
            "items_count_initial": result["initial"]["items_count"],
            "organic_items_count_initial": result["initial"]["organic_items_count"],

            "has_result_final": result["final"]["has_result"],
            "items_count_final": result["final"]["items_count"],
            "organic_items_count_final": result["final"]["organic_items_count"],
        })

        if POLITE_DELAY_BETWEEN_VERSES:
            time.sleep(POLITE_DELAY_BETWEEN_VERSES)

    log10_vals, z_vals, mm_vals = compute_normalizations(final_counts)
    fusion_vals = [fusion_score(m, z) for m, z in zip(mm_vals, z_vals)]

    fieldnames = [
        "book", "chapter", "verse", "query",
        "retry_applied",

        "raw_hit_count_initial",
        "raw_hit_count_final",

        "has_result_initial",
        "items_count_initial",
        "organic_items_count_initial",

        "has_result_final",
        "items_count_final",
        "organic_items_count_final",

        "log10_raw_hit_count",
        "z_log10_raw_hit_count",
        "minmax_log10_raw_hit_count",
        "fusion_score"
    ]

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, r in enumerate(rows):
            out = dict(r)
            out["log10_raw_hit_count"] = round(log10_vals[i], 6)
            out["z_log10_raw_hit_count"] = round(z_vals[i], 6)
            out["minmax_log10_raw_hit_count"] = round(mm_vals[i], 6)
            out["fusion_score"] = fusion_vals[i]
            writer.writerow(out)

    print(f"✅ Done. Wrote {len(rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
