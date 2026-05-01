import os
import csv
import sys
import time
import math
import statistics
from typing import Dict, List, Tuple, Any, Optional

import requests

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# this is to search a specific verse if the repair fails for one of the verses

# python ./verse_indexer_spot_search.py "Luke 12:1"

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

# Polling / politeness
POLL_INTERVAL_SECONDS = 2
POLITE_DELAY_BETWEEN_VERSES = 0.05
MAX_POLL_SECONDS = 120  # safety timeout per task

# Selective retry thresholds (same logic we used successfully with SearchAPI.io)
ABSOLUTE_FLOOR = 10_000
RELATIVE_DIVISOR = 50




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

#def main():
def main(query: str):
    #refs = iter_refs()
    #iterator = refs if tqdm is None else tqdm(refs, desc=f"DataForSEO totals ({BOOK_NAME})")

    # moved to inbound param
    # query = f"{BOOK_NAME} {ch}:{v}"

    result = fetch_count_with_selective_retry(query, [])
    print(f"Result : {result}")


if __name__ == "__main__":
    print(f"number of args : {len(sys.argv)}")
    if len(sys.argv) != 2:
        print("Usage: python verse_indexer_spot_search.py [exact query in quotes, e.g. Luke 6:31]")
        sys.exit(1)

    main(sys.argv[1])
