import os
import csv
import time
import math
import statistics
from typing import Dict, List, Tuple, Any, Optional, Set

import requests

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

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
GET_TASK_URL = f"{BASE_URL}/serp/google/organic/task_get/regular"

BOOK_NAME = "1 Chronicles"
OUT_CSV = f"{BOOK_NAME}_total_hits_dataforseo.csv"

# Polling / politeness
POLL_INTERVAL_SECONDS = 2
POLITE_DELAY_BETWEEN_VERSES = 0.05
MAX_POLL_SECONDS = 120

ABSOLUTE_FLOOR = 10_000
RELATIVE_DIVISOR = 50


# ============================================================
# VERSES PER CHAPTER (unchanged)
# ============================================================

VERSES_PER_CHAPTER: Dict[int, int] = {
    1: 54,  2: 55,  3: 24,  4: 43,  5: 41,  6: 66,  7: 40,  8: 40,
    9: 44, 10: 14, 11: 47, 12: 41, 13: 14, 14: 17, 15: 29, 16: 43,
   17: 27, 18: 17, 19: 19, 20:  8, 21: 30, 22: 19, 23: 32, 24: 31,
   25: 31, 26: 32, 27: 34, 28: 21, 29: 30
}


def iter_refs() -> List[Tuple[int, int]]:
    refs: List[Tuple[int, int]] = []
    for ch, count in VERSES_PER_CHAPTER.items():
        for v in range(1, count + 1):
            refs.append((ch, v))
    return refs


# ============================================================
# RESUME LOGIC
# ============================================================

def load_processed_refs(csv_path: str) -> Set[Tuple[int, int]]:
    """Load already processed (chapter, verse) pairs from existing CSV."""
    processed = set()
    if not os.path.exists(csv_path):
        return processed

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ch = int(row["chapter"])
                    v = int(row["verse"])
                    processed.add((ch, v))
                except (KeyError, ValueError):
                    continue
    except Exception as e:
        print(f"⚠️ Warning: Could not fully read existing CSV ({e}). Starting fresh.")
    
    print(f"✅ Found {len(processed)} already processed verses in {csv_path}")
    return processed


# ============================================================
# DATAFORSEO FUNCTIONS (unchanged)
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
    return r.json()["tasks"][0]["id"]


def get_task_result(task_id: str) -> Dict[str, Any]:
    start = time.time()
    while True:
        r = requests.get(f"{GET_TASK_URL}/{task_id}", auth=AUTH)
        r.raise_for_status()
        data = r.json()
        task = data["tasks"][0]

        if task.get("status_code") == 20000 and task.get("result"):
            return task

        if (time.time() - start) > MAX_POLL_SECONDS:
            return task

        time.sleep(POLL_INTERVAL_SECONDS)


def extract_totals(task: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "has_result": False,
        "se_results_count": 0,
        "items_count": 0,
        "organic_items_count": 0,
    }
    try:
        result = task.get("result", [None])[0]
        if isinstance(result, dict):
            out["has_result"] = True
            out["se_results_count"] = int(result.get("se_results_count", 0) or 0)
            items = result.get("items") or []
            out["items_count"] = len(items)
            organic_count = sum(1 for it in items if isinstance(it, dict) and it.get("type") == "organic")
            out["organic_items_count"] = organic_count
    except Exception:
        pass
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
    # initial
    task_id = post_task(query)
    task = get_task_result(task_id)
    totals_initial = extract_totals(task)
    initial_val = totals_initial["se_results_count"]

    retry_applied = False
    totals_final = totals_initial
    final_val = initial_val

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
# NORMALIZATION (unchanged)
# ============================================================

def compute_normalizations(values: List[int]):
    log10_vals = [math.log10(v + 1) for v in values]
    if not log10_vals:
        return [], [], []
    mean = statistics.mean(log10_vals)
    stdev = statistics.pstdev(log10_vals) or 1.0
    z_vals = [(x - mean) / stdev for x in log10_vals]
    mn, mx = min(log10_vals), max(log10_vals)
    denom = (mx - mn) if (mx - mn) != 0 else 1.0
    mm_vals = [(x - mn) / denom for x in log10_vals]
    return log10_vals, z_vals, mm_vals


def fusion_score(minmax_val: float, z_val: float) -> float:
    z01 = max(0.0, min(1.0, (z_val + 3.0) / 6.0))
    return round(0.6 * minmax_val + 0.4 * z01, 6)


# ============================================================
# MAIN WITH RESUME + CRASH RECOVERY
# ============================================================

def main():
    refs = iter_refs()
    processed = load_processed_refs(OUT_CSV)
    
    # Filter remaining references
    remaining = [ref for ref in refs if ref not in processed]
    print(f"📍 Continuing with {len(remaining)} / {len(refs)} verses")

    iterator = remaining if tqdm is None else tqdm(remaining, desc=f"DataForSEO totals ({BOOK_NAME})")

    fieldnames = [
        "book", "chapter", "verse", "query",
        "retry_applied",
        "raw_hit_count_initial", "raw_hit_count_final",
        "has_result_initial", "items_count_initial", "organic_items_count_initial",
        "has_result_final", "items_count_final", "organic_items_count_final",
        "log10_raw_hit_count", "z_log10_raw_hit_count",
        "minmax_log10_raw_hit_count", "fusion_score"
    ]

    # Open in append mode (create header only if file is new)
    file_exists = os.path.exists(OUT_CSV)
    mode = "a" if file_exists else "w"
    
    with open(OUT_CSV, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        rows_to_normalize: List[int] = []  # We'll collect final counts for normalization later

        try:
            for ch, v in iterator:
                query = f"{BOOK_NAME} {ch}:{v}"

                result = fetch_count_with_selective_retry(query, rows_to_normalize)

                raw_final = result["raw_hit_count_final"]
                if raw_final > 0:
                    rows_to_normalize.append(raw_final)

                row = {
                    "book": BOOK_NAME,
                    "chapter": ch,
                    "verse": v,
                    "query": query,
                    "retry_applied": result["retry_applied"],

                    "raw_hit_count_initial": result["raw_hit_count_initial"],
                    "raw_hit_count_final": raw_final,

                    "has_result_initial": result["initial"]["has_result"],
                    "items_count_initial": result["initial"]["items_count"],
                    "organic_items_count_initial": result["initial"]["organic_items_count"],

                    "has_result_final": result["final"]["has_result"],
                    "items_count_final": result["final"]["items_count"],
                    "organic_items_count_final": result["final"]["organic_items_count"],
                }

                # Write immediately (so crash = minimal loss)
                writer.writerow(row)
                f.flush()  # Ensure it's written to disk

                if POLITE_DELAY_BETWEEN_VERSES:
                    time.sleep(POLITE_DELAY_BETWEEN_VERSES)

            print("✅ All verses processed.")

        except Exception as e:
            print(f"❌ Error occurred: {e}")
            print("💾 Progress has been saved to CSV. You can resume by running the script again.")
            raise
        finally:
            # Recompute normalizations on the full dataset (including previous runs)
            print("📊 Recomputing normalizations on full dataset...")
            full_counts = []
            with open(OUT_CSV, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                full_counts = [int(row["raw_hit_count_final"]) for row in reader]

            log10_vals, z_vals, mm_vals = compute_normalizations(full_counts)
            fusion_vals = [fusion_score(m, z) for m, z in zip(mm_vals, z_vals)]

            # Rewrite the file with normalized columns
            temp_file = OUT_CSV + ".tmp"
            with open(OUT_CSV, "r", newline="", encoding="utf-8") as f_in, \
                 open(temp_file, "w", newline="", encoding="utf-8") as f_out:
                reader = csv.DictReader(f_in)
                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                writer.writeheader()
                for i, row in enumerate(reader):
                    row["log10_raw_hit_count"] = round(log10_vals[i], 6)
                    row["z_log10_raw_hit_count"] = round(z_vals[i], 6)
                    row["minmax_log10_raw_hit_count"] = round(mm_vals[i], 6)
                    row["fusion_score"] = fusion_vals[i]
                    writer.writerow(row)
            
            os.replace(temp_file, OUT_CSV)
            print(f"✅ Done. Final file: {OUT_CSV} ({len(full_counts)} rows)")


if __name__ == "__main__":
    main()