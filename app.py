import math
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

BASE_URL = "https://c3po.crossfit.com/api/leaderboards/v2/competitions/open/2026/leaderboards"

# Simple in-memory cache so you don't hammer the API on refresh
_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60


def _get_json(params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _parse_reps(score_display: Optional[str], breakdown: Optional[str] = None) -> Optional[int]:
    # For-time workouts: top athletes have scoreDisplay="11:16" (time) but breakdown="354 reps\n..."
    # Non-completers: scoreDisplay="221 reps", breakdown="..."
    # Try breakdown first so completers get their actual rep count, not the minutes digit.
    for text in (breakdown, score_display):
        if not text:
            continue
        m = re.search(r"(\d+)\s*rep", text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def _percentile_from_rank(rank: int, total_competitors: int) -> float:
    # rank=1 -> 100, rank=N -> ~0
    return 100.0 * (1.0 - (rank - 1) / total_competitors)


def _make_page_samples(total_pages: int, target_buckets: int = 20) -> List[int]:
    if total_pages <= 0:
        return [1]
    step = max(1, math.ceil(total_pages / target_buckets))
    # sample the middle page of each bucket for stability
    pages = []
    for start in range(1, total_pages + 1, step):
        end = min(total_pages, start + step - 1)
        mid = (start + end) // 2
        pages.append(mid)
    # de-dupe and keep sorted
    return sorted(set(pages))


def _page_has_submissions(page_json: Dict[str, Any]) -> bool:
    rows = page_json.get("leaderboardRows") or []
    return any(row.get("scores", [{}])[0].get("scoreDisplay") for row in rows)


def _find_last_submission_page(base_params: Dict[str, Any], total_pages: int) -> Tuple[int, Dict[str, Any]]:
    """Binary search for the last page that has at least one submitted score.
    Returns (page_number, page_json) so the caller can reuse the fetched data."""
    lo, hi = 1, total_pages
    last_json: Dict[str, Any] = {}
    while lo < hi:
        mid = (lo + hi + 1) // 2
        page_json = _get_json({**base_params, "page": mid})
        if _page_has_submissions(page_json):
            lo = mid
            last_json = page_json
        else:
            hi = mid - 1
    if not last_json:
        last_json = _get_json({**base_params, "page": lo})
    return lo, last_json


def _max_submitted_rank(page_json: Dict[str, Any]) -> int:
    """Return the highest overallRank among athletes with a submitted score on this page."""
    rows = page_json.get("leaderboardRows") or []
    max_rank = 0
    for row in rows:
        if row.get("scores", [{}])[0].get("scoreDisplay"):
            try:
                max_rank = max(max_rank, int(row.get("overallRank") or 0))
            except ValueError:
                pass
    return max_rank


def _extract_points_from_page(page_json: Dict[str, Any], fallback_total: int = 0, total_submitted: int = 0) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Returns:
      points: [{percentile, reps, rank}]
      submitted_rows: rows with valid score & parseable reps
      total_rows: all rows seen on page
    """
    pagination = page_json.get("pagination") or {}
    total_competitors = int(pagination.get("totalCompetitors") or 0) or fallback_total

    rows = page_json.get("leaderboardRows") or []
    points: List[Dict[str, Any]] = []
    submitted_rows = 0
    total_rows = 0

    for row in rows:
        total_rows += 1
        try:
            rank = int(row.get("overallRank") or 0)
        except ValueError:
            continue

        scores = row.get("scores") or []
        first = scores[0] if scores else {}
        valid = str(first.get("valid") or "") == "1"
        reps = _parse_reps(first.get("scoreDisplay"), first.get("breakdown"))

        if valid and reps is not None:
            submitted_rows += 1

        # For the curve, we only plot points that have reps + a real rank
        pct_base = total_submitted or total_competitors
        if reps is None or rank <= 0 or pct_base <= 0:
            continue

        points.append(
            {
                "rank": rank,
                "reps": reps,
                "percentile": _percentile_from_rank(rank, pct_base),
            }
        )

    return points, submitted_rows, total_rows


def _cache_key(params: Dict[str, Any]) -> str:
    # stable-ish key (sorted params)
    return "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/curve")
def api_curve():
    """
    Query params:
      division (default 1)
      region (default 0)
      scaled (default 0)
      view (default 0)
      sort (default 0)
      buckets (default 20)
    """
    division = request.args.get("division", "1")
    region = request.args.get("region", "0")
    scaled = request.args.get("scaled", "0")
    view = request.args.get("view", "0")
    sort = request.args.get("sort", "0")
    buckets = int(request.args.get("buckets", "20"))

    base_params = {
        "view": view,
        "division": division,
        "region": region,
        "scaled": scaled,
        "sort": sort,
    }

    ck = _cache_key({**base_params, "buckets": buckets})
    now = time.time()
    cached = _CACHE.get(ck)
    if cached and (now - cached["ts"]) < CACHE_TTL_SECONDS:
        return jsonify(cached["payload"])

    # 1) initial request to get totalPages + totalCompetitors
    first = _get_json(base_params)
    pagination = first.get("pagination") or {}
    total_pages = int(pagination.get("totalPages") or 0)
    total_competitors = int(pagination.get("totalCompetitors") or 0)

    # 2) Binary search for the last page with submissions, then sample within that range.
    #    The leaderboard has many registered-but-unsubmitted athletes at the end; sampling
    #    across all pages yields mostly empty rows and only 1-2 data points.
    last_sub_page, last_sub_json = _find_last_submission_page(base_params, total_pages)
    total_submitted = _max_submitted_rank(last_sub_json)
    sample_pages = _make_page_samples(last_sub_page, buckets)
    # Always include page 1 (top performers) without an extra request
    if 1 not in sample_pages:
        sample_pages = sorted([1] + sample_pages)

    all_points: List[Dict[str, Any]] = []
    submitted_rows = 0
    total_rows = 0

    # 3) fetch each sampled page and extract points
    for p in sample_pages:
        # Reuse already-fetched pages where possible
        if p == 1:
            page_json = first
        elif p == last_sub_page:
            page_json = last_sub_json
        else:
            page_json = _get_json({**base_params, "page": p})
        pts, sub, tot = _extract_points_from_page(page_json, total_competitors, total_submitted)
        all_points.extend(pts)
        submitted_rows += sub
        total_rows += tot

    # 4) dedupe points by rank (keep max reps if duplicates appear)
    by_rank: Dict[int, Dict[str, Any]] = {}
    for pt in all_points:
        r = pt["rank"]
        if r not in by_rank or pt["reps"] > by_rank[r]["reps"]:
            by_rank[r] = pt

    points = list(by_rank.values())
    # Sort by percentile ascending so x-axis goes 0 -> 100
    points.sort(key=lambda x: x["percentile"])

    # Submission rate estimate (sample-based)
    submission_rate = (submitted_rows / total_rows) if total_rows else None

    payload = {
        "meta": {
            "year": 2026,
            "competition": "open",
            "division": division,
            "region": region,
            "scaled": scaled,
            "view": view,
            "sort": sort,
            "totalPages": total_pages,
            "totalCompetitors": total_competitors,
            "lastSubmissionPage": last_sub_page,
            "totalSubmitted": total_submitted,
            "sampledPages": len(sample_pages),
            "sampledRows": total_rows,
            "sampledSubmittedRows": submitted_rows,
            "estimatedSubmissionRate": submission_rate,  # 0..1 or null
            "note": "Submission rate is estimated from sampled pages (valid score rows / total rows sampled).",
        },
        "points": points,  # [{percentile, reps, rank}]
    }

    _CACHE[ck] = {"ts": now, "payload": payload}
    return jsonify(payload)


if __name__ == "__main__":
    app.run(debug=True)
