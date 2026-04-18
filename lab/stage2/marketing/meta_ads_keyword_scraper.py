"""
Fabrika — meta_ads_keyword_scraper.py
Scrapes Meta Ads Library by keywords derived from Phase 1 search seeds.
Surfaces ALL advertisers targeting Coursiv's audience — including unknown competitors.

This is the supply-side signal (Signal 2) for competitor identification.
Every advertiser appearing across multiple keyword searches is a real competitor.

Reads:  lab/search_seeds.json  (pain + consideration + benefit queries)
        lab/stage1/processing/.env  (APIFY_API_TOKEN)
Writes: lab/stage2/marketing/output/raw/meta_keyword_ads_raw.json
        lab/stage2/marketing/output/meta_keyword_ads.csv
        lab/stage2/marketing/output/competitor_signals.csv  ← key output

Usage:
  py lab/stage2/marketing/meta_ads_keyword_scraper.py
  py lab/stage2/marketing/meta_ads_keyword_scraper.py --dry-run   # show queries only
"""

import json
import os
import sys
import csv
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter, defaultdict
from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parents[2]
ENV_PATH      = ROOT / "stage1" / "processing" / ".env"
SEEDS_PATH    = ROOT / "search_seeds.json"
OUTPUT_DIR    = Path(__file__).resolve().parent / "output"
RAW_DIR       = OUTPUT_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

RAW_JSON_PATH    = RAW_DIR / "meta_keyword_ads_raw.json"
CSV_PATH         = OUTPUT_DIR / "meta_keyword_ads.csv"
COMPETITOR_PATH  = OUTPUT_DIR / "competitor_signals.csv"

load_dotenv(ENV_PATH)
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
if not APIFY_API_TOKEN:
    print("ERROR: APIFY_API_TOKEN not set in lab/stage1/processing/.env")
    sys.exit(1)

try:
    from apify_client import ApifyClient
except ImportError:
    print("ERROR: apify-client not installed. Run: pip install apify-client")
    sys.exit(1)

APIFY_ACTOR_ID = "curious_coder/facebook-ads-library-scraper"

# ── Keywords derived from Phase 1 seeds ──────────────────────────────────────
# These are the terms Coursiv's audience actually uses — scraping these
# surfaces whoever is competing for that audience's attention in Meta Ads.
KEYWORD_SEARCHES = [
    {"keyword": "learn AI",              "category": "pain",         "max_results": 100},
    {"keyword": "AI no coding",          "category": "pain",         "max_results": 100},
    {"keyword": "AI overwhelmed",        "category": "pain",         "max_results": 100},
    {"keyword": "AI left behind",        "category": "pain",         "max_results": 100},
    {"keyword": "AI career change",      "category": "pain",         "max_results": 100},
    {"keyword": "AI course beginner",    "category": "consideration","max_results": 100},
    {"keyword": "ChatGPT course",        "category": "consideration","max_results": 100},
    {"keyword": "AI certificate",        "category": "consideration","max_results": 100},
    {"keyword": "prompt engineering",    "category": "consideration","max_results": 100},
    {"keyword": "learn ChatGPT",         "category": "consideration","max_results": 100},
    {"keyword": "AI productivity",       "category": "benefit",      "max_results": 100},
    {"keyword": "AI side hustle",        "category": "benefit",      "max_results": 100},
    {"keyword": "AI skills career",      "category": "benefit",      "max_results": 100},
    {"keyword": "master AI",             "category": "benefit",      "max_results": 100},
    {"keyword": "28 day AI challenge",   "category": "direct",       "max_results": 100},
]


def load_seeds() -> dict:
    if not SEEDS_PATH.exists():
        print(f"WARNING: search_seeds.json not found at {SEEDS_PATH}")
        return {}
    with open(SEEDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def scrape_keyword(client: ApifyClient, keyword: str, max_results: int = 100) -> list:
    print(f"\n  Keyword: '{keyword}' (max {max_results})")

    url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=ALL&q={keyword.replace(' ', '+')}&search_type=keyword_unordered"

    actor_input = {
        "urls": [{"url": url}],
        "maxResults": max_results,
    }

    try:
        run = client.actor(APIFY_ACTOR_ID).call(run_input=actor_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        items = items[:max_results]
        print(f"  → {len(items)} ads (capped at {max_results})")
        return items
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


def extract_page_name(ad: dict) -> str:
    try:
        return ad.get("snapshot", {}).get("page_name", "") or ad.get("page_name", "")
    except:
        return ""


def flatten_ad(ad: dict, keyword: str, category: str) -> dict:
    snapshot   = ad.get("snapshot") or {}
    body       = snapshot.get("body") or {}
    ad_copy    = body.get("text", "") if isinstance(body, dict) else str(body)
    start_date = ad.get("startDate") or ad.get("start_date_formatted", "")
    end_date   = ad.get("endDate") or ad.get("end_date_formatted", "")

    days_running = None
    if start_date:
        try:
            from datetime import datetime
            start = datetime.fromisoformat(str(start_date).replace(" ", "T").replace("Z", "+00:00")[:19])
            end   = datetime.fromisoformat(str(end_date).replace(" ", "T").replace("Z", "+00:00")[:19]) if end_date else datetime.now()
            days_running = (end - start).days
        except:
            pass

    creative_type = "unknown"
    if snapshot.get("videos"):   creative_type = "video"
    elif snapshot.get("images"): creative_type = "image"
    elif snapshot.get("cards"):  creative_type = "carousel"

    return {
        "search_keyword":    keyword,
        "keyword_category":  category,
        "scraped_at":        datetime.now(timezone.utc).isoformat(),
        "page_name":         extract_page_name(ad),
        "page_id":           ad.get("page_id", ""),
        "ad_copy":           ad_copy[:300],
        "headline":          snapshot.get("title", ""),
        "cta":               snapshot.get("cta_type", ""),
        "landing_page":      snapshot.get("link_url", ""),
        "creative_type":     creative_type,
        "start_date":        start_date,
        "end_date":          end_date,
        "days_running":      days_running,
        "is_active":         ad.get("is_active", ""),
        "platforms":         "|".join(ad.get("publisher_platform", [])),
        "ad_library_url":    ad.get("ad_library_url", ""),
        "raw_json":          json.dumps(ad),
    }


def build_competitor_signals(all_rows: list) -> list:
    """
    For each page that appeared in keyword searches, count:
    - how many different keywords they appeared in (cross-keyword frequency = competitor strength)
    - total ads
    - longest running ad
    - which keyword categories they cover
    """
    page_keywords    = defaultdict(set)
    page_ad_count    = Counter()
    page_max_days    = defaultdict(int)
    page_categories  = defaultdict(set)
    page_sample_copy = {}

    for row in all_rows:
        page = row["page_name"]
        if not page or page == "Coursiv":  # exclude ourselves
            continue
        kw   = row["search_keyword"]
        cat  = row["keyword_category"]
        days = row["days_running"] or 0

        page_keywords[page].add(kw)
        page_ad_count[page] += 1
        if days > page_max_days[page]:
            page_max_days[page] = days
        page_categories[page].add(cat)
        if page not in page_sample_copy and row["ad_copy"]:
            page_sample_copy[page] = row["ad_copy"][:150]

    # Score: pages appearing in more keyword searches = stronger competitor signal
    signals = []
    for page in page_keywords:
        keyword_count = len(page_keywords[page])
        signals.append({
            "page_name":        page,
            "keyword_count":    keyword_count,        # PRIMARY SIGNAL: how many of our keywords they advertise on
            "total_ads":        page_ad_count[page],
            "max_days_running": page_max_days[page],
            "keyword_categories": "|".join(sorted(page_categories[page])),
            "keywords_matched": "|".join(sorted(page_keywords[page])),
            "competitor_tier":  "DIRECT" if keyword_count >= 4 else ("ADJACENT" if keyword_count >= 2 else "WEAK"),
            "sample_ad_copy":   page_sample_copy.get(page, ""),
        })

    # Sort by keyword_count desc, then total_ads desc
    signals.sort(key=lambda x: (-x["keyword_count"], -x["total_ads"]))
    return signals


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Show queries without scraping")
    args = parser.parse_args()

    print("=" * 60)
    print("Fabrika — Meta Ads Keyword Scraper")
    print("Surfaces competitors by audience keyword overlap")
    print("=" * 60)
    print(f"\nKeyword searches planned: {len(KEYWORD_SEARCHES)}")
    for ks in KEYWORD_SEARCHES:
        print(f"  [{ks['category']:15s}] '{ks['keyword']}' (max {ks['max_results']} ads)")

    estimated_cost = (len(KEYWORD_SEARCHES) * 0.10) + (sum(k['max_results'] for k in KEYWORD_SEARCHES) / 1000 * 0.20)
    print(f"\nEstimated Apify cost: ~${estimated_cost:.1f}")
    print(f"Estimated time: ~{len(KEYWORD_SEARCHES) * 2} minutes")

    if args.dry_run:
        print("\n[DRY RUN — not scraping]")
        return

    client   = ApifyClient(APIFY_API_TOKEN)
    all_raw  = []
    all_rows = []

    for i, ks in enumerate(KEYWORD_SEARCHES, 1):
        print(f"\n[{i}/{len(KEYWORD_SEARCHES)}]")
        ads = scrape_keyword(client, ks["keyword"], ks["max_results"])

        for ad in ads:
            ad["_keyword"] = ks["keyword"]
            ad["_category"] = ks["category"]
            all_raw.append(ad)

        rows = [flatten_ad(ad, ks["keyword"], ks["category"]) for ad in ads]
        all_rows.extend(rows)
        time.sleep(3)  # polite delay between searches

    # Save raw JSON
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_raw, f, indent=2, ensure_ascii=False)
    print(f"\nRaw JSON: {RAW_JSON_PATH}  ({len(all_raw)} ads)")

    # Save flat CSV
    if all_rows:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"CSV: {CSV_PATH}  ({len(all_rows)} rows)")

    # Build and save competitor signals
    signals = build_competitor_signals(all_rows)
    if signals:
        with open(COMPETITOR_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(signals[0].keys()))
            writer.writeheader()
            writer.writerows(signals)
        print(f"Competitor signals: {COMPETITOR_PATH}  ({len(signals)} pages)")

    # Print competitor summary
    print(f"\n{'='*60}")
    print("COMPETITOR SIGNAL SUMMARY")
    print("="*60)
    print(f"Total unique advertisers found: {len(signals)}")
    print(f"\nDIRECT competitors (4+ keyword matches):")
    for s in [x for x in signals if x['competitor_tier'] == 'DIRECT'][:15]:
        print(f"  {s['page_name']:35s} {s['keyword_count']} keywords | {s['total_ads']} ads | max {s['max_days_running']}d")
    print(f"\nADJACENT competitors (2-3 keyword matches):")
    for s in [x for x in signals if x['competitor_tier'] == 'ADJACENT'][:15]:
        print(f"  {s['page_name']:35s} {s['keyword_count']} keywords | {s['total_ads']} ads | max {s['max_days_running']}d")

    print(f"\nNext: review competitor_signals.csv then run strategy synthesis")


if __name__ == "__main__":
    main()
