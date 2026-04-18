"""
Fabrika — meta_ads_scraper.py
Scrapes Meta Ads Library for all target pages defined in topic_config.json.
Uses Apify's Facebook Ad Library Scraper actor.

Reads:  lab/topic_config.json  (meta_ads.target_pages)
Writes: lab/stage2/marketing/output/raw/meta_ads_raw.json
        lab/stage2/marketing/output/meta_ads.csv

Usage:
    py lab/stage2/marketing/meta_ads_scraper.py
    py lab/stage2/marketing/meta_ads_scraper.py --page "Coursiv"
    py lab/stage2/marketing/meta_ads_scraper.py --priority primary
"""

import json
import os
import sys
import time
import argparse
import csv
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Load env ─────────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).resolve().parents[2] / "stage1" / "processing" / ".env"
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

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parents[2]
CONFIG_PATH   = ROOT / "topic_config.json"
OUTPUT_DIR    = Path(__file__).resolve().parent / "output" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_JSON_PATH = OUTPUT_DIR / "meta_ads_raw.json"
CSV_PATH      = Path(__file__).resolve().parent / "output" / "meta_ads.csv"

# Apify actor — Facebook Ad Library Scraper
APIFY_ACTOR_ID = "curious_coder/facebook-ads-library-scraper"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"ERROR: topic_config.json not found at {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def scrape_page(client: ApifyClient, page_config: dict) -> list:
    page_name   = page_config["page_name"]
    search_term = page_config.get("search_term", page_name)
    country     = page_config.get("country", "ALL")
    active_status = page_config.get("active_status", "all")

    print(f"\n  Scraping: {page_name}  |  search: '{search_term}'  |  country: {country}")

    status_map = {"all": "ALL", "active": "ACTIVE", "inactive": "INACTIVE"}

    actor_input = {
        "urls": [
            {
                "url": f"https://www.facebook.com/ads/library/?active_status={active_status}&ad_type=all&country={country}&q={search_term.replace(' ', '+')}&search_type=keyword_unordered"
            }
        ],
        "maxResults": 200,
    }

    try:
        run   = client.actor(APIFY_ACTOR_ID).call(run_input=actor_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"  → {len(items)} ads retrieved")
        return items
    except Exception as e:
        print(f"  ERROR scraping {page_name}: {e}")
        return []


def flatten_ad(ad: dict, page_config: dict) -> dict:
    """Flatten Apify output to a flat CSV row."""

    # Days running — primary performance proxy
    start_date = ad.get("startDate") or ad.get("adCreationTime", "")
    end_date   = ad.get("endDate", "")
    days_running = None
    if start_date:
        try:
            start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            end   = datetime.fromisoformat(end_date.replace("Z", "+00:00")) if end_date else datetime.now(timezone.utc)
            days_running = (end - start).days
        except Exception:
            pass

    snapshot = ad.get("snapshot") or {}
    body     = snapshot.get("body") or {}
    ad_copy  = body.get("text", "") if isinstance(body, dict) else str(body)
    title    = snapshot.get("title", "") or ad.get("adTitle", "")
    cta      = snapshot.get("cta_type", "") or snapshot.get("ctaType", "")
    link_url = snapshot.get("link_url", "") or snapshot.get("linkUrl", "")

    creative_type = "unknown"
    if snapshot.get("videos"):   creative_type = "video"
    elif snapshot.get("images"): creative_type = "image"
    elif snapshot.get("cards"):  creative_type = "carousel"

    return {
        "scraped_at":          datetime.now(timezone.utc).isoformat(),
        "target_page":         page_config["page_name"],
        "page_priority":       page_config.get("priority", ""),
        "ad_id":               ad.get("adArchiveID") or ad.get("id", ""),
        "page_id":             ad.get("pageID", ""),
        "page_name_scraped":   ad.get("pageName", ""),
        "ad_copy":             ad_copy,
        "headline":            title,
        "cta":                 cta,
        "landing_page":        link_url,
        "creative_type":       creative_type,
        "start_date":          start_date,
        "end_date":            end_date,
        "days_running":        days_running,
        "is_active":           ad.get("isActive", ""),
        "platforms":           "|".join(ad.get("publisherPlatform", [])),
        "num_variations":      ad.get("multipleVersionsAdsCount", 1),
        "spend_lower":         ad.get("spendLowerBound", ""),
        "spend_upper":         ad.get("spendUpperBound", ""),
        "impressions_lower":   ad.get("impressionsLowerBound", ""),
        "impressions_upper":   ad.get("impressionsUpperBound", ""),
        "raw_json":            json.dumps(ad),
    }


def write_csv(rows: list, path: Path):
    if not rows:
        print("No rows to write.")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV saved: {path}  ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--page",     help="Scrape only this page name")
    parser.add_argument("--priority", help="Filter by priority string (e.g. primary, competitor)")
    args = parser.parse_args()

    config       = load_config()
    target_pages = config.get("meta_ads", {}).get("target_pages", [])

    if not target_pages:
        print("ERROR: No target_pages in topic_config.json meta_ads section")
        sys.exit(1)

    if args.page:
        target_pages = [p for p in target_pages if p["page_name"].lower() == args.page.lower()]
        if not target_pages:
            print(f"No page named '{args.page}' in config")
            sys.exit(1)

    if args.priority:
        target_pages = [p for p in target_pages if args.priority.lower() in p.get("priority","").lower()]
        if not target_pages:
            print(f"No pages with priority '{args.priority}'")
            sys.exit(1)

    print("=" * 60)
    print("Fabrika — Meta Ads Scraper")
    print("=" * 60)
    print(f"Pages to scrape: {len(target_pages)}")
    for p in target_pages:
        print(f"  • {p['page_name']}  [{p.get('priority','')}]")

    client   = ApifyClient(APIFY_API_TOKEN)
    all_raw  = []
    all_rows = []

    for i, page_config in enumerate(target_pages, 1):
        print(f"\n[{i}/{len(target_pages)}]")
        ads = scrape_page(client, page_config)
        for ad in ads:
            ad["_page_config"] = page_config
            all_raw.append(ad)
        all_rows.extend([flatten_ad(ad, page_config) for ad in ads])
        if i < len(target_pages):
            time.sleep(2)

    # Save raw JSON
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_raw, f, indent=2, ensure_ascii=False)
    print(f"\nRaw JSON: {RAW_JSON_PATH}  ({len(all_raw)} ads)")

    # Save CSV
    write_csv(all_rows, CSV_PATH)

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY BY PAGE")
    print("=" * 60)
    by_page = {}
    for row in all_rows:
        p = row["target_page"]
        by_page[p] = by_page.get(p, 0) + 1
    for page, count in sorted(by_page.items(), key=lambda x: -x[1]):
        print(f"  {page:30s} {count} ads")
    print(f"\nTotal: {len(all_rows)} ads across {len(by_page)} pages")
    print(f"\nNext: py lab/stage1/processing/run_pipeline.py")


if __name__ == "__main__":
    main()
