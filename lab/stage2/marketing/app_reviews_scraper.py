"""
Fabrika — app_reviews_scraper.py
Scrapes Google Play and Apple App Store reviews for all apps
defined in topic_config.json.

Reads:  lab/topic_config.json
Writes: lab/stage2/marketing/output/raw/reviews_raw.json
        lab/stage2/marketing/output/app_reviews.csv

Usage:
    py lab/stage2/marketing/app_reviews_scraper.py
    py lab/stage2/marketing/app_reviews_scraper.py --platform google
    py lab/stage2/marketing/app_reviews_scraper.py --platform apple
"""

import json
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[2] / "stage1" / "processing" / ".env"
load_dotenv(ENV_PATH)

ROOT          = Path(__file__).resolve().parents[2]
CONFIG_PATH   = ROOT / "topic_config.json"
OUTPUT_DIR    = Path(__file__).resolve().parent / "output" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_JSON_PATH = OUTPUT_DIR / "reviews_raw.json"
CSV_PATH      = Path(__file__).resolve().parent / "output" / "app_reviews.csv"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"ERROR: topic_config.json not found at {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def scrape_google_play(app_config: dict) -> list:
    try:
        from google_play_scraper import reviews, Sort
    except ImportError:
        print("ERROR: google-play-scraper not installed. Run: pip install google-play-scraper")
        return []

    app_id  = app_config["app_id"]
    name    = app_config["name"]
    count   = app_config.get("count", 1000)
    country = app_config.get("country", "us")
    lang    = app_config.get("lang", "en")
    sort_map = {
        "most_relevant": Sort.MOST_RELEVANT,
        "newest":        Sort.NEWEST,
        "rating":        Sort.RATING,
    }
    sort = sort_map.get(app_config.get("sort", "most_relevant"), Sort.MOST_RELEVANT)

    print(f"\n  Google Play: {name} ({app_id})")
    all_reviews = []
    continuation_token = None
    batch = 200

    while len(all_reviews) < count:
        remaining = min(batch, count - len(all_reviews))
        try:
            result, continuation_token = reviews(
                app_id, lang=lang, country=country, sort=sort,
                count=remaining, continuation_token=continuation_token,
                filter_score_with=app_config.get("filter_score_with"),
            )
        except Exception as e:
            print(f"  ERROR: {e}")
            break
        if not result:
            break
        all_reviews.extend(result)
        print(f"  → {len(all_reviews)} reviews so far...")
        if not continuation_token:
            break

    print(f"  Done: {len(all_reviews)} reviews")
    return all_reviews


def flatten_gplay(review: dict, app_config: dict) -> dict:
    return {
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "platform":     "google_play",
        "app_id":       app_config["app_id"],
        "app_name":     app_config["name"],
        "app_priority": app_config.get("priority", ""),
        "review_id":    review.get("reviewId", ""),
        "author":       review.get("userName", ""),
        "rating":       review.get("score", ""),
        "review_text":  review.get("content", ""),
        "thumbs_up":    review.get("thumbsUpCount", 0),
        "review_date":  str(review.get("at", "")),
        "app_version":  review.get("appVersion", ""),
        "reply_text":   review.get("replyContent", ""),
        "reply_date":   str(review.get("repliedAt", "")),
    }


def scrape_apple(app_config: dict) -> list:
    try:
        from app_store_scraper import AppStore
    except ImportError:
        print("ERROR: app-store-scraper not installed. Run: pip install app-store-scraper")
        return []

    app_id  = app_config["app_id"]
    name    = app_config["name"]
    count   = app_config.get("count", 500)
    country = app_config.get("country", "us")

    print(f"\n  App Store: {name} ({app_id})")
    try:
        app = AppStore(country=country, app_name=name, app_id=int(app_id))
        app.review(how_many=count)
        result = app.reviews
        print(f"  Done: {len(result)} reviews")
        return result
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


def flatten_apple(review: dict, app_config: dict) -> dict:
    return {
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "platform":     "apple_app_store",
        "app_id":       app_config["app_id"],
        "app_name":     app_config["name"],
        "app_priority": app_config.get("priority", ""),
        "review_id":    str(review.get("reviewId", "")),
        "author":       review.get("userName", ""),
        "rating":       review.get("rating", ""),
        "review_text":  review.get("review", ""),
        "title":        review.get("title", ""),
        "thumbs_up":    "",
        "review_date":  str(review.get("date", "")),
        "app_version":  review.get("appVersion", ""),
        "reply_text":   "",
        "reply_date":   "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=["google", "apple", "both"], default="both")
    args = parser.parse_args()

    config = load_config()
    print("=" * 60)
    print("Fabrika — App Reviews Scraper")
    print("=" * 60)

    all_raw  = []
    all_rows = []

    if args.platform in ("google", "both"):
        apps = config.get("google_play", {}).get("apps", [])
        print(f"\nGoogle Play: {len(apps)} app(s)")
        for app_config in apps:
            raw = scrape_google_play(app_config)
            all_raw.extend([{**r, "_platform": "google_play"} for r in raw])
            all_rows.extend([flatten_gplay(r, app_config) for r in raw])

    if args.platform in ("apple", "both"):
        apps = config.get("apple_app_store", {}).get("apps", [])
        print(f"\nApple App Store: {len(apps)} app(s)")
        for app_config in apps:
            raw = scrape_apple(app_config)
            all_raw.extend([{**r, "_platform": "apple_app_store"} for r in raw])
            all_rows.extend([flatten_apple(r, app_config) for r in raw])

    # Save raw JSON
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_raw, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nRaw JSON: {RAW_JSON_PATH}  ({len(all_raw)} reviews)")

    # Save CSV
    if all_rows:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"CSV: {CSV_PATH}  ({len(all_rows)} rows)")
    else:
        print("No reviews scraped.")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    by_app = {}
    for row in all_rows:
        key = f"{row['platform']} / {row['app_name']}"
        by_app[key] = by_app.get(key, 0) + 1
    for app, count in sorted(by_app.items(), key=lambda x: -x[1]):
        print(f"  {app:50s}  {count}")

    if all_rows:
        from collections import Counter
        ratings = [int(r["rating"]) for r in all_rows if r["rating"]]
        if ratings:
            dist = Counter(ratings)
            print("\nRating distribution:")
            for star in sorted(dist.keys()):
                print(f"  {star}★  {dist[star]:5d}  {'█' * (dist[star] // 20)}")

    print(f"\nTotal: {len(all_rows)} reviews")
    print(f"Next: py lab/stage1/processing/run_pipeline.py")


if __name__ == "__main__":
    main()
