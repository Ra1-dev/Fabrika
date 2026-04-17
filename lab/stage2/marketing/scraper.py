"""
scraper.py  — VGM Stage 2 Marketing Intelligence
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Uses Apify actors to reliably scrape:
  - TikTok Creative Center top ads (by keyword)
  - Meta Ads Library (by keyword)

No Playwright, no blocking, runs on Apify's residential IPs.

Setup:
    pip install apify-client
    set APIFY_TOKEN=apify_api_YOUR_TOKEN_HERE

Usage:
    py lab/stage2/marketing/scraper.py
    py lab/stage2/marketing/scraper.py --keywords "study abroad" "college admissions"
    py lab/stage2/marketing/scraper.py --platform tiktok
    py lab/stage2/marketing/scraper.py --platform meta

Output:
    lab/stage2/marketing/output/raw/
        tiktok_raw.json
        meta_raw.json
        scrape_log.json
"""

import os, json, time, argparse
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RAW_DIR    = SCRIPT_DIR / "output" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
TODAY       = datetime.now().strftime("%Y-%m-%d")

# ── Apify actor IDs ────────────────────────────────────────────────────────
# These are the most reliable actors for each platform
TIKTOK_ACTOR = "codebyte/tiktok-creative-center-top-ads"   # TikTok Creative Center
META_ACTOR   = "apify/facebook-ads-scraper"                # Meta Ads Library

# ── Default keywords ───────────────────────────────────────────────────────
COMPETITOR_KEYWORDS = [
    "Leverage Edu",
    "CollegeVine",
    "Crimson Education",
    "Edvoy",
    "ApplyBoard",
]

PAIN_POINT_KEYWORDS = [
    "study abroad international student",
    "college admissions help",
    "college counselor affordable",
    "US college application",
    "how to apply US university",
    "study abroad scholarship",
    "college admissions AI",
]

ALL_KEYWORDS = COMPETITOR_KEYWORDS + PAIN_POINT_KEYWORDS


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _days_running(start: str, end: str = "") -> int:
    try:
        from datetime import datetime as dt
        s = dt.strptime(start[:10], "%Y-%m-%d")
        e = dt.strptime(end[:10], "%Y-%m-%d") if end else dt.now()
        return max(0, (e - s).days)
    except Exception:
        return 0


def _performance_score(ad: dict) -> int:
    """Performance proxy when exact dates aren't available."""
    days = int(ad.get("days_running", 0) or 0)
    if days > 0:
        return days
    reach = int(ad.get("reach", 0) or 0)
    if reach > 0:
        return max(1, reach // 15000)
    ctr   = float(ad.get("ctr", 0) or 0)
    if ctr > 0:
        return max(1, int(ctr * 20))
    likes = int(ad.get("likes", 0) or 0)
    if likes > 0:
        return max(1, likes // 500)
    return 1


def _normalise_tiktok(raw: dict, keyword: str) -> dict:
    start = raw.get("firstShownDate", raw.get("first_shown_date",
            raw.get("startDate", raw.get("start_date", ""))))
    end   = raw.get("lastShownDate",  raw.get("last_shown_date",
            raw.get("endDate",   raw.get("end_date",   ""))))
    days  = _days_running(start, end)

    ad = {
        "platform":      "tiktok",
        "keyword":       keyword,
        "scraped_at":    TODAY,
        "advertiser":    raw.get("brandName",    raw.get("brand_name",
                         raw.get("advertiserName", raw.get("advertiser_name", "")))),
        "headline":      raw.get("adTitle",      raw.get("ad_title",
                         raw.get("title",        raw.get("videoTitle", "")))),
        "body":          raw.get("adText",       raw.get("ad_text",
                         raw.get("description",  raw.get("caption", "")))),
        "cta":           raw.get("ctaText",      raw.get("cta_text",
                         raw.get("cta",          ""))),
        "industry":      raw.get("industryKey",  raw.get("industry_key",
                         raw.get("industry",     ""))),
        "objective":     raw.get("objectiveKey", raw.get("objective_key", "")),
        "country":       raw.get("countryCode",  raw.get("country_code",
                         raw.get("country",      ""))),
        "reach":         int(raw.get("reach",       raw.get("impressionCount",
                         raw.get("impression_count", 0))) or 0),
        "ctr":           float(raw.get("ctr", 0) or 0),
        "likes":         int(raw.get("likeCount",   raw.get("like_count", 0)) or 0),
        "start_date":    start,
        "end_date":      end,
        "days_running":  days,
        "video_url":     raw.get("videoUrl",    raw.get("video_url", "")),
        "thumbnail_url": raw.get("coverUrl",    raw.get("cover_url", "")),
    }
    if ad["days_running"] == 0:
        ad["days_running"] = _performance_score(ad)
    return ad


def _normalise_meta(raw: dict, keyword: str) -> dict:
    bodies = raw.get("adCreativeBodies", raw.get("ad_creative_bodies", [])) or []
    links  = raw.get("adCreativeLinkTitles",
             raw.get("ad_creative_link_titles", [])) or []
    start  = raw.get("adDeliveryStartTime",
             raw.get("ad_delivery_start_time", ""))
    end    = raw.get("adDeliveryStopTime",
             raw.get("ad_delivery_stop_time", ""))
    days   = _days_running(start, end)

    spend  = raw.get("spend", {}) or {}
    impr   = raw.get("impressions", {}) or {}

    ad = {
        "platform":        "meta",
        "keyword":         keyword,
        "scraped_at":      TODAY,
        "advertiser":      raw.get("pageName",   raw.get("page_name",
                           raw.get("advertiserName", ""))),
        "page_id":         raw.get("pageId",     raw.get("page_id", "")),
        "headline":        links[0]  if links  else "",
        "body":            bodies[0] if bodies else "",
        "cta":             raw.get("ctaType",    raw.get("cta_type",
                           raw.get("cta_text",   ""))),
        "spend_low":       spend.get("lowerBound", spend.get("lower_bound", "")),
        "spend_high":      spend.get("upperBound", spend.get("upper_bound", "")),
        "impressions_low": impr.get("lowerBound",  impr.get("lower_bound", "")),
        "impressions_high": impr.get("upperBound", impr.get("upper_bound", "")),
        "platforms":       ", ".join(
                               raw.get("publisherPlatforms",
                               raw.get("publisher_platforms", [])) or []
                           ),
        "start_date":      start,
        "end_date":        end,
        "days_running":    days if days > 0 else 1,
        "ad_id":           raw.get("id", ""),
    }
    return ad


# ══════════════════════════════════════════════════════════════════════════
# TIKTOK via Apify
# ══════════════════════════════════════════════════════════════════════════

def scrape_tiktok_apify(keywords: list[str]) -> dict:
    """
    Use Apify's TikTok Creative Center actor.
    Runs on residential IPs, no auth required.
    Actor: codebyte/tiktok-creative-center-top-ads
    """
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("  pip install apify-client")
        return {}

    client  = ApifyClient(APIFY_TOKEN)
    results = {}

    print("\n" + "─"*50)
    print("  TIKTOK — Apify Creative Center")
    print("─"*50)

    for kw in keywords:
        print(f"\n  Keyword: '{kw}'...")
        try:
            run = client.actor(TIKTOK_ACTOR).call(
                run_input={
                    "keywords":   [kw],
                    "period":     "180",        # last 180 days
                    "orderBy":    "reach",       # highest reach = most successful
                    "maxItems":   50,
                    "industry":   "Education",
                    "country":    "ALL",
                },
                timeout_secs=120,
            )

            ads = []
            dataset_id = run.get("defaultDatasetId")
            if dataset_id:
                for item in client.dataset(dataset_id).iterate_items():
                    ads.append(_normalise_tiktok(item, kw))

            results[kw] = ads
            print(f"  → {len(ads)} ads")

        except Exception as e:
            print(f"  → Error: {e}")
            results[kw] = []

        time.sleep(2)

    total = sum(len(v) for v in results.values())
    print(f"\n  TikTok total: {total} ads across {len(keywords)} keywords")
    return results


# ══════════════════════════════════════════════════════════════════════════
# META via Apify
# ══════════════════════════════════════════════════════════════════════════

def scrape_meta_apify(keywords: list[str]) -> dict:
    """
    Use Apify's Facebook Ads Scraper actor.
    Runs on residential IPs, handles Meta's WAF.
    Actor: apify/facebook-ads-scraper
    """
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("  pip install apify-client")
        return {}

    client  = ApifyClient(APIFY_TOKEN)
    results = {}

    print("\n" + "─"*50)
    print("  META — Apify Ads Library")
    print("─"*50)

    for kw in keywords:
        print(f"\n  Keyword: '{kw}'...")
        try:
            run = client.actor(META_ACTOR).call(
                run_input={
                    "searchTerms":   [kw],
                    "adType":        "ALL",
                    "activeStatus":  "ALL",
                    "country":       "ALL",
                    "maxItems":      50,
                    "sortBy":        "impressions",
                },
                timeout_secs=120,
            )

            ads = []
            dataset_id = run.get("defaultDatasetId")
            if dataset_id:
                for item in client.dataset(dataset_id).iterate_items():
                    ads.append(_normalise_meta(item, kw))

            results[kw] = ads
            print(f"  → {len(ads)} ads")

        except Exception as e:
            print(f"  → Error: {e}")
            # Try fallback actor if primary fails
            results[kw] = _meta_fallback(client, kw)

        time.sleep(3)

    total = sum(len(v) for v in results.values())
    print(f"\n  Meta total: {total} ads across {len(keywords)} keywords")
    return results


def _meta_fallback(client, keyword: str) -> list[dict]:
    """Try alternative Meta actor if primary fails."""
    FALLBACK_ACTORS = [
        "pocesar/facebook-ads-library-scraper",
        "curious_coder/facebook-ads-library-scraper",
    ]
    for actor_id in FALLBACK_ACTORS:
        try:
            print(f"  Trying fallback: {actor_id}...")
            run = client.actor(actor_id).call(
                run_input={
                    "query":     keyword,
                    "country":   "ALL",
                    "maxItems":  30,
                },
                timeout_secs=90,
            )
            ads = []
            dataset_id = run.get("defaultDatasetId")
            if dataset_id:
                for item in client.dataset(dataset_id).iterate_items():
                    ads.append(_normalise_meta(item, keyword))
            if ads:
                print(f"  → {len(ads)} ads via fallback")
                return ads
        except Exception as e:
            print(f"  → Fallback error: {e}")
    return []


# ══════════════════════════════════════════════════════════════════════════
# SAVE
# ══════════════════════════════════════════════════════════════════════════

def save(tiktok: dict, meta: dict, keywords: list[str]):
    with open(RAW_DIR / "tiktok_raw.json", "w", encoding="utf-8") as f:
        json.dump(tiktok, f, indent=2, ensure_ascii=False)

    with open(RAW_DIR / "meta_raw.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    tt_total   = sum(len(v) for v in tiktok.values())
    meta_total = sum(len(v) for v in meta.values())

    log = {
        "scraped_at":  TODAY,
        "method":      "apify",
        "keywords":    keywords,
        "tiktok":      {"total": tt_total, "per_keyword": {k: len(v) for k,v in tiktok.items()}},
        "meta":        {"total": meta_total, "per_keyword": {k: len(v) for k,v in meta.items()}},
    }
    with open(RAW_DIR / "scrape_log.json", "w") as f:
        json.dump(log, f, indent=2)

    print(f"\n  Saved:")
    print(f"    {RAW_DIR / 'tiktok_raw.json'}  ({tt_total} ads)")
    print(f"    {RAW_DIR / 'meta_raw.json'}    ({meta_total} ads)")
    print(f"    {RAW_DIR / 'scrape_log.json'}")
    return log


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Ad Scraper (Apify) — VGM Stage 2")
    parser.add_argument("--keywords",    nargs="+", default=None)
    parser.add_argument("--platform",   choices=["tiktok","meta","both"], default="both")
    parser.add_argument("--keyword-set",choices=["competitors","pain_points","all"],
                        default="all")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not set.")
        print("  1. Go to apify.com → Settings → Integrations → copy Personal API token")
        print("  2. set APIFY_TOKEN=apify_api_YOUR_TOKEN")
        return

    keywords = args.keywords or (
        COMPETITOR_KEYWORDS  if args.keyword_set == "competitors" else
        PAIN_POINT_KEYWORDS  if args.keyword_set == "pain_points" else
        ALL_KEYWORDS
    )

    print("="*50)
    print("  AD SCRAPER — VGM Stage 2 (Apify)")
    print(f"  Platform: {args.platform}")
    print(f"  Keywords: {len(keywords)}")
    print(f"  Token:    {APIFY_TOKEN[:20]}...")
    print("="*50)

    tiktok, meta = {}, {}

    if args.platform in ("tiktok", "both"):
        tiktok = scrape_tiktok_apify(keywords)

    if args.platform in ("meta", "both"):
        meta = scrape_meta_apify(keywords)

    log = save(tiktok, meta, keywords)

    print("\n" + "="*50)
    print("  DONE")
    print(f"  TikTok: {log['tiktok']['total']} ads")
    print(f"  Meta:   {log['meta']['total']} ads")
    print("\n  Next:")
    print("  py lab/stage2/marketing/analyser.py --budget 1000")
    print("="*50)


if __name__ == "__main__":
    main()
