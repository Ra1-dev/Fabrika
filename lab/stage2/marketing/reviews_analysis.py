"""
Fabrika — reviews_analysis.py
LLM extraction on Google Play + iOS app reviews.
Extracts: consideration set, verbatim pain language, verbatim benefit language,
audience segment, content category.

Reads:  lab/stage2/marketing/output/app_reviews.csv
        lab/topic_config.json (for custom_prompt_context)
Writes: lab/stage2/marketing/output/reviews_insights.csv
        lab/stage2/marketing/output/consideration_set_reviews.csv

Usage:
  py lab/stage2/marketing/reviews_analysis.py
  py lab/stage2/marketing/reviews_analysis.py --limit 100   # test run
  py lab/stage2/marketing/reviews_analysis.py --min-rating 1 --max-rating 2  # 1-2 star only
  py lab/stage2/marketing/reviews_analysis.py --from-row 200  # resume after interruption
"""

import json
import os
import sys
import csv
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).resolve().parents[2]
ENV_PATH     = ROOT / "stage1" / "processing" / ".env"
CONFIG_PATH  = ROOT / "topic_config.json"
INPUT_PATH   = Path(__file__).resolve().parent / "output" / "app_reviews.csv"
OUTPUT_DIR   = Path(__file__).resolve().parent / "output"
OUTPUT_PATH  = OUTPUT_DIR / "reviews_insights.csv"
CONSID_PATH  = OUTPUT_DIR / "consideration_set_reviews.csv"

load_dotenv(ENV_PATH)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set")
    sys.exit(1)

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic not installed. Run: pip install anthropic")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed.")
    sys.exit(1)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_prompt(review_text: str, rating: int, app_name: str, config: dict) -> str:
    context = config["llm_classification"]["custom_prompt_context"]
    competitors = config["nlp"]["competitors"]
    spelling_variants = config["nlp"].get("spelling_variants", [])
    pain_keywords = config["nlp"]["keywords_of_interest"]

    return f"""You are analyzing an app store review for competitive intelligence on AI learning products.

CONTEXT:
{context}

PRODUCT BEING REVIEWED: {app_name}
STAR RATING: {rating}/5
REVIEW TEXT:
\"\"\"{review_text}\"\"\"

KNOWN COMPETITORS TO WATCH FOR: {', '.join(competitors)}
SPELLING VARIANTS OF PRIMARY PRODUCT: {', '.join(spelling_variants)}
PAIN KEYWORDS TO WATCH FOR: {', '.join(pain_keywords[:15])}

Extract the following. Return ONLY valid JSON, no preamble, no markdown:

{{
  "consideration_set": [
    {{
      "product_mentioned": "exact name as written",
      "sentiment": "recommended | complained_about | compared | dismissed | neutral",
      "verbatim_quote": "exact phrase mentioning the product"
    }}
  ],
  "pain_quotes": [
    "exact verbatim quote expressing pain, frustration, or unmet need"
  ],
  "benefit_quotes": [
    "exact verbatim quote expressing a positive outcome or desired result"
  ],
  "audience_segment": "busy_knowledge_worker_28_50 | career_switcher_45_plus | aspiring_entrepreneur_sidehustler | remote_worker_freelancer | absolute_beginner_curious_explorer | unclear",
  "content_category": "frustration_time_constraints | technical_barrier_complaint | beginner_question | success_story | platform_comparison | career_transition_anxiety | course_recommendation | abandonment_regret | consideration_set_signal | general_feedback",
  "tension_tag": "professional_upskiller | side_hustler | unclear",
  "summary": "one sentence summary of what this review tells us about user needs"
}}

Rules:
- Only extract verbatim quotes — never paraphrase
- If nothing relevant found for a field, return empty array [] or "unclear"
- consideration_set should only include learning products/platforms, not general tools
- If review is in a non-English language, still extract what you can and note language in summary"""


def extract_insights(client, review_text: str, rating: int, app_name: str, config: dict) -> dict:
    prompt = build_prompt(review_text, rating, app_name, config)
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse error: {e}", "raw_response": message.content[0].text[:200]}
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",      type=int, default=None, help="Process only N reviews (test mode)")
    parser.add_argument("--min-rating", type=int, default=1,    help="Min star rating to process (default: 1)")
    parser.add_argument("--max-rating", type=int, default=5,    help="Max star rating to process (default: 5)")
    parser.add_argument("--from-row",   type=int, default=0,    help="Resume from row N (0-indexed)")
    args = parser.parse_args()

    config = load_config()
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Load reviews
    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded {len(df)} reviews from {INPUT_PATH}")

    # Filter by rating
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df = df[df['rating'].between(args.min_rating, args.max_rating)].copy()
    print(f"After rating filter ({args.min_rating}-{args.max_rating}★): {len(df)} reviews")

    # Skip empty reviews
    df = df[df['review_text'].notna() & (df['review_text'].str.len() > 20)].copy()
    print(f"After removing empty reviews: {len(df)} reviews")

    # Apply limit
    if args.limit:
        df = df.head(args.limit)
        print(f"Limited to {args.limit} reviews (test mode)")

    # Resume support
    if args.from_row > 0:
        df = df.iloc[args.from_row:].copy()
        print(f"Resuming from row {args.from_row}")

    print(f"\nProcessing {len(df)} reviews with Claude API...")
    print(f"Estimated cost: ~${len(df) * 0.001:.2f}")
    print(f"Estimated time: ~{len(df) * 2 // 60} minutes\n")

    # Process
    insights_rows = []
    consideration_rows = []
    errors = 0
    token_count = 0

    # ── Checkpoint: load existing progress ───────────────────────────────────
    CHECKPOINT_PATH = OUTPUT_DIR / "reviews_insights_checkpoint.csv"
    already_done = set()
    if CHECKPOINT_PATH.exists() and args.from_row == 0:
        try:
            existing = pd.read_csv(CHECKPOINT_PATH)
            already_done = set(existing["row_index"].tolist())
            insights_rows = existing.to_dict("records")
            print(f"Resumed from checkpoint: {len(insights_rows)} rows already processed")
        except Exception as e:
            print(f"Could not load checkpoint: {e}")

    def save_checkpoint():
        if not insights_rows:
            return
        fieldnames = list(insights_rows[0].keys())
        with open(CHECKPOINT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(insights_rows)

    for i, (_, row) in enumerate(df.iterrows()):
        review_text = str(row['review_text'])
        rating      = int(row['rating']) if pd.notna(row['rating']) else 0
        app_name    = str(row.get('app_name', 'Coursiv'))
        platform    = str(row.get('platform', ''))
        review_date = str(row.get('review_date', ''))
        row_index   = args.from_row + i

        if row_index in already_done:
            continue

        if i % 50 == 0:
            print(f"  [{i}/{len(df)}] Processing... ({errors} errors so far)")
            save_checkpoint()

        result = extract_insights(client, review_text, rating, app_name, config)

        if "error" in result:
            errors += 1
            insights_rows.append({
                "row_index":        args.from_row + i,
                "platform":         platform,
                "app_name":         app_name,
                "rating":           rating,
                "review_date":      review_date,
                "review_text":      review_text[:300],
                "audience_segment": "error",
                "content_category": "error",
                "tension_tag":      "error",
                "pain_quotes":      "",
                "benefit_quotes":   "",
                "consideration_set": "",
                "summary":          result.get("error", ""),
            })
            continue

        # Flatten for CSV
        pain_quotes    = " | ".join(result.get("pain_quotes", []))
        benefit_quotes = " | ".join(result.get("benefit_quotes", []))
        consid_json    = json.dumps(result.get("consideration_set", []))

        insights_rows.append({
            "row_index":         args.from_row + i,
            "platform":          platform,
            "app_name":          app_name,
            "rating":            rating,
            "review_date":       review_date,
            "review_text":       review_text[:300],
            "audience_segment":  result.get("audience_segment", ""),
            "content_category":  result.get("content_category", ""),
            "tension_tag":       result.get("tension_tag", ""),
            "pain_quotes":       pain_quotes,
            "benefit_quotes":    benefit_quotes,
            "consideration_set": consid_json,
            "summary":           result.get("summary", ""),
        })

        # Extract consideration set entries
        for entry in result.get("consideration_set", []):
            if entry.get("product_mentioned"):
                consideration_rows.append({
                    "source":           f"app_review_{platform}",
                    "product_mentioned": entry["product_mentioned"],
                    "sentiment":         entry.get("sentiment", ""),
                    "verbatim_quote":    entry.get("verbatim_quote", ""),
                    "app_name":          app_name,
                    "rating":            rating,
                    "review_date":       review_date,
                })

        # Rate limiting — be gentle with API
        time.sleep(0.5)

    # Save insights
    if insights_rows:
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(insights_rows[0].keys()))
            writer.writeheader()
            writer.writerows(insights_rows)
        print(f"\nInsights saved: {OUTPUT_PATH} ({len(insights_rows)} rows)")

    # Save consideration set
    if consideration_rows:
        with open(CONSID_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(consideration_rows[0].keys()))
            writer.writeheader()
            writer.writerows(consideration_rows)
        print(f"Consideration set saved: {CONSID_PATH} ({len(consideration_rows)} entries)")

    # Summary
    print(f"\n{'='*60}")
    print(f"REVIEWS ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"Processed: {len(insights_rows)} reviews")
    print(f"Errors: {errors}")

    if insights_rows:
        from collections import Counter
        cats = Counter(r['content_category'] for r in insights_rows if r['content_category'] not in ('error',''))
        segs = Counter(r['audience_segment'] for r in insights_rows if r['audience_segment'] not in ('error','unclear',''))

        print(f"\nContent categories:")
        for cat, count in cats.most_common():
            print(f"  {cat:40s} {count}")

        print(f"\nAudience segments:")
        for seg, count in segs.most_common():
            print(f"  {seg:40s} {count}")

        print(f"\nConsideration set entries: {len(consideration_rows)}")
        if consideration_rows:
            prod_counts = Counter(r['product_mentioned'] for r in consideration_rows)
            print(f"Products mentioned:")
            for prod, count in prod_counts.most_common(10):
                print(f"  {prod:30s} {count}")

    print(f"\nNext: py lab/stage2/marketing/ads_analysis.py")


if __name__ == "__main__":
    main()
