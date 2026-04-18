"""
Fabrika — ads_analysis.py
LLM extraction on high-signal Coursiv Meta Ads (Tier 1 active + Tier 2 proven).
Extracts: hook, format, offer structure, pain point, LF8 driver, CTA, why it works.

Reads:  lab/stage2/marketing/output/coursiv_ads_high_signal.csv
        lab/topic_config.json
Writes: lab/stage2/marketing/output/ads_insights.csv
        lab/stage2/marketing/output/hooks_library.csv
        lab/stage2/marketing/output/patterns_summary.md

Usage:
  py lab/stage2/marketing/ads_analysis.py
  py lab/stage2/marketing/ads_analysis.py --limit 20   # test run
  py lab/stage2/marketing/ads_analysis.py --tier TIER_1_ACTIVE  # active only
  py lab/stage2/marketing/ads_analysis.py --from-row 100  # resume
"""

import json
import os
import sys
import csv
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv
from collections import Counter

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[2]
ENV_PATH    = ROOT / "stage1" / "processing" / ".env"
CONFIG_PATH = ROOT / "topic_config.json"
INPUT_PATH  = Path(__file__).resolve().parent / "output" / "coursiv_ads_high_signal.csv"
OUTPUT_DIR  = Path(__file__).resolve().parent / "output"
OUTPUT_PATH = OUTPUT_DIR / "ads_insights.csv"
HOOKS_PATH  = OUTPUT_DIR / "hooks_library.csv"
PATTERNS_PATH = OUTPUT_DIR / "patterns_summary.md"

load_dotenv(ENV_PATH)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set")
    sys.exit(1)

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic not installed.")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed.")
    sys.exit(1)

# ── LF8 Cashvertising drivers ─────────────────────────────────────────────────
LF8_DRIVERS = [
    "survival_enjoyment_life_extension",
    "freedom_from_fear_pain_danger",
    "to_be_superior_winning_keeping_up",
    "social_approval",
    "care_protection_of_loved_ones",
    "comfortable_living_conditions",
    "enjoyment_of_food_beverages",
    "sexual_companionship",
]


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_prompt(ad_copy: str, headline: str, cta: str, creative_type: str,
                 days_running: float, tier: str, config: dict) -> str:

    audiences = config["llm_classification"]["audiences"]
    pain_keywords = config["nlp"]["keywords_of_interest"]

    return f"""You are a direct-response creative strategist analyzing a Meta ad for an AI learning app.

AD DATA:
- Creative type: {creative_type}
- Tier: {tier} (TIER_1_ACTIVE = currently live, TIER_2_PROVEN = ran 30+ days)
- Days running: {days_running} days
- Headline: {headline}
- CTA button: {cta}
- Ad copy:
\"\"\"{ad_copy}\"\"\"

TARGET AUDIENCE SEGMENTS FOR THIS PRODUCT:
{json.dumps(audiences, indent=2)}

KNOWN PAIN KEYWORDS: {', '.join(pain_keywords[:12])}

LF8 CASHVERTISING DRIVERS (pick the ONE most dominant):
{json.dumps(LF8_DRIVERS, indent=2)}

Analyze this ad and return ONLY valid JSON, no preamble, no markdown:

{{
  "hook": "The exact first line or opening statement that grabs attention (verbatim from copy, or describe the visual hook if video)",
  "hook_type": "question | bold_claim | stat | story | demo | fear | social_proof | curiosity | before_after | direct_offer",
  "format_type": "ugc_talking_head | screen_recording | animation | static_image | carousel | meme | split_screen | testimonial | text_overlay_video",
  "offer_structure": "free_trial | discount | certificate | challenge_28day | social_proof | no_coding_required | career_outcome | time_saving | other",
  "pain_point_addressed": "exact pain point from the ad — use the user's language not marketing language",
  "lf8_driver": "one value from the LF8 list above",
  "target_segment": "one value from the audience segments above",
  "visual_style": "polished | raw_ugc | animated | text_heavy | minimal | screenshot",
  "why_this_works": "one sentence — what psychological trigger makes this ad effective",
  "what_makes_it_weak": "one sentence — what could be improved or what risk this creative carries",
  "scalability_signal": "high | medium | low — based on days running and creative type",
  "gap_opportunity": "one sentence — what angle this ad leaves unexploited that a competitor could use"
}}"""


def extract_ad_insights(client, row: dict, config: dict) -> dict:
    ad_copy      = str(row.get('ad_copy', ''))
    headline     = str(row.get('headline', ''))
    cta          = str(row.get('cta', ''))
    creative_type = str(row.get('creative_type', 'unknown'))
    days_running = float(row.get('days_running_real', 0) or 0)
    tier         = str(row.get('tier', ''))

    if len(ad_copy.strip()) < 5 and len(headline.strip()) < 5:
        return {"error": "insufficient ad text"}

    prompt = build_prompt(ad_copy, headline, cta, creative_type, days_running, tier, config)

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse: {e}"}
    except Exception as e:
        return {"error": str(e)}


def write_patterns_summary(insights_rows: list, output_path: Path):
    """Generate a markdown summary of patterns across all analyzed ads."""

    active = [r for r in insights_rows if r.get('tier') == 'TIER_1_ACTIVE']
    proven = [r for r in insights_rows if r.get('tier') == 'TIER_2_PROVEN']
    valid  = [r for r in insights_rows if not r.get('error')]

    def top_n(field, rows, n=5):
        counts = Counter(r.get(field, '') for r in rows if r.get(field) and r.get(field) != 'error')
        return counts.most_common(n)

    lines = [
        "# Coursiv Ad Pattern Analysis",
        f"Generated: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"Total ads analyzed: {len(valid)} ({len(active)} active, {len(proven)} proven 30+ days)",
        "",
        "---",
        "",
        "## Hook Types",
        *[f"- **{k}**: {v} ads" for k, v in top_n('hook_type', valid)],
        "",
        "## Format Types",
        *[f"- **{k}**: {v} ads" for k, v in top_n('format_type', valid)],
        "",
        "## Offer Structures",
        *[f"- **{k}**: {v} ads" for k, v in top_n('offer_structure', valid)],
        "",
        "## LF8 Drivers",
        *[f"- **{k}**: {v} ads" for k, v in top_n('lf8_driver', valid)],
        "",
        "## Target Segments",
        *[f"- **{k}**: {v} ads" for k, v in top_n('target_segment', valid)],
        "",
        "## Visual Styles",
        *[f"- **{k}**: {v} ads" for k, v in top_n('visual_style', valid)],
        "",
        "---",
        "",
        "## Top Hooks (Active Ads)",
        *[f"- {r.get('hook', '')[:100]}" for r in sorted(active, key=lambda x: float(x.get('days_running_real', 0) or 0), reverse=True)[:10] if r.get('hook')],
        "",
        "## Top Hooks (Proven 30+ Day Ads)",
        *[f"- [{r.get('days_running_real', '?')}d] {r.get('hook', '')[:100]}" for r in sorted(proven, key=lambda x: float(x.get('days_running_real', 0) or 0), reverse=True)[:10] if r.get('hook')],
        "",
        "---",
        "",
        "## Why These Ads Work (sample)",
        *[f"- {r.get('why_this_works', '')}" for r in valid[:15] if r.get('why_this_works')],
        "",
        "## Gap Opportunities Identified",
        *[f"- {r.get('gap_opportunity', '')}" for r in valid if r.get('gap_opportunity') and r.get('scalability_signal') == 'high'],
        "",
    ]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",    type=int,    default=None)
    parser.add_argument("--tier",     type=str,    default=None, help="TIER_1_ACTIVE or TIER_2_PROVEN")
    parser.add_argument("--from-row", type=int,    default=0)
    args = parser.parse_args()

    config = load_config()
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded {len(df)} high-signal ads")
    print(f"Tier distribution: {df['tier'].value_counts().to_dict()}")

    if args.tier:
        df = df[df['tier'] == args.tier].copy()
        print(f"Filtered to {args.tier}: {len(df)} ads")

    if args.from_row > 0:
        df = df.iloc[args.from_row:].copy()
        print(f"Resuming from row {args.from_row}")

    if args.limit:
        df = df.head(args.limit)
        print(f"Limited to {args.limit} ads (test mode)")

    # Deduplicate by ad_copy — many rows have same copy, different creatives
    # Keep the one with most days running
    df['days_running_real'] = pd.to_numeric(df['days_running_real'], errors='coerce').fillna(0)
    df_deduped = df.sort_values('days_running_real', ascending=False).drop_duplicates(
        subset=['ad_copy'], keep='first'
    )
    print(f"After deduplication by copy: {len(df_deduped)} unique ads (was {len(df)})")

    print(f"\nProcessing {len(df_deduped)} ads with Claude API...")
    print(f"Estimated cost: ~${len(df_deduped) * 0.001:.2f}")
    print(f"Estimated time: ~{max(1, len(df_deduped) * 2 // 60)} minutes\n")

    insights_rows = []
    hooks_rows    = []
    errors        = 0

    for i, (_, row) in enumerate(df_deduped.iterrows()):
        if i % 20 == 0:
            print(f"  [{i}/{len(df_deduped)}] Processing... ({errors} errors)")

        result = extract_ad_insights(client, row.to_dict(), config)

        flat_row = {
            "row_index":         args.from_row + i,
            "tier":              row.get('tier', ''),
            "days_running_real": row.get('days_running_real', ''),
            "page_name_real":    row.get('page_name_real', ''),
            "creative_type":     row.get('creative_type', ''),
            "ad_copy":           str(row.get('ad_copy', ''))[:200],
            "headline":          str(row.get('headline', '')),
            "cta":               str(row.get('cta', '')),
            "ad_library_url":    str(row.get('ad_library_url', '')),
            "start_date_real":   str(row.get('start_date_real', '')),
        }

        if "error" in result:
            errors += 1
            flat_row.update({k: "error" for k in [
                "hook", "hook_type", "format_type", "offer_structure",
                "pain_point_addressed", "lf8_driver", "target_segment",
                "visual_style", "why_this_works", "what_makes_it_weak",
                "scalability_signal", "gap_opportunity"
            ]})
        else:
            flat_row.update({
                "hook":                 result.get("hook", ""),
                "hook_type":            result.get("hook_type", ""),
                "format_type":          result.get("format_type", ""),
                "offer_structure":      result.get("offer_structure", ""),
                "pain_point_addressed": result.get("pain_point_addressed", ""),
                "lf8_driver":           result.get("lf8_driver", ""),
                "target_segment":       result.get("target_segment", ""),
                "visual_style":         result.get("visual_style", ""),
                "why_this_works":       result.get("why_this_works", ""),
                "what_makes_it_weak":   result.get("what_makes_it_weak", ""),
                "scalability_signal":   result.get("scalability_signal", ""),
                "gap_opportunity":      result.get("gap_opportunity", ""),
            })

            if result.get("hook"):
                hooks_rows.append({
                    "hook":          result["hook"],
                    "hook_type":     result.get("hook_type", ""),
                    "days_running":  row.get('days_running_real', 0),
                    "tier":          row.get('tier', ''),
                    "creative_type": row.get('creative_type', ''),
                    "lf8_driver":    result.get("lf8_driver", ""),
                    "why_it_works":  result.get("why_this_works", ""),
                    "ad_url":        str(row.get('ad_library_url', '')),
                })

        insights_rows.append(flat_row)
        time.sleep(0.5)

    # Save insights CSV
    if insights_rows:
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(insights_rows[0].keys()))
            writer.writeheader()
            writer.writerows(insights_rows)
        print(f"\nAd insights saved: {OUTPUT_PATH} ({len(insights_rows)} rows)")

    # Save hooks library
    if hooks_rows:
        hooks_sorted = sorted(hooks_rows, key=lambda x: float(x.get('days_running', 0) or 0), reverse=True)
        with open(HOOKS_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(hooks_rows[0].keys()))
            writer.writeheader()
            writer.writerows(hooks_sorted)
        print(f"Hooks library saved: {HOOKS_PATH} ({len(hooks_rows)} hooks)")

    # Write patterns summary markdown
    write_patterns_summary(insights_rows, PATTERNS_PATH)
    print(f"Patterns summary saved: {PATTERNS_PATH}")

    # Final summary
    print(f"\n{'='*60}")
    print(f"ADS ANALYSIS COMPLETE")
    print(f"{'='*60}")
    valid = [r for r in insights_rows if r.get('hook_type') != 'error']
    print(f"Analyzed: {len(valid)} unique ads ({errors} errors)")

    if valid:
        print(f"\nHook types:")
        for k, v in Counter(r['hook_type'] for r in valid if r.get('hook_type')).most_common():
            print(f"  {k:30s} {v}")
        print(f"\nLF8 drivers:")
        for k, v in Counter(r['lf8_driver'] for r in valid if r.get('lf8_driver')).most_common():
            print(f"  {k:50s} {v}")
        print(f"\nGap opportunities flagged as HIGH scalability:")
        high = [r for r in valid if r.get('scalability_signal') == 'high' and r.get('gap_opportunity')]
        for r in high[:5]:
            print(f"  - {r['gap_opportunity'][:100]}")

    print(f"\nNext: review {PATTERNS_PATH} then run strategy synthesis")


if __name__ == "__main__":
    main()
