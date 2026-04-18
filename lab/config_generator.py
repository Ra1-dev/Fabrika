"""
Fabrika config_generator.py — v2
Generates topic_config.json for ALL scrapers:
  - Reddit
  - YouTube
  - Google Play reviews
  - Apple App Store reviews
  - Meta Ads Library (via Apify)

Seeded from search_seeds.json (Phase 1 output) so every query
traces back to the product's own positioning vocabulary.

Usage:
  py lab/config_generator.py --seeds lab/search_seeds.json --preview
  py lab/config_generator.py --seeds lab/search_seeds.json
"""

import anthropic
import json
import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load env
load_dotenv(Path(__file__).parent / "stage1" / "processing" / ".env")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set in lab/stage1/processing/.env")
    sys.exit(1)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── Hardcoded IDs confirmed from Phase 1 research ──────────────────────────
COURSIV_IDS = {
    "google_play_id": "io.zimran.coursiv",
    "ios_app_id_main": "6478281150",       # Coursiv – AI Tools Mastery (Zimran Limited)
    "ios_app_id_certificates": "6758934906",  # Coursiv - AI Certificates (Coursiv Limited)
    "ios_app_id_junior": "6749324183",     # Coursiv Junior — SKIP (different product)
    "trustpilot_domain": "coursiv.io",
}

# ── System prompt ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a marketing data analyst building scraper configs for competitive intelligence.
You will be given Phase 1 research seeds derived from a product's own positioning vocabulary.
Your job is to expand these seeds into comprehensive scraper configs for five platforms.

Rules:
- Every query you generate must be grounded in the seed vocabulary provided
- Do not invent generic category queries — stay close to the product's actual language
- For Reddit: mix subreddit-level and cross-Reddit search queries
- For YouTube: focus on queries that surface real user journeys, reviews, and comparisons
- For Meta Ads: provide page names of likely advertisers (exact Facebook page names)
- For app reviews: focus on what signals to extract, not just scraping params
- Output ONLY valid JSON, no markdown, no preamble, no explanation
"""

def build_prompt(seeds: dict) -> str:
    return f"""Here are Phase 1 research seeds derived from Coursiv's own positioning vocabulary:

PAIN QUERIES (what users feel):
{json.dumps(seeds.get('pain_search_queries', []), indent=2)}

CONSIDERATION QUERIES (comparison intent):
{json.dumps(seeds.get('consideration_queries', []), indent=2)}

BENEFIT QUERIES (outcome desire):
{json.dumps(seeds.get('benefit_queries', []), indent=2)}

PRIORITIZED SUBREDDITS WITH REASONING:
{json.dumps(seeds.get('subreddits_prioritized', []), indent=2)}

YOUTUBE/TIKTOK TERMS:
{json.dumps(seeds.get('tiktok_youtube_search_terms', []), indent=2)}

NOTES FOR RESEARCH:
{json.dumps(seeds.get('notes_for_phase_2_executor', []), indent=2)}

Using these seeds, generate a complete topic_config.json with ALL of the following sections.
Each section must be grounded in the vocabulary above — do not add generic queries not implied by the seeds.

Generate this exact JSON structure:

{{
  "topic": "AI learning app for non-technical busy professionals",
  "description": "...(1-2 sentence research focus derived from seeds)...",
  
  "reddit": {{
    "subreddits": ["list of 12-15 subreddit names WITHOUT r/ prefix"],
    "search_queries": ["list of 14-16 queries using seed vocabulary"],
    "max_items": 500
  }},
  
  "youtube": {{
    "search_queries": ["list of 14-16 queries using seed vocabulary and youtube terms"],
    "max_results_per_query": 10
  }},
  
  "google_play": {{
    "apps": [
      {{
        "app_id": "io.zimran.coursiv",
        "name": "Coursiv",
        "priority": "primary",
        "scrape_reviews": true,
        "country": "us",
        "lang": "en",
        "sort": "most_relevant",
        "count": 1000,
        "filter_score_with": null,
        "notes": "Pull all ratings. Tag 1-star and 5-star extremes for pain/benefit extraction."
      }}
    ],
    "extract_signals": [
      "competitor mentions in review text",
      "cancellation reasons",
      "feature requests",
      "comparison language (better than, worse than, switched from)",
      "verbatim pain vocabulary",
      "verbatim benefit vocabulary"
    ]
  }},
  
  "apple_app_store": {{
    "apps": [
      {{
        "app_id": "6478281150",
        "name": "Coursiv - AI Tools Mastery",
        "priority": "primary",
        "country": "us",
        "sort": "mostRecent",
        "count": 500,
        "notes": "Original app, most reviews. Main source."
      }},
      {{
        "app_id": "6758934906",
        "name": "Coursiv - AI Certificates",
        "priority": "secondary",
        "country": "us",
        "sort": "mostRecent",
        "count": 200,
        "notes": "Newer rebranded app. Pull for recent user language."
      }}
    ],
    "extract_signals": [
      "competitor mentions in review text",
      "cancellation reasons",
      "feature requests",
      "comparison language",
      "verbatim pain vocabulary",
      "verbatim benefit vocabulary"
    ]
  }},
  
  "meta_ads": {{
    "target_pages": [
      {{
        "page_name": "Coursiv",
        "search_term": "Coursiv",
        "priority": "primary — pull ALL ads for Phase 1A",
        "country": "US",
        "active_status": "all",
        "notes": "Our own product. Pull every ad to understand what they are testing."
      }},
      {{
        "page_name": "Brilliant",
        "search_term": "Brilliant.org",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Direct competitor — interactive learning app"
      }},
      {{
        "page_name": "Codecademy",
        "search_term": "Codecademy",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Competitor — coding and AI skills"
      }},
      {{
        "page_name": "DataCamp",
        "search_term": "DataCamp",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Competitor — data and AI skills"
      }},
      {{
        "page_name": "Skillshare",
        "search_term": "Skillshare",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Adjacent competitor — creative and professional skills"
      }},
      {{
        "page_name": "LinkedIn Learning",
        "search_term": "LinkedIn Learning",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Adjacent competitor — professional upskilling"
      }},
      {{
        "page_name": "DeepLearning.AI",
        "search_term": "DeepLearning.AI",
        "priority": "competitor",
        "country": "US",
        "active_status": "all",
        "notes": "Competitor — AI-specific courses"
      }},
      {{
        "page_name": "Udemy",
        "search_term": "Udemy",
        "priority": "adjacent",
        "country": "US",
        "active_status": "active",
        "notes": "Large adjacent competitor — implied in Coursiv positioning"
      }}
    ],
    "extract_signals": [
      "hook (first line of copy or first 3 seconds)",
      "format (UGC / talking head / screen recording / static / carousel / animation)",
      "offer structure (free trial / discount / guarantee / social proof)",
      "pain point addressed",
      "LF8 emotional driver",
      "CTA text",
      "landing page URL",
      "duration active (days running)",
      "number of active variations (scaling signal)"
    ],
    "performance_filter": {{
      "min_days_running": 14,
      "notes": "Ads running 14+ days are likely profitable. Sort by duration descending."
    }}
  }},
  
  "nlp": {{
    "competitors": ["list of 12-15 competitor names to detect in scraped text"],
    "keywords_of_interest": ["list of 15-20 keywords derived from seed pain/benefit vocabulary"],
    "spelling_variants": ["Coursiv", "Coursive", "Cursiv"]
  }},
  
  "llm_classification": {{
    "content_categories": [
      "frustration_time_constraints",
      "technical_barrier_complaint",
      "beginner_question",
      "success_story",
      "platform_comparison",
      "career_transition_anxiety",
      "course_recommendation",
      "abandonment_regret",
      "consideration_set_signal"
    ],
    "audiences": [
      "busy_knowledge_worker_28_50",
      "career_switcher_45_plus",
      "aspiring_entrepreneur_sidehustler",
      "remote_worker_freelancer",
      "absolute_beginner_curious_explorer"
    ],
    "custom_prompt_context": "..."
  }}
}}

Fill in all values. The custom_prompt_context should instruct the LLM classifier to focus on extracting:
consideration set signals (products mentioned by name), verbatim pain language matching the seeds, and which of the 5 audience segments the author belongs to."""


def generate_config(seeds_path: str) -> dict:
    # Load seeds
    with open(seeds_path, "r", encoding="utf-8") as f:
        seeds = json.load(f)

    print(f"Loaded seeds from: {seeds_path}")
    print(f"Calling Claude API to generate complete scraper config...")

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": build_prompt(seeds)}
        ]
    )

    raw = message.content[0].text.strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    config = json.loads(raw)

    # Inject confirmed IDs (override anything Claude generated)
    config["_confirmed_ids"] = COURSIV_IDS
    config["_derivation"] = {
        "seeds_file": seeds_path,
        "generated_by": "config_generator.py v2",
        "note": "All queries derived from Phase 1 search_seeds.json. App IDs hardcoded from Phase 1 web research."
    }

    return config


def main():
    parser = argparse.ArgumentParser(description="Generate Fabrika scraper config from Phase 1 seeds")
    parser.add_argument("--seeds", default="lab/search_seeds.json", help="Path to search_seeds.json")
    parser.add_argument("--output", default="lab/topic_config.json", help="Output path for config")
    parser.add_argument("--preview", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    # Resolve seeds path
    seeds_path = args.seeds
    if not os.path.exists(seeds_path):
        # Try relative to script location
        seeds_path = Path(__file__).parent.parent / args.seeds
        if not os.path.exists(seeds_path):
            print(f"ERROR: Seeds file not found at {args.seeds}")
            print("Make sure search_seeds.json is in the lab/ folder")
            sys.exit(1)

    config = generate_config(str(seeds_path))

    print("\n" + "="*60)
    print("GENERATED CONFIG SUMMARY")
    print("="*60)
    print(f"Topic: {config.get('topic')}")
    print(f"Reddit subreddits: {len(config.get('reddit', {}).get('subreddits', []))}")
    print(f"Reddit queries: {len(config.get('reddit', {}).get('search_queries', []))}")
    print(f"YouTube queries: {len(config.get('youtube', {}).get('search_queries', []))}")
    print(f"Google Play apps: {len(config.get('google_play', {}).get('apps', []))}")
    print(f"iOS App Store apps: {len(config.get('apple_app_store', {}).get('apps', []))}")
    print(f"Meta Ads pages: {len(config.get('meta_ads', {}).get('target_pages', []))}")
    print(f"NLP competitors tracked: {len(config.get('nlp', {}).get('competitors', []))}")
    print(f"NLP keywords: {len(config.get('nlp', {}).get('keywords_of_interest', []))}")

    if args.preview:
        print("\n[PREVIEW MODE — not saving]")
        print("\nFull config:")
        print(json.dumps(config, indent=2, ensure_ascii=False))
    else:
        output_path = args.output
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print(f"\nSaved to: {output_path}")
        print("Next step: review the config, then run scrapers in order:")
        print("  1. py lab/stage1/scrapers/reddit/reddit_scraper.py")
        print("  2. py lab/stage1/scrapers/youtube/youtube_scraper_v2.py")
        print("  3. py lab/stage2/marketing/coursiv_meta_ads.py   (Apify)")
        print("  4. py lab/stage2/marketing/app_reviews_scraper.py")


if __name__ == "__main__":
    main()
