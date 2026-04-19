"""
Fabrika — fabrika_generator.py

Reads all collected data, auto-builds two creative matrices (static + video),
scores every combination, and outputs top 50 ranked creative briefs for each.

Inputs (reads automatically):
  lab/stage2/marketing/output/ads_insights.csv
  lab/stage2/marketing/output/meta_keyword_ads.csv
  lab/stage2/marketing/output/reviews_insights_checkpoint.csv
  lab/stage1/output/clean/reddit_enriched.csv
  active_coursive_analysis.csv  (place in project root or lab/stage2/marketing/output/)

Outputs:
  lab/stage2/marketing/output/static_creatives_top50.csv
  lab/stage2/marketing/output/video_creatives_top50.csv
  lab/stage2/marketing/output/fabrika_report.html

Usage:
  py fabrika_generator.py
  py fabrika_generator.py --no-llm    # skip LLM enrichment, use template copy only
"""

import os
import sys
import csv
import json
import time
import argparse
import itertools
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

try:
    import pandas as pd
except ImportError:
    print("ERROR: pip install pandas"); sys.exit(1)

from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).resolve().parent
ENV_PATH     = ROOT / "lab" / "stage1" / "processing" / ".env"
OUTPUT_DIR   = ROOT / "lab" / "stage2" / "marketing" / "output"
REDDIT_PATH  = ROOT / "lab" / "stage1" / "output" / "clean" / "reddit_enriched.csv"

ADS_INSIGHTS_PATH   = OUTPUT_DIR / "ads_insights.csv"
KW_ADS_PATH         = OUTPUT_DIR / "meta_keyword_ads.csv"
REVIEWS_PATH        = OUTPUT_DIR / "reviews_insights_checkpoint.csv"
ACTIVE_ADS_PATH     = OUTPUT_DIR / "active_coursive_analysis.csv"
ACTIVE_ADS_ROOT     = ROOT / "active_coursive_analysis.csv"

load_dotenv(ENV_PATH)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — DATA LOADING + PARSING
# ══════════════════════════════════════════════════════════════════════════════

def load_all_data() -> dict:
    data = {}

    # Ads insights (proven Coursiv hooks)
    if ADS_INSIGHTS_PATH.exists():
        df = pd.read_csv(ADS_INSIGHTS_PATH)
        df["days_running_real"] = pd.to_numeric(df["days_running_real"], errors="coerce").fillna(0)
        data["ads_insights"] = df
        print(f"  ads_insights: {len(df)} rows")
    else:
        data["ads_insights"] = pd.DataFrame()
        print("  ads_insights: NOT FOUND")

    # Active ads with transcriptions (new video + static data)
    active_path = ACTIVE_ADS_PATH if ACTIVE_ADS_PATH.exists() else ACTIVE_ADS_ROOT
    if active_path.exists():
        df = pd.read_csv(active_path)
        df["days_running_real"] = pd.to_numeric(df["days_running_real"], errors="coerce").fillna(0)
        data["active_ads"] = df
        print(f"  active_ads: {len(df)} rows")
    else:
        data["active_ads"] = pd.DataFrame()
        print("  active_ads: NOT FOUND")

    # Competitor keyword ads
    if KW_ADS_PATH.exists():
        df = pd.read_csv(KW_ADS_PATH)
        df["days_running"] = pd.to_numeric(df["days_running"], errors="coerce").fillna(0)
        data["competitor_ads"] = df
        print(f"  competitor_ads: {len(df)} rows")
    else:
        data["competitor_ads"] = pd.DataFrame()
        print("  competitor_ads: NOT FOUND")

    # Reddit enriched
    if REDDIT_PATH.exists():
        df = pd.read_csv(REDDIT_PATH)
        data["reddit"] = df
        print(f"  reddit: {len(df)} rows")
    else:
        data["reddit"] = pd.DataFrame()
        print("  reddit: NOT FOUND")

    # Reviews
    if REVIEWS_PATH.exists():
        df = pd.read_csv(REVIEWS_PATH)
        data["reviews"] = df
        print(f"  reviews: {len(df)} rows")
    else:
        data["reviews"] = pd.DataFrame()
        print("  reviews: NOT FOUND")

    return data


def extract_axes(data: dict) -> dict:
    """Auto-extract axis values from data. Returns axes dict for static and video."""

    axes = {"static": {}, "video": {}}

    # ── DIRECTIONS (shared) ───────────────────────────────────────────────────
    # Derived from Reddit pain categories + LF8 gap analysis + competitor gaps
    directions = [
        {
            "id": "D1",
            "name": "Fear of Being Left Behind",
            "description": "You are falling behind colleagues/peers who are already using AI",
            "lf8": "to_be_superior_winning_keeping_up",
            "reddit_signal": "career_transition_anxiety",
            "reddit_count": 293,
            "competitor_owned": True,  # Tony Robbins + Udemy own this
            "coursiv_proven": True,
            "audience": "busy_knowledge_worker_28_50",
            "pain_quote": "I feel like everyone around me is using AI and I'm getting left behind",
        },
        {
            "id": "D2",
            "name": "No Time to Learn",
            "description": "I want to learn AI but I have no time — 15 min/day is the answer",
            "lf8": "to_be_superior_winning_keeping_up",
            "reddit_signal": "frustration_time_constraints",
            "reddit_count": 377,
            "competitor_owned": False,
            "coursiv_proven": True,
            "audience": "busy_knowledge_worker_28_50",
            "pain_quote": "I want to learn but I genuinely have no time between work and family",
        },
        {
            "id": "D3",
            "name": "Technical Barrier",
            "description": "I'm not technical — AI feels like it's not for me",
            "lf8": "freedom_from_fear_pain_danger",
            "reddit_signal": "technical_barrier_complaint",
            "reddit_count": 166,
            "competitor_owned": False,
            "coursiv_proven": True,  # 181-day no-code winner
            "audience": "absolute_beginner_curious_explorer",
            "pain_quote": "I'm not a programmer and every AI tutorial assumes you know how to code",
        },
        {
            "id": "D4",
            "name": "Career Outcome",
            "description": "AI skills = better job, promotion, more money",
            "lf8": "comfortable_living_conditions",
            "reddit_signal": "career_transition_anxiety",
            "reddit_count": 293,
            "competitor_owned": False,
            "coursiv_proven": False,  # only lightly tested
            "audience": "career_switcher_45_plus",
            "pain_quote": "I need to upskill to stay relevant and get the promotion I deserve",
        },
        {
            "id": "D5",
            "name": "Wealth Window",
            "description": "AI will create millionaires — get in now before the window closes",
            "lf8": "comfortable_living_conditions",
            "reddit_signal": "consideration_set_signal",
            "reddit_count": 211,
            "competitor_owned": False,
            "coursiv_proven": True,  # 68-day runner
            "audience": "aspiring_entrepreneur_sidehustler",
            "pain_quote": "AI feels like the internet in 1996 — I don't want to miss this wave",
        },
        {
            "id": "D6",
            "name": "Occupation-Specific AI Gap",
            "description": "Accountants/marketers/managers are still doing X manually — there's a smarter way",
            "lf8": "to_be_superior_winning_keeping_up",
            "reddit_signal": "frustration_time_constraints",
            "reddit_count": 377,
            "competitor_owned": False,  # nobody is doing this at scale
            "coursiv_proven": False,  # just started testing (accountant ads)
            "audience": "busy_knowledge_worker_28_50",
            "pain_quote": "I spend hours every week on tasks that AI could probably do in minutes",
        },
        {
            "id": "D7",
            "name": "Identity Transformation",
            "description": "Become the most dangerous/valuable person in your office",
            "lf8": "social_approval",
            "reddit_signal": "success_story",
            "reddit_count": 280,
            "competitor_owned": False,
            "coursiv_proven": False,  # just started testing
            "audience": "busy_knowledge_worker_28_50",
            "pain_quote": "I want to be the person my team comes to for AI advice, not the one asking",
        },
    ]

    # ── STATIC AXES ───────────────────────────────────────────────────────────

    static_hooks = [
        {"id": "SH1", "hook": "FINAL CALL — Grab your AI Certificate for $20 — This offer dies tonight",
         "type": "urgency_deadline", "proven_days": 181, "source": "coursiv_proven"},
        {"id": "SH2", "hook": "LAST CHANCE — -50% Discount! Get AI certificate in 28 days or watch 2026 leave you behind",
         "type": "urgency_discount", "proven_days": 90, "source": "coursiv_proven"},
        {"id": "SH3", "hook": "KEEP IT PRIVATE UNTIL IT'S DONE — AI will create more millionaires in 5 years than the internet did in 20",
         "type": "secret_wealth", "proven_days": 0, "source": "coursiv_active_test"},
        {"id": "SH4", "hook": "DON'T SAY WE DIDN'T WARN YOU — AI Certificate $20 — One Night Only",
         "type": "warning_threat", "proven_days": 0, "source": "coursiv_active_test"},
        {"id": "SH5", "hook": "BECOME THE MOST DANGEROUS PERSON IN YOUR OFFICE",
         "type": "identity_transformation", "proven_days": 0, "source": "coursiv_active_test"},
        {"id": "SH6", "hook": "If you are an [Occupation], we want you on AI Certificate Program",
         "type": "occupation_invitation", "proven_days": 0, "source": "coursiv_active_test"},
        {"id": "SH7", "hook": "If you're still doing everything manually, you're already behind",
         "type": "competitor_validated", "proven_days": 120, "source": "competitor_udemy"},
        {"id": "SH8", "hook": "OOPS — 50% Discount. Please, become a master of AI especially if you're over 40",
         "type": "apology_discount", "proven_days": 0, "source": "coursiv_active_test"},
    ]

    static_visual_themes = [
        {"id": "VT1", "theme": "Neon Dark", "description": "Black background, neon pink/red text, cyberpunk aesthetic", "source": "coursiv_active"},
        {"id": "VT2", "theme": "Dragon/Asian Street", "description": "Dark red city night scene with dragon motifs", "source": "coursiv_active"},
        {"id": "VT3", "theme": "Nature/Floral", "description": "Spring flowers, butterflies, pastel tones — unexpected contrast with AI content", "source": "coursiv_active"},
        {"id": "VT4", "theme": "Notebook/Handwritten", "description": "Lined notebook paper, handwritten-style font — personal and intimate", "source": "coursiv_active"},
        {"id": "VT5", "theme": "Tool Icons Grid", "description": "ChatGPT/Claude/Canva app icons in a grid showing the 28-day curriculum", "source": "coursiv_proven"},
        {"id": "VT6", "theme": "Bold Minimal Text", "description": "Clean white/light background, large bold typography only", "source": "coursiv_active"},
        {"id": "VT7", "theme": "Retro Warning", "description": "Vintage newspaper/wanted poster aesthetic with urgent warning copy", "source": "coursiv_active"},
        {"id": "VT8", "theme": "African/Geometric Vibrant", "description": "Black + red/gold geometric patterns, high contrast bold", "source": "coursiv_active"},
    ]

    static_offers = [
        {"id": "SO1", "offer": "$20 AI Certificate — tonight only", "urgency": "extreme", "price_anchor": "$20", "source": "coursiv_proven"},
        {"id": "SO2", "offer": "$20 AI Certificate — 50% discount, limited time", "urgency": "high", "price_anchor": "$20", "source": "coursiv_proven"},
        {"id": "SO3", "offer": "28-Day AI Challenge — 15 min/day — no coding required", "urgency": "medium", "price_anchor": None, "source": "coursiv_proven"},
        {"id": "SO4", "offer": "Free intro lesson + $20 to get certified", "urgency": "low", "price_anchor": "free", "source": "gap_competitor_cisco_uses_free"},
    ]

    # ── VIDEO AXES ────────────────────────────────────────────────────────────

    video_hooks = [
        {"id": "VH1", "hook": "Why is nobody talking about what this woman just said?",
         "type": "viral_reaction", "proven_days": 12, "source": "coursiv_active"},
        {"id": "VH2", "hook": "I'm begging you, become a master of AI — especially if you're over 40",
         "type": "direct_age_plea", "proven_days": 10, "source": "coursiv_active"},
        {"id": "VH3", "hook": "Most [occupations] over 40 are still doing [X] manually. Here's the smarter way.",
         "type": "occupation_problem", "proven_days": 11, "source": "coursiv_active"},
        {"id": "VH4", "hook": "This is how I became the most dangerous person in my office",
         "type": "identity_transformation", "proven_days": 10, "source": "coursiv_active"},
        {"id": "VH5", "hook": "Third attempt. My wife doesn't know I'm here again. This time is different.",
         "type": "relatable_failure", "proven_days": 4, "source": "coursiv_active_new"},
        {"id": "VH6", "hook": "Stop telling ChatGPT to make it better. Bad prompt, bad result.",
         "type": "actionable_tip", "proven_days": 11, "source": "coursiv_active"},
        {"id": "VH7", "hook": "I'm begging you — if retirement is on your mind, don't ignore AI",
         "type": "retirement_anxiety", "proven_days": 11, "source": "coursiv_active"},
        {"id": "VH8", "hook": "You here for the analyst position too? [job interview scenario]",
         "type": "scenario_open", "proven_days": 4, "source": "coursiv_active_new"},
        {"id": "VH9", "hook": "If you're still doing everything manually, you're already behind",
         "type": "competitor_validated", "proven_days": 120, "source": "competitor_udemy"},
        {"id": "VH10", "hook": "I'm out, guys. Have a great weekend. [colleague leaves early because AI finished the work]",
         "type": "aspirational_outcome", "proven_days": 4, "source": "coursiv_active_new"},
        {"id": "VH11", "hook": "Honey, I have to bring work home this weekend again. [family tension narrative]",
         "type": "relatable_pain_scene", "proven_days": 4, "source": "coursiv_active_new"},
        {"id": "VH12", "hook": "This is the quickest way to go from AI beginner to AI fluent. It's 1996 and the internet just came out.",
         "type": "historical_analogy", "proven_days": 11, "source": "coursiv_active"},
    ]

    video_plots = [
        {"id": "VP1", "plot": "Twitter/X Reaction", "description": "Open on viral tweet screenshot, presenter reacts and explains — borrows external credibility", "source": "coursiv_active"},
        {"id": "VP2", "plot": "Two-Person Dialogue / Podcast", "description": "Authority conversation format — two people debate or explain, high perceived credibility", "source": "coursiv_active"},
        {"id": "VP3", "plot": "Direct-to-Camera Monologue", "description": "One person speaks directly to viewer — personal, urgent, emotional", "source": "coursiv_active"},
        {"id": "VP4", "plot": "Cartoon / Animated Scenario", "description": "Character goes through relatable work/life situation — job interview, promotion, family conflict", "source": "coursiv_active_new"},
        {"id": "VP5", "plot": "Before/After Job Scenario", "description": "Two candidates — one uses AI, one doesn't — AI user wins the job/promotion", "source": "coursiv_active_new"},
        {"id": "VP6", "plot": "Actionable Tip Demo", "description": "Here's a specific mistake + here's the fix — positions Coursiv as expert solution", "source": "coursiv_active"},
        {"id": "VP7", "plot": "TED-Style Presenter", "description": "Single speaker on stage or clean background — authoritative, educational, aspirational", "source": "coursiv_active"},
    ]

    video_talent_formats = [
        {"id": "TF1", "format": "Real UGC — talking head, casual setting", "description": "Authentic person speaking directly, phone/home background", "source": "coursiv_active"},
        {"id": "TF2", "format": "Polished presenter with template overlay", "description": "Clean speaker + Coursiv's recognizable AI tool icon template", "source": "coursiv_proven"},
        {"id": "TF3", "format": "AI-generated cartoon characters", "description": "Animated office/interview scenario, no real humans needed", "source": "coursiv_active_new"},
        {"id": "TF4", "format": "ASMR / silent open + text", "description": "Hook is visual/text, no speech for first 2-3 seconds, then cuts to content", "source": "coursiv_active"},
    ]

    axes["shared"] = {"directions": directions}
    axes["static"] = {
        "hooks": static_hooks,
        "visual_themes": static_visual_themes,
        "offers": static_offers,
    }
    axes["video"] = {
        "hooks": video_hooks,
        "plots": video_plots,
        "talent_formats": video_talent_formats,
    }

    return axes


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — SCORING
# ══════════════════════════════════════════════════════════════════════════════

def score_combination(direction: dict, hook: dict, axis3: dict, axis4: dict,
                       matrix_type: str, data: dict) -> dict:
    """
    Score a creative combination 0-100 across three equal dimensions:
    1. Proven signal (30-day runner or competitor validated = higher score)
    2. Gap opportunity (angle not owned by competitor + not over-tested by Coursiv)
    3. Audience match (Reddit pain volume for this direction)
    """

    # ── 1. Proven Signal Score (0-33) ────────────────────────────────────────
    hook_days = hook.get("proven_days", 0)
    if hook_days >= 90:
        proven_score = 33
    elif hook_days >= 30:
        proven_score = 25
    elif hook_days >= 10:
        proven_score = 15
    elif hook.get("source", "").startswith("competitor") and hook_days >= 60:
        proven_score = 20  # competitor-validated hook
    elif hook.get("source", "").startswith("competitor"):
        proven_score = 10
    else:
        proven_score = 5  # new/untested

    # ── 2. Gap Opportunity Score (0-33) ──────────────────────────────────────
    gap_score = 0
    # Not competitor-owned direction = higher opportunity
    if not direction.get("competitor_owned", False):
        gap_score += 15
    else:
        gap_score += 5

    # Not already heavily tested by Coursiv = fresher territory
    if not direction.get("coursiv_proven", False):
        gap_score += 12
    else:
        gap_score += 6

    # Hook source bonus for genuinely new angles
    source = hook.get("source", "")
    if "new" in source:
        gap_score += 6
    elif "active_test" in source:
        gap_score += 4

    gap_score = min(gap_score, 33)

    # ── 3. Audience Match Score (0-34) ───────────────────────────────────────
    reddit_count = direction.get("reddit_count", 0)
    if reddit_count >= 350:
        audience_score = 34
    elif reddit_count >= 250:
        audience_score = 28
    elif reddit_count >= 150:
        audience_score = 20
    elif reddit_count >= 50:
        audience_score = 12
    else:
        audience_score = 6

    total_score = proven_score + gap_score + audience_score

    return {
        "total_score": total_score,
        "proven_signal": proven_score,
        "gap_opportunity": gap_score,
        "audience_match": audience_score,
    }


def diverse_top_n(combinations: list, n: int, max_per_direction: int = 7, max_per_hook: int = 6) -> list:
    """
    Select top N combinations enforcing diversity across directions and hooks.
    Iterates through score-sorted list, skipping entries that exceed per-direction
    or per-hook caps. Ensures all 7 directions are represented.
    """
    direction_counts = {}
    hook_counts = {}
    selected = []

    for combo in combinations:
        d = combo["direction"]
        h = combo["hook_id"]
        if direction_counts.get(d, 0) >= max_per_direction:
            continue
        if hook_counts.get(h, 0) >= max_per_hook:
            continue
        selected.append(combo)
        direction_counts[d] = direction_counts.get(d, 0) + 1
        hook_counts[h] = hook_counts.get(h, 0) + 1
        if len(selected) >= n:
            break

    # If we still need more (rare), fill without constraints
    if len(selected) < n:
        ids = {id(s) for s in selected}
        for combo in combinations:
            if id(combo) not in ids:
                selected.append(combo)
                if len(selected) >= n:
                    break

    return selected


def build_matrix(matrix_type: str, axes: dict, data: dict) -> list:
    """Build all combinations for a matrix type and score them."""
    directions = axes["shared"]["directions"]
    combinations = []

    if matrix_type == "static":
        hooks       = axes["static"]["hooks"]
        themes      = axes["static"]["visual_themes"]
        offers      = axes["static"]["offers"]
        axis3_label = "visual_theme"
        axis4_label = "offer"

        for d, h, t, o in itertools.product(directions, hooks, themes, offers):
            scores = score_combination(d, h, t, o, "static", data)
            combinations.append({
                "direction_id": d["id"],
                "direction": d["name"],
                "hook_id": h["id"],
                "hook": h["hook"],
                "hook_type": h["type"],
                "hook_proven_days": h["proven_days"],
                "hook_source": h["source"],
                "visual_theme_id": t["id"],
                "visual_theme": t["theme"],
                "visual_theme_desc": t["description"],
                "offer_id": o["id"],
                "offer": o["offer"],
                "offer_urgency": o["urgency"],
                "audience_segment": d["audience"],
                "reddit_pain_volume": d["reddit_count"],
                "lf8_driver": d["lf8"],
                "pain_quote": d["pain_quote"],
                "competitor_owned_angle": d["competitor_owned"],
                **scores,
            })

    elif matrix_type == "video":
        hooks   = axes["video"]["hooks"]
        plots   = axes["video"]["plots"]
        talents = axes["video"]["talent_formats"]
        axis3_label = "plot"
        axis4_label = "talent_format"

        for d, h, p, t in itertools.product(directions, hooks, plots, talents):
            scores = score_combination(d, h, p, t, "video", data)
            combinations.append({
                "direction_id": d["id"],
                "direction": d["name"],
                "hook_id": h["id"],
                "hook": h["hook"],
                "hook_type": h["type"],
                "hook_proven_days": h["proven_days"],
                "hook_source": h["source"],
                "plot_id": p["id"],
                "plot": p["plot"],
                "plot_description": p["description"],
                "talent_format_id": t["id"],
                "talent_format": t["format"],
                "talent_format_desc": t["description"],
                "audience_segment": d["audience"],
                "reddit_pain_volume": d["reddit_count"],
                "lf8_driver": d["lf8"],
                "pain_quote": d["pain_quote"],
                "competitor_owned_angle": d["competitor_owned"],
                **scores,
            })

    # Sort by total_score descending
    combinations.sort(key=lambda x: -x["total_score"])
    return combinations


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — LLM ENRICHMENT (suggested copy + what to test + competitor context)
# ══════════════════════════════════════════════════════════════════════════════

def enrich_with_llm(creative: dict, matrix_type: str, client) -> dict:
    """Call Claude API to generate suggested copy + context for a creative brief."""

    if matrix_type == "static":
        prompt = f"""You are a direct-response creative strategist for Coursiv — an AI learning platform.
Product: 28-day AI Certificate program, $20, 15 min/day, no coding required, for professionals 28-50.

Generate a complete creative brief for this static ad combination:

DIRECTION: {creative['direction']} — {creative['pain_quote']}
HOOK (headline): {creative['hook']}
VISUAL THEME: {creative['visual_theme']} — {creative['visual_theme_desc']}
OFFER: {creative['offer']}
TARGET AUDIENCE: {creative['audience_segment']}
LF8 DRIVER: {creative['lf8_driver']}

Return ONLY valid JSON:
{{
  "headline": "exact headline text (under 8 words, punchy)",
  "subheadline": "supporting line under headline (1 sentence)",
  "body_copy": "2-3 lines of body copy using the pain quote vocabulary",
  "cta_text": "call to action button text (2-4 words)",
  "suggested_copy_full": "complete ad copy as it would appear on the creative",
  "what_to_test": "one specific A/B test hypothesis for this creative",
  "competitor_context": "one sentence on what competitor is running similar angle and how this differs",
  "risk": "one sentence on the main risk or weakness of this creative"
}}"""

    else:  # video
        prompt = f"""You are a direct-response creative strategist for Coursiv — an AI learning platform.
Product: 28-day AI Certificate program, $20, 15 min/day, no coding required, for professionals 28-50.

Generate a complete creative brief for this video ad combination:

DIRECTION: {creative['direction']} — {creative['pain_quote']}
HOOK (first 3 seconds): {creative['hook']}
PLOT STRUCTURE: {creative['plot']} — {creative['plot_description']}
TALENT FORMAT: {creative['talent_format']} — {creative['talent_format_desc']}
TARGET AUDIENCE: {creative['audience_segment']}
LF8 DRIVER: {creative['lf8_driver']}

Return ONLY valid JSON:
{{
  "hook_script": "exact first 3 seconds of spoken script",
  "scene_description": "what the viewer sees in the first 3 seconds",
  "narrative_arc": "2-3 sentences describing the full video story from hook to CTA",
  "key_moment": "the single most important scene or line in the video",
  "cta_script": "exact CTA line at end of video",
  "suggested_script_outline": "full beat-by-beat outline: [0-3s hook] [3-15s problem] [15-25s solution] [25-30s CTA]",
  "what_to_test": "one specific A/B test hypothesis for this video",
  "competitor_context": "one sentence on what competitor is running similar angle and how this differs",
  "risk": "one sentence on the main risk or weakness of this creative"
}}"""

    try:
        import anthropic
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        # Try to recover truncated JSON by finding last complete key-value pair
        raw_clean = raw.strip()
        try:
            result = json.loads(raw_clean)
        except json.JSONDecodeError:
            # Attempt recovery: truncate to last complete field
            last_comma = raw_clean.rfind('",')
            if last_comma > 0:
                raw_clean = raw_clean[:last_comma+1] + '"}'
                try:
                    result = json.loads(raw_clean)
                except:
                    result = {}
            else:
                result = {}
        creative.update(result)
    except Exception as e:
        creative["llm_error"] = str(e)
        print(f"    LLM error: {e}")

    return creative


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(rows: list, path: Path):
    if not rows:
        return
    # Collect all keys across all rows to handle LLM adding unexpected fields
    all_keys = []
    seen = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved: {path} ({len(rows)} rows)")


def generate_html_report(static_top50: list, video_top50: list, output_path: Path):
    """Generate a rich HTML report with both matrices."""

    def score_bar(score: int) -> str:
        pct = score
        color = "#10b981" if score >= 70 else "#f59e0b" if score >= 50 else "#ef4444"
        return f'<div style="background:#1e1e2e;border-radius:4px;height:6px;width:100%;margin-top:3px"><div style="background:{color};width:{pct}%;height:6px;border-radius:4px"></div></div>'

    def source_badge(source: str) -> str:
        if "proven" in source:
            return '<span style="background:#1f3a1f;color:#6ee7b7;padding:2px 6px;border-radius:3px;font-size:10px">PROVEN</span>'
        elif "competitor" in source:
            return '<span style="background:#3a1f1f;color:#fca5a5;padding:2px 6px;border-radius:3px;font-size:10px">COMP</span>'
        elif "new" in source:
            return '<span style="background:#1f1f3a;color:#a5b4fc;padding:2px 6px;border-radius:3px;font-size:10px">NEW</span>'
        else:
            return '<span style="background:#2a2a1f;color:#fde68a;padding:2px 6px;border-radius:3px;font-size:10px">TESTING</span>'

    def render_table(rows: list, matrix_type: str) -> str:
        if matrix_type == "static":
            axis3_key, axis4_key = "visual_theme", "offer"
        else:
            axis3_key, axis4_key = "plot", "talent_format"

        html = ""
        for i, r in enumerate(rows, 1):
            copy_field = r.get("suggested_copy_full") or r.get("suggested_script_outline") or "—"
            what_to_test = r.get("what_to_test", "—")
            comp_context = r.get("competitor_context", "—")
            risk = r.get("risk", "—")

            html += f"""
<div style="background:#1a1a24;border:1px solid #2a2a3a;border-radius:10px;padding:18px;margin-bottom:16px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px">
    <div>
      <span style="font-size:11px;color:#666">#{i}</span>
      <span style="font-size:13px;font-weight:700;color:#e0e0e0;margin-left:8px">{r['direction']}</span>
      {source_badge(r.get('hook_source',''))}
      {'<span style="background:#3d1f1f;color:#fca5a5;padding:2px 6px;border-radius:3px;font-size:10px;margin-left:4px">CONTESTED</span>' if r.get('competitor_owned_angle') else ''}
    </div>
    <div style="text-align:right">
      <div style="font-size:20px;font-weight:700;color:#6366f1">{r['total_score']}</div>
      <div style="font-size:10px;color:#666">/ 100</div>
    </div>
  </div>

  <div style="font-size:14px;color:#e0e0e0;font-weight:600;margin-bottom:8px;padding:8px 12px;background:#12121a;border-radius:6px;border-left:3px solid #6366f1">
    "{r['hook']}"
  </div>

  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:12px;font-size:11px">
    <div style="background:#12121a;padding:8px;border-radius:6px">
      <div style="color:#666;margin-bottom:2px">{'Visual Theme' if matrix_type=='static' else 'Plot Structure'}</div>
      <div style="color:#ccc">{r.get(axis3_key,'—')}</div>
    </div>
    <div style="background:#12121a;padding:8px;border-radius:6px">
      <div style="color:#666;margin-bottom:2px">{'Offer' if matrix_type=='static' else 'Talent Format'}</div>
      <div style="color:#ccc">{r.get(axis4_key,'—')[:50]}</div>
    </div>
    <div style="background:#12121a;padding:8px;border-radius:6px">
      <div style="color:#666;margin-bottom:2px">Audience</div>
      <div style="color:#ccc">{r.get('audience_segment','—').replace('_',' ')}</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px">
    <div><div style="font-size:10px;color:#888">Proven Signal</div><div style="font-size:13px;color:#6366f1">{r['proven_signal']}/33</div>{score_bar(r['proven_signal']*3)}</div>
    <div><div style="font-size:10px;color:#888">Gap Opportunity</div><div style="font-size:13px;color:#10b981">{r['gap_opportunity']}/33</div>{score_bar(r['gap_opportunity']*3)}</div>
    <div><div style="font-size:10px;color:#888">Audience Match</div><div style="font-size:13px;color:#f59e0b">{r['audience_match']}/34</div>{score_bar(r['audience_match']*3)}</div>
  </div>

  <div style="font-size:11px;color:#aaa;font-style:italic;margin-bottom:10px">
    Pain: "{r.get('pain_quote','')[:100]}"
  </div>

  {f'<div style="background:#0d1a0d;border-radius:6px;padding:10px;margin-bottom:8px;font-size:12px;color:#d1fae5;word-wrap:break-word;white-space:pre-wrap"><strong style="color:#6ee7b7">Suggested Copy:</strong><br>{str(copy_field)}</div>' if copy_field != "—" else ""}

  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;font-size:11px;word-wrap:break-word">
    <div style="background:#1a1505;border-radius:6px;padding:8px">
      <div style="color:#f59e0b;font-weight:700;margin-bottom:3px">A/B Test</div>
      <div style="color:#aaa">{what_to_test}</div>
    </div>
    <div style="background:#0d1520;border-radius:6px;padding:8px">
      <div style="color:#60a5fa;font-weight:700;margin-bottom:3px">Competitor Context</div>
      <div style="color:#aaa">{comp_context}</div>
    </div>
    <div style="background:#1a0a0a;border-radius:6px;padding:8px">
      <div style="color:#f87171;font-weight:700;margin-bottom:3px">Risk</div>
      <div style="color:#aaa">{risk}</div>
    </div>
  </div>
</div>"""
        return html

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Fabrika — Creative Matrix Report</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f0f13;color:#e0e0e0;padding:24px;line-height:1.5}}
  h1{{font-size:26px;font-weight:700;color:#fff;margin-bottom:4px}}
  h2{{font-size:16px;font-weight:700;color:#fff;margin:32px 0 16px;padding-bottom:8px;border-bottom:1px solid #2a2a3a}}
  .subtitle{{color:#666;font-size:14px;margin-bottom:32px}}
  .tab-bar{{display:flex;gap:8px;margin-bottom:24px}}
  .tab{{padding:8px 20px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;background:#1a1a24;color:#888;border:1px solid #2a2a3a}}
  .tab.active{{background:#6366f1;color:#fff;border-color:#6366f1}}
  .tab-content{{display:none}}.tab-content.active{{display:block}}
</style>
</head>
<body>
<h1>Fabrika — Creative Matrix Report</h1>
<p class="subtitle">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;·&nbsp; Product: Coursiv &nbsp;·&nbsp; Market: US</p>

<div class="tab-bar">
  <div class="tab active" onclick="switchTab('static')">📸 Static Creatives (Top 50)</div>
  <div class="tab" onclick="switchTab('video')">🎬 Video Creatives (Top 50)</div>
</div>

<div id="static" class="tab-content active">
  <h2>Static Creative Matrix — Top 50</h2>
  <p style="font-size:12px;color:#666;margin-bottom:20px">
    Axes: Direction × Hook × Visual Theme × Offer &nbsp;·&nbsp;
    Scored: Proven Signal (33) + Gap Opportunity (33) + Audience Match (34) = 100
  </p>
  {render_table(static_top50, "static")}
</div>

<div id="video" class="tab-content">
  <h2>Video Creative Matrix — Top 50</h2>
  <p style="font-size:12px;color:#666;margin-bottom:20px">
    Axes: Direction × Hook × Plot × Talent Format &nbsp;·&nbsp;
    Scored: Proven Signal (33) + Gap Opportunity (33) + Audience Match (34) = 100
  </p>
  {render_table(video_top50, "video")}
</div>

<script>
function switchTab(name) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.getElementById(name).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  HTML report: {output_path}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--html-only", action="store_true", help="Regenerate HTML from existing CSVs without re-scoring or LLM")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM enrichment")
    args = parser.parse_args()

    print("=" * 60)
    print("Fabrika — Creative Matrix Generator")
    print("=" * 60)


    if args.html_only:
        print("\n--html-only mode: reading existing CSVs and regenerating HTML...")
        import csv as csv_mod
        def load_csv_as_dicts(path):
            with open(path, newline="", encoding="utf-8") as f:
                return list(csv_mod.DictReader(f))
        static_top50 = load_csv_as_dicts(OUTPUT_DIR / "static_creatives_top50.csv")
        video_top50  = load_csv_as_dicts(OUTPUT_DIR / "video_creatives_top50.csv")
        # Convert score fields to int for rendering
        for r in static_top50 + video_top50:
            for k in ["total_score","proven_signal","gap_opportunity","audience_match"]:
                try: r[k] = int(float(r.get(k, 0) or 0))
                except: r[k] = 0
        generate_html_report(static_top50, video_top50, OUTPUT_DIR / "fabrika_report.html")
        print("\nDone. Open fabrika_report.html in browser.")
        return

    print("\nPhase 1: Loading data...")
    data = load_all_data()

    print("\nPhase 2: Extracting axes from data...")
    axes = extract_axes(data)
    print(f"  Directions: {len(axes['shared']['directions'])}")
    print(f"  Static — Hooks: {len(axes['static']['hooks'])}, Themes: {len(axes['static']['visual_themes'])}, Offers: {len(axes['static']['offers'])}")
    print(f"  Video  — Hooks: {len(axes['video']['hooks'])}, Plots: {len(axes['video']['plots'])}, Talents: {len(axes['video']['talent_formats'])}")

    print("\nPhase 3: Building and scoring matrices...")
    static_all = build_matrix("static", axes, data)
    video_all  = build_matrix("video", axes, data)
    print(f"  Static combinations: {len(static_all):,}")
    print(f"  Video combinations:  {len(video_all):,}")

    static_top50 = diverse_top_n(static_all, 50, max_per_direction=7, max_per_hook=6)
    video_top50  = diverse_top_n(video_all,  50, max_per_direction=7, max_per_hook=6)
    print(f"  Static top 50: scores {static_top50[-1]['total_score']} — {static_top50[0]['total_score']}")
    print(f"  Video top 50:  scores {video_top50[-1]['total_score']} — {video_top50[0]['total_score']}")

    if not args.no_llm and ANTHROPIC_API_KEY:
        print("\nPhase 4: LLM enrichment (suggested copy, test hypotheses, competitor context)...")
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        except ImportError:
            print("  anthropic not installed — skipping LLM enrichment")
            client = None

        if client:
            for i, creative in enumerate(static_top50):
                if i % 10 == 0:
                    print(f"  Static [{i+1}/50]...")
                static_top50[i] = enrich_with_llm(creative, "static", client)
                time.sleep(0.4)

            for i, creative in enumerate(video_top50):
                if i % 10 == 0:
                    print(f"  Video [{i+1}/50]...")
                video_top50[i] = enrich_with_llm(creative, "video", client)
                time.sleep(0.4)
    else:
        print("\nPhase 4: Skipping LLM enrichment (--no-llm or no API key)")

    print("\nPhase 5: Saving outputs...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(static_top50, OUTPUT_DIR / "static_creatives_top50.csv")
    save_csv(video_top50,  OUTPUT_DIR / "video_creatives_top50.csv")
    generate_html_report(static_top50, video_top50, OUTPUT_DIR / "fabrika_report.html")

    print(f"\n{'=' * 60}")
    print("FABRIKA COMPLETE")
    print(f"{'=' * 60}")
    print(f"Static top 50: lab/stage2/marketing/output/static_creatives_top50.csv")
    print(f"Video top 50:  lab/stage2/marketing/output/video_creatives_top50.csv")
    print(f"HTML report:   lab/stage2/marketing/output/fabrika_report.html")
    print(f"\nOpen fabrika_report.html in browser to review all 100 briefs.")


if __name__ == "__main__":
    main()
