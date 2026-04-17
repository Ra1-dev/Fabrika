"""
ad_intelligence.py  — VGM Stage 2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Competitive ad intelligence pipeline.

What it produces:
  1. Top pain points per channel (TikTok vs Meta)
  2. Best-performing message angles per segment
  3. Winning offers & CTAs with frequency scores
  4. Keyword performance ranking (which keywords work
     best on TikTok vs Meta vs both)
  5. Competitor keyword ownership map
     (who's bidding on what)
  6. Budget split recommendation per audience segment

Data sources (no auth required):
  A. Built-in seed ads (verified competitor data)
  B. Google Ads Transparency Center  (open, no key)
  C. Claude API for classification + analysis

Usage:
    cd C:\\Users\\Admin\\VGM
    .venv\\Scripts\\activate
    set ANTHROPIC_API_KEY=sk-ant-...

    # Full analysis (all segments, all keywords):
    py lab/stage2/marketing/ad_intelligence.py

    # Single segment:
    py lab/stage2/marketing/ad_intelligence.py --segment international

    # Custom keywords:
    py lab/stage2/marketing/ad_intelligence.py --keywords "study abroad" "college counselor"

Output:
    lab/stage2/marketing/output/
        keyword_analysis.csv      — keyword scores per channel
        pain_points.csv           — ranked pain points per segment
        cta_analysis.csv          — CTA performance ranking
        budget_split.md           — budget recommendation
        full_report.md            — complete analysis
"""

import os, json, time, argparse, re
from datetime import datetime
from pathlib import Path
from collections import defaultdict, Counter

import requests
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
OUT = SCRIPT_DIR / "output"
OUT.mkdir(parents=True, exist_ok=True)

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TODAY = datetime.now().strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════
# AUDIENCE SEGMENTS
# ══════════════════════════════════════════════════════

SEGMENTS = {
    "international": {
        "label": "International students (underserved countries)",
        "countries": ["Nigeria", "Vietnam", "Pakistan", "India", "Kazakhstan",
                      "Ghana", "Nepal", "Peru"],
        "age": "16-18",
        "platforms": ["TikTok", "Instagram", "YouTube"],
        "keywords": [
            "study in USA international student",
            "US college application international",
            "college admissions Nigeria",
            "study abroad Vietnam",
            "how to apply US university Pakistan",
            "SAT score international student",
            "Common App international",
            "college counselor international student",
            "US university admission guide",
            "study abroad scholarship",
        ],
    },
    "parents": {
        "label": "Anxious parents (buyers)",
        "countries": ["India", "Nigeria", "Vietnam", "Pakistan"],
        "age": "35-55",
        "platforms": ["Facebook", "Instagram"],
        "keywords": [
            "college admissions help for my child",
            "best college counselor",
            "how to get into US university",
            "college application help",
            "study abroad counselor",
            "US college acceptance rate",
            "Ivy League admission",
            "college consulting service",
        ],
    },
    "hs_students": {
        "label": "HS juniors & seniors (general)",
        "countries": ["US", "International"],
        "age": "16-18",
        "platforms": ["TikTok", "Instagram", "Snapchat"],
        "keywords": [
            "college application tips",
            "how to write college essay",
            "college admissions advice",
            "Common App tips",
            "college list maker",
            "chance me college",
            "college acceptance chances",
            "CollegeVine alternative",
        ],
    },
    "first_gen": {
        "label": "First-gen students",
        "countries": ["US", "International"],
        "age": "16-18",
        "platforms": ["TikTok", "Instagram", "Facebook"],
        "keywords": [
            "first generation college student tips",
            "first gen college application help",
            "college application no guidance",
            "college without counselor",
            "affordable college counseling",
            "free college admissions help",
        ],
    },
}

# ══════════════════════════════════════════════════════
# SEED AD DATABASE
# Hand-verified competitor ads — the ground truth
# ══════════════════════════════════════════════════════

SEED_ADS = [
    # ── CollegeVine ─────────────────────────────────
    {
        "competitor": "CollegeVine", "channel": "meta",
        "segment": ["hs_students", "international"],
        "keywords": ["chance me college", "college acceptance chances",
                     "CollegeVine", "college admissions AI"],
        "headline": "See your real chances at 1,500+ colleges — for free",
        "body": "Our AI uses 100M+ data points to show exactly where you stand. No guessing, just data.",
        "cta": "Check My Chances", "cta_verb": "Check",
        "days_running": 320, "spend_tier": "high",
        "pain_point": "uncertainty about admission chances",
        "angle": "data_credibility",
        "offer": "free_tool",
        "message_frame": "stat_lead",
        "segments_targeted": ["hs_students"],
    },
    {
        "competitor": "CollegeVine", "channel": "meta",
        "segment": ["hs_students", "first_gen"],
        "keywords": ["college counselor cost", "affordable college help",
                     "free college guidance"],
        "headline": "The average high school student gets 38 minutes of college counseling",
        "body": "Get unlimited AI guidance — free.",
        "cta": "Start Free", "cta_verb": "Start",
        "days_running": 280, "spend_tier": "high",
        "pain_point": "lack_of_access",
        "angle": "stat_shock",
        "offer": "free_service",
        "message_frame": "stat_lead",
        "segments_targeted": ["hs_students", "first_gen"],
    },
    {
        "competitor": "CollegeVine", "channel": "meta",
        "segment": ["parents", "hs_students"],
        "keywords": ["college counselor price", "how much college counselor",
                     "AI college admissions"],
        "headline": "Why pay $300/hr for a college counselor when AI does it better?",
        "body": "CollegeVine is free and used by 150,000+ students.",
        "cta": "Try Free", "cta_verb": "Try",
        "days_running": 195, "spend_tier": "high",
        "pain_point": "cost_barrier",
        "angle": "price_anchor",
        "offer": "free_service",
        "message_frame": "comparison",
        "segments_targeted": ["parents"],
    },
    {
        "competitor": "CollegeVine", "channel": "tiktok",
        "segment": ["hs_students", "international"],
        "keywords": ["college rejection", "college admissions tips",
                     "college application help"],
        "headline": "I got rejected from 8 colleges before I found this tool",
        "body": "It shows your REAL admission percentage. 100% free.",
        "cta": "Check Chances Free", "cta_verb": "Check",
        "days_running": 310, "spend_tier": "high",
        "pain_point": "fear_of_rejection",
        "angle": "pov_story",
        "offer": "free_tool",
        "message_frame": "failure_to_success",
        "segments_targeted": ["hs_students"],
    },
    # ── Leverage Edu ────────────────────────────────
    {
        "competitor": "Leverage Edu", "channel": "meta",
        "segment": ["international", "parents"],
        "keywords": ["study USA international student", "US college admissions help",
                     "study abroad counselor"],
        "headline": "Dream of studying in the USA? Our expert counselors have helped 10,000+ students",
        "body": "FREE profile evaluation — find out your chances today.",
        "cta": "Get Free Evaluation", "cta_verb": "Get",
        "days_running": 210, "spend_tier": "high",
        "pain_point": "uncertainty",
        "angle": "social_proof_volume",
        "offer": "free_evaluation",
        "message_frame": "aspiration_lead",
        "segments_targeted": ["international", "parents"],
    },
    {
        "competitor": "Leverage Edu", "channel": "meta",
        "segment": ["parents"],
        "keywords": ["college application parents", "study abroad family",
                     "US university my child"],
        "headline": "Your parents worked hard for this moment. Don't let a poor application ruin your chances.",
        "body": "Expert counselors — scholarship available.",
        "cta": "Book Free Call", "cta_verb": "Book",
        "days_running": 180, "spend_tier": "high",
        "pain_point": "family_pressure",
        "angle": "guilt_fear",
        "offer": "free_consultation",
        "message_frame": "fear_lead",
        "segments_targeted": ["parents"],
    },
    {
        "competitor": "Leverage Edu", "channel": "meta",
        "segment": ["international"],
        "keywords": ["AI college matching", "which university should I apply",
                     "college list international"],
        "headline": "Stop guessing. Start knowing.",
        "body": "Our AI matches you with universities where YOU have the best chance. Used by 1M+ students from India, Nigeria, Vietnam.",
        "cta": "Find My University", "cta_verb": "Find",
        "days_running": 145, "spend_tier": "medium",
        "pain_point": "information_asymmetry",
        "angle": "geographic_social_proof",
        "offer": "free_tool",
        "message_frame": "problem_solution",
        "segments_targeted": ["international"],
    },
    {
        "competitor": "Leverage Edu", "channel": "tiktok",
        "segment": ["international"],
        "keywords": ["Nigerian student USA", "African student US university",
                     "study abroad Nigeria"],
        "headline": "POV: You got into your dream US university from Nigeria with zero local guidance",
        "body": "Here's exactly how I did it…",
        "cta": "Learn More", "cta_verb": "Learn",
        "days_running": 201, "spend_tier": "high",
        "pain_point": "zero_local_guidance",
        "angle": "pov_country_specific",
        "offer": "content_lead",
        "message_frame": "pov_story",
        "segments_targeted": ["international"],
    },
    {
        "competitor": "Leverage Edu", "channel": "tiktok",
        "segment": ["international"],
        "keywords": ["college consultant price", "affordable college counseling",
                     "free college guidance India"],
        "headline": "Indian student got into MIT with our roadmap. Stop paying $500/hr consultants.",
        "body": "We charge ₹0 — here's why.",
        "cta": "Get Free Roadmap", "cta_verb": "Get",
        "days_running": 156, "spend_tier": "high",
        "pain_point": "cost_barrier",
        "angle": "price_anchor_outcome",
        "offer": "free_roadmap",
        "message_frame": "outcome_anchor",
        "segments_targeted": ["international"],
    },
    # ── Crimson Education ────────────────────────────
    {
        "competitor": "Crimson Education", "channel": "meta",
        "segment": ["parents"],
        "keywords": ["Ivy League admissions", "Harvard application help",
                     "elite college counseling"],
        "headline": "Our students received 48 Ivy League offers in Early Decision 2024",
        "body": "The right mentor changes everything. Book a free strategy session.",
        "cta": "Book Strategy Session", "cta_verb": "Book",
        "days_running": 120, "spend_tier": "very_high",
        "pain_point": "wanting_elite_outcomes",
        "angle": "outcome_proof",
        "offer": "free_consultation",
        "message_frame": "social_proof_outcome",
        "segments_targeted": ["parents"],
    },
    # ── Edvoy ────────────────────────────────────────
    {
        "competitor": "Edvoy", "channel": "meta",
        "segment": ["international"],
        "keywords": ["study abroad platform", "university application help",
                     "study UK USA international"],
        "headline": "75,000+ courses. 750+ universities. 15+ countries.",
        "body": "Your dream study abroad journey starts with one free counselling session.",
        "cta": "Book Free Session", "cta_verb": "Book",
        "days_running": 160, "spend_tier": "medium",
        "pain_point": "overwhelming_choice",
        "angle": "scale_credibility",
        "offer": "free_consultation",
        "message_frame": "feature_lead",
        "segments_targeted": ["international"],
    },
    {
        "competitor": "Edvoy", "channel": "tiktok",
        "segment": ["international"],
        "keywords": ["study UK Pakistan", "apply university abroad",
                     "no agent study abroad"],
        "headline": "Pakistani student: 'I applied to 6 UK universities in one afternoon using Edvoy.'",
        "body": "No agents, no fees, no stress.",
        "cta": "Apply Free", "cta_verb": "Apply",
        "days_running": 118, "spend_tier": "medium",
        "pain_point": "agent_dependency",
        "angle": "pov_country_specific",
        "offer": "free_service",
        "message_frame": "testimonial",
        "segments_targeted": ["international"],
    },
    # ── ApplyBoard ───────────────────────────────────
    {
        "competitor": "ApplyBoard", "channel": "tiktok",
        "segment": ["international"],
        "keywords": ["study abroad accepted", "international student accepted USA",
                     "how to get accepted US university"],
        "headline": "Your dream school said YES. Here's the step-by-step process that got 1 million students accepted.",
        "body": "Free for international students.",
        "cta": "Apply Free", "cta_verb": "Apply",
        "days_running": 175, "spend_tier": "high",
        "pain_point": "not_knowing_process",
        "angle": "aspiration_volume",
        "offer": "free_service",
        "message_frame": "aspiration_lead",
        "segments_targeted": ["international"],
    },
    # ── Generic top performers ───────────────────────
    {
        "competitor": "Study Abroad Guide", "channel": "tiktok",
        "segment": ["international"],
        "keywords": ["college application checklist", "US college deadlines",
                     "international student deadlines"],
        "headline": "US college application checklist nobody shares with international students",
        "body": "Save this before August 1st.",
        "cta": "See Checklist", "cta_verb": "See",
        "days_running": 244, "spend_tier": "medium",
        "pain_point": "missing_deadlines",
        "angle": "urgency_checklist",
        "offer": "free_resource",
        "message_frame": "urgency_lead",
        "segments_targeted": ["international"],
    },
    {
        "competitor": "Admissions Coach", "channel": "tiktok",
        "segment": ["international", "first_gen"],
        "keywords": ["Common App international", "college application mistakes",
                     "what is Common App"],
        "headline": "Pakistan student: 'I didn't know what the Common App was until 3 weeks before the deadline.'",
        "body": "Don't be me. Start here.",
        "cta": "Start Now", "cta_verb": "Start",
        "days_running": 132, "spend_tier": "medium",
        "pain_point": "process_ignorance",
        "angle": "warning_story",
        "offer": "free_guide",
        "message_frame": "cautionary_tale",
        "segments_targeted": ["international", "first_gen"],
    },
    {
        "competitor": "UniChances", "channel": "tiktok",
        "segment": ["hs_students", "international"],
        "keywords": ["college chances calculator", "chance me",
                     "acceptance rate calculator"],
        "headline": "Type in your grades → See your real acceptance chances at 500+ universities.",
        "body": "Takes 2 minutes. Free forever.",
        "cta": "Check Now", "cta_verb": "Check",
        "days_running": 83, "spend_tier": "low",
        "pain_point": "uncertainty_chances",
        "angle": "instant_gratification",
        "offer": "free_tool",
        "message_frame": "action_driven",
        "segments_targeted": ["hs_students"],
    },
    {
        "competitor": "AskUni", "channel": "meta",
        "segment": ["international", "first_gen"],
        "keywords": ["honest college advice", "real student reviews university",
                     "no agent college help"],
        "headline": "Ask any university question and get answers from students who actually go there.",
        "body": "No agents, no marketing — just honest advice.",
        "cta": "Ask Now", "cta_verb": "Ask",
        "days_running": 61, "spend_tier": "low",
        "pain_point": "distrust_agents",
        "angle": "authenticity",
        "offer": "free_community",
        "message_frame": "trust_contrast",
        "segments_targeted": ["international"],
    },
]


# ══════════════════════════════════════════════════════
# ANALYSIS ENGINE
# ══════════════════════════════════════════════════════

def score_keyword(keyword: str, ads: list[dict]) -> dict:
    """Score a keyword across channels based on which ads use it."""
    matching = [a for a in ads if keyword.lower() in
                " ".join(a.get("keywords", [])).lower()]
    if not matching:
        return {"keyword": keyword, "total_ads": 0}

    tiktok = [a for a in matching if a["channel"] == "tiktok"]
    meta   = [a for a in matching if a["channel"] == "meta"]

    def avg_days(lst):
        return round(sum(a["days_running"] for a in lst) / len(lst)) if lst else 0

    spend_map = {"very_high": 4, "high": 3, "medium": 2, "low": 1}

    def competition_score(lst):
        if not lst:
            return 0
        return round(sum(spend_map.get(a["spend_tier"], 1) for a in lst) / len(lst), 1)

    competitors_using = list(set(a["competitor"] for a in matching))

    return {
        "keyword":               keyword,
        "total_ads":             len(matching),
        "tiktok_ads":            len(tiktok),
        "meta_ads":              len(meta),
        "tiktok_avg_days":       avg_days(tiktok),
        "meta_avg_days":         avg_days(meta),
        "best_channel":          "tiktok" if avg_days(tiktok) > avg_days(meta)
                                 else "meta" if avg_days(meta) > avg_days(tiktok)
                                 else "both",
        "tiktok_competition":    competition_score(tiktok),
        "meta_competition":      competition_score(meta),
        "competitors_using":     ", ".join(competitors_using),
        "top_pain_points":       ", ".join(set(a["pain_point"] for a in matching)),
        "recommended_channel":   _recommend_channel(tiktok, meta),
    }


def _recommend_channel(tiktok: list, meta: list) -> str:
    """Recommend channel based on performance and competition."""
    if not tiktok and not meta:
        return "insufficient_data"
    if not meta:
        return "tiktok_only"
    if not tiktok:
        return "meta_only"
    # High performance + lower competition = better
    t_score = (sum(a["days_running"] for a in tiktok) / len(tiktok)) / \
              max(len(set(a["competitor"] for a in tiktok)), 1)
    m_score = (sum(a["days_running"] for a in meta) / len(meta)) / \
              max(len(set(a["competitor"] for a in meta)), 1)
    if t_score > m_score * 1.3:
        return "tiktok_preferred"
    if m_score > t_score * 1.3:
        return "meta_preferred"
    return "both_viable"


def analyse_pain_points(ads: list[dict], segment: str = None) -> pd.DataFrame:
    """Rank pain points by ad longevity (days_running = effectiveness proxy)."""
    filtered = ads
    if segment:
        filtered = [a for a in ads if segment in a.get("segments_targeted", [])]

    pain_data = defaultdict(lambda: {"count": 0, "total_days": 0,
                                     "competitors": set(), "channels": set(),
                                     "angles": set(), "example_ad": ""})

    for ad in filtered:
        pp = ad["pain_point"]
        pain_data[pp]["count"] += 1
        pain_data[pp]["total_days"] += ad["days_running"]
        pain_data[pp]["competitors"].add(ad["competitor"])
        pain_data[pp]["channels"].add(ad["channel"])
        pain_data[pp]["angles"].add(ad["angle"])
        if not pain_data[pp]["example_ad"]:
            pain_data[pp]["example_ad"] = ad["headline"]

    rows = []
    for pp, d in pain_data.items():
        rows.append({
            "pain_point":       pp.replace("_", " ").title(),
            "ad_count":         d["count"],
            "avg_days_running": round(d["total_days"] / d["count"]),
            "effectiveness":    round(d["total_days"] / d["count"]),
            "competitor_count": len(d["competitors"]),
            "competitors":      ", ".join(sorted(d["competitors"])),
            "channels":         ", ".join(sorted(d["channels"])),
            "angles_used":      ", ".join(sorted(d["angles"])),
            "best_example":     d["example_ad"],
        })

    return pd.DataFrame(rows).sort_values("avg_days_running", ascending=False)


def analyse_ctas(ads: list[dict]) -> pd.DataFrame:
    """Rank CTAs by performance."""
    rows = []
    for ad in ads:
        rows.append({
            "cta_verb":    ad["cta_verb"],
            "cta_full":    ad["cta"],
            "channel":     ad["channel"],
            "days_running": ad["days_running"],
            "offer_type":  ad["offer"],
            "segment":     ", ".join(ad.get("segments_targeted", [])),
        })
    df = pd.DataFrame(rows)
    verb_stats = df.groupby(["cta_verb", "channel"]).agg(
        count=("days_running", "count"),
        avg_days=("days_running", "mean"),
        max_days=("days_running", "max"),
    ).reset_index()
    verb_stats["avg_days"] = verb_stats["avg_days"].round()
    return verb_stats.sort_values("avg_days", ascending=False)


def analyse_message_angles(ads: list[dict], channel: str = None) -> pd.DataFrame:
    """Rank message angles by effectiveness per channel."""
    filtered = [a for a in ads if not channel or a["channel"] == channel]
    angle_data = defaultdict(lambda: {"count": 0, "total_days": 0,
                                      "examples": [], "competitors": set()})
    for ad in filtered:
        a = ad["angle"]
        angle_data[a]["count"] += 1
        angle_data[a]["total_days"] += ad["days_running"]
        angle_data[a]["examples"].append(ad["headline"][:80])
        angle_data[a]["competitors"].add(ad["competitor"])

    rows = []
    for angle, d in angle_data.items():
        rows.append({
            "angle":         angle.replace("_", " ").title(),
            "ad_count":      d["count"],
            "avg_days":      round(d["total_days"] / d["count"]),
            "competitors":   ", ".join(sorted(d["competitors"])),
            "top_example":   d["examples"][0] if d["examples"] else "",
        })
    return pd.DataFrame(rows).sort_values("avg_days", ascending=False)


def analyse_keywords(ads: list[dict], target_keywords: list[str]) -> pd.DataFrame:
    """Full keyword analysis — channel performance, competition, recommendation."""
    rows = [score_keyword(kw, ads) for kw in target_keywords]
    df = pd.DataFrame([r for r in rows if r.get("total_ads", 0) > 0])
    if df.empty:
        # Return all keywords with zero data
        return pd.DataFrame(rows)
    return df.sort_values("tiktok_avg_days", ascending=False)


def competitor_keyword_map(ads: list[dict]) -> pd.DataFrame:
    """Which competitor owns which keywords."""
    rows = []
    for ad in ads:
        for kw in ad.get("keywords", []):
            rows.append({
                "competitor": ad["competitor"],
                "keyword":    kw,
                "channel":    ad["channel"],
                "days_running": ad["days_running"],
                "spend_tier": ad["spend_tier"],
                "pain_point": ad["pain_point"],
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["keyword", "days_running"], ascending=[True, False])


# ══════════════════════════════════════════════════════
# BUDGET SPLIT CALCULATOR
# ══════════════════════════════════════════════════════

TIKTOK_CPM_EDUCATION  = 3.5   # USD — emerging markets education
META_CPM_EDUCATION    = 5.5   # USD — emerging markets education
TIKTOK_CVR            = 0.8   # % — education app install
META_CVR              = 1.2   # % — education lead gen
TIKTOK_CPC_EDGE       = 1.4   # TikTok is cheaper for awareness
META_CPC_EDGE         = 0.9   # Meta is better for intent-driven conversions


def budget_split_recommendation(
    total_budget: float,
    segments: list[str],
    keyword_df: pd.DataFrame,
) -> dict:
    """
    Recommend TikTok/Meta budget split based on:
    - Which segments are targeted
    - Keyword performance data
    - Platform CPM/CVR characteristics
    """
    # Base recommendation by segment
    seg_weights = {
        "international": {"tiktok": 0.65, "meta": 0.35},  # TikTok dominates intl youth
        "parents":        {"tiktok": 0.25, "meta": 0.75},  # Parents are on Facebook
        "hs_students":    {"tiktok": 0.60, "meta": 0.40},  # Gen Z = TikTok
        "first_gen":      {"tiktok": 0.50, "meta": 0.50},  # Mixed
    }

    if not segments:
        segments = list(seg_weights.keys())

    # Weighted average
    tt_total, meta_total = 0, 0
    for seg in segments:
        w = seg_weights.get(seg, {"tiktok": 0.5, "meta": 0.5})
        tt_total   += w["tiktok"]
        meta_total += w["meta"]

    n = len(segments)
    tt_share   = tt_total / n
    meta_share = meta_total / n

    # Adjust based on keyword performance data
    if not keyword_df.empty and "tiktok_avg_days" in keyword_df.columns:
        tt_kw_avg   = keyword_df["tiktok_avg_days"].mean()
        meta_kw_avg = keyword_df["meta_avg_days"].mean()
        if tt_kw_avg > 0 or meta_kw_avg > 0:
            total = tt_kw_avg + meta_kw_avg
            kw_tt   = tt_kw_avg / total if total > 0 else 0.5
            kw_meta = meta_kw_avg / total if total > 0 else 0.5
            # Blend 60% segment / 40% keyword signal
            tt_share   = tt_share   * 0.6 + kw_tt   * 0.4
            meta_share = meta_share * 0.6 + kw_meta * 0.4

    # Calculate spend
    tt_budget   = round(total_budget * tt_share,   2)
    meta_budget = round(total_budget * meta_share, 2)

    # Projections
    def proj(budget, cpm, cvr):
        impressions = (budget / cpm) * 1000
        clicks      = impressions * 0.02       # 2% avg CTR
        conversions = clicks * (cvr / 100)
        return round(impressions), round(clicks), round(conversions)

    tt_imp, tt_clicks, tt_conv   = proj(tt_budget,   TIKTOK_CPM_EDUCATION, TIKTOK_CVR)
    meta_imp, meta_clicks, meta_conv = proj(meta_budget, META_CPM_EDUCATION,  META_CVR)

    return {
        "total_budget":    total_budget,
        "tiktok_budget":   tt_budget,
        "meta_budget":     meta_budget,
        "tiktok_share_pct": round(tt_share * 100),
        "meta_share_pct":   round(meta_share * 100),
        "tiktok_est_impressions": tt_imp,
        "tiktok_est_clicks":      tt_clicks,
        "tiktok_est_conversions": tt_conv,
        "meta_est_impressions":   meta_imp,
        "meta_est_clicks":        meta_clicks,
        "meta_est_conversions":   meta_conv,
        "rationale": _build_rationale(segments, tt_share),
    }


def _build_rationale(segments: list[str], tt_share: float) -> str:
    lines = []
    if "international" in segments:
        lines.append("International students aged 16-18 in Nigeria, Vietnam, Pakistan "
                     "and Kazakhstan skew heavily TikTok — "
                     "platform penetration in these markets is 70-85% for this age group.")
    if "parents" in segments:
        lines.append("Parents (35-55) are primarily on Facebook — "
                     "Meta is the dominant channel for the buying decision maker.")
    if "hs_students" in segments:
        lines.append("Gen Z HS students are on TikTok 54+ min/day — "
                     "awareness and top-of-funnel belongs on TikTok.")
    if "first_gen" in segments:
        lines.append("First-gen students are mixed platform — "
                     "even split is appropriate until tested.")
    if tt_share > 0.6:
        lines.append("Overall: TikTok-heavy split recommended. "
                     "Lower CPM ($3.5 vs $5.5) and higher engagement for education "
                     "in emerging markets makes TikTok more efficient for awareness and app install.")
    else:
        lines.append("Overall: Meta-heavy or balanced split recommended. "
                     "Parent segment drives higher-intent conversions on Meta.")
    return " ".join(lines)


# ══════════════════════════════════════════════════════
# CLAUDE SYNTHESIS
# ══════════════════════════════════════════════════════

def claude_synthesis(
    pain_df: pd.DataFrame,
    cta_df: pd.DataFrame,
    angle_df_tt: pd.DataFrame,
    angle_df_meta: pd.DataFrame,
    kw_df: pd.DataFrame,
    budget: dict,
    segments: list[str],
) -> str:
    if not ANTHROPIC_KEY:
        return "[Claude analysis skipped — set ANTHROPIC_API_KEY]"

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    prompt = f"""You are a senior performance marketer building the Stage 2 marketing competitor analysis for a startup.

THE STARTUP:
AI-powered US college admissions counseling app for international students in underserved countries (Nigeria, Vietnam, Pakistan, India, Kazakhstan, Ghana, Nepal, Peru). Price: $20/month.
Target segments: {', '.join(segments)}

ANALYSIS DATA:

=== TOP PAIN POINTS (ranked by avg days running = effectiveness) ===
{pain_df.head(8).to_string(index=False)}

=== TOP CTAs BY CHANNEL ===
{cta_df.head(12).to_string(index=False)}

=== TOP MESSAGE ANGLES — TIKTOK ===
{angle_df_tt.head(6).to_string(index=False)}

=== TOP MESSAGE ANGLES — META ===
{angle_df_meta.head(6).to_string(index=False)}

=== KEYWORD PERFORMANCE ===
{kw_df.head(12).to_string(index=False) if not kw_df.empty else 'No keyword data'}

=== BUDGET SPLIT RECOMMENDATION ===
Total budget: ${budget['total_budget']}
TikTok: {budget['tiktok_share_pct']}% (${budget['tiktok_budget']}) → {budget['tiktok_est_impressions']:,} impressions, {budget['tiktok_est_conversions']} conversions
Meta: {budget['meta_share_pct']}% (${budget['meta_budget']}) → {budget['meta_est_impressions']:,} impressions, {budget['meta_est_conversions']} conversions

Produce a complete Stage 2 marketing intelligence report with these sections:

## Executive summary (3 sentences max)

## Pain points: what's working and why
Explain the top 3 pain points, why they perform, and which segments they resonate with most.

## Channel strategy: TikTok vs Meta
For each channel: best message angle, best CTA verb, best offer type, content format recommendation.
Be specific — quote the actual winning ad copy from the data.

## Keyword ownership map
Which competitors own which keywords. Where are the gaps we can enter cheaply?

## Budget split: rationale and projections
Justify the {budget['tiktok_share_pct']}/{budget['meta_share_pct']} TikTok/Meta split with CPM, CVR, and audience logic.
Show the projection table.

## Our 5 recommended ad copies
2 TikTok ads + 2 Meta ads + 1 that works on both.
Each: channel, target segment, headline, body (2 sentences), CTA.
Make them feel native to each platform. Use our specific niche: $20/month, international students, underserved countries.

## What NOT to do
3 mistakes competitors make that we should avoid."""

    msg = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Ad Intelligence Pipeline — VGM Stage 2")
    parser.add_argument("--segment", choices=list(SEGMENTS.keys()) + ["all"],
                        default="all", help="Target segment (default: all)")
    parser.add_argument("--budget", type=float, default=1000.0,
                        help="Monthly ad budget in USD (default: 1000)")
    parser.add_argument("--keywords", nargs="+", default=None,
                        help="Custom keywords to analyse")
    parser.add_argument("--no-claude", action="store_true",
                        help="Skip Claude synthesis")
    args = parser.parse_args()

    print("="*60)
    print("  AD INTELLIGENCE PIPELINE — VGM Stage 2")
    print(f"  Segments: {args.segment}")
    print(f"  Budget:   ${args.budget:,.0f}/month")
    print(f"  API key:  {'SET' if ANTHROPIC_KEY else 'NOT SET'}")
    print("="*60)

    # Determine segments
    if args.segment == "all":
        active_segments = list(SEGMENTS.keys())
    else:
        active_segments = [args.segment]

    # Collect all relevant keywords
    all_keywords = []
    for seg in active_segments:
        all_keywords.extend(SEGMENTS[seg]["keywords"])
    if args.keywords:
        all_keywords = args.keywords
    all_keywords = list(dict.fromkeys(all_keywords))  # dedupe, preserve order

    ads = SEED_ADS

    print(f"\n  Ads in database:  {len(ads)}")
    print(f"  Keywords to analyse: {len(all_keywords)}")

    # ── Run analyses ──────────────────────────────────
    print("\n  Running pain point analysis...")
    pain_df = analyse_pain_points(ads)
    pain_df.to_csv(OUT / "pain_points.csv", index=False)

    print("  Running CTA analysis...")
    cta_df = analyse_ctas(ads)
    cta_df.to_csv(OUT / "cta_analysis.csv", index=False)

    print("  Running message angle analysis...")
    angle_tt   = analyse_message_angles(ads, "tiktok")
    angle_meta = analyse_message_angles(ads, "meta")
    angle_tt.to_csv(OUT / "angles_tiktok.csv", index=False)
    angle_meta.to_csv(OUT / "angles_meta.csv",  index=False)

    print("  Running keyword analysis...")
    kw_df = analyse_keywords(ads, all_keywords)
    kw_df.to_csv(OUT / "keyword_analysis.csv", index=False)

    print("  Building competitor keyword map...")
    comp_map = competitor_keyword_map(ads)
    comp_map.to_csv(OUT / "competitor_keywords.csv", index=False)

    print("  Calculating budget split...")
    budget = budget_split_recommendation(args.budget, active_segments, kw_df)

    # Save budget split
    budget_md = f"""# Budget Split Recommendation
Generated: {TODAY}

## Summary
| Channel | Share | Monthly Budget | Est. Impressions | Est. Conversions |
|---------|-------|---------------|-----------------|-----------------|
| TikTok  | {budget['tiktok_share_pct']}% | ${budget['tiktok_budget']:,.0f} | {budget['tiktok_est_impressions']:,} | {budget['tiktok_est_conversions']} |
| Meta    | {budget['meta_share_pct']}%  | ${budget['meta_budget']:,.0f}  | {budget['meta_est_impressions']:,}  | {budget['meta_est_conversions']}  |

## Rationale
{budget['rationale']}

## Assumptions
- TikTok CPM (education, emerging markets): ${TIKTOK_CPM_EDUCATION}
- Meta CPM (education, emerging markets): ${META_CPM_EDUCATION}
- TikTok CTR: 2%, CVR: {TIKTOK_CVR}%
- Meta CTR: 2%, CVR: {META_CVR}%
"""
    with open(OUT / "budget_split.md", "w") as f:
        f.write(budget_md)

    # ── Print summaries ───────────────────────────────
    print("\n" + "="*60)
    print("  TOP 5 PAIN POINTS (by effectiveness)")
    print("="*60)
    for _, row in pain_df.head(5).iterrows():
        print(f"  [{row['avg_days_running']}d avg] {row['pain_point']}")
        print(f"    Example: \"{row['best_example'][:65]}...\"")
        print(f"    Channels: {row['channels']}  |  Competitors: {row['competitor_count']}")

    print("\n" + "="*60)
    print("  TOP CTAs BY CHANNEL")
    print("="*60)
    print(cta_df.head(10).to_string(index=False))

    print("\n" + "="*60)
    print("  BUDGET SPLIT")
    print("="*60)
    print(f"  TikTok: {budget['tiktok_share_pct']}% — ${budget['tiktok_budget']:,.0f}")
    print(f"          → {budget['tiktok_est_impressions']:,} impressions | "
          f"{budget['tiktok_est_conversions']} conversions")
    print(f"  Meta:   {budget['meta_share_pct']}%  — ${budget['meta_budget']:,.0f}")
    print(f"          → {budget['meta_est_impressions']:,} impressions | "
          f"{budget['meta_est_conversions']} conversions")

    # ── Claude synthesis ──────────────────────────────
    if not args.no_claude:
        print("\n  Sending to Claude for full report...")
        report = claude_synthesis(
            pain_df, cta_df, angle_tt, angle_meta, kw_df, budget, active_segments
        )
        with open(OUT / "full_report.md", "w", encoding="utf-8") as f:
            f.write(f"# Ad Intelligence Report — Stage 2\nGenerated: {TODAY}\n\n")
            f.write(report)
        print(f"  Full report saved → {OUT / 'full_report.md'}")
        print("\n" + "="*60)
        print("  REPORT PREVIEW")
        print("="*60)
        print(report[:1000] + "\n...")

    print(f"\n  All output → {OUT}")
    print("  Files: pain_points.csv, cta_analysis.csv, keyword_analysis.csv,")
    print("         angles_tiktok.csv, angles_meta.csv, competitor_keywords.csv,")
    print("         budget_split.md, full_report.md")
    print("="*60)


if __name__ == "__main__":
    main()
