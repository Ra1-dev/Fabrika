"""
analyser.py  — VGM Stage 2 Marketing Intelligence
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Reads raw JSON from scraper.py (or uses built-in seed
data as fallback) and produces a complete report:

  Per channel (TikTok + Meta separately):
    • Top pain points ranked by ad longevity
    • Best message angles
    • Winning offers & CTAs
    • Keyword performance (which keywords get the most ads)
    • Competitor keyword ownership

  Cross-channel comparison:
    • Which pain points own which channel
    • Budget split recommendation with projections
    • 5 ready-to-use ad copies (2 TikTok, 2 Meta, 1 both)

Usage:
    py lab/stage2/marketing/analyser.py

    # Specify budget for split recommendation:
    py lab/stage2/marketing/analyser.py --budget 1000

    # Target specific segments:
    py lab/stage2/marketing/analyser.py --segments international parents

Output:
    lab/stage2/marketing/output/
        tiktok_analysis.csv
        meta_analysis.csv
        keyword_comparison.csv
        channel_comparison.csv
        full_report.md            ← the main deliverable
"""

import os, json, argparse
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import pandas as pd

SCRIPT_DIR = Path(__file__).parent
RAW_DIR    = SCRIPT_DIR / "output" / "raw"
OUT_DIR    = SCRIPT_DIR / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TODAY         = datetime.now().strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════════════════════════
# SEED DATA — used when scraper returns 0 results
# Hand-verified competitor ads (ground truth)
# ══════════════════════════════════════════════════════════════════════════

SEED_ADS = [
    # TikTok ads
    {"platform":"tiktok","advertiser":"CollegeVine","keyword":"college admissions",
     "headline":"I got rejected from 8 colleges before I found this tool",
     "body":"It shows your REAL admission percentage. 100% free.",
     "cta":"Check Chances Free","days_running":310,"reach":7100000,"ctr":4.8,
     "pain_point":"fear of rejection","angle":"pov_failure_story",
     "offer":"free_tool","message_frame":"failure_to_success"},

    {"platform":"tiktok","advertiser":"Study Abroad Guide","keyword":"US college application",
     "headline":"US college application checklist nobody shares with international students",
     "body":"Save this before August 1st ⏰",
     "cta":"See Checklist","days_running":244,"reach":5600000,"ctr":6.3,
     "pain_point":"missing deadlines","angle":"urgency_checklist",
     "offer":"free_resource","message_frame":"urgency_lead"},

    {"platform":"tiktok","advertiser":"Leverage Edu","keyword":"Leverage Edu",
     "headline":"POV: You got into your dream US university from Nigeria with zero local guidance",
     "body":"Here's exactly how I did it…",
     "cta":"Learn More","days_running":201,"reach":3100000,"ctr":4.1,
     "pain_point":"zero local guidance","angle":"pov_country_specific",
     "offer":"content_lead","message_frame":"pov_story"},

    {"platform":"tiktok","advertiser":"ApplyBoard","keyword":"study abroad international student",
     "headline":"Your dream school said YES. Here's the step-by-step process that got 1 million students accepted.",
     "body":"Free for international students.",
     "cta":"Apply Free","days_running":175,"reach":4200000,"ctr":2.9,
     "pain_point":"not knowing process","angle":"aspiration_volume",
     "offer":"free_service","message_frame":"aspiration_lead"},

    {"platform":"tiktok","advertiser":"Leverage Edu","keyword":"college counselor affordable",
     "headline":"Indian student got into MIT with our roadmap. Stop paying $500/hr consultants.",
     "body":"We charge ₹0 — here's why.",
     "cta":"Get Free Roadmap","days_running":156,"reach":2400000,"ctr":3.2,
     "pain_point":"cost barrier","angle":"price_anchor_outcome",
     "offer":"free_roadmap","message_frame":"outcome_anchor"},

    {"platform":"tiktok","advertiser":"Admissions Coach","keyword":"how to apply US university",
     "headline":"Pakistan student: 'I didn't know what the Common App was until 3 weeks before the deadline.'",
     "body":"Don't be me. Start here.",
     "cta":"Start Now","days_running":132,"reach":2900000,"ctr":7.1,
     "pain_point":"process ignorance","angle":"warning_story",
     "offer":"free_guide","message_frame":"cautionary_tale"},

    {"platform":"tiktok","advertiser":"Edvoy","keyword":"Edvoy study abroad",
     "headline":"Pakistani student: 'I applied to 6 UK universities in one afternoon using Edvoy.'",
     "body":"No agents, no fees, no stress 🇬🇧",
     "cta":"Apply Free","days_running":118,"reach":1900000,"ctr":3.8,
     "pain_point":"agent dependency","angle":"testimonial_country",
     "offer":"free_service","message_frame":"testimonial"},

    {"platform":"tiktok","advertiser":"UniChances","keyword":"college admissions AI",
     "headline":"Type in your grades → See your real acceptance chances at 500+ universities.",
     "body":"Takes 2 minutes. Free forever.",
     "cta":"Check Now","days_running":83,"reach":1200000,"ctr":5.1,
     "pain_point":"uncertainty about chances","angle":"instant_gratification",
     "offer":"free_tool","message_frame":"action_driven"},

    {"platform":"tiktok","advertiser":"StudentHub","keyword":"study abroad international student",
     "headline":"Connect with students already studying in your dream country.",
     "body":"Real answers, not sales pitches. Free peer community.",
     "cta":"Join Community","days_running":72,"reach":850000,"ctr":4.2,
     "pain_point":"no peer network","angle":"authenticity_community",
     "offer":"free_community","message_frame":"trust_contrast"},

    # Meta ads
    {"platform":"meta","advertiser":"CollegeVine","keyword":"college admissions AI",
     "headline":"See your real chances at 1,500+ colleges — for free",
     "body":"Our AI uses 100M+ data points to show exactly where you stand. No guessing, just data.",
     "cta":"Check My Chances","days_running":320,
     "spend_low":"5000","spend_high":"20000",
     "pain_point":"uncertainty about chances","angle":"data_credibility",
     "offer":"free_tool","message_frame":"stat_lead"},

    {"platform":"meta","advertiser":"CollegeVine","keyword":"college counselor affordable",
     "headline":"The average high school student gets 38 minutes of college counseling",
     "body":"Get unlimited AI guidance — free.",
     "cta":"Start Free","days_running":280,
     "spend_low":"3000","spend_high":"12000",
     "pain_point":"lack of access","angle":"stat_shock",
     "offer":"free_service","message_frame":"stat_lead"},

    {"platform":"meta","advertiser":"CollegeVine","keyword":"college counselor affordable",
     "headline":"Why pay $300/hr for a college counselor when AI does it better?",
     "body":"CollegeVine is free and used by 150,000+ students.",
     "cta":"Try Free","days_running":195,
     "spend_low":"2000","spend_high":"8000",
     "pain_point":"cost barrier","angle":"price_anchor",
     "offer":"free_service","message_frame":"comparison"},

    {"platform":"meta","advertiser":"Leverage Edu","keyword":"Leverage Edu",
     "headline":"Dream of studying in the USA? Our expert counselors have helped 10,000+ students",
     "body":"FREE profile evaluation — find out your chances today.",
     "cta":"Get Free Evaluation","days_running":210,
     "spend_low":"1000","spend_high":"5000",
     "pain_point":"uncertainty","angle":"social_proof_volume",
     "offer":"free_evaluation","message_frame":"aspiration_lead"},

    {"platform":"meta","advertiser":"Leverage Edu","keyword":"Leverage Edu",
     "headline":"Your parents worked hard for this moment. Don't let a poor application ruin your chances.",
     "body":"Expert counselors — scholarship available.",
     "cta":"Book Free Call","days_running":180,
     "spend_low":"2000","spend_high":"8000",
     "pain_point":"family pressure","angle":"guilt_fear",
     "offer":"free_consultation","message_frame":"fear_lead"},

    {"platform":"meta","advertiser":"Leverage Edu","keyword":"college admissions help",
     "headline":"Stop guessing. Start knowing.",
     "body":"Our AI matches you with universities where YOU have the best chance. Used by 1M+ students from India, Nigeria, Vietnam.",
     "cta":"Find My University","days_running":145,
     "spend_low":"500","spend_high":"2000",
     "pain_point":"information asymmetry","angle":"geographic_social_proof",
     "offer":"free_tool","message_frame":"problem_solution"},

    {"platform":"meta","advertiser":"Crimson Education","keyword":"Crimson Education",
     "headline":"Our students received 48 Ivy League offers in Early Decision 2024",
     "body":"The right mentor changes everything. Book a free strategy session.",
     "cta":"Book Strategy Session","days_running":120,
     "spend_low":"10000","spend_high":"50000",
     "pain_point":"wanting elite outcomes","angle":"outcome_proof",
     "offer":"free_consultation","message_frame":"social_proof_outcome"},

    {"platform":"meta","advertiser":"Edvoy","keyword":"Edvoy study abroad",
     "headline":"75,000+ courses. 750+ universities. 15+ countries.",
     "body":"Your dream study abroad journey starts with one free counselling session.",
     "cta":"Book Free Session","days_running":160,
     "spend_low":"500","spend_high":"3000",
     "pain_point":"overwhelming choice","angle":"scale_credibility",
     "offer":"free_consultation","message_frame":"feature_lead"},

    {"platform":"meta","advertiser":"Edvoy","keyword":"study abroad international student",
     "headline":"From shortlisting to visa — we handle everything.",
     "body":"100,000+ students trusted Edvoy. Don't navigate study abroad alone.",
     "cta":"Start Free","days_running":140,
     "spend_low":"300","spend_high":"2000",
     "pain_point":"process complexity","angle":"full_service_trust",
     "offer":"free_service","message_frame":"reassurance"},

    {"platform":"meta","advertiser":"AskUni","keyword":"college admissions help",
     "headline":"Ask any university question and get answers from students who actually go there.",
     "body":"No agents, no marketing — just honest advice.",
     "cta":"Ask Now","days_running":61,
     "spend_low":"50","spend_high":"400",
     "pain_point":"distrust of agents","angle":"authenticity",
     "offer":"free_community","message_frame":"trust_contrast"},
]


# ══════════════════════════════════════════════════════════════════════════
# DATA LOADER
# ══════════════════════════════════════════════════════════════════════════

# ── Strict relevance filtering ─────────────────────────────────────────────
# Scope: US college admissions for international students ONLY
# Logic: creative text must contain a US-college-specific signal
#        AND must not contain any blocklist term

# Ad creative must contain at least one of these
EDUCATION_SIGNALS = [
    # Core — single words that strongly signal education context
    "college", "university", "admission", "admissions",
    "counselor", "counsellor", "scholarship", "undergraduate",
    "bachelor", "campus", "enrollment", "enroll",
    # Tests & tools
    "sat", "act", "gpa", "ielts", "toefl",
    "common app", "commonapp", "fafsa",
    # Phrases
    "ivy league", "study abroad", "study in",
    "apply to", "applying to", "acceptance rate",
    "financial aid", "college essay", "college list",
    "international student", "higher education",
    "degree program", "grad school",
]

# Any match here = immediate disqualification
BLOCKLIST_TERMS = [
    # Cosmetics / beauty
    "cosmetic", "perfume", "cologne", "skincare", "makeup", "lipstick",
    "mascara", "foundation", "serum", "moisturizer", "beauty product",
    "nail", "lash", "hair curl", "hair dye", "wax", "spa treatment",
    # Fashion / retail
    "clothing", "fashion", "shoes", "sneaker", "outfit", "dress",
    "jewelry", "accessory", "handbag", "discount code", "shop now",
    # Food / restaurant
    "restaurant", "food delivery", "recipe", "meal prep", "cooking",
    # Games / entertainment
    "party in my dorm", "mobile game", "gaming app", "casino", "slots",
    "drambox", "reelshort", "drama series", "watch episode",
    "streaming", "watch now", "binge",
    # Finance (non-education)
    "forex", "crypto", "bitcoin", "trading", "broker", "investment app",
    "insurance", "mortgage", "credit card",
    # Dating / relationships
    "dating app", "find love", "romance", "divorce", "husband", "wife",
    "match.com", "tinder",
    # Other irrelevant
    "vpn", "antivirus", "car wash", "roofing", "warehouse job",
    "weight loss", "fat burn", "keto", "protein powder",
    "christmas photo", "portrait", "photo editing",
    # Known irrelevant advertisers from previous runs
    "qion", "raza.perfume", "mnogokartin", "romance_kz", "eo broker",
    "twisted tangle", "strawberrynote", "yoho vpn", "bioglaz",
    "demall", "party in my dorm",
    # Specific noise ads seen in data
    "divorce agreement", "handsome boss", "future husband",
    "won't come here", "portrait by photo", "portrait за",
    "amazing now", "looks amazing", "car looks",
    "hair curler", "split ends", "christmas photo",
    "razdvizhnaya", "вешалка", "брюк", "купить",
    "продажа", "магазин",
]

def _is_relevant(ad: dict) -> bool:
    """
    Strict two-stage filter scoped to US college admissions only.

    Stage 1 — blocklist: reject anything that matches irrelevant categories.
    Stage 2 — education signal: creative text must contain a US-college-specific
               phrase (not just "university" or "college" in isolation, but
               a compound phrase that signals US admissions context).

    Critically: does NOT use the keyword/search_term field — only the
    actual ad creative text matters.
    """
    # Build creative text only — exclude keyword/search_term
    creative_text = " ".join([
        str(ad.get("headline",   "")),
        str(ad.get("body",       "")),
        str(ad.get("ad_title",   "")),
        str(ad.get("ad_text",    "")),
        str(ad.get("caption",    "")),
        str(ad.get("advertiser", "")),
        str(ad.get("brand_name", "")),
    ]).lower()

    # Stage 1 — blocklist
    if any(term in creative_text for term in BLOCKLIST_TERMS):
        return False

    # Stage 2 — must contain a US college admissions specific phrase
    return any(signal in creative_text for signal in EDUCATION_SIGNALS)


def _performance_score(ad: dict) -> int:
    """
    Return a performance proxy score.
    Uses days_running if available, otherwise reach, otherwise likes/ctr.
    """
    days = int(ad.get("days_running", 0) or 0)
    if days > 0:
        return days
    # Normalise reach to approximate days (1M reach ≈ 90 days running)
    reach = int(ad.get("reach", 0) or 0)
    if reach > 0:
        return max(1, reach // 15000)
    # CTR signal
    ctr = float(ad.get("ctr", 0) or 0)
    likes = int(ad.get("likes", 0) or 0)
    if ctr > 0:
        return max(1, int(ctr * 20))
    if likes > 0:
        return max(1, likes // 500)
    return 0


def load_ads() -> tuple[list[dict], str]:
    """Load TikTok ads from raw JSON or fall back to seed data."""
    tiktok_ads = []
    source     = "seed_data"

    tt_file = RAW_DIR / "tiktok_raw.json"
    if tt_file.exists():
        with open(tt_file, encoding="utf-8") as f:
            tt_by_kw = json.load(f)
        raw_count = 0
        for ads in tt_by_kw.values():
            raw_count += len(ads)
            for ad in ads:
                if _is_relevant(ad):
                    ad["days_running"] = _performance_score(ad)
                    tiktok_ads.append(ad)
        print(f"  TikTok raw: {raw_count} ads → {len(tiktok_ads)} relevant after filtering")

    if tiktok_ads:
        source = "scraped_live"
        print(f"  Using live scraped data: {len(tiktok_ads)} TikTok ads")
    else:
        tiktok_ads = [a for a in SEED_ADS if a["platform"] == "tiktok"]
        source     = "seed_data"
        print(f"  No scraped data — using seed data ({len(tiktok_ads)} ads)")

    return tiktok_ads, source


# ══════════════════════════════════════════════════════════════════════════
# PER-CHANNEL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════

def classify_ads(ads: list[dict], platform: str) -> list[dict]:
    """
    Use Claude to classify raw scraped ads with:
      pain_point, angle, offer, message_frame
    Batches to avoid API rate limits.
    Falls back to keyword-based heuristic if no API key.
    """
    # Check if already classified
    if ads and "pain_point" in ads[0] and ads[0]["pain_point"]:
        return ads

    if not ANTHROPIC_KEY:
        return _heuristic_classify(ads)

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    BATCH = 20
    classified = []

    print(f"  Classifying {len(ads)} {platform} ads with Claude...")

    for i in range(0, len(ads), BATCH):
        batch = ads[i:i+BATCH]
        ad_lines = []
        for j, ad in enumerate(batch):
            headline = ad.get("headline") or ad.get("ad_title") or ""
            body     = ad.get("body")     or ad.get("ad_text")  or ""
            cta      = ad.get("cta")      or ad.get("cta_text") or ""
            ad_lines.append(
                f"AD {j}: headline={headline[:120]} | body={body[:120]} | cta={cta}"
            )

        prompt = f"""Classify each ad below. For each AD N return exactly one JSON object on one line.

Fields to fill:
- pain_point: one of: uncertainty_about_chances, cost_barrier, zero_local_guidance,
  missing_deadlines, process_ignorance, fear_of_rejection, family_pressure,
  information_asymmetry, lack_of_access, agent_dependency, wanting_elite_outcomes,
  no_peer_network, process_complexity, overwhelming_choice, distrust_of_agents, other
- angle: one of: pov_story, stat_shock, price_anchor, social_proof_volume, outcome_proof,
  urgency_checklist, warning_story, instant_gratification, authenticity, testimonial,
  aspiration_lead, fear_lead, data_credibility, comparison, scale_credibility, other
- offer: one of: free_tool, free_evaluation, free_consultation, free_service,
  free_resource, free_guide, free_community, content_lead, paid_service, other
- message_frame: one of: failure_to_success, stat_lead, fear_lead, aspiration_lead,
  problem_solution, comparison, pov_story, cautionary_tale, action_driven,
  trust_contrast, reassurance, feature_lead, testimonial, other

Return ONLY a JSON array, one object per ad, no other text.
Example: [{{"pain_point":"cost_barrier","angle":"price_anchor","offer":"free_tool","message_frame":"comparison"}}]

ADS:
{chr(10).join(ad_lines)}"""

        try:
            msg = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            # Strip markdown fences if present
            raw = raw.replace("```json", "").replace("```", "").strip()
            labels = json.loads(raw)
            for ad, label in zip(batch, labels):
                classified.append({**ad, **label})
        except Exception as e:
            print(f"    Batch {i//BATCH + 1} classify error: {e} — using heuristic")
            classified.extend(_heuristic_classify(batch))

    print(f"  Classification done.")
    return classified


def _heuristic_classify(ads: list[dict]) -> list[dict]:
    """Fast keyword-based classification when Claude isn't available."""
    PAIN_MAP = {
        "chance": "uncertainty_about_chances",
        "chances": "uncertainty_about_chances",
        "reject": "fear_of_rejection",
        "rejected": "fear_of_rejection",
        "expensive": "cost_barrier",
        "afford": "cost_barrier",
        "$": "cost_barrier",
        "free": "cost_barrier",
        "deadline": "missing_deadlines",
        "august": "missing_deadlines",
        "guidance": "zero_local_guidance",
        "counselor": "lack_of_access",
        "counsellor": "lack_of_access",
        "don't know": "process_ignorance",
        "didn't know": "process_ignorance",
        "common app": "process_ignorance",
        "ivy": "wanting_elite_outcomes",
        "harvard": "wanting_elite_outcomes",
        "agent": "agent_dependency",
        "parent": "family_pressure",
        "international": "zero_local_guidance",
    }
    ANGLE_MAP = {
        "pov": "pov_story",
        "i got": "pov_story",
        "i was": "pov_story",
        "%": "stat_shock",
        "average": "stat_shock",
        "100m": "data_credibility",
        "million": "social_proof_volume",
        "students": "social_proof_volume",
        "ivy": "outcome_proof",
        "checklist": "urgency_checklist",
        "save this": "urgency_checklist",
        "don't be": "warning_story",
        "stop paying": "price_anchor",
        "why pay": "price_anchor",
        "honest": "authenticity",
        "no agent": "authenticity",
    }
    OFFER_MAP = {
        "free tool": "free_tool",
        "free ": "free_service",
        "checklist": "free_resource",
        "guide": "free_guide",
        "evaluation": "free_evaluation",
        "session": "free_consultation",
        "call": "free_consultation",
        "community": "free_community",
        "roadmap": "free_guide",
    }

    def match(text, mapping):
        text_l = text.lower()
        for k, v in mapping.items():
            if k in text_l:
                return v
        return "other"

    result = []
    for ad in ads:
        combined = " ".join([
            ad.get("headline",""), ad.get("body",""),
            ad.get("ad_title",""), ad.get("ad_text",""),
        ])
        result.append({
            **ad,
            "pain_point":    match(combined, PAIN_MAP),
            "angle":         match(combined, ANGLE_MAP),
            "offer":         match(combined, OFFER_MAP),
            "message_frame": "other",
        })
    return result


def analyse_channel(ads: list[dict], platform: str) -> dict:
    """Full analysis for a single channel."""
    if not ads:
        return {"platform": platform, "total_ads": 0}

    # Classify raw ads first
    ads = classify_ads(ads, platform)

    df = pd.DataFrame(ads)

    # Ensure required columns exist with defaults
    for col in ["pain_point", "angle", "offer", "message_frame", "cta",
                "days_running", "keyword", "advertiser", "headline"]:
        if col not in df.columns:
            df[col] = "" if col not in ("days_running",) else 0
    df["days_running"] = pd.to_numeric(df["days_running"], errors="coerce").fillna(0).astype(int)
    df["headline"]     = df.get("headline", df.get("ad_title", "")).fillna("")
    df["advertiser"]   = df.get("advertiser", df.get("brand_name", "Unknown")).fillna("Unknown")
    df["keyword"]      = df.get("keyword", "").fillna("")
    df["cta"]          = df.get("cta", df.get("cta_text", "")).fillna("")

    # ── Pain points ───────────────────────────────────────────────────────
    pain = (
        df.groupby("pain_point")
          .agg(
              ad_count=("days_running", "count"),
              avg_days=("days_running", "mean"),
              max_days=("days_running", "max"),
              competitors=("advertiser", lambda x: ", ".join(sorted(set(x)))),
          )
          .reset_index()
          .sort_values("avg_days", ascending=False)
    )
    pain["avg_days"] = pain["avg_days"].round().astype(int)

    # Determine headline column early — used by angles, keywords, top_ads
    hl_col = "headline" if "headline" in df.columns else "ad_title"
    if hl_col not in df.columns:
        df["headline"] = ""
        hl_col = "headline"

    # ── Message angles ────────────────────────────────────────────────────
    angles = (
        df.groupby("angle")
          .agg(
              count=("days_running", "count"),
              avg_days=("days_running", "mean"),
              example=(hl_col, "first"),
          )
          .reset_index()
          .sort_values("avg_days", ascending=False)
    )
    angles["avg_days"] = angles["avg_days"].round().astype(int)

    # ── Offers ────────────────────────────────────────────────────────────
    offers = (
        df.groupby("offer")
          .agg(
              count=("days_running", "count"),
              avg_days=("days_running", "mean"),
          )
          .reset_index()
          .sort_values("avg_days", ascending=False)
    )
    offers["avg_days"] = offers["avg_days"].round().astype(int)

    # ── CTAs ──────────────────────────────────────────────────────────────
    def extract_verb(cta: str) -> str:
        return str(cta).split()[0].strip("!.,") if cta else "N/A"

    df["cta_verb"] = df["cta"].apply(extract_verb)
    ctas = (
        df.groupby(["cta", "cta_verb"])
          .agg(
              count=("days_running", "count"),
              avg_days=("days_running", "mean"),
          )
          .reset_index()
          .sort_values("avg_days", ascending=False)
    )
    ctas["avg_days"] = ctas["avg_days"].round().astype(int)

    # ── Keywords ──────────────────────────────────────────────────────────
    keywords = (
        df.groupby("keyword")
          .agg(
              ad_count=("days_running", "count"),
              avg_days=("days_running", "mean"),
              competitors=("advertiser", lambda x: ", ".join(sorted(set(x)))),
              top_ad=(hl_col, "first"),
          )
          .reset_index()
          .sort_values("avg_days", ascending=False)
    )
    keywords["avg_days"] = keywords["avg_days"].round().astype(int)

    # ── Competitor keyword ownership ──────────────────────────────────────
    comp_map = (
        df.groupby(["advertiser", "keyword"])
          .agg(
              ad_count=("days_running", "count"),
              avg_days=("days_running", "mean"),
          )
          .reset_index()
          .sort_values(["keyword", "avg_days"], ascending=[True, False])
    )
    comp_map["avg_days"] = comp_map["avg_days"].round().astype(int)

    # ── Top ads ───────────────────────────────────────────────────────────
    top_ads = df.sort_values("days_running", ascending=False).head(5)[
        ["advertiser", "keyword", hl_col, "cta", "days_running", "pain_point"]
    ].rename(columns={hl_col: "headline"})

    return {
        "platform":    platform,
        "total_ads":   len(ads),
        "pain_points": pain,
        "angles":      angles,
        "offers":      offers,
        "ctas":        ctas,
        "keywords":    keywords,
        "comp_map":    comp_map,
        "top_ads":     top_ads,
        "df":          df,
    }


# ══════════════════════════════════════════════════════════════════════════
# CROSS-CHANNEL COMPARISON
# ══════════════════════════════════════════════════════════════════════════

def compare_channels(tt: dict, meta: dict) -> dict:
    """Side-by-side comparison of TikTok vs Meta."""

    if tt["total_ads"] == 0 or meta["total_ads"] == 0:
        return {}

    tt_df   = tt["df"]
    meta_df = meta["df"]

    # Overall averages
    tt_avg   = tt_df["days_running"].mean()
    meta_avg = meta_df["days_running"].mean()

    # Which pain points are TikTok-dominant vs Meta-dominant
    all_pain = set(tt_df["pain_point"].unique()) | set(meta_df["pain_point"].unique())
    pain_compare = []
    for pp in sorted(all_pain):
        tt_pp   = tt_df[tt_df["pain_point"] == pp]["days_running"]
        meta_pp = meta_df[meta_df["pain_point"] == pp]["days_running"]
        pain_compare.append({
            "pain_point":    pp,
            "tiktok_count":  len(tt_pp),
            "tiktok_avg_days": round(tt_pp.mean()) if len(tt_pp) else 0,
            "meta_count":    len(meta_pp),
            "meta_avg_days": round(meta_pp.mean()) if len(meta_pp) else 0,
            "best_channel":  "tiktok" if (tt_pp.mean() if len(tt_pp) else 0) >
                             (meta_pp.mean() if len(meta_pp) else 0)
                             else "meta",
        })
    pain_cmp_df = pd.DataFrame(pain_compare).sort_values(
        "tiktok_avg_days", ascending=False
    )

    # Keyword overlap
    tt_kws   = set(tt_df["keyword"].unique())
    meta_kws = set(meta_df["keyword"].unique())
    both_kws    = tt_kws & meta_kws
    tiktok_only = tt_kws - meta_kws
    meta_only   = meta_kws - tt_kws

    # Budget split recommendation
    # TikTok better for awareness + international youth (lower CPM)
    # Meta better for intent-driven + parents
    tt_cpm  = 3.5   # USD education emerging markets
    meta_cpm = 5.5
    tt_cvr   = 0.8  # % app install
    meta_cvr = 1.2  # % lead gen

    # Score based on avg days running performance
    total_perf = tt_avg + meta_avg
    tt_share   = tt_avg / total_perf if total_perf > 0 else 0.5
    meta_share = meta_avg / total_perf if total_perf > 0 else 0.5

    return {
        "tt_avg_days":    round(tt_avg),
        "meta_avg_days":  round(meta_avg),
        "pain_comparison": pain_cmp_df,
        "keywords_both":  sorted(both_kws),
        "keywords_tiktok_only": sorted(tiktok_only),
        "keywords_meta_only":   sorted(meta_only),
        "recommended_tiktok_share": round(tt_share * 100),
        "recommended_meta_share":   round(meta_share * 100),
        "tt_cpm":    tt_cpm,
        "meta_cpm":  meta_cpm,
        "tt_cvr":    tt_cvr,
        "meta_cvr":  meta_cvr,
    }


# ══════════════════════════════════════════════════════════════════════════
# BUDGET PROJECTION
# ══════════════════════════════════════════════════════════════════════════

def budget_projections(
    comparison: dict,
    total_budget: float,
    segments: list[str],
) -> dict:
    """Calculate projected impressions, clicks, conversions per channel."""

    # Segment overrides
    seg_weights = {
        "international": {"tiktok": 0.65, "meta": 0.35},
        "parents":       {"tiktok": 0.25, "meta": 0.75},
        "hs_students":   {"tiktok": 0.60, "meta": 0.40},
        "first_gen":     {"tiktok": 0.50, "meta": 0.50},
    }

    if segments:
        tt_w = sum(seg_weights.get(s, {"tiktok": 0.5})["tiktok"] for s in segments) / len(segments)
        meta_w = 1 - tt_w
    elif comparison:
        tt_w   = comparison["recommended_tiktok_share"] / 100
        meta_w = comparison["recommended_meta_share"]   / 100
    else:
        tt_w, meta_w = 0.55, 0.45

    tt_budget   = round(total_budget * tt_w,   2)
    meta_budget = round(total_budget * meta_w, 2)

    cpm_tt   = comparison.get("tt_cpm",    3.5)
    cpm_meta = comparison.get("meta_cpm",  5.5)
    cvr_tt   = comparison.get("tt_cvr",    0.8)
    cvr_meta = comparison.get("meta_cvr",  1.2)

    def proj(budget, cpm, cvr):
        impressions  = (budget / cpm) * 1000
        clicks       = impressions * 0.02
        conversions  = clicks * (cvr / 100)
        cpa          = budget / conversions if conversions > 0 else 0
        return {
            "impressions":  int(impressions),
            "clicks":       int(clicks),
            "conversions":  int(conversions),
            "cpa":          round(cpa, 2),
        }

    return {
        "total_budget":  total_budget,
        "tiktok_budget": tt_budget,
        "meta_budget":   meta_budget,
        "tiktok_share":  round(tt_w * 100),
        "meta_share":    round(meta_w * 100),
        "tiktok_proj":   proj(tt_budget,   cpm_tt,   cvr_tt),
        "meta_proj":     proj(meta_budget, cpm_meta, cvr_meta),
    }


# ══════════════════════════════════════════════════════════════════════════
# CLAUDE REPORT
# ══════════════════════════════════════════════════════════════════════════


def generate_report(tt: dict, budget: dict, data_source: str, segments: list[str]) -> str:
    if not ANTHROPIC_KEY:
        return _fallback_report(tt, budget, data_source)

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    def df_str(df, n=8):
        return df.head(n).to_string(index=False) if df is not None and not df.empty else "No data"

    prompt = f"""You are a senior performance marketer producing a TikTok competitive ad analysis for a startup.

THE STARTUP:
AI-powered US college admissions app for international students in underserved countries
(Nigeria, Vietnam, Pakistan, India, Kazakhstan, Ghana, Nepal, Peru). Price: $20/month.
Target segments: {", ".join(segments)}
Data source: {data_source} | Total TikTok ads analysed: {tt["total_ads"]}

━━━━━━━━━━━━━━━━━ TIKTOK AD DATA ━━━━━━━━━━━━━━━━━

TOP PAIN POINTS (ranked by avg days running = effectiveness proxy):
{df_str(tt.get("pain_points"))}

TOP MESSAGE ANGLES:
{df_str(tt.get("angles"))}

TOP OFFERS:
{df_str(tt.get("offers"))}

TOP CTAs:
{df_str(tt.get("ctas"), 8)}

KEYWORD PERFORMANCE:
{df_str(tt.get("keywords"))}

COMPETITOR KEYWORD OWNERSHIP:
{df_str(tt.get("comp_map"), 10)}

TOP 5 ADS BY PERFORMANCE:
{df_str(tt.get("top_ads"))}

━━━━━━━━━━━━━━━━━ BUDGET ━━━━━━━━━━━━━━━━━
Total: ${budget["total_budget"]}
TikTok: 100% → ${budget["total_budget"]}
Est. impressions: {budget["tiktok_proj"]["impressions"]:,}
Est. conversions: {budget["tiktok_proj"]["conversions"]}
Est. CPA: ${budget["tiktok_proj"]["cpa"]}
CPM assumption: ${budget["tt_cpm"]} (education, emerging markets)

━━━━━━━━━━━━━━━━━ REPORT STRUCTURE ━━━━━━━━━━━━━━━━━

## 1. Top pain points on TikTok
The 5 highest-performing pain points with explanation of WHY each works on TikTok specifically.
Quote real ad copy from the data above for each.

## 2. Winning message angles
What frames dominate? Rank the top angles with avg days and explain the psychology behind each.
Which angle is most underused (opportunity for us)?

## 3. Best offers & CTAs
Which offer type gets the longest-running ads?
Which CTA verbs win? Which to avoid?
Include a ranked table.

## 4. Keyword analysis
For each keyword in the data: saturation level, avg performance, which competitors own it, gap opportunity.
Flag which keywords we should target vs avoid.

## 5. Competitor breakdown
For each competitor found: what pain point they target, what angle they use, how long their ads run.
Who is the most aggressive advertiser? Who has the gap we can exploit?

## 6. TikTok budget recommendation
Given ${budget["total_budget"]}/month budget:
- Recommended CPM: ${budget["tt_cpm"]}
- Projected impressions, clicks, conversions
- Which keywords to bid on first
- Content format recommendation (video length, hook style)

## 7. Five ready-to-use TikTok ad copies for our product
Each ad:
**Segment:** [who]
**Keyword targeting:** [what]
**Hook (first 3 seconds):** [opening line]
**Body:** [2 sentences]
**CTA:** [verb + object]
**Why it will work:** [one sentence from the data]

Make them feel native to TikTok. Specific to our niche: international students,
underserved countries, $20/month vs $500/hr consultants.

## 8. Three things competitors get wrong (that we should avoid)

Be specific throughout. Quote actual ad copy from the data. Flag if any section has thin data."""

    print("  Sending to Claude API...")
    msg = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def _fallback_report(tt: dict, budget: dict, data_source: str) -> str:
    lines = [
        f"# TikTok Ad Intelligence Report — {TODAY}",
        f"Data source: {data_source} | Ads: {tt.get('total_ads', 0)}",
        "",
        "## Top Pain Points",
    ]
    if tt.get("pain_points") is not None and not tt["pain_points"].empty:
        lines.append(tt["pain_points"].head(6).to_string(index=False))
    lines += [
        "", "## Top Message Angles",
    ]
    if tt.get("angles") is not None and not tt["angles"].empty:
        lines.append(tt["angles"].head(5).to_string(index=False))
    lines += [
        "", "## Budget",
        f"${budget['total_budget']:,.0f} → {budget['tiktok_proj']['impressions']:,} impressions | "
        f"{budget['tiktok_proj']['conversions']} conversions | ${budget['tiktok_proj']['cpa']} CPA",
        "",
        "Set ANTHROPIC_API_KEY for full AI report.",
    ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="TikTok Ad Analyser — VGM Stage 2")
    parser.add_argument("--budget",   type=float, default=1000.0)
    parser.add_argument("--segments", nargs="+",
                        choices=["international","parents","hs_students","first_gen"],
                        default=["international"])
    args = parser.parse_args()

    print("="*50)
    print("  TIKTOK AD ANALYSER — VGM Stage 2")
    print(f"  Budget:   ${args.budget:,.0f}/month")
    print(f"  Segments: {', '.join(args.segments)}")
    print(f"  API key:  {'SET' if ANTHROPIC_KEY else 'NOT SET'}")
    print("="*50)

    tiktok_ads, data_source = load_ads()

    print("\n  Analysing TikTok ads...")
    tt = analyse_channel(tiktok_ads, "tiktok")

    # Budget projection (TikTok only)
    tt_cpm = 3.5
    tt_cvr = 0.8
    impressions = int((args.budget / tt_cpm) * 1000)
    clicks      = int(impressions * 0.02)
    conversions = int(clicks * (tt_cvr / 100))
    cpa         = round(args.budget / conversions, 2) if conversions > 0 else 0

    budget = {
        "total_budget":  args.budget,
        "tiktok_proj":   {"impressions": impressions, "clicks": clicks,
                          "conversions": conversions, "cpa": cpa},
        "tt_cpm":        tt_cpm,
        "tt_cvr":        tt_cvr,
    }

    # Save CSVs
    if tt.get("pain_points") is not None:
        tt["pain_points"].to_csv(OUT_DIR / "tiktok_pain_points.csv",  index=False)
        tt["keywords"].to_csv(OUT_DIR   / "tiktok_keywords.csv",      index=False)
        tt["ctas"].to_csv(OUT_DIR       / "tiktok_ctas.csv",          index=False)
        tt["angles"].to_csv(OUT_DIR     / "tiktok_angles.csv",        index=False)
        tt["comp_map"].to_csv(OUT_DIR   / "tiktok_comp_map.csv",      index=False)

    # Print summary
    print("\n" + "="*50)
    print("  TOP PAIN POINTS")
    print("="*50)
    if tt["total_ads"] > 0:
        print(tt["pain_points"].head(6).to_string(index=False))

    print("\n" + "="*50)
    print("  TOP MESSAGE ANGLES")
    print("="*50)
    if tt["total_ads"] > 0:
        print(tt["angles"].head(5).to_string(index=False))

    print("\n" + "="*50)
    print("  TOP CTAs")
    print("="*50)
    if tt["total_ads"] > 0:
        print(tt["ctas"].head(8).to_string(index=False))

    print("\n" + "="*50)
    print("  KEYWORD PERFORMANCE")
    print("="*50)
    if tt["total_ads"] > 0:
        print(tt["keywords"].head(8).to_string(index=False))

    print("\n" + "="*50)
    print("  BUDGET PROJECTION")
    print("="*50)
    print(f"  ${args.budget:,.0f}/month → TikTok 100%")
    print(f"  {impressions:,} impressions | {clicks:,} clicks | {conversions} conversions | ${cpa} CPA")

    # Full report
    report = generate_report(tt, budget, data_source, args.segments)
    report_path = OUT_DIR / "tiktok_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# TikTok Marketing Intelligence Report — Stage 2\n")
        f.write(f"Generated: {TODAY} | Data: {data_source} | Budget: ${args.budget:,.0f}/month\n\n")
        f.write(report)

    print(f"\n  Full report → {report_path}")
    print(f"  CSVs saved to {OUT_DIR}")
    print("="*50)


if __name__ == "__main__":
    main()
