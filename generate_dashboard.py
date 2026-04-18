"""
Fabrika — generate_dashboard.py
Generates a comprehensive visual HTML dashboard from all collected data.
Includes competitor ad analysis, bilingual explanations (EN/RU), and strategic insights.

Usage:
    py generate_dashboard.py
Output:
    lab/stage2/marketing/output/dashboard.html
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

BASE        = Path("lab/stage2/marketing/output")
REDDIT_PATH = Path("lab/stage1/output/clean/reddit_enriched.csv")

print("Loading data...")

ads_tiered   = pd.read_csv(BASE / "coursiv_ads_tiered.csv")
ads_insights = pd.read_csv(BASE / "ads_insights.csv") if (BASE / "ads_insights.csv").exists() else pd.DataFrame()
reviews      = pd.read_csv(BASE / "app_reviews.csv")
competitors  = pd.read_csv(BASE / "competitor_signals.csv")
reddit       = pd.read_csv(REDDIT_PATH) if REDDIT_PATH.exists() else pd.DataFrame()
kw_ads_raw   = pd.read_csv(BASE / "meta_keyword_ads.csv") if (BASE / "meta_keyword_ads.csv").exists() else pd.DataFrame()

# ── Process competitor keyword ads ────────────────────────────────────────────
NOISE_PAGES = [
    "Ayatique","One For All Project","EE Times - Electronic Engineering Times",
    "Call to Leap","Nolae","TikTok - US","SkillCourse By Satish Dhawale",
    "Università degli Studi Guglielmo Marconi","Siemens","Shobhit University India",
    "Kittl","Expert Edu Search","MarketingBlocks","Omneky","BrightCHAMPS",
    "MultiLingual AI Learn","St. Joan's School, Salt Lake",
]

comp_ads = pd.DataFrame()
top_comp_ads = []
comp_format_dist = {}
comp_cta_dist = {}

if not kw_ads_raw.empty:
    kw_ads_raw["days_running"] = pd.to_numeric(kw_ads_raw["days_running"], errors="coerce")
    comp_ads = kw_ads_raw[
        ~kw_ads_raw["ad_copy"].str.contains("product.brand", na=True) &
        (kw_ads_raw["ad_copy"].str.len() > 20) &
        (~kw_ads_raw["page_name"].isin(NOISE_PAGES))
    ].copy()
    top_comp_ads = comp_ads.nlargest(15, "days_running")[
        ["page_name","days_running","creative_type","search_keyword","ad_copy","cta","ad_library_url"]
    ].to_dict("records")
    comp_format_dist = comp_ads["creative_type"].value_counts().to_dict()
    comp_cta_dist = comp_ads[comp_ads["cta"].notna() & (comp_ads["cta"] != "")]["cta"].value_counts().head(6).to_dict()

# ── Compute stats ─────────────────────────────────────────────────────────────
tier_counts = ads_tiered["tier"].value_counts().to_dict()
high_signal = ads_tiered[ads_tiered["tier"].isin(["TIER_1_ACTIVE","TIER_2_PROVEN"])]
creative_counts = high_signal["creative_type"].value_counts().to_dict()
review_dist = {f"{int(k)}\u2605": v for k,v in reviews["rating"].value_counts().sort_index().items()}
reddit_cats = {}
reddit_segs = {}
if not reddit.empty and "ai_llm_content_category" in reddit.columns:
    reddit_cats = reddit["ai_llm_content_category"].value_counts().to_dict()
    reddit_segs = reddit["ai_llm_target_audience"].value_counts().to_dict()
top_competitors = competitors.head(12)[["page_name","keyword_count","total_ads","max_days_running"]].to_dict("records")
hooks = []
if not ads_insights.empty and "hook" in ads_insights.columns:
    hooks = ads_insights[["hook","days_running_real","hook_type","lf8_driver","why_this_works","gap_opportunity"]].dropna(subset=["hook"]).to_dict("records")
    hooks = sorted(hooks, key=lambda x: float(x.get("days_running_real",0) or 0), reverse=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def bar_chart(data: dict, color: str = "#4F46E5") -> str:
    if not data:
        return "<p style='color:#666;font-size:13px'>No data available</p>"
    max_val = max(data.values()) or 1
    bars = ""
    for label, value in sorted(data.items(), key=lambda x: -x[1]):
        pct = value / max_val * 100
        bars += f"""<div class="bar-row">
            <div class="bar-label" title="{label}">{str(label)[:32]}</div>
            <div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%;background:{color}"></div></div>
            <div class="bar-value">{value:,}</div>
        </div>"""
    return f"<div class='bar-chart'>{bars}</div>"

def bi(en: str, ru: str) -> str:
    return f"""<div class="bilingual">
      <div class="lang-en"><span class="lang-tag">EN</span> {en}</div>
      <div class="lang-ru"><span class="lang-tag">RU</span> {ru}</div>
    </div>"""

def comp_note(name: str, max_days: int) -> str:
    tech = ["Amazon Web Services","Google","Google Workspace","NVIDIA","AT&T","Alibaba Cloud","Meta for Developers"]
    direct = ["Coursera","Udemy","Codecademy","DataCamp","LinkedIn Learning","Skillshare","DeepLearning.AI"]
    coding = ["Codingal","AWSDevelopers","Code With Harry","Cisco Networking Academy","Learn with Cisco","Langflow"]
    if name in tech: return "Tech giant — broad AI awareness ads, not direct competitor"
    if name in direct: return "Direct competitor — professional upskilling + AI courses"
    if name in coding: return "Indirect — coding/dev education, partially overlapping audience"
    if max_days > 50: return "Small direct competitor — watch their creative angles"
    return "Low signal — likely noise or adjacent category"

# ── Build HTML ────────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Fabrika \u2014 Coursiv Research Dashboard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f0f13;color:#e0e0e0;padding:24px;line-height:1.5}}
h1{{font-size:26px;font-weight:700;color:#fff;margin-bottom:4px}}
.subtitle{{color:#666;font-size:14px;margin-bottom:32px}}
h2{{font-size:12px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.6px;margin-bottom:16px}}
h3{{font-size:14px;font-weight:600;color:#bbb;margin:18px 0 10px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:20px}}
.card{{background:#1a1a24;border:1px solid #2a2a3a;border-radius:12px;padding:20px}}
.card-full{{grid-column:1/-1}}
.stat-row{{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap}}
.stat{{background:#12121a;border-radius:8px;padding:16px;flex:1;min-width:110px;text-align:center}}
.stat-value{{font-size:26px;font-weight:700;color:#6366f1}}
.stat-label{{font-size:11px;color:#666;margin-top:4px}}
.bar-chart{{display:flex;flex-direction:column;gap:7px}}
.bar-row{{display:flex;align-items:center;gap:10px}}
.bar-label{{font-size:12px;color:#bbb;width:190px;flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.bar-track{{flex:1;background:#1e1e2e;border-radius:4px;height:18px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:4px}}
.bar-value{{font-size:11px;color:#666;width:44px;text-align:right;flex-shrink:0}}
.insight{{background:#1a1228;border-left:3px solid #6366f1;padding:12px 16px;border-radius:0 8px 8px 0;margin-bottom:12px}}
.insight .num{{font-size:11px;color:#6366f1;font-weight:700;text-transform:uppercase;margin-bottom:4px}}
.insight p{{font-size:13px;color:#d1d5db;margin-bottom:8px}}
.insight.green{{background:#0d1f1a;border-color:#10b981}}.insight.green .num{{color:#10b981}}
.insight.amber{{background:#1a1505;border-color:#f59e0b}}.insight.amber .num{{color:#f59e0b}}
.insight.red{{background:#1a0a0a;border-color:#ef4444}}.insight.red .num{{color:#ef4444}}
.bilingual{{background:#12121a;border-radius:8px;padding:12px;margin-top:8px;font-size:12px;line-height:1.6}}
.lang-en{{color:#a5b4fc;margin-bottom:4px}}.lang-ru{{color:#86efac}}
.lang-tag{{display:inline-block;background:#1e1e30;color:#666;padding:0 5px;border-radius:3px;font-size:10px;font-weight:700;margin-right:4px}}
.hook-card{{background:#12121a;border-radius:8px;padding:12px;margin-bottom:10px;border:1px solid #1e1e2e}}
.hook-text{{font-size:14px;color:#e0e0e0;font-weight:600;margin-bottom:6px}}
.hook-meta{{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px}}
.tag{{background:#1e1e30;color:#9ca3af;padding:2px 8px;border-radius:4px;font-size:11px}}
.tag.days{{background:#1f2d1f;color:#6ee7b7}}
.tag.type{{background:#1e1e30;color:#a5b4fc}}
.hook-why{{font-size:12px;color:#888;font-style:italic;line-height:1.5}}
.hook-gap{{font-size:12px;color:#f59e0b;margin-top:6px}}
.comp-table{{width:100%;border-collapse:collapse;font-size:12px}}
.comp-table th{{text-align:left;color:#555;padding:6px 8px;border-bottom:1px solid #2a2a3a;font-weight:600}}
.comp-table td{{padding:6px 8px;border-bottom:1px solid #1a1a24;vertical-align:top}}
.comp-table tr:hover td{{background:#1e1e2a}}
.ad-url{{font-size:11px;color:#6366f1;text-decoration:none}}
.ad-url:hover{{text-decoration:underline}}
.competitor-ad{{background:#12121a;border-radius:8px;padding:12px;margin-bottom:10px;border:1px solid #1e1e2e}}
.competitor-ad .page{{font-size:13px;font-weight:700;color:#e0e0e0;margin-bottom:4px}}
.competitor-ad .copy{{font-size:12px;color:#aaa;line-height:1.5;margin-bottom:8px;font-style:italic}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:700px){{.two-col{{grid-template-columns:1fr}}}}
.task-card{{background:#0d1f1a;border:1px solid #10b981;border-radius:8px;padding:16px}}
.task-card.amber{{background:#1a1505;border-color:#f59e0b}}
.task-title{{font-size:13px;font-weight:700;color:#10b981;margin-bottom:8px}}
.task-card.amber .task-title{{color:#f59e0b}}
.task-card p{{font-size:12px;color:#aaa;line-height:1.7}}
hr{{border:none;border-top:1px solid #2a2a3a;margin:20px 0}}
</style>
</head>
<body>

<h1>Fabrika \u2014 Coursiv Market Research Dashboard</h1>
<p class="subtitle">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;\u00b7&nbsp; Analyst: Rauan &nbsp;\u00b7&nbsp; Project: Zimran.io Marketing Analyst Test Task</p>

<div class="stat-row">
  <div class="stat"><div class="stat-value">{len(ads_tiered):,}</div><div class="stat-label">Coursiv Ads Scraped</div></div>
  <div class="stat"><div class="stat-value">{tier_counts.get('TIER_1_ACTIVE',0)}</div><div class="stat-label">Currently Active</div></div>
  <div class="stat"><div class="stat-value">{tier_counts.get('TIER_2_PROVEN',0)}</div><div class="stat-label">Proven 30+ Days</div></div>
  <div class="stat"><div class="stat-value">{len(comp_ads)}</div><div class="stat-label">Competitor Ads</div></div>
  <div class="stat"><div class="stat-value">{len(reviews):,}</div><div class="stat-label">App Reviews</div></div>
  <div class="stat"><div class="stat-value">{len(reddit):,}</div><div class="stat-label">Reddit Posts</div></div>
  <div class="stat"><div class="stat-value">{len(competitors)}</div><div class="stat-label">Advertisers Found</div></div>
</div>

<div class="grid">

<div class="card card-full">
  <h2>Key Strategic Findings / \u041a\u043b\u044e\u0447\u0435\u0432\u044b\u0435 \u0441\u0442\u0440\u0430\u0442\u0435\u0433\u0438\u0447\u0435\u0441\u043a\u0438\u0435 \u0432\u044b\u0432\u043e\u0434\u044b</h2>
  <div class="insight">
    <div class="num">Finding 1 \u2014 One Hook Rules Everything</div>
    <p>Coursiv runs 15 unique copy variants across 451 high-signal ads. Every single one uses the same LF8 driver: <strong>to_be_superior_winning_keeping_up</strong>. Best performer: \u201cKick-start your AI No-Code journey with Coursiv \U0001f680\u201d \u2014 ran 181 days. All 6 other LF8 drivers untested.</p>
    {bi("Coursiv found one emotional trigger that converts and is running it globally across 10+ languages. Strength (proven formula) AND critical vulnerability \u2014 any competitor testing an alternative emotional angle can outflank them.","Coursiv \u043d\u0430\u0448\u0451\u043b \u043e\u0434\u0438\u043d \u044d\u043c\u043e\u0446\u0438\u043e\u043d\u0430\u043b\u044c\u043d\u044b\u0439 \u0442\u0440\u0438\u0433\u0433\u0435\u0440, \u043a\u043e\u0442\u043e\u0440\u044b\u0439 \u043a\u043e\u043d\u0432\u0435\u0440\u0442\u0438\u0440\u0443\u0435\u0442, \u0438 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u0442 \u0435\u0433\u043e \u0433\u043b\u043e\u0431\u0430\u043b\u044c\u043d\u043e \u043d\u0430 10+ \u044f\u0437\u044b\u043a\u0430\u0445. \u042d\u0442\u043e \u043e\u0434\u043d\u043e\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u0441\u0438\u043b\u0430 (\u043f\u0440\u043e\u0432\u0435\u0440\u0435\u043d\u043d\u0430\u044f \u0444\u043e\u0440\u043c\u0443\u043b\u0430) \u0418 \u043a\u0440\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u0443\u044f\u0437\u0432\u0438\u043c\u043e\u0441\u0442\u044c \u2014 \u043b\u044e\u0431\u043e\u0439 \u043a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442, \u043f\u0440\u043e\u0442\u0435\u0441\u0442\u0438\u0440\u043e\u0432\u0430\u0432\u0448\u0438\u0439 \u0430\u043b\u044c\u0442\u0435\u0440\u043d\u0430\u0442\u0438\u0432\u043d\u044b\u0439 \u044d\u043c\u043e\u0446\u0438\u043e\u043d\u0430\u043b\u044c\u043d\u044b\u0439 \u0443\u0433\u043e\u043b, \u043c\u043e\u0436\u0435\u0442 \u0438\u0445 \u043e\u0431\u043e\u0439\u0442\u0438.")}
  </div>
  <div class="insight green">
    <div class="num">Finding 2 \u2014 Zero Format Diversity = Opportunity</div>
    <p>73% of proven ads are text-overlay-video with polished visual style. No UGC, no testimonials, no talking heads anywhere in their entire ad account.</p>
    {bi("Coursiv has never tested UGC or talking-head testimonial formats. In a category where the audience is skeptical of marketing claims, a raw authentic UGC format showing real user results could dramatically outperform their current polished style.","\u043d\u0438\u043a\u043e\u0433\u0434\u0430 \u043d\u0435 \u0442\u0435\u0441\u0442\u0438\u0440\u043e\u0432\u0430\u043b UGC \u0438\u043b\u0438 \u0444\u043e\u0440\u043c\u0430\u0442\u044b \u0441 \u043e\u0442\u0437\u044b\u0432\u0430\u043c\u0438 \u0440\u0435\u0430\u043b\u044c\u043d\u044b\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439. \u0410\u0443\u0442\u0435\u043d\u0442\u0438\u0447\u043d\u044b\u0439 UGC-\u0444\u043e\u0440\u043c\u0430\u0442 \u0441 \u0440\u0435\u0430\u043b\u044c\u043d\u044b\u043c\u0438 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u0430\u043c\u0438 \u043c\u043e\u0436\u0435\u0442 \u0440\u0435\u0437\u043a\u043e \u043f\u0440\u0435\u0432\u0437\u043e\u0439\u0442\u0438 \u043f\u043e\u043b\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0439 \u0441\u0442\u0438\u043b\u044c Coursiv.")}
  </div>
  <div class="insight amber">
    <div class="num">Finding 3 \u2014 The \u201cNo-Code\u201d Angle Is Underused</div>
    <p>Their 181-day winner uses \u201cNo-Code\u201d framing. But only 3 of 15 unique ads lead with this angle. Reddit shows \u2018technical_barrier_complaint\u2019 as the 3rd largest category (166 posts).</p>
    {bi("The gap between what the audience fears most (technical complexity) and what Coursiv talks about most (status/superiority) is a direct creative opportunity.","Coursiv \u0430\u0434\u0440\u0435\u0441\u0443\u0435\u0442 \u0442\u0435\u0445\u043d\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0431\u0430\u0440\u044c\u0435\u0440 \u0442\u043e\u043b\u044c\u043a\u043e \u0432 20% \u043a\u0440\u0435\u0430\u0442\u0438\u0432\u043e\u0432. \u0420\u0430\u0437\u0440\u044b\u0432 \u043c\u0435\u0436\u0434\u0443 \u0442\u0435\u043c, \u0447\u0435\u0433\u043e \u0430\u0443\u0434\u0438\u0442\u043e\u0440\u0438\u044f \u0431\u043e\u0438\u0442\u0441\u044f \u0431\u043e\u043b\u044c\u0448\u0435 \u0432\u0441\u0435\u0433\u043e, \u0438 \u0442\u0435\u043c, \u043e \u0447\u0451\u043c \u0433\u043e\u0432\u043e\u0440\u0438\u0442 Coursiv, \u2014 \u044d\u0442\u043e \u043f\u0440\u044f\u043c\u0430\u044f \u0442\u0432\u043e\u0440\u0447\u0435\u0441\u043a\u0430\u044f \u0432\u043e\u0437\u043c\u043e\u0436\u043d\u043e\u0441\u0442\u044c.")}
  </div>
  <div class="insight red">
    <div class="num">Finding 4 \u2014 Competitors Copying The Same Hook</div>
    <p>Tony Robbins: \u201cYou won\u2019t get replaced by AI. You\u2019ll get replaced by someone who understands how to use it.\u201d Udemy: \u201cSmart students are building AI agents while others chill.\u201d The dominant hook is becoming a commodity.</p>
    {bi("The 'don't get left behind' angle is now used by Tony Robbins and Udemy. As this angle saturates, Coursiv urgently needs new emotional territory.","\u0423\u0433\u043e\u043b 'don't get left behind' \u0441\u0435\u0439\u0447\u0430\u0441 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u044e\u0442 \u0422\u043e\u043d\u0438 \u0420\u043e\u0431\u0431\u0438\u043d\u0441 \u0438 Udemy. \u041f\u043e \u043c\u0435\u0440\u0435 \u043d\u0430\u0441\u044b\u0449\u0435\u043d\u0438\u044f Coursiv \u0441\u0440\u043e\u0447\u043d\u043e \u043d\u0443\u0436\u043d\u0430 \u043d\u043e\u0432\u0430\u044f \u044d\u043c\u043e\u0446\u0438\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u044f.")}
  </div>
</div>

<div class="card card-full">
  <h2>Coursiv Ad Performance Analysis / \u0410\u043d\u0430\u043b\u0438\u0437 \u0440\u0435\u043a\u043b\u0430\u043c\u044b Coursiv</h2>
  <div class="two-col">
    <div>
      <h3>Ad Tier Distribution / \u0420\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u0435 \u043f\u043e \u0443\u0440\u043e\u0432\u043d\u044f\u043c</h3>
      {bar_chart(tier_counts, "#6366f1")}
      {bi("74.8% of all Coursiv ads ran less than 14 days (failed tests). Only 1.1% (67 ads) are currently live. 6.1% (384 ads) ran 30+ days — these proven winners are the primary study material.","74.8% \u0432\u0441\u0435\u0439 \u0440\u0435\u043a\u043b\u0430\u043c\u044b Coursiv \u0440\u0430\u0431\u043e\u0442\u0430\u043b\u0430 \u043c\u0435\u043d\u0435\u0435 14 \u0434\u043d\u0435\u0439. \u0422\u043e\u043b\u044c\u043a\u043e 6.1% (384 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f) \u0440\u0430\u0431\u043e\u0442\u0430\u043b\u0438 30+ \u0434\u043d\u0435\u0439 \u2014 \u044d\u0442\u0438 \u043f\u0440\u043e\u0432\u0435\u0440\u0435\u043d\u043d\u044b\u0435 \u043f\u043e\u0431\u0435\u0434\u0438\u0442\u0435\u043b\u0438 \u2014 \u043e\u0441\u043d\u043e\u0432\u043d\u043e\u0439 \u043c\u0430\u0442\u0435\u0440\u0438\u0430\u043b \u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0438\u0437\u0430.")}
    </div>
    <div>
      <h3>Creative Format \u2014 Proven + Active Ads Only</h3>
      {bar_chart(creative_counts, "#8b5cf6")}
      {bi("Among proven and active ads: video 67%, image 32%, carousel 1%. Coursiv has essentially never tested carousel in their winning ads.","Video 67%, \u0438\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u0438\u0435 32%, \u043a\u0430\u0440\u0443\u0441\u0435\u043b\u044c 1%. \u041a\u0430\u0440\u0443\u0441\u0435\u043b\u044c\u043d\u044b\u0435 \u0444\u043e\u0440\u043c\u0430\u0442\u044b \u043d\u0438\u043a\u043e\u0433\u0434\u0430 \u043d\u0435 \u0442\u0435\u0441\u0442\u0438\u0440\u043e\u0432\u0430\u043b\u0438\u0441\u044c \u0432 \u043f\u043e\u0431\u0435\u0434\u043d\u044b\u0445 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f\u0445.")}
    </div>
  </div>
</div>

<div class="card card-full">
  <h2>Coursiv Hooks Library \u2014 Ranked by Days Running / \u0411\u0438\u0431\u043b\u0438\u043e\u0442\u0435\u043a\u0430 \u0445\u0443\u043a\u043e\u0432 \u043f\u043e \u0434\u043b\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u0438 \u043f\u043e\u043a\u0430\u0437\u0430</h2>
  {bi("Days running = performance proxy. The longer an ad ran, the more profitable it was. These hooks are ranked from best to worst performer. The 'why it works' explains the psychological mechanism. The 'gap' shows what each hook leaves unexploited.","Длительность показа = показатель эффективности. Чем дольше работало объявление, тем оно было прибыльнее. Хуки ранжированы от лучшего к худшему. 'Почему работает' объясняет психологический механизм. 'Пробел' показывает, что каждый хук оставляет неиспользованным.")}
  {"".join([f'''<div class="hook-card">
    <div class="hook-text">"{h.get("hook","")[:120]}"</div>
    <div class="hook-meta">
      <span class="tag days">\U0001f550 {h.get("days_running_real","?")}d running</span>
      <span class="tag type">{h.get("hook_type","")}</span>
      <span class="tag">{h.get("lf8_driver","")[:38]}</span>
    </div>
    <div class="hook-why">\U0001f4a1 {h.get("why_this_works","")[:220]}</div>
    {"" if not h.get("gap_opportunity") else "<div class='hook-gap'>&#9888; Gap: " + str(h.get("gap_opportunity",""))[:200] + "</div>"}
  </div>''' for h in hooks])}
</div>

<div class="card card-full">
  <h2>Competitor Ads \u2014 Top Performers by Days Running / \u0420\u0435\u043a\u043b\u0430\u043c\u0430 \u043a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442\u043e\u0432</h2>
  {bi("These are the longest-running competitor ads found across 15 keyword searches. Noise filtered out. Long-running = profitable. Click 'View' to see the full creative on Facebook Ad Library.","Это самые долго работающие объявления конкурентов из 15 поисковых запросов. Шум отфильтрован. Долго работающие = прибыльные. Нажми 'View' для просмотра полного креатива в Facebook Ads Library.")}
  <div style="overflow-x:auto;margin-bottom:20px">
  <table class="comp-table">
    <tr><th>Advertiser</th><th>Days</th><th>Format</th><th>Found via Keyword</th><th>Ad Copy Preview</th><th>CTA</th><th>View</th></tr>
    {"".join([f"""<tr>
      <td><strong>{ad.get("page_name","")[:26]}</strong></td>
      <td><span class="tag days">{int(ad.get("days_running",0)) if ad.get("days_running") else "?"}d</span></td>
      <td><span class="tag type">{ad.get("creative_type","")}</span></td>
      <td><span class="tag">{ad.get("search_keyword","")}</span></td>
      <td style="font-size:11px;color:#aaa;max-width:260px">{str(ad.get("ad_copy",""))[:110].replace(chr(10)," ")}...</td>
      <td><span class="tag">{ad.get("cta","")}</span></td>
      <td>{"<a class='ad-url' href='" + str(ad.get("ad_library_url","")) + "' target='_blank'>View \u2192</a>" if ad.get("ad_library_url") else "\u2014"}</td>
    </tr>""" for ad in top_comp_ads])}
  </table>
  </div>

  <div class="two-col">
    <div>
      <h3>Competitor Format Distribution</h3>
      {bar_chart(comp_format_dist, "#f59e0b")}
      {bi("Video 56%, image 35%, carousel 10%. The category has converged on video-first. The opportunity lies in format innovation, not format following.","Video 56%, \u0438\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u0438\u0435 35%, \u043a\u0430\u0440\u0443\u0441\u0435\u043b\u044c 10%. \u0412\u043e\u0437\u043c\u043e\u0436\u043d\u043e\u0441\u0442\u044c \u2014 \u0432 \u0438\u043d\u043d\u043e\u0432\u0430\u0446\u0438\u0438 \u0444\u043e\u0440\u043c\u0430\u0442\u0430, \u0430 \u043d\u0435 \u0432 \u0441\u043b\u0435\u0434\u043e\u0432\u0430\u043d\u0438\u0438 \u0437\u0430 \u043d\u0438\u043c.")}
    </div>
    <div>
      <h3>Competitor CTA Distribution</h3>
      {bar_chart(comp_cta_dist, "#ec4899")}
      {bi("'Learn More' dominates at 52% — a weak, non-committal CTA. Only 12% use 'Sign Up'. Coursiv could test more direct CTAs to differentiate.","'Learn More' \u2014 \u0441\u043b\u0430\u0431\u044b\u0439 CTA, \u0434\u043e\u043c\u0438\u043d\u0438\u0440\u0443\u0435\u0442 \u0443 52% \u043a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442\u043e\u0432. Coursiv \u043c\u043e\u0436\u0435\u0442 \u043f\u0440\u043e\u0442\u0435\u0441\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u0431\u043e\u043b\u0435\u0435 \u043f\u0440\u044f\u043c\u044b\u0435 CTA ('Start Free', 'Get Certified').")}
    </div>
  </div>

  <hr>
  <h3>Notable Competitor Hooks \u2014 What They\u2019re Betting On / \u041d\u0430 \u0447\u0442\u043e \u0441\u0442\u0430\u0432\u044f\u0442 \u043a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442\u044b</h3>
  <div class="competitor-ad">
    <div class="page">Udemy <span class="tag days">120d</span> <span class="tag type">video</span></div>
    <div class="copy">"If you're still doing everything manually, you're already behind. Build an AI agent that automates your workflow and keeps your output sharp."</div>
    {bi("Same fear-of-falling-behind angle as Coursiv, but leads with a specific concrete outcome (AI agent) rather than a journey. More actionable.","Тот же страх отставания, но приводит к конкретному результату (AI-агент). Более actionable, чем 'journey' Coursiv.")}
  </div>
  <div class="competitor-ad">
    <div class="page">Tony Robbins <span class="tag days">15d</span> <span class="tag type">video</span></div>
    <div class="copy">"HERE\u2019S THE TRUTH THAT NOBODY WANTS TO SAY OUT LOUD: You won\u2019t get replaced by AI. You\u2019ll get replaced by someone who understands how to use it."</div>
    {bi("Identical core message to Coursiv. Tony Robbins entering this space signals the hook is going mainstream — losing differentiation value fast.","Идентичное сообщение Coursiv. Выход Тони Роббинса сигнализирует, что хук становится массовым и быстро теряет ценность дифференциации.")}
  </div>
  <div class="competitor-ad">
    <div class="page">Cisco Networking Academy <span class="tag days">60d</span> <span class="tag type">image</span></div>
    <div class="copy">"Discover how AI can simplify your daily tasks and boost productivity. Join our free course: Introduction to Modern AI."</div>
    {bi("Uses FREE as primary hook — something Coursiv never does. Free trial or free first lesson could be a major conversion lever worth testing.","Использует БЕСПЛАТНО как основной хук — Coursiv этого никогда не делает. Бесплатный пробный период может стать крупным рычагом конверсии.")}
  </div>
</div>

<div class="card">
  <h2>Reddit \u2014 What People Talk About (1,998 posts)</h2>
  {bar_chart(reddit_cats, "#6366f1")}
  {bi("Top 3: beginner_question (446) = 'where do I start', frustration_time_constraints (377) = 'want to learn but no time', career_transition_anxiety (293) = 'fear of being replaced'. These three = Coursiv's entire value proposition, validated by 1,998 real users.","Топ-3: вопрос_новичка (446) = 'с чего начать', фрустрация_нехватка_времени (377) = 'хочу учиться, но нет времени', тревога_карьерного_перехода (293) = 'страх быть замененным'. Эти три = всё ценностное предложение Coursiv, подтверждённое 1998 реальными пользователями.")}
</div>

<div class="card">
  <h2>Reddit \u2014 Audience Segments</h2>
  {bar_chart(reddit_segs, "#10b981")}
  {bi("No single segment dominates — genuinely diverse audience. busy_knowledge_worker_28_50 leads (658), then absolute_beginner (445), aspiring_entrepreneur (358). Validates Coursiv's multi-track approach but also explains their brand positioning confusion between 'professional upskiller' and 'side-hustler'.","Ни один сегмент не доминирует. Это подтверждает многоплановый подход Coursiv (AI Mastery, Dropshipping, Remote Work), но также объясняет путаницу с позиционированием бренда между 'профессиональным апскиллером' и 'сайд-хастлером'.")}
</div>

<div class="card">
  <h2>Google Play Reviews \u2014 Rating Distribution (1,000 reviews)</h2>
  {bar_chart(review_dist, "#14b8a6")}
  {bi("63.4% five-star, 15.4% one-star. Bimodal distribution typical for subscription apps. The 154 one-star reviews are most strategically valuable: they reveal what creates buyer's remorse and what competitors can exploit in their messaging.","63.4% пятизвёздочных, 15.4% однозвёздочных. 154 однозвёздочных отзыва стратегически наиболее ценны: они раскрывают, что вызывает сожаление о покупке, и что конкуренты могут использовать в своих сообщениях.")}
</div>

<div class="card card-full">
  <h2>Competitor Signal Matrix / \u041c\u0430\u0442\u0440\u0438\u0446\u0430 \u043a\u043e\u043d\u043a\u0443\u0440\u0435\u043d\u0442\u043d\u044b\u0445 \u0441\u0438\u0433\u043d\u0430\u043b\u043e\u0432</h2>
  {bi("Ranked by keyword overlap across 15 seed searches. Note: only 2 keywords completed before Apify credits ran out, so max keyword_count = 3. max_days_running is the primary reliability signal.","Ранжированы по пересечению ключевых слов. Только 2 ключевых слова завершены полностью, поэтому max keyword_count = 3. max_days_running — основной сигнал надёжности.")}
  <table class="comp-table" style="margin-top:12px">
    <tr><th>Advertiser</th><th>Keyword Matches</th><th>Ads Found</th><th>Max Days</th><th>Strategic Note</th></tr>
    {"".join([f"""<tr>
      <td><strong>{c["page_name"][:28]}</strong></td>
      <td style="text-align:center">{c["keyword_count"]}</td>
      <td style="text-align:center">{c["total_ads"]}</td>
      <td><span class="tag days">{c["max_days_running"]}d</span></td>
      <td style="font-size:11px;color:#888">{comp_note(c["page_name"], c["max_days_running"])}</td>
    </tr>""" for c in top_competitors])}
  </table>
</div>

<div class="card card-full">
  <h2>Team Tasks / \u0417\u0430\u0434\u0430\u0447\u0438 \u0434\u043b\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u044b</h2>
  <div class="two-col">
    <div class="task-card">
      <div class="task-title">Task A \u2014 Creative Strategist / \u041a\u0440\u0435\u0430\u0442\u0438\u0432\u043d\u044b\u0439 \u0441\u0442\u0440\u0430\u0442\u0435\u0433</div>
      <p>
        Open <strong>review_active_ads.csv</strong> and <strong>review_proven_ads.csv</strong>.<br><br>
        Click each <strong>ad_library_url</strong> link to view the full creative on Facebook Ad Library.<br><br>
        Fill in a new column <strong>manual_notes</strong> with:<br>
        \u2022 Hook (first 3 seconds / first line of copy)<br>
        \u2022 Format (UGC / talking head / screen recording / animation / static)<br>
        \u2022 Visual style (polished / raw / text-heavy / minimal)<br>
        \u2022 Pain point addressed<br>
        \u2022 One strength + one weakness<br><br>
        <strong>Start with proven_ads.csv, sort by days_running_real descending. The 181-day and 161-day ads are most important.</strong><br><br>
        <em>\u041e\u0442\u043a\u0440\u043e\u0439 review_active_ads.csv \u0438 review_proven_ads.csv. \u041a\u043b\u0438\u043a\u043d\u0438 ad_library_url, \u0437\u0430\u043f\u043e\u043b\u043d\u0438 manual_notes: \u0445\u0443\u043a, \u0444\u043e\u0440\u043c\u0430\u0442, \u0441\u0442\u0438\u043b\u044c, \u0431\u043e\u043b\u044c, \u0441\u0438\u043b\u0430 \u0438 \u0441\u043b\u0430\u0431\u043e\u0441\u0442\u044c.</em>
      </p>
    </div>
    <div class="task-card amber">
      <div class="task-title">Task B \u2014 Data Analyst / \u0410\u043d\u0430\u043b\u0438\u0442\u0438\u043a</div>
      <p>
        Open <strong>reddit_enriched.csv</strong> in Excel or Google Sheets.<br><br>
        Filter column <strong>ai_llm_content_category</strong> = <strong>consideration_set_signal</strong> (211 posts).<br><br>
        For each post, read <strong>title</strong> and open <strong>url</strong>. Note:<br>
        \u2022 What products are being compared?<br>
        \u2022 Which "wins" in the thread?<br>
        \u2022 What language do users use when complaining about competitors?<br>
        \u2022 What verbatim quotes could work as ad hooks?<br><br>
        Also check <strong>ai_llm_key_insight</strong> column for AI-extracted insights ready to use.<br><br>
        <em>\u041e\u0442\u043a\u0440\u043e\u0439 reddit_enriched.csv. \u0424\u0438\u043b\u044c\u0442\u0440 consideration_set_signal (211 \u043f\u043e\u0441\u0442\u043e\u0432). \u0417\u0430\u043f\u0438\u0448\u0438: \u043a\u0430\u043a\u0438\u0435 \u043f\u0440\u043e\u0434\u0443\u043a\u0442\u044b \u0441\u0440\u0430\u0432\u043d\u0438\u0432\u0430\u044e\u0442\u0441\u044f, \u043a\u0442\u043e \u043f\u043e\u0431\u0435\u0436\u0434\u0430\u0435\u0442, \u0432\u0435\u0440\u0431\u0430\u043b\u044c\u043d\u044b\u0435 \u0446\u0438\u0442\u0430\u0442\u044b \u0434\u043b\u044f \u0445\u0443\u043a\u043e\u0432.</em>
      </p>
    </div>
  </div>
</div>

</div>
</body>
</html>"""

output_path = BASE / "dashboard.html"
with open(output_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n Dashboard saved: {output_path}")
print("  Open in any browser.")
print(f"  Sections: Key Findings, Coursiv Ads, Hooks Library, Competitor Ads,")
print(f"            Reddit Intelligence, App Reviews, Competitor Matrix, Team Tasks")
