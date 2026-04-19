# Fabrika

**Market intelligence + creative generation engine. Built for Zimran Business Cup.**

Fabrika turns raw market data into a production-ready creative system. It scrapes audience signal from Reddit, YouTube, Meta Ads Library, and app stores — runs a multi-stage AI enrichment pipeline — identifies competitors from data rather than assumptions — and generates a scored combinatorial creative matrix across two axes: static and video.

Every insight traces back to a specific data source. Every competitor was identified by signal, not guessed. Every creative brief includes suggested copy, A/B test hypothesis, competitor context, and risk assessment.

---

## Architecture

```
fabrika/
├── lab/
│   ├── topic_config.json              ← active research config (all scrapers read this)
│   ├── search_seeds.json              ← Phase 1 vocabulary seeds (feeds config_generator)
│   ├── config_generator.py            ← generates topic_config from seeds via Claude API
│   ├── requirements.txt
│   │
│   ├── stage1/                        ← Data collection + enrichment (VGM v2 core)
│   │   ├── scrapers/
│   │   │   ├── reddit/reddit_scraper.py
│   │   │   └── youtube/youtube_scraper_v2.py
│   │   ├── processing/
│   │   │   ├── run_pipeline.py        ← orchestrates steps 1-4
│   │   │   ├── step1_clean.py
│   │   │   ├── step2_features.py
│   │   │   ├── step3_nlp.py
│   │   │   └── step4_llm_classify.py
│   │   └── output/
│   │       └── clean/                 ← reddit_enriched.csv, youtube_enriched.csv
│   │
│   └── stage2/
│       └── marketing/
│           ├── meta_ads_scraper.py         ← brand + competitor ads by page name
│           ├── meta_ads_keyword_scraper.py ← competitor discovery by keyword overlap
│           ├── app_reviews_scraper.py      ← Google Play + iOS reviews
│           ├── ads_analysis.py             ← LLM analysis: hooks, LF8 drivers, gaps
│           ├── reviews_analysis.py         ← LLM analysis: pain quotes, competitors
│           └── output/                     ← all CSVs + dashboard.html
│
├── fabrika_generator.py               ← builds scored creative matrix, outputs top 50x2
└── generate_dashboard.py              ← generates HTML research dashboard
```

---

## The Derivation Chain

No phase uses assumed inputs. Every input traces to a prior-phase output.

```
Brand materials (landing page, app store, ads)
  → topic_config.json  ←  search_seeds.json
    → Reddit + YouTube + App Reviews scraped
      → Pain vocabulary + audience segments + consideration sets
        → Competitor identification (keyword overlap scoring)
          → Competitor Meta Ads pulled
            → Strategic synthesis
              → Creative matrix (Direction x Hook x Plot/Theme x Format/Offer)
                → Top 50 static + top 50 video creative briefs
```

---

## Quick Start

```bash
git clone https://github.com/Ra1-dev/fabrika.git
cd fabrika

python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate

pip install -r lab/requirements.txt

# Configure API keys
cp lab/stage1/processing/.env.example lab/stage1/processing/.env
# Set: ANTHROPIC_API_KEY, APIFY_API_TOKEN
cp lab/stage1/scrapers/youtube/.env.example lab/stage1/scrapers/youtube/.env
# Set: YOUTUBE_API_KEY
```

---

## Running The Pipeline

```bash
# 1. Generate topic config from Phase 1 seeds
py lab/config_generator.py --seeds lab/search_seeds.json

# 2. Scrape brand Meta Ads
py lab/stage2/marketing/meta_ads_scraper.py --page "YourBrand"

# 3. Scrape app reviews
py lab/stage2/marketing/app_reviews_scraper.py

# 4. Scrape Reddit + YouTube audience signal
py lab/stage1/scrapers/reddit/reddit_scraper.py --max-items 2000
py lab/stage1/scrapers/youtube/youtube_scraper_v2.py --max-items 200

# 5. Run enrichment pipeline (LLM classification)
cd lab/stage1/processing && py run_pipeline.py

# 6. Identify competitors by keyword overlap
py lab/stage2/marketing/meta_ads_keyword_scraper.py

# 7. Analyze brand ads + app reviews
py lab/stage2/marketing/ads_analysis.py
py lab/stage2/marketing/reviews_analysis.py

# 8. Generate research dashboard
py generate_dashboard.py

# 9. Generate creative matrix (top 50 static + top 50 video)
py fabrika_generator.py

# Regenerate HTML report from existing data (no API cost)
py fabrika_generator.py --html-only
```

---

## Outputs

| File | Description |
|---|---|
| `lab/stage2/marketing/output/dashboard.html` | Full research dashboard — open in browser |
| `lab/stage2/marketing/output/static_creatives_top50.csv` | Top 50 static creative briefs |
| `lab/stage2/marketing/output/video_creatives_top50.csv` | Top 50 video creative briefs |
| `lab/stage2/marketing/output/fabrika_report.html` | Full creative matrix report — open in browser |
| `lab/stage2/marketing/output/hooks_library.csv` | All hooks ranked by days running |
| `lab/stage2/marketing/output/competitor_signals.csv` | Competitors ranked by keyword overlap |
| `lab/stage2/marketing/output/patterns_summary.md` | Ad pattern analysis in markdown |

---

## Scoring Model

Each creative combination is scored 0-100 across three equal dimensions:

| Dimension | Weight | What it measures |
|---|---|---|
| Proven signal | 33 | Hook or direction validated by 30+ day ad run (brand or competitor) |
| Gap opportunity | 33 | Angle not owned by competitors + not over-tested by brand |
| Audience match | 34 | Reddit pain volume for this direction across 1,998 classified posts |

The top 50 are selected with enforced diversity — max 7 creatives per direction, max 6 per hook — so the output spans the full strategic landscape rather than concentrating on a single high-scoring angle.

---

## API Keys Required

| Key | Location | Used for |
|---|---|---|
| `ANTHROPIC_API_KEY` | `lab/stage1/processing/.env` | LLM pipeline, config generation, ads/reviews analysis, creative brief generation |
| `APIFY_API_TOKEN` | `lab/stage1/processing/.env` | Meta Ads Library scraping |
| `YOUTUBE_API_KEY` | `lab/stage1/scrapers/youtube/.env` | YouTube Data API v3 |

Reddit scraping uses the public JSON API — no key required.

---

## Built On

Fabrika is built on top of [VGM v2](https://github.com/Ra1-dev/VGM) — a topic-agnostic market intelligence engine originally built for HackNU 2026. VGM provides the Reddit/YouTube scrapers, the 4-step enrichment pipeline, and the config generator. Fabrika extends it with Meta Ads scraping, app store reviews, competitor identification by keyword overlap, LLM ad analysis, and automated creative matrix generation.
