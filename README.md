# Fabrika

**Market intelligence + creative generation engine. Built for Zimran.io Marketing Data Analyst test task — applied to Coursiv (io.zimran.coursiv).**

Fabrika turns market research into a production-ready creative system. It scrapes audience signal from Reddit, YouTube, Meta Ads Library, and app stores, runs a multi-stage AI enrichment pipeline, identifies competitors from data (not assumptions), and generates a combinatorial creative matrix.

Every insight traces back to a specific data source. Every competitor was identified by signal, not guessed.

---

## What It Does

### Data Collection
- **Reddit scraper** — public JSON API, no auth required. Reads subreddits and queries from `topic_config.json`
- **YouTube scraper** — YouTube Data API v3 (free tier). Reads queries from `topic_config.json`
- **Meta Ads scraper** — Apify `curious_coder/facebook-ads-library-scraper`. Pulls ads by page name or keyword
- **Meta Ads keyword scraper** — surfaces unknown competitors by searching 15 audience-derived keywords
- **App reviews scraper** — `google-play-scraper` + `app-store-scraper`. Reads app IDs from `topic_config.json`

### Enrichment Pipeline (inherited from VGM v2)
```
raw CSV → Step 1: Clean → Step 2: Features → Step 3: NLP → Step 4: LLM classify → enriched CSV
```

### Analysis
- **ads_analysis.py** — LLM extraction on Coursiv's own ads: hook, format, LF8 driver, gap opportunities
- **reviews_analysis.py** — LLM extraction on app reviews: pain quotes, competitor mentions, audience segment
- **generate_dashboard.py** — HTML dashboard with bilingual EN/RU explanations of all findings

---

## The Derivation Chain

No phase uses assumed inputs. Every input traces to a prior-phase output.

```
Coursiv's own materials (landing page, app store, ads)
  → topic_config.json (generated from search_seeds.json)
    → Reddit + YouTube + App Reviews scraped
      → Pain points + audience segments + consideration sets
        → Competitor identification (keyword overlap scoring)
          → Competitor Meta Ads pulled
            → Strategic synthesis
              → Creative matrix (Direction × Hook × Plot × Format)
                → 50 creative briefs
```

---

## Quick Start

```bash
git clone https://github.com/Ra1-dev/fabrika.git
cd fabrika

python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate

pip install -r lab/requirements.txt
pip install google-play-scraper apify-client anthropic

# Configure API keys
cp lab/stage1/processing/.env.example lab/stage1/processing/.env
# Set: ANTHROPIC_API_KEY, APIFY_API_TOKEN
cp lab/stage1/scrapers/youtube/.env.example lab/stage1/scrapers/youtube/.env
# Set: YOUTUBE_API_KEY
```

## Running The Pipeline

```bash
# 1. Generate topic config from Phase 1 seeds
py lab/config_generator.py --seeds lab/search_seeds.json

# 2. Scrape Coursiv's own Meta Ads (Phase 1A)
py lab/stage2/marketing/meta_ads_scraper.py --page "Coursiv"

# 3. Scrape app reviews (Phase 1B)
py lab/stage2/marketing/app_reviews_scraper.py

# 4. Scrape Reddit + YouTube (Phase 2)
py lab/stage1/scrapers/reddit/reddit_scraper.py --max-items 2000
py lab/stage1/scrapers/youtube/youtube_scraper_v2.py --max-items 200

# 5. Run enrichment pipeline
cd lab/stage1/processing && py run_pipeline.py

# 6. Identify competitors by keyword overlap (Phase 3)
py lab/stage2/marketing/meta_ads_keyword_scraper.py

# 7. Analyze Coursiv's own ads
py lab/stage2/marketing/ads_analysis.py

# 8. Analyze app reviews
py lab/stage2/marketing/reviews_analysis.py

# 9. Generate dashboard
py generate_dashboard.py
```

---

## Key Findings (Coursiv Case Study)

1. **One hook rules everything** — 15 unique copy variants, all using the same LF8 driver (`to_be_superior_winning_keeping_up`). Best performer ran 181 days.
2. **Zero format diversity** — 73% text-overlay-video, polished style. No UGC, no testimonials ever tested.
3. **"No-Code" angle underused** — their best ad uses it, only 3 of 15 ads lead with it. Reddit shows technical barrier as 3rd largest pain category.
4. **Hook is becoming a commodity** — Tony Robbins and Udemy now run identical positioning.

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
│   ├── stage1/                        ← Data collection + enrichment (inherited from VGM v2)
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
│           ├── meta_ads_scraper.py         ← Coursiv + competitor ads by page name
│           ├── meta_ads_keyword_scraper.py ← competitor discovery by keyword
│           ├── app_reviews_scraper.py      ← Google Play + iOS reviews
│           ├── ads_analysis.py             ← LLM analysis of Coursiv's ads
│           ├── reviews_analysis.py         ← LLM analysis of app reviews
│           └── output/
│               ├── coursiv_ads_tiered.csv
│               ├── coursiv_ads_high_signal.csv
│               ├── review_active_ads.csv    ← for manual team review
│               ├── review_proven_ads.csv    ← for manual team review
│               ├── ads_insights.csv
│               ├── hooks_library.csv
│               ├── patterns_summary.md
│               ├── app_reviews.csv
│               ├── competitor_signals.csv
│               └── dashboard.html           ← open in browser
│
└── generate_dashboard.py              ← generates dashboard.html from all outputs
```

---

## API Keys Required

| Key | Location | Required for |
|---|---|---|
| `ANTHROPIC_API_KEY` | `lab/stage1/processing/.env` | LLM pipeline + config generation + ads/reviews analysis |
| `APIFY_API_TOKEN` | `lab/stage1/processing/.env` | Meta Ads scraping |
| `YOUTUBE_API_KEY` | `lab/stage1/scrapers/youtube/.env` | YouTube scraper |

Reddit scraping requires no API key.

---

## Built On

Fabrika is built on top of [VGM v2](https://github.com/Ra1-dev/VGM) — a topic-agnostic market intelligence engine originally built for HackNU 2026. VGM provides the Reddit/YouTube scrapers, the 4-step enrichment pipeline, and the config generator. Fabrika extends it with Meta Ads scraping, app store reviews, competitor identification, ad analysis, and creative generation.
