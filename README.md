# Viral Growth Machine (VGM) — v2

**Build for Zimran Business Cup 2026 | EdTech Market Validation**

VGM is a topic-agnostic market intelligence engine. Point it at any niche, and it scrapes public discourse from Reddit and YouTube, enriches the data through a 4-step AI pipeline, and generates data-backed insights about market demand, pain points, and product opportunities.

**v1** reverse-engineered how Claude AI became one of the most talked-about products on the internet.
**v2** repurposed the same engine to validate demand for a personalized US college admissions counseling app — proving the opportunity with real data before building the product.

---

## How it works

```
topic_config.json          ← generated automatically by config_generator.py
        ↓
Reddit scraper  +  YouTube scraper    ← read queries/subreddits from config
        ↓
4-step enrichment pipeline
  Step 1: Clean        — dedup, normalize, date filter
  Step 2: Features     — engagement scores, virality flags, time features
  Step 3: NLP          — VADER sentiment, TF-IDF keywords, pain point detection
  Step 4: LLM classify — Claude API labels content category, audience, growth type
        ↓
Market validation charts  ← 10 focused charts proving product-market fit
```

---

## Quick Start

```
# 1. Clone and install
git clone https://github.com/Ra1-dev/VGM.git
cd VGM
py -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux
pip install -r lab/requirements.txt

# 2. Set up API keys
cp lab/stage1/processing/.env.example lab/stage1/processing/.env
# Edit .env: set ANTHROPIC_API_KEY and LLM_PROVIDER=anthropic
# Also set YOUTUBE_API_KEY in lab/stage1/scrapers/youtube/.env

# 3. Generate a config for your niche (costs ~$0.01)
py lab/config_generator.py --topic "your niche here" --preview

# 4. Run the scrapers
cd lab/stage1/scrapers/reddit  &&  py reddit_scraper.py
cd ../youtube                  &&  py youtube_scraper_v2.py

# 5. Run the enrichment pipeline
cd ../../processing  &&  py run_pipeline.py

# 6. Generate market validation charts
py lab/stage2/pitch_charts/demand_validation.py
```

---

## The config generator

The key addition in v2. Instead of hardcoding queries for one product, you describe any niche in plain English and Claude generates the entire scraping config automatically:

```
py lab/config_generator.py --topic "personal finance for Gen Z"
py lab/config_generator.py --topic "fitness and nutrition science"
py lab/config_generator.py --topic "US college admissions counseling"
py lab/config_generator.py --topic "your niche" --preview   # preview without saving
```

Each generated config includes subreddits, search queries, competitor lists, pain point keywords, and LLM classification categories — all tailored to the niche. The scrapers, pipeline, and charts all read from this config automatically.

---

## v2 case study: US college admissions

We ran VGM against the college admissions space to validate demand for a personalized counseling app. Dataset: **908 Reddit posts + 338 YouTube videos** across 40 months.

### Key findings

**1. The demand gap is stark**
Profile review requests and essay feedback posts have the highest engagement in the dataset (1,065 and 902 avg upvotes respectively) but the lowest post volume — only 5 posts each in the original dataset. People desperately want personalized feedback and can barely find it.

**2. Anxiety is the dominant emotion — and it's seasonal**
Frustration & anxiety is the largest content category (125 posts). Post volume and negativity spike sharply in August–December (application season) and March–April (decision season) — predictable, recurring moments when students need help most.

**3. Negative sentiment outperforms positive 2:1**
Rejection stories average 598 upvotes. Acceptance stories average 198. The pain of rejection drives far more community engagement than the joy of acceptance — the problem is emotionally charged.

**4. Every audience segment is highly engaged**
Anxious parents (489 avg upvotes), Ivy applicants (262), First-gen students (243), International students (181), HS juniors & seniors (166). This is a broad market with multiple distinct customer segments.

**5. Existing solutions leave people frustrated**
Posts mentioning Common App, private consultants, and other tools skew negative in sentiment. The market is underserved by current products.

**6. The keywords map directly to product features**
`harvard`, `rejection`, `essay`, `gpa/sat`, `parents`, `timeline` — each high-engagement keyword is a direct argument for one feature of the product.

---

## Market validation charts (v2)

10 charts in `lab/stage2/pitch_charts/charts/`:

| Chart | What it proves |
|-------|---------------|
| `01_problem_scale.png` | The conversation is growing and emotionally charged |
| `02_demand_supply_gap.png` | Highest-demand content has the lowest supply |
| `03_competitor_frustration.png` | Existing tools leave users frustrated |
| `04_customer_segments.png` | Multiple engaged audience segments = broad market |
| `05_what_to_build.png` | Keywords map directly to product features |
| `06_seasonal_anxiety.png` | Anxiety peaks exactly when students need help |
| `07_pain_point_frequency.png` | Pain points are frequent and high-stakes |
| `08_unanswered_questions.png` | High discussion, no consensus = unsolved problems |
| `09_wish_i_knew.png` | Admitted students reveal what your app should teach |
| `10_real_voices.png` | Verbatim Reddit posts showing the problem |

---

## Project structure

```
VGM/
├── README.md
├── lab/
│   ├── topic_config.json              ← active niche config (generated by config_generator.py)
│   ├── config_generator.py            ← generates topic_config.json from a topic name
│   ├── requirements.txt
│   │
│   ├── stage1/                        # Data collection + enrichment
│   │   ├── scrapers/
│   │   │   ├── reddit/reddit_scraper.py          # Reddit public JSON scraper
│   │   │   ├── youtube/youtube_scraper_v2.py      # YouTube Data API v3 scraper
│   │   │   └── twitter_api_nitter_merged/         # Twitter/X (limited, Google CSE)
│   │   │
│   │   ├── processing/
│   │   │   ├── run_pipeline.py        # Orchestrator — runs step1 → step4
│   │   │   ├── step1_clean.py         # Dedup, normalize, date filter
│   │   │   ├── step2_features.py      # Engagement scores, virality flags
│   │   │   ├── step3_nlp.py           # VADER sentiment, TF-IDF, pain point detection
│   │   │   └── step4_llm_classify.py  # LLM classification (anthropic/ollama/openai/gemini)
│   │   │
│   │   └── output/
│   │       ├── raw/                   # Scraper output CSVs
│   │       ├── step1_cleaned/
│   │       ├── step2_features/
│   │       ├── step3_nlp/
│   │       └── clean/                 # Final enriched CSVs + llm_consumption.csv
│   │
│   └── stage2/                        # Analysis + automation
│       ├── pitch_charts/
│       │   ├── demand_validation.py   # 10 market validation charts (v2)
│       │   └── charts/*.png
│       ├── v1_descriptive/            # 7 original VGM charts
│       ├── v2_sentiment/              # 5 sentiment charts
│       ├── v3_virality_drivers/       # 4 virality charts
│       └── part3_automation/
│           ├── machine.py             # Full cycle orchestrator
│           ├── anomaly_detector.py    # 5 z-score signal detectors
│           └── alerter.py             # Console + Slack delivery
│

```

---

## API keys needed

| Key | Where | Required for |
|-----|-------|-------------|
| `ANTHROPIC_API_KEY` | `lab/stage1/processing/.env` | LLM classification (step 4) |
| `YOUTUBE_API_KEY` | `lab/stage1/scrapers/youtube/.env` | YouTube scraper |
| `SLACK_WEBHOOK_URL` | `lab/stage1/processing/.env` | Slack alerts (optional) |

Reddit scraping requires no API key — uses public JSON endpoints.
LLM classification also works free with Ollama locally (`LLM_PROVIDER=ollama`).

---

## Cost to run

| Component | Cost |
|-----------|------|
| Config generation (Claude API) | ~$0.01 per niche |
| Reddit scraping | $0 |
| YouTube scraping | $0 (free tier) |
| LLM classification with Ollama | $0 (local) |
| LLM classification with Claude API | ~$0.50 per 1,000 posts |

---

## AI tools used

- **Claude Code** — code generation, refactoring, architecture
- **Claude (claude.ai)** — strategy, analysis, documentation
- **Claude Sonnet 4.5 API** — config generation + batch LLM classification
- **Ollama (Qwen 3 8B)** — free local alternative for classification
- **VADER (nltk)** — rule-based sentiment analysis
- **scikit-learn TF-IDF** — keyword extraction
