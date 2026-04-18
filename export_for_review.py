"""
export_for_review.py
Exports two CSV files for manual team inspection:
  1. top_100_active.csv   — all active ads sorted by days running
  2. top_100_proven.csv   — top 100 proven (30+ day) ads sorted by days running desc

Each row includes the Facebook Ad Library URL for direct inspection.
"""

import pandas as pd
import json

df = pd.read_csv('lab/stage2/marketing/output/coursiv_ads_tiered.csv')

# ── Helper: extract full ad copy from raw_json (some ads have richer text) ───
def get_full_copy(raw):
    try:
        data = json.loads(raw)
        snapshot = data.get('snapshot', {})
        body = snapshot.get('body', {})
        if isinstance(body, dict):
            return body.get('text', '')
        return str(body)
    except:
        return ''

def get_link_description(raw):
    try:
        data = json.loads(raw)
        snapshot = data.get('snapshot', {})
        return snapshot.get('link_description', '') or ''
    except:
        return ''

def get_video_preview_url(raw):
    try:
        data = json.loads(raw)
        snapshot = data.get('snapshot', {})
        videos = snapshot.get('videos', [])
        if videos:
            return videos[0].get('video_preview_image_url', '')
        images = snapshot.get('images', [])
        if images:
            return images[0].get('original_image_url', '') or images[0].get('url', '')
        return ''
    except:
        return ''

def get_page_likes(raw):
    try:
        data = json.loads(raw)
        return data.get('snapshot', {}).get('page_like_count', '')
    except:
        return ''

# Enrich
df['full_ad_copy']       = df['raw_json'].apply(get_full_copy)
df['link_description']   = df['raw_json'].apply(get_link_description)
df['preview_image_url']  = df['raw_json'].apply(get_video_preview_url)
df['page_likes']         = df['raw_json'].apply(get_page_likes)

# Output columns — what the team needs for manual review
REVIEW_COLS = [
    'tier',
    'days_running_real',
    'is_active_real',
    'page_name_real',
    'creative_type',
    'full_ad_copy',
    'headline',
    'cta',
    'link_description',
    'landing_page',
    'start_date_real',
    'end_date_real',
    'publisher_real',
    'num_variations',
    'page_likes',
    'preview_image_url',
    'ad_library_url',
]

# ── EXPORT 1: All active ads sorted by days running ──────────────────────────
active = df[df['tier'] == 'TIER_1_ACTIVE'].copy()
active = active.sort_values('days_running_real', ascending=False)
active_out = active[REVIEW_COLS].head(100)

active_path = 'lab/stage2/marketing/output/review_active_ads.csv'
active_out.to_csv(active_path, index=False)

print(f"Active ads export: {active_path}")
print(f"  Total active: {len(active)} ads")
print(f"  Exported: {len(active_out)} rows")
print(f"\n  Top 10 by days running:")
for _, row in active_out.head(10).iterrows():
    days = row['days_running_real']
    copy = str(row['full_ad_copy'])[:80].replace('\n', ' ')
    print(f"    [{days:.0f}d | {row['creative_type']}] {copy}")

# ── EXPORT 2: Top 100 proven ads (30+ days) sorted by days running ────────────
proven = df[df['tier'] == 'TIER_2_PROVEN'].copy()
proven = proven.sort_values('days_running_real', ascending=False)
proven_out = proven[REVIEW_COLS].head(100)

proven_path = 'lab/stage2/marketing/output/review_proven_ads.csv'
proven_out.to_csv(proven_path, index=False)

print(f"\nProven ads export: {proven_path}")
print(f"  Total proven (30+ days): {len(proven)} ads")
print(f"  Exported: {len(proven_out)} rows")
print(f"\n  Top 10 by days running:")
for _, row in proven_out.head(10).iterrows():
    days = row['days_running_real']
    copy = str(row['full_ad_copy'])[:80].replace('\n', ' ')
    print(f"    [{days:.0f}d | {row['creative_type']}] {copy}")

print(f"""
============================================================
INSTRUCTIONS FOR MANUAL REVIEWERS
============================================================
Two files exported for your review:

1. review_active_ads.csv  ({len(active_out)} ads)
   → Currently live ads. These are Coursiv's active bets.
   → Click 'ad_library_url' to see the full creative on Facebook.
   → For each ad, note:
       - Hook (what is the first line / first 3 seconds?)
       - Format (talking head / screen recording / static / UGC?)
       - Offer (free trial / discount / certificate / challenge?)
       - Pain point addressed
       - Visual style (polished / raw / animated / text-heavy?)

2. review_proven_ads.csv  ({len(proven_out)} ads)
   → Ads that ran 30+ days (longest: {proven['days_running_real'].max():.0f} days).
   → These are PROVEN WINNERS — real money ran behind them.
   → Same review criteria as above.
   → Pay special attention to ads running 60+ days — these are their best performers.

Prioritize reviewing proven ads first, then active.
Fill in a column called 'manual_notes' with your observations.
============================================================
""")
