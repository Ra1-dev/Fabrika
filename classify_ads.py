import pandas as pd
import json

df = pd.read_csv('lab/stage2/marketing/output/coursiv_ads_clean.csv')

# ── Classify into tiers ───────────────────────────────────────────────────────
def classify_tier(row):
    is_active = row['is_active_real']
    days = row['days_running_real']

    if is_active == True or str(is_active).lower() == 'true':
        return 'TIER_1_ACTIVE'
    if pd.notna(days):
        if days >= 30:
            return 'TIER_2_PROVEN'   # ran 30+ days — proven winner
        if days >= 14:
            return 'TIER_3_TESTED'   # ran 14-30 days — got some testing
        return 'TIER_4_NOISE'        # ran <14 days — likely a failed test
    return 'TIER_4_NOISE'

df['tier'] = df.apply(classify_tier, axis=1)

# ── Summary ───────────────────────────────────────────────────────────────────
print("=" * 60)
print("AD TIER CLASSIFICATION")
print("=" * 60)
tier_counts = df['tier'].value_counts()
for tier, count in tier_counts.items():
    pct = count / len(df) * 100
    print(f"  {tier:25s}  {count:5d} ads  ({pct:.1f}%)")

print(f"\n  Total: {len(df)} ads")

# ── Per tier breakdown ────────────────────────────────────────────────────────
for tier in ['TIER_1_ACTIVE', 'TIER_2_PROVEN', 'TIER_3_TESTED']:
    subset = df[df['tier'] == tier]
    if len(subset) == 0:
        continue
    print(f"\n{'=' * 60}")
    print(f"{tier} — {len(subset)} ads")
    print(f"{'=' * 60}")
    print(f"  Creative types: {subset['creative_type'].value_counts().to_dict()}")
    print(f"  Pages: {subset['page_name_real'].value_counts().to_dict()}")
    if subset['days_running_real'].notna().any():
        days = subset['days_running_real'].dropna()
        if len(days) > 0:
            print(f"  Days running — min: {days.min():.0f}, max: {days.max():.0f}, median: {days.median():.0f}")
    print(f"\n  Sample ad copies (first 5):")
    for i, row in subset.head(5).iterrows():
        copy = str(row['ad_copy'])[:120].replace('\n', ' ')
        days_str = f"{row['days_running_real']:.0f}d" if pd.notna(row['days_running_real']) else "?"
        print(f"    [{days_str}] {copy}")

# ── Save tiered CSV ───────────────────────────────────────────────────────────
out_path = 'lab/stage2/marketing/output/coursiv_ads_tiered.csv'
df.to_csv(out_path, index=False)
print(f"\nTiered CSV saved: {out_path}")

# ── Save high-signal subset (Tier 1 + 2) for analysis ────────────────────────
high_signal = df[df['tier'].isin(['TIER_1_ACTIVE', 'TIER_2_PROVEN'])].copy()
high_signal = high_signal.sort_values(
    by=['tier', 'days_running_real'],
    ascending=[True, False]
)
high_signal_path = 'lab/stage2/marketing/output/coursiv_ads_high_signal.csv'
high_signal.to_csv(high_signal_path, index=False)
print(f"High-signal CSV saved: {high_signal_path}  ({len(high_signal)} ads)")
print("\nThese are the ads to analyze for hooks, formats, and offers.")
print("Next: run Reddit and YouTube scrapers, then enrichment pipeline.")
