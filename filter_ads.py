import pandas as pd
import json

df = pd.read_csv('lab/stage2/marketing/output/meta_ads.csv')

# Extract real page name from raw_json
df['page_name_real'] = df['raw_json'].apply(
    lambda x: json.loads(x).get('page_name', '') if pd.notna(x) else ''
)

# Extract start/end dates from raw_json (they're nested)
def extract_field(raw, field):
    try:
        return json.loads(raw).get(field)
    except:
        return None

df['start_date_real']  = df['raw_json'].apply(lambda x: extract_field(x, 'start_date_formatted'))
df['end_date_real']    = df['raw_json'].apply(lambda x: extract_field(x, 'end_date_formatted'))
df['is_active_real']   = df['raw_json'].apply(lambda x: extract_field(x, 'is_active'))
df['publisher_real']   = df['raw_json'].apply(lambda x: json.dumps(extract_field(x, 'publisher_platform')) if extract_field(x, 'publisher_platform') else '')
df['page_id_real']     = df['raw_json'].apply(lambda x: extract_field(x, 'page_id'))

# Coursiv-owned pages only
coursiv_pages = ['Coursiv', 'Coursiv AI Mastery', 'Coursiv: No-Code AI']
df_clean = df[df['page_name_real'].isin(coursiv_pages)].copy()

# Calculate days running from real dates
from datetime import datetime
def calc_days(row):
    try:
        start = datetime.fromisoformat(row['start_date_real'].replace(' ', 'T'))
        end_str = row['end_date_real']
        if pd.notna(end_str) and end_str:
            end = datetime.fromisoformat(end_str.replace(' ', 'T'))
        else:
            end = datetime.now()
        return (end - start).days
    except:
        return None

df_clean['days_running_real'] = df_clean.apply(calc_days, axis=1)

# Select clean output columns
output_cols = [
    'page_name_real', 'page_id_real', 'ad_copy', 'headline', 'cta',
    'landing_page', 'creative_type', 'start_date_real', 'end_date_real',
    'days_running_real', 'is_active_real', 'publisher_real',
    'num_variations', 'ad_library_url', 'raw_json'
]

# Add ad_library_url from raw_json
df_clean['ad_library_url'] = df_clean['raw_json'].apply(
    lambda x: extract_field(x, 'ad_library_url')
)

# Save
out_path = 'lab/stage2/marketing/output/coursiv_ads_clean.csv'
df_clean[output_cols].to_csv(out_path, index=False)

print(f"Clean Coursiv ads saved: {out_path}")
print(f"Total: {len(df_clean)} ads")
print(f"\nBy page:")
print(df_clean['page_name_real'].value_counts())
print(f"\nCreative type breakdown:")
print(df_clean['creative_type'].value_counts())
print(f"\nActive vs inactive:")
print(df_clean['is_active_real'].value_counts())
print(f"\nDays running (for ads with dates):")
days = df_clean['days_running_real'].dropna()
if len(days) > 0:
    print(f"  Min: {days.min():.0f} days")
    print(f"  Max: {days.max():.0f} days")
    print(f"  Median: {days.median():.0f} days")
    print(f"  Ads running 14+ days: {len(days[days >= 14])}")
    print(f"  Ads running 30+ days: {len(days[days >= 30])}")
