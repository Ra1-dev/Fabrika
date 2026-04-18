import pandas as pd
import json

df = pd.read_csv('lab/stage2/marketing/output/meta_ads.csv')

df['page_name_real'] = df['raw_json'].apply(
    lambda x: json.loads(x).get('page_name', '') if pd.notna(x) else ''
)

print("Page name distribution (top 20):")
print(df['page_name_real'].value_counts().head(20))
print(f'\nTotal rows: {len(df)}')
print(f"Coursiv-only rows: {len(df[df['page_name_real'] == 'Coursiv'])}")
