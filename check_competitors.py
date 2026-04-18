import pandas as pd

df = pd.read_csv('lab/stage2/marketing/output/competitor_signals.csv')
print('Total advertisers:', len(df))

print('\nAll with 2+ keyword matches:')
cols = ['page_name','keyword_count','total_ads','max_days_running','keywords_matched']
print(df[df['keyword_count'] >= 2][cols].to_string())

print('\nTop 20 by total ads (single keyword):')
cols2 = ['page_name','total_ads','max_days_running','sample_ad_copy']
print(df[df['keyword_count'] == 1].nlargest(20, 'total_ads')[cols2].to_string())
