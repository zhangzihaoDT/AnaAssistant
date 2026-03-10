import pandas as pd
from pathlib import Path

file_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
print(f"Loading data from: {file_path}")

df = pd.read_parquet(file_path)

if not pd.api.types.is_datetime64_any_dtype(df['invoice_upload_time']):
    df['invoice_upload_time'] = pd.to_datetime(df['invoice_upload_time'], errors='coerce')

target_date_start = pd.Timestamp("2026-03-09")
target_date_end = pd.Timestamp("2026-03-10")

# Base filter: Time and Series
cond_base = (
    (df['invoice_upload_time'] >= target_date_start) & 
    (df['invoice_upload_time'] < target_date_end) & 
    (df['series'] == 'LS6')
)

df_base = df[cond_base]
print(f"\nTotal LS6 yesterday: {len(df_base)}")

# Check 52
cond_52 = df_base['product_name'].str.contains("52", na=False)
count_52 = len(df_base[cond_52])
print(f"Contains '52': {count_52}")

# Check 66
cond_66 = df_base['product_name'].str.contains("66", na=False)
count_66 = len(df_base[cond_66])
print(f"Contains '66': {count_66}")

# Check 52 OR 66
cond_or = df_base['product_name'].str.contains("52|66", na=False, regex=True)
count_or = len(df_base[cond_or])
print(f"Contains '52' OR '66' (Real Extended Range): {count_or}")

print("\nBreakdown by product_name:")
print(df_base['product_name'].value_counts())
