import pandas as pd
import json
import numpy as np

# Paths
data_path = '/Users/zihao_/Documents/coding/dataset/formatted/order_data.parquet'
biz_def_path = '/Users/zihao_/Documents/github/26W06_Tool_calls/schema/business_definition.json'

# Load data
df = pd.read_parquet(data_path)
with open(biz_def_path, 'r') as f:
    biz_def = json.load(f)

time_periods = biz_def['time_periods']

def get_model(product_name):
    if pd.isna(product_name): return '其他'
    product_name = str(product_name)
    if '新一代' in product_name and 'LS6' in product_name: return 'CM2'
    elif '全新' in product_name and 'LS6' in product_name: return 'CM1'
    elif 'LS6' in product_name and '全新' not in product_name and '新一代' not in product_name: return 'CM0'
    elif '全新' in product_name and 'L6' in product_name: return 'DM1'
    elif 'L6' in product_name and '全新' not in product_name: return 'DM0'
    elif 'LS8' in product_name: return 'LS8'
    elif 'LS9' in product_name: return 'LS9'
    return '其他'

df['model'] = df['product_name'].apply(get_model)

target_models = ["CM0", "DM0", "CM1", "DM1", "CM2", "LS9", "LS8"]
stats = {}

for model in target_models:
    if model not in time_periods: continue
    start_day = pd.to_datetime(time_periods[model]['start'])
    end_day = pd.to_datetime(time_periods[model]['end'])
    window_end_excl = end_day + pd.Timedelta(days=1)
    
    df_model = df[df['model'] == model]
    mask_time = (df_model['intention_payment_time'].notna()) & \
                (df_model['intention_payment_time'] >= start_day) & \
                (df_model['intention_payment_time'] < window_end_excl)
    mask_retained = df_model['intention_refund_time'].isna() | \
                    (df_model['intention_refund_time'] > window_end_excl)
    
    retained_df = df_model.loc[mask_time & mask_retained, ['order_number', 'intention_payment_time']].dropna().drop_duplicates(subset=['order_number'])
    total_retained = retained_df['order_number'].nunique()
    
    retained_df['days_from_start'] = (retained_df['intention_payment_time'].dt.normalize() - start_day.normalize()).dt.days
    
    top3_retained = retained_df[retained_df['days_from_start'] < 3]['order_number'].nunique()
    top7_retained = retained_df[retained_df['days_from_start'] < 7]['order_number'].nunique()
    
    stats[model] = {
        "total_retained": total_retained,
        "top3_retained": top3_retained,
        "top7_retained": top7_retained
    }

print(json.dumps(stats, indent=2))
