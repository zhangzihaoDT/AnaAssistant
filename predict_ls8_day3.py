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

ls8_start = pd.to_datetime(time_periods['LS8']['start'])
ls8_end = pd.to_datetime(time_periods['LS8']['end'])
window_end_excl = ls8_end + pd.Timedelta(days=1)

df_ls8 = df[df['model'] == 'LS8']
mask_time = (df_ls8['intention_payment_time'].notna()) & \
            (df_ls8['intention_payment_time'] >= ls8_start) & \
            (df_ls8['intention_payment_time'] < window_end_excl)
mask_retained = df_ls8['intention_refund_time'].isna() | (df_ls8['intention_refund_time'] > window_end_excl)

ls8_retained_df = df_ls8.loc[mask_time & mask_retained, ['order_number', 'intention_payment_time']].dropna().drop_duplicates(subset=['order_number'])
ls8_retained_df['days_from_start'] = (ls8_retained_df['intention_payment_time'].dt.normalize() - ls8_start.normalize()).dt.days

ls8_top3_orders = ls8_retained_df[ls8_retained_df['days_from_start'] < 3]['order_number'].nunique()
ls8_top7_orders = ls8_retained_df[ls8_retained_df['days_from_start'] < 7]['order_number'].nunique()

historical_data = {
    "CM0": {"presale_top3_retained": 3907, "presale_top7_retained": 5178, "total_retained": 24399, "total_locks": 11577, "listing_top3_locks": 3837},
    "DM0": {"presale_top3_retained": 3471, "presale_top7_retained": 4201, "total_retained": 14845, "total_locks": 2664, "listing_top3_locks": 1444},
    "CM1": {"presale_top3_retained": 2761, "presale_top7_retained": 4099, "total_retained": 21113, "total_locks": 5961, "listing_top3_locks": 2923},
    "DM1": {"presale_top3_retained": 3083, "presale_top7_retained": 4260, "total_retained": 14991, "total_locks": 4487, "listing_top3_locks": 2055},
    "CM2": {"presale_top3_retained": 7297, "presale_top7_retained": 9856, "total_retained": 49993, "total_locks": 19660, "listing_top3_locks": 8154},
    "LS9": {"presale_top3_retained": 4376, "presale_top7_retained": 6389, "total_retained": 8849, "total_locks": 3319, "listing_top3_locks": 2133},
}

print(f"\n【已知事实】LS8 预售前3日留存小订数: {ls8_top3_orders} | 前7日留存小订数: {ls8_top7_orders}")
print("\n--- 预测 LS8 上市后【前3日】锁单数 (基于【全周期30日总锁单】倒推) ---")
for model_name, stats in historical_data.items():
    top3_ratio = stats['presale_top3_retained'] / stats['total_retained']
    top7_ratio = stats['presale_top7_retained'] / stats['total_retained']
    conv_30d = stats['total_locks'] / stats['total_retained']
    
    pred_total_avg = ((ls8_top3_orders / top3_ratio) + (ls8_top7_orders / top7_ratio)) / 2
    pred_locks_30d = pred_total_avg * conv_30d
    
    listing_top3_ratio = stats['listing_top3_locks'] / stats['total_locks']
    pred_listing_top3_locks = pred_locks_30d * listing_top3_ratio
    
    print(f"[对标 {model_name}]")
    print(f"  -> 预测 LS8 最终累计留存小订: {int(pred_total_avg)} | 预测 30日总锁单: {int(pred_locks_30d)}")
    print(f"  -> 假定 上市前3日锁单 占 30日总锁单 的比例: {listing_top3_ratio*100:.1f}%")
    print(f"  => 预测 LS8 上市后前3日总锁单: {int(pred_listing_top3_locks)}")

X = np.array([v['total_locks'] for v in historical_data.values()])
Y = np.array([v['listing_top3_locks'] for v in historical_data.values()])
slope, intercept = np.polyfit(X, Y, 1)

print("\n--- 基于一元线性回归模型 (预测30日总锁单数 -> 上市后前3日锁单数) ---")
print(f"方程: 上市前3日锁单 = {slope:.2f} * 30日总锁单数 + {intercept:.2f}")
pred_30d_locks_mean = np.mean([
    (((ls8_top3_orders / (v['presale_top3_retained']/v['total_retained'])) + 
      (ls8_top7_orders / (v['presale_top7_retained']/v['total_retained']))) / 2) * 
    (v['total_locks']/v['total_retained'])
    for v in historical_data.values()
])
pred_reg = slope * pred_30d_locks_mean + intercept
print(f"输入预估30日总锁单均值: {int(pred_30d_locks_mean)}")
print(f"-> 预测 LS8 上市后前3日锁单: {int(pred_reg)}")