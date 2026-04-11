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

# Calculate LS8 Top 3 days intention orders
ls8_start = pd.to_datetime(time_periods['LS8']['start'])
ls8_end = pd.to_datetime(time_periods['LS8']['end'])
ls8_window_end = ls8_start + pd.Timedelta(days=3)

df_ls8 = df[df['model'] == 'LS8']
mask_time = (df_ls8['intention_payment_time'].notna()) & \
            (df_ls8['intention_payment_time'] >= ls8_start) & \
            (df_ls8['intention_payment_time'] < ls8_window_end)
mask_retained = df_ls8['intention_refund_time'].isna() | (df_ls8['intention_refund_time'] > ls8_window_end)
ls8_day3_orders = df_ls8.loc[mask_time & mask_retained, 'order_number'].dropna().drop_duplicates().nunique()

# Historical stats
historical_data = {
    "CM0": {"presale_day3_retained": 3907, "listing_day3_locks": 3837, "locks_from_presale_day3": 588, "total_retained": 24399, "last_day2_retained": 2041, "last_day1_retained": 6517},
    "DM0": {"presale_day3_retained": 3471, "listing_day3_locks": 1444, "locks_from_presale_day3": 358, "total_retained": 14845, "last_day2_retained": 1583, "last_day1_retained": 3539},
    "CM1": {"presale_day3_retained": 2761, "listing_day3_locks": 2923, "locks_from_presale_day3": 683, "total_retained": 21113, "last_day2_retained": 2665, "last_day1_retained": 3737},
    "DM1": {"presale_day3_retained": 3083, "listing_day3_locks": 2055, "locks_from_presale_day3": 633, "total_retained": 14991, "last_day2_retained": 1147, "last_day1_retained": 3750},
    "CM2": {"presale_day3_retained": 7297, "listing_day3_locks": 8154, "locks_from_presale_day3": 1934, "total_retained": 49993, "last_day2_retained": 7731, "last_day1_retained": 14342},
    "LS9": {"presale_day3_retained": 4376, "listing_day3_locks": 2133, "locks_from_presale_day3": 1035, "total_retained": 8849, "last_day2_retained": 893, "last_day1_retained": 1567},
}

print(f"\n【已知事实】LS8 预售前3日留存小订数: {ls8_day3_orders}")
print("\n--- 预测 LS8 上市后【前3日】锁单数 (基于各历史车型实际表现) ---")
for model_name, stats in historical_data.items():
    conv = stats['locks_from_presale_day3'] / stats['presale_day3_retained']
    pct = stats['locks_from_presale_day3'] / stats['listing_day3_locks']
    pred_locks = (ls8_day3_orders * conv) / pct
    
    scale_factor = ls8_day3_orders / stats['presale_day3_retained']
    req_total = stats['total_retained'] * scale_factor
    req_last_day2 = stats['last_day2_retained'] * scale_factor
    req_last_day1 = stats['last_day1_retained'] * scale_factor
    
    print(f"[对标 {model_name}]")
    print(f"  假定 预售前3日小订 在 上市前3日的转化率: {conv*100:.1f}%")
    print(f"  假定 上市前3日锁单中 预售前3日小订占比: {pct*100:.1f}%")
    print(f"  预测 LS8 上市后前3日总锁单: {int(pred_locks)}")
    print(f"  -> 隐含条件：若完全对标 {model_name} 的客群结构，LS8 需在整个预售期达成【累计留存小订】: {int(req_total)} 单")
    print(f"  -> 隐含条件：其中倒数Day2小订数应达: {int(req_last_day2)} 单")
    print(f"  -> 隐含条件：其中上市当天(倒数Day1)小订数应达: {int(req_last_day1)} 单")

X = np.array([v['presale_day3_retained'] for v in historical_data.values()])
Y = np.array([v['listing_day3_locks'] for v in historical_data.values()])
n = len(X)
m_x = np.mean(X)
m_y = np.mean(Y)
ss_xy = np.sum(Y*X) - n*m_y*m_x
ss_xx = np.sum(X*X) - n*m_x*m_x
slope = ss_xy / ss_xx
intercept = m_y - slope*m_x

r_num = (n*np.sum(X*Y) - np.sum(X)*np.sum(Y))
r_den = np.sqrt((n*np.sum(X**2) - np.sum(X)**2)*(n*np.sum(Y**2) - np.sum(Y)**2))
r_value = r_num / r_den

print("\n--- 基于一元线性回归模型 (预售前3日小订数 -> 上市后前3日锁单数) ---")
print(f"方程: 上市前3日锁单 = {slope:.2f} * 预售前3日小订数 + {intercept:.2f} (R^2 = {r_value**2:.2f})")
pred_reg = slope * ls8_day3_orders + intercept
print(f"预测 LS8 上市后前3日锁单: {int(pred_reg)}")
