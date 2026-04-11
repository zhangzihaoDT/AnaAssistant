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

# Calculate LS8 proper retained orders for Top 3 and Top 7 days
ls8_start = pd.to_datetime(time_periods['LS8']['start'])
ls8_end = pd.to_datetime(time_periods['LS8']['end'])
window_end_excl = ls8_end + pd.Timedelta(days=1)

df_ls8 = df[df['model'] == 'LS8']
mask_time = (df_ls8['intention_payment_time'].notna()) & \
            (df_ls8['intention_payment_time'] >= ls8_start) & \
            (df_ls8['intention_payment_time'] < window_end_excl)
# 真正意义上的留存小订：在整个预售期结束前未退款
mask_retained = df_ls8['intention_refund_time'].isna() | (df_ls8['intention_refund_time'] > window_end_excl)

ls8_retained_df = df_ls8.loc[mask_time & mask_retained, ['order_number', 'intention_payment_time']].dropna().drop_duplicates(subset=['order_number'])
ls8_retained_df['days_from_start'] = (ls8_retained_df['intention_payment_time'].dt.normalize() - ls8_start.normalize()).dt.days

ls8_top3_orders = ls8_retained_df[ls8_retained_df['days_from_start'] < 3]['order_number'].nunique()
ls8_top7_orders = ls8_retained_df[ls8_retained_df['days_from_start'] < 7]['order_number'].nunique()
ls8_current_total = ls8_retained_df['order_number'].nunique()

# Historical stats
historical_data = {
    "CM0": {"top3_retained": 3907, "top7_retained": 5178, "total_retained": 24399, "total_locks": 11577},
    "DM0": {"top3_retained": 3471, "top7_retained": 4201, "total_retained": 14845, "total_locks": 2664},
    "CM1": {"top3_retained": 2761, "top7_retained": 4099, "total_retained": 21113, "total_locks": 5961},
    "DM1": {"top3_retained": 3083, "top7_retained": 4260, "total_retained": 14991, "total_locks": 4487},
    "CM2": {"top3_retained": 7297, "top7_retained": 9856, "total_retained": 49993, "total_locks": 19660},
    "LS9": {"top3_retained": 4376, "top7_retained": 6389, "total_retained": 8849, "total_locks": 3319},
}

print(f"\n【LS8 当前状态事实】(注: 预售期尚未结束)")
print(f"  - 前3日留存小订数: {ls8_top3_orders}")
print(f"  - 前7日留存小订数: {ls8_top7_orders}")
print(f"  - 当前累计留存小订数: {ls8_current_total}")

print("\n--- 预测 LS8 【整个预售周期】的小订总数及【上市后30日】锁单数 ---")
for model_name, stats in historical_data.items():
    top3_ratio = stats['top3_retained'] / stats['total_retained']
    top7_ratio = stats['top7_retained'] / stats['total_retained']
    conv_30d = stats['total_locks'] / stats['total_retained']
    
    # 分别基于前3日和前7日推演整个周期的总留存小订
    pred_total_from_top3 = ls8_top3_orders / top3_ratio
    pred_total_from_top7 = ls8_top7_orders / top7_ratio
    pred_total_avg = (pred_total_from_top3 + pred_total_from_top7) / 2
    
    # 预测30日锁单
    pred_locks = pred_total_avg * conv_30d
    
    print(f"\n[对标 {model_name}]")
    print(f"  历史特征 -> 前3日小订占比: {top3_ratio*100:.1f}% | 前7日小订占比: {top7_ratio*100:.1f}% | 30日转化率: {conv_30d*100:.1f}%")
    print(f"  推演过程 -> 按前3日占比预测总小订: {int(pred_total_from_top3)} | 按前7日占比预测总小订: {int(pred_total_from_top7)}")
    print(f"  综合推演 -> 预测 LS8 最终累计留存小订: {int(pred_total_avg)}")
    print(f"  最终预测 -> 预测 LS8 上市后 30日总锁单数: {int(pred_locks)}")

# 回归预测
X_top7 = np.array([v['top7_retained'] for v in historical_data.values()])
Y_total_locks = np.array([v['total_locks'] for v in historical_data.values()])
slope, intercept = np.polyfit(X_top7, Y_total_locks, 1)
r_matrix = np.corrcoef(X_top7, Y_total_locks)
r_squared = r_matrix[0, 1]**2

print("\n--- 基于一元线性回归模型 (前7日小订数 -> 30日总锁单数) ---")
print(f"方程: 30日总锁单 = {slope:.2f} * 前7日小订数 + {intercept:.2f} (R^2 = {r_squared:.2f})")
pred_reg = slope * ls8_top7_orders + intercept
print(f"预测 LS8 30日总锁单数: {int(pred_reg)}")
