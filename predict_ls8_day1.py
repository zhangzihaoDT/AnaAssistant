import numpy as np

# 已知事实
ls8_top3_orders = 6870

# 历史统计数据
# presale_top3_retained: 预售前3日留存小订
# listing_top3_locks: 上市前3日锁单数
# locks_from_presale_top3: 上市前3日锁单中，来自于预售前3日小订的数量
# listing_day1_locks: 上市首日(Day1)锁单数
historical_data = {
    "CM0": {"presale_top3_retained": 3907, "listing_top3_locks": 3837, "locks_from_presale_top3": 588, "listing_day1_locks": 2236},
    "DM0": {"presale_top3_retained": 3471, "listing_top3_locks": 1444, "locks_from_presale_top3": 358, "listing_day1_locks": 761},
    "CM1": {"presale_top3_retained": 2761, "listing_top3_locks": 2923, "locks_from_presale_top3": 683, "listing_day1_locks": 1712},
    "DM1": {"presale_top3_retained": 3083, "listing_top3_locks": 2055, "locks_from_presale_top3": 633, "listing_day1_locks": 1322},
    "CM2": {"presale_top3_retained": 7297, "listing_top3_locks": 8154, "locks_from_presale_top3": 1934, "listing_day1_locks": 5977},
    "LS9": {"presale_top3_retained": 4376, "listing_top3_locks": 2133, "locks_from_presale_top3": 1035, "listing_day1_locks": 1782},
}

print(f"\n【已知事实】LS8 预售前3日留存小订数: {ls8_top3_orders}")
print("\n--- 预测 LS8 上市后【首日(Day1)】锁单数 (基于各历史车型实际表现) ---")
for model_name, stats in historical_data.items():
    # 1. 预测前3日锁单数
    conv = stats['locks_from_presale_top3'] / stats['presale_top3_retained']
    pct = stats['locks_from_presale_top3'] / stats['listing_top3_locks']
    pred_top3_locks = (ls8_top3_orders * conv) / pct
    
    # 2. 提取历史首日占比
    day1_ratio = stats['listing_day1_locks'] / stats['listing_top3_locks']
    
    # 3. 预测首日锁单数
    pred_day1_locks = pred_top3_locks * day1_ratio
    
    print(f"[对标 {model_name}]")
    print(f"  预测 LS8 上市后前3日总锁单: {int(pred_top3_locks)}")
    print(f"  假定 历史上市首日占前3日锁单的比例: {day1_ratio*100:.1f}%")
    print(f"  预测 LS8 上市首日(Day1)锁单数: {int(pred_day1_locks)}")

# 线性回归部分 (预售前3日小订数 -> 上市首日锁单数)
X = np.array([v['presale_top3_retained'] for v in historical_data.values()])
Y = np.array([v['listing_day1_locks'] for v in historical_data.values()])
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

print("\n--- 基于一元线性回归模型 (预售前3日小订数 -> 上市首日锁单数) ---")
print(f"方程: 上市首日锁单 = {slope:.2f} * 预售前3日小订数 + {intercept:.2f} (R^2 = {r_value**2:.2f})")
pred_reg = slope * ls8_top3_orders + intercept
print(f"预测 LS8 上市首日锁单数: {int(pred_reg)}")
