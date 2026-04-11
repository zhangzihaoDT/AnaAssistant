import pandas as pd
import json
import warnings

# 忽略 pandas 的一些警告输出
warnings.filterwarnings('ignore')

def main():
    # 1. 配置数据源路径
    data_path = '/Users/zihao_/Documents/coding/dataset/formatted/order_data.parquet'
    biz_def_path = '/Users/zihao_/Documents/github/26W06_Tool_calls/schema/business_definition.json'

    # 2. 加载数据
    print("正在加载数据...")
    df = pd.read_parquet(data_path)
    with open(biz_def_path, 'r', encoding='utf-8') as f:
        biz_def = json.load(f)

    time_periods = biz_def.get('time_periods', {})

    # 3. 根据 business_definition 匹配车型
    def get_model(product_name):
        if pd.isna(product_name):
            return '其他'
        product_name = str(product_name)
        # 严格按照业务定义映射
        if '新一代' in product_name and 'LS6' in product_name:
            return 'CM2'
        elif '全新' in product_name and 'LS6' in product_name:
            return 'CM1'
        elif 'LS6' in product_name and '全新' not in product_name and '新一代' not in product_name:
            return 'CM0'
        elif '全新' in product_name and 'L6' in product_name:
            return 'DM1'
        elif 'L6' in product_name and '全新' not in product_name:
            return 'DM0'
        elif 'LS8' in product_name:
            return 'LS8'
        elif 'LS9' in product_name:
            return 'LS9'
        elif 'LS7' in product_name:
            return 'LS7'
        elif 'L7' in product_name:
            return 'L7'
        else:
            return '其他'

    df['model'] = df['product_name'].apply(get_model)

    # 4. 计算指定车型的预售期留存小订数
    target_models = ["CM0", "DM0", "CM1", "DM1", "CM2", "LS9", "LS8"]
    results = []
    distribution_results = []

    for model in target_models:
        if model not in time_periods:
            print(f"警告：车型 {model} 不在预售周期定义中，跳过。")
            continue
            
        start_day = pd.to_datetime(time_periods[model]['start'])
        end_day = pd.to_datetime(time_periods[model]['end'])
        
        # 预售结束日期的次日0点作为时间窗口的排他性边界 (exclusive)
        window_end_excl = end_day + pd.Timedelta(days=1)
        
        # 筛选该车型
        df_model = df[df['model'] == model]
        
        # 条件1：小订支付时间在预售周期内 [start, end]
        mask_time = (df_model['intention_payment_time'].notna()) & \
                    (df_model['intention_payment_time'] >= start_day) & \
                    (df_model['intention_payment_time'] < window_end_excl)
        
        # 条件2：留存小订（未发生小订退款，或者退款时间晚于预售窗口期结束时间）
        mask_retained = df_model['intention_refund_time'].isna() | \
                        (df_model['intention_refund_time'] > window_end_excl)
        
        # 应用条件并去重 order_number
        retained_orders = df_model.loc[mask_time & mask_retained, 'order_number'].dropna().drop_duplicates()
        retained_count = int(retained_orders.nunique())
        
        # 计算上市后30日锁单数与转化率
        listing_day = end_day
        if model == "CM0":
            listing_day = listing_day + pd.Timedelta(days=1)
            
        finish_str = time_periods[model].get('finish')
        finish_day = pd.to_datetime(finish_str) if finish_str else listing_day
        finish_excl = finish_day + pd.Timedelta(days=1)
        
        lock_30d_end_excl = min(listing_day + pd.Timedelta(days=31), finish_excl)
        
        mask_lock = (df_model['lock_time'].notna()) & \
                    (df_model['lock_time'] >= listing_day) & \
                    (df_model['lock_time'] < lock_30d_end_excl)
                    
        locked_orders_30d = df_model.loc[mask_lock, ['order_number', 'lock_time']].dropna(subset=['order_number']).drop_duplicates(subset=['order_number'])
        
        # 留存小订中发生了上市后30日锁单的数量
        locked_retained_df = locked_orders_30d[locked_orders_30d['order_number'].isin(retained_orders)].copy()
        locked_count = int(locked_retained_df['order_number'].nunique())
        
        if locked_count > 0:
            locked_retained_df['days_since_listing'] = (locked_retained_df['lock_time'] - listing_day).dt.days
            daily_counts = locked_retained_df.groupby('days_since_listing')['order_number'].nunique()
            for day_idx, count in daily_counts.items():
                distribution_results.append({
                    "车型": model,
                    "上市后第N天": int(day_idx) + 1,  # 第1天即为上市当天
                    "锁单数": int(count),
                    "占30日锁单比例": float(count) / locked_count
                })
        
        conversion_rate = (locked_count / retained_count * 100) if retained_count > 0 else 0.0
        
        results.append({
            "车型": model,
            "预售开始时间": time_periods[model]['start'],
            "预售结束时间": time_periods[model]['end'],
            "留存小订数": retained_count,
            "上市后30日锁单数": locked_count,
            "转化率": f"{conversion_rate:.1f}%"
        })

    # 5. 输出结果
    results_df = pd.DataFrame(results)
    print("\n--- 各车型预售周期留存小订数计算结果 ---")
    print(results_df.to_string(index=False))

    if distribution_results:
        dist_df = pd.DataFrame(distribution_results)
        
        # 补全1-30天的数据以防有的天数为0
        all_days = list(range(1, 32))
        pivot_count = dist_df.pivot_table(index="上市后第N天", columns="车型", values="锁单数", fill_value=0).reindex(all_days, fill_value=0)
        pivot_pct = dist_df.pivot_table(index="上市后第N天", columns="车型", values="占30日锁单比例", fill_value=0).reindex(all_days, fill_value=0)
        
        # 格式化百分比
        pivot_pct_str = pivot_pct.map(lambda x: f"{x*100:.1f}%")
        
        print("\n--- 各车型上市后30日内每日锁单数分布 ---")
        print(pivot_count.to_string())
        
        print("\n--- 各车型上市后30日内每日锁单占比分布 ---")
        print(pivot_pct_str.to_string())

if __name__ == "__main__":
    main()
