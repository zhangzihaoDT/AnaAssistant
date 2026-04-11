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
    intention_dist_results = []

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
                
            # === 新增：分析这些锁单订单的“小订时间”在预售周期的分布 ===
            # 将 intention_payment_time 关联回来
            tmp_intention = df_model[['order_number', 'intention_payment_time']].dropna(subset=['order_number']).drop_duplicates(subset=['order_number'])
            locked_retained_df = locked_retained_df.merge(tmp_intention, on='order_number', how='left')
            
            # 计算距离预售开始的天数，以及距离预售结束的天数
            locked_retained_df['intention_days_from_start'] = (locked_retained_df['intention_payment_time'].dt.normalize() - start_day.normalize()).dt.days
            locked_retained_df['intention_days_to_end'] = (end_day.normalize() - locked_retained_df['intention_payment_time'].dt.normalize()).dt.days
            
            day1_cnt = (locked_retained_df['intention_days_from_start'] == 0).sum()
            day2_cnt = (locked_retained_df['intention_days_from_start'] == 1).sum()
            day3_cnt = (locked_retained_df['intention_days_from_start'] == 2).sum()
            top3_cnt = day1_cnt + day2_cnt + day3_cnt
            
            last_day3_cnt = (locked_retained_df['intention_days_to_end'] == 2).sum()
            last_day2_cnt = (locked_retained_df['intention_days_to_end'] == 1).sum()
            last_day1_cnt = (locked_retained_df['intention_days_to_end'] == 0).sum()
            
            middle_cnt = locked_count - top3_cnt - last_day1_cnt - last_day2_cnt - last_day3_cnt
            
            def pct(c):
                return f"{c/locked_count*100:.1f}%" if locked_count > 0 else "0.0%"
            
            intention_dist_results.append({
                "车型": model,
                "总锁单": locked_count,
                "Day1": int(day1_cnt), "Day1占比": pct(day1_cnt),
                "Day2": int(day2_cnt), "Day2占比": pct(day2_cnt),
                "Day3": int(day3_cnt), "Day3占比": pct(day3_cnt),
                "前3日累计": int(top3_cnt), "前3日占比": pct(top3_cnt),
                "中间期": int(middle_cnt), "中间占比": pct(middle_cnt),
                "倒数Day3": int(last_day3_cnt), "倒数Day3占比": pct(last_day3_cnt),
                "倒数Day2": int(last_day2_cnt), "倒数Day2占比": pct(last_day2_cnt),
                "倒数Day1(上市)": int(last_day1_cnt), "倒数Day1占比": pct(last_day1_cnt)
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
        
        # 汇总展示特定周期 (Day1, Day2, Day3, 前3日累计, 前7日累计)
        summary_rows = []
        summary_labels = ["Day1", "Day2", "Day3", "前3日累计", "前7日累计"]
        
        for model in pivot_count.columns:
            counts = pivot_count[model]
            pcts = pivot_pct[model]
            
            d1_cnt, d1_pct = counts[1], pcts[1]
            d2_cnt, d2_pct = counts[2], pcts[2]
            d3_cnt, d3_pct = counts[3], pcts[3]
            
            top3_cnt = counts.loc[1:3].sum()
            top3_pct = pcts.loc[1:3].sum()
            
            top7_cnt = counts.loc[1:7].sum()
            top7_pct = pcts.loc[1:7].sum()
            
            summary_rows.append({
                "车型": model,
                "Day1_锁单": int(d1_cnt), "Day1_占比": f"{d1_pct*100:.1f}%",
                "Day2_锁单": int(d2_cnt), "Day2_占比": f"{d2_pct*100:.1f}%",
                "Day3_锁单": int(d3_cnt), "Day3_占比": f"{d3_pct*100:.1f}%",
                "前3日累计_锁单": int(top3_cnt), "前3日累计_占比": f"{top3_pct*100:.1f}%",
                "前7日累计_锁单": int(top7_cnt), "前7日累计_占比": f"{top7_pct*100:.1f}%"
            })
            
        summary_df = pd.DataFrame(summary_rows)
        
        print("\n--- 各车型上市后核心周期（前1-3天及前7日）锁单分布与转化 ---")
        print(summary_df.to_string(index=False))

    if intention_dist_results:
        intention_dist_df = pd.DataFrame(intention_dist_results)
        print("\n--- 各车型上市后30日锁单的小订时间在预售周期内的分布 ---")
        
        # 调整列顺序便于查看
        cols = [
            "车型", "总锁单", 
            "Day1", "Day1占比", "Day2", "Day2占比", "Day3", "Day3占比", "前3日累计", "前3日占比",
            "中间期", "中间占比",
            "倒数Day3", "倒数Day3占比", "倒数Day2", "倒数Day2占比", "倒数Day1(上市)", "倒数Day1占比"
        ]
        print(intention_dist_df[cols].to_string(index=False))

if __name__ == "__main__":
    main()
