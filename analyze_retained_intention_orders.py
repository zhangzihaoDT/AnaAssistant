"""
该脚本主要用于分析历史车型的预售周期留存小订数及锁单转化情况，并据此推演 LS8 的留存小订与最终锁单量。
主要输出包含以下分析模块：
1. 留存小订总数及总体转化率
2. 上市后 1-3 天及前 7 日锁单分布
3. 上市后 30 日锁单的“预售时间”分布
4. 预售各阶段留存小订基数及转化率
5. 留存小订的预售时间分布
6. LS8 留存小订数推演
7. LS8 上市后30日锁单数推演
8. (补充资料) 上市后【前3日】锁单的预售时间分布
9. (补充资料) 上市后【Day1】锁单的预售时间分布
"""

import pandas as pd
import json
import warnings

# 忽略 pandas 的一些警告输出
warnings.filterwarnings('ignore')

def main():
    # 1. 配置数据源路径
    import pathlib
    data_path = '/Users/zihao_/Documents/coding/dataset/formatted/order_data.parquet'
    biz_def_path = str(pathlib.Path(__file__).resolve().parent / 'schema' / 'business_definition.json')

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
    target_models = ["CM0", "DM0", "CM1", "DM1", "CM2", "LS9"]
    results = []
    lock_30d_results = []
    distribution_results = []
    intention_dist_results = []
    intention_group_conversion_results = []
    top3_locked_intention_dist_results = []
    day1_locked_intention_dist_results = []
    retained_intention_dist_results = []

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
        
        locked_orders_30d_count = int(locked_orders_30d['order_number'].nunique())
        retained_lock_ratio = (locked_count / locked_orders_30d_count * 100) if locked_orders_30d_count > 0 else 0.0
        
        # 计算首日 (Day1) 的对应数据
        lock_day1_end_excl = listing_day + pd.Timedelta(days=1)
        mask_lock_day1 = (df_model['lock_time'].notna()) & \
                         (df_model['lock_time'] >= listing_day) & \
                         (df_model['lock_time'] < lock_day1_end_excl)
        
        locked_orders_day1 = df_model.loc[mask_lock_day1, ['order_number']].dropna(subset=['order_number']).drop_duplicates(subset=['order_number'])
        locked_orders_day1_count = int(locked_orders_day1['order_number'].nunique())
        
        locked_retained_day1_df = locked_orders_day1[locked_orders_day1['order_number'].isin(retained_orders)]
        locked_day1_count = int(locked_retained_day1_df['order_number'].nunique())
        
        retained_lock_ratio_day1 = (locked_day1_count / locked_orders_day1_count * 100) if locked_orders_day1_count > 0 else 0.0
        
        lock_30d_results.append({
            "车型": model,
            "上市开始时间": listing_day.strftime('%Y-%m-%d'),
            "上市后30日锁单数": locked_orders_30d_count,
            "上市后30日留存小订转化数": locked_count,
            "留存小订转化占比": f"{retained_lock_ratio:.1f}%",
            "上市后首日锁单数": locked_orders_day1_count,
            "上市后首日留存小订转化数": locked_day1_count,
            "首日留存小订转化占比": f"{retained_lock_ratio_day1:.1f}%"
        })
        
        # === 新增/扩展：计算预售周期各阶段的【留存小订总数】和【对应的锁单转化率】 ===
        # 将留存小订 (retained_orders) 结合意向支付时间提取出来
        retained_intention_df = df_model.loc[mask_time & mask_retained, ['order_number', 'intention_payment_time']].dropna(subset=['order_number']).drop_duplicates(subset=['order_number'])
        
        retained_intention_df['intention_days_from_start'] = (retained_intention_df['intention_payment_time'].dt.normalize() - start_day.normalize()).dt.days
        retained_intention_df['intention_days_to_end'] = (end_day.normalize() - retained_intention_df['intention_payment_time'].dt.normalize()).dt.days
        
        # 各阶段留存小订基数
        base_day1_cnt = (retained_intention_df['intention_days_from_start'] == 0).sum()
        base_day2_cnt = (retained_intention_df['intention_days_from_start'] == 1).sum()
        base_day3_cnt = (retained_intention_df['intention_days_from_start'] == 2).sum()
        base_top3_cnt = base_day1_cnt + base_day2_cnt + base_day3_cnt
        
        base_last_day3_cnt = (retained_intention_df['intention_days_to_end'] == 2).sum()
        base_last_day2_cnt = (retained_intention_df['intention_days_to_end'] == 1).sum()
        base_last_day1_cnt = (retained_intention_df['intention_days_to_end'] == 0).sum()
        
        base_middle_cnt = retained_count - base_top3_cnt - base_last_day1_cnt - base_last_day2_cnt - base_last_day3_cnt
        
        def pct_base(c):
            return f"{c/retained_count*100:.1f}%" if retained_count > 0 else "0.0%"
            
        retained_intention_dist_results.append({
            "车型": model,
            "总留存小订": retained_count,
            "Day1": int(base_day1_cnt), "Day1占比": pct_base(base_day1_cnt),
            "前3日累计": int(base_top3_cnt), "前3日占比": pct_base(base_top3_cnt),
            "中间期": int(base_middle_cnt), "中间占比": pct_base(base_middle_cnt),
            "倒数Day2": int(base_last_day3_cnt), "倒数Day2占比": pct_base(base_last_day3_cnt),
            "倒数Day1": int(base_last_day2_cnt), "倒数Day1占比": pct_base(base_last_day2_cnt),
            "倒数Day0(上市当天)": int(base_last_day1_cnt), "倒数Day0占比": pct_base(base_last_day1_cnt)
        })
        
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
                
            # 将 intention_payment_time 关联回锁单的集合
            locked_retained_df = locked_retained_df.merge(retained_intention_df[['order_number', 'intention_days_from_start', 'intention_days_to_end']], on='order_number', how='left')
            
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
                "前3日累计": int(top3_cnt), "前3日占比": pct(top3_cnt),
                "中间期": int(middle_cnt), "中间占比": pct(middle_cnt),
                "倒数Day2": int(last_day3_cnt), "倒数Day2占比": pct(last_day3_cnt),
                "倒数Day1": int(last_day2_cnt), "倒数Day1占比": pct(last_day2_cnt),
                "倒数Day0(上市当天)": int(last_day1_cnt), "倒数Day0占比": pct(last_day1_cnt)
            })
            
            def conv_pct(lock_c, base_c):
                return f"{lock_c/base_c*100:.1f}%" if base_c > 0 else "0.0%"
            
            intention_group_conversion_results.append({
                "车型": model,
                "总体留存小订": retained_count, "总转化率": f"{(locked_count/retained_count*100):.1f}%" if retained_count > 0 else "0.0%",
                "Day1留存小订": int(base_day1_cnt), "Day1转化率": conv_pct(day1_cnt, base_day1_cnt),
                "前3日留存小订": int(base_top3_cnt), "前3日转化率": conv_pct(top3_cnt, base_top3_cnt),
                "中间期留存小订": int(base_middle_cnt), "中间期转化率": conv_pct(middle_cnt, base_middle_cnt),
                "倒数Day2留存小订": int(base_last_day3_cnt), "倒数Day2转化率": conv_pct(last_day3_cnt, base_last_day3_cnt),
                "倒数Day1留存小订": int(base_last_day2_cnt), "倒数Day1转化率": conv_pct(last_day2_cnt, base_last_day2_cnt),
                "倒数Day0(上市当天)留存小订": int(base_last_day1_cnt), "倒数Day0转化率": conv_pct(last_day1_cnt, base_last_day1_cnt)
            })
            
            # === 新增：分析上市后【前3日】锁单订单的“小订时间”在预售周期的分布 ===
            top3_locked_df = locked_retained_df[locked_retained_df['days_since_listing'] < 3].copy()
            locked_count_top3 = int(top3_locked_df['order_number'].nunique())
            
            if locked_count_top3 > 0:
                t3_day1_cnt = (top3_locked_df['intention_days_from_start'] == 0).sum()
                t3_day2_cnt = (top3_locked_df['intention_days_from_start'] == 1).sum()
                t3_day3_cnt = (top3_locked_df['intention_days_from_start'] == 2).sum()
                t3_top3_cnt = t3_day1_cnt + t3_day2_cnt + t3_day3_cnt
                
                t3_last_day3_cnt = (top3_locked_df['intention_days_to_end'] == 2).sum()
                t3_last_day2_cnt = (top3_locked_df['intention_days_to_end'] == 1).sum()
                t3_last_day1_cnt = (top3_locked_df['intention_days_to_end'] == 0).sum()
                
                t3_middle_cnt = locked_count_top3 - t3_top3_cnt - t3_last_day1_cnt - t3_last_day2_cnt - t3_last_day3_cnt
                
                def pct_t3(c):
                    return f"{c/locked_count_top3*100:.1f}%" if locked_count_top3 > 0 else "0.0%"
                
                top3_locked_intention_dist_results.append({
                    "车型": model,
                    "前3日总锁单": locked_count_top3,
                    "Day1": int(t3_day1_cnt), "Day1占比": pct_t3(t3_day1_cnt),
                    "前3日累计": int(t3_top3_cnt), "前3日占比": pct_t3(t3_top3_cnt),
                    "中间期": int(t3_middle_cnt), "中间占比": pct_t3(t3_middle_cnt),
                    "倒数Day2": int(t3_last_day3_cnt), "倒数Day2占比": pct_t3(t3_last_day3_cnt),
                    "倒数Day1": int(t3_last_day2_cnt), "倒数Day1占比": pct_t3(t3_last_day2_cnt),
                    "倒数Day0(上市当天)": int(t3_last_day1_cnt), "倒数Day0占比": pct_t3(t3_last_day1_cnt)
                })
                
            # === 新增：分析上市后【Day1】锁单订单的“小订时间”在预售周期的分布 ===
            day1_locked_df = locked_retained_df[locked_retained_df['days_since_listing'] == 0].copy()
            locked_count_day1 = int(day1_locked_df['order_number'].nunique())
            
            if locked_count_day1 > 0:
                d1_day1_cnt = (day1_locked_df['intention_days_from_start'] == 0).sum()
                d1_day2_cnt = (day1_locked_df['intention_days_from_start'] == 1).sum()
                d1_day3_cnt = (day1_locked_df['intention_days_from_start'] == 2).sum()
                d1_top3_cnt = d1_day1_cnt + d1_day2_cnt + d1_day3_cnt
                
                d1_last_day3_cnt = (day1_locked_df['intention_days_to_end'] == 2).sum()
                d1_last_day2_cnt = (day1_locked_df['intention_days_to_end'] == 1).sum()
                d1_last_day1_cnt = (day1_locked_df['intention_days_to_end'] == 0).sum()
                
                d1_middle_cnt = locked_count_day1 - d1_top3_cnt - d1_last_day1_cnt - d1_last_day2_cnt - d1_last_day3_cnt
                
                def pct_d1(c):
                    return f"{c/locked_count_day1*100:.1f}%" if locked_count_day1 > 0 else "0.0%"
                
                day1_locked_intention_dist_results.append({
                    "车型": model,
                    "Day1总锁单": locked_count_day1,
                    "Day1": int(d1_day1_cnt), "Day1占比": pct_d1(d1_day1_cnt),
                    "前3日累计": int(d1_top3_cnt), "前3日占比": pct_d1(d1_top3_cnt),
                    "中间期": int(d1_middle_cnt), "中间占比": pct_d1(d1_middle_cnt),
                    "倒数Day2": int(d1_last_day3_cnt), "倒数Day2占比": pct_d1(d1_last_day3_cnt),
                    "倒数Day1": int(d1_last_day2_cnt), "倒数Day1占比": pct_d1(d1_last_day2_cnt),
                    "倒数Day0(上市当天)": int(d1_last_day1_cnt), "倒数Day0占比": pct_d1(d1_last_day1_cnt)
                })
        else:
            # 没有锁单的车型
            intention_group_conversion_results.append({
                "车型": model,
                "总体留存小订": retained_count, "总转化率": "0.0%",
                "Day1留存小订": int(base_day1_cnt), "Day1转化率": "0.0%",
                "前3日留存小订": int(base_top3_cnt), "前3日转化率": "0.0%",
                "中间期留存小订": int(base_middle_cnt), "中间期转化率": "0.0%",
                "倒数Day2留存小订": int(base_last_day3_cnt), "倒数Day2转化率": "0.0%",
                "倒数Day1留存小订": int(base_last_day2_cnt), "倒数Day1转化率": "0.0%",
                "倒数Day0(上市当天)留存小订": int(base_last_day1_cnt), "倒数Day0转化率": "0.0%"
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
    if lock_30d_results:
        lock_30d_df = pd.DataFrame(lock_30d_results)
        print("\n--- 各车型上市 30 日锁单数计算结果 ---")
        print(lock_30d_df.to_string(index=False))

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
        
        print("\n--- 各车型上市后核心周期（前1-3天及前7日）锁单分布 ---")
        print(summary_df.to_string(index=False))

    if intention_dist_results:
        intention_dist_df = pd.DataFrame(intention_dist_results)
        print("\n--- 各车型上市后30日锁单的小订时间在预售周期内的分布 ---")
        
        # 调整列顺序便于查看，并根据要求简化 CLI 输出
        cols = [
            "车型", "总锁单", 
            "Day1", "Day1占比", "前3日累计", "前3日占比",
            "中间期", "中间占比",
            "倒数Day2", "倒数Day2占比",
            "倒数Day1", "倒数Day1占比",
            "倒数Day0(上市当天)", "倒数Day0占比"
        ]
        print(intention_dist_df[cols].to_string(index=False))

    if intention_group_conversion_results:
        conv_df = pd.DataFrame(intention_group_conversion_results)
        print("\n--- 各车型预售各阶段【留存小订基数】及【对应的锁单转化率】 ---")
        
        # 调整列顺序便于查看
        conv_cols = [
            "车型", "总体留存小订", "总转化率",
            "Day1留存小订", "Day1转化率",
            "前3日留存小订", "前3日转化率",
            "中间期留存小订", "中间期转化率",
            "倒数Day2留存小订", "倒数Day2转化率",
            "倒数Day1留存小订", "倒数Day1转化率",
            "倒数Day0(上市当天)留存小订", "倒数Day0转化率"
        ]
        print(conv_df[conv_cols].to_string(index=False))

    if retained_intention_dist_results:
        retained_dist_df = pd.DataFrame(retained_intention_dist_results)
        print("\n--- 各车型预售周期【留存小订数】在预售周期内的分布 ---")
        
        cols_retained = [
            "车型", "总留存小订", 
            "Day1", "Day1占比", "前3日累计", "前3日占比",
            "中间期", "中间占比",
            "倒数Day2", "倒数Day2占比",
            "倒数Day1", "倒数Day1占比",
            "倒数Day0(上市当天)", "倒数Day0占比"
        ]
        print(retained_dist_df[cols_retained].to_string(index=False))

    # 6. 推演 LS8 的最终留存小订数
    ls8_model = "LS8"
    if ls8_model in time_periods:
        ls8_start = pd.to_datetime(time_periods[ls8_model]['start'])
        ls8_end = pd.to_datetime(time_periods[ls8_model]['end'])
        
        df_ls8 = df[df['model'] == ls8_model]
        
        window_end_excl_ls8 = ls8_end + pd.Timedelta(days=1)
        
        mask_time_ls8 = (df_ls8['intention_payment_time'].notna()) & \
                        (df_ls8['intention_payment_time'] >= ls8_start) & \
                        (df_ls8['intention_payment_time'] < window_end_excl_ls8)
        
        mask_retained_ls8 = df_ls8['intention_refund_time'].isna() | \
                            (df_ls8['intention_refund_time'] > window_end_excl_ls8)
                            
        ls8_retained_df = df_ls8.loc[mask_time_ls8 & mask_retained_ls8, ['order_number', 'intention_payment_time']].drop_duplicates(subset=['order_number'])
        
        ls8_retained_df['days_from_start'] = (ls8_retained_df['intention_payment_time'].dt.normalize() - ls8_start.normalize()).dt.days
        ls8_retained_df['days_to_end'] = (ls8_end.normalize() - ls8_retained_df['intention_payment_time'].dt.normalize()).dt.days
        
        ls8_day1_cnt = (ls8_retained_df['days_from_start'] == 0).sum()
        ls8_top3_cnt = (ls8_retained_df['days_from_start'] < 3).sum()
        
        ls8_max_day = ls8_retained_df['days_from_start'].max()
        # 由于运行当日的数据通常不完整，使用 max_day - 1 作为完整的测算范围卡点
        # 如果 max_day 为 0（即预售第一天刚开始），则降级为 0
        ls8_effective_max_day = max(0, ls8_max_day - 1) if pd.notna(ls8_max_day) else 0
        
        total_presale_days = (ls8_end - ls8_start).days + 1
        middle_period_days = max(0, total_presale_days - 3 - 3)
        
        # 中间期实际已发生的完整天数对应的订单
        ls8_middle_df = ls8_retained_df[(ls8_retained_df['days_from_start'] >= 3) & 
                                        (ls8_retained_df['days_from_start'] <= ls8_effective_max_day) & 
                                        (ls8_retained_df['days_to_end'] >= 3)]
        ls8_middle_actual_cnt = len(ls8_middle_df)
        
        max_middle_day_idx = total_presale_days - 1 - 3  # 中间期最后一天（不含末尾3天）的索引
        
        effective_middle_max = min(ls8_effective_max_day, max_middle_day_idx)
        passed_middle_days = effective_middle_max - 3 + 1 if effective_middle_max >= 3 else 0
        
        ls8_middle_avg = ls8_middle_actual_cnt / passed_middle_days if passed_middle_days > 0 else 0
        remaining_middle_days = middle_period_days - passed_middle_days
        ls8_projected_middle = ls8_middle_actual_cnt + ls8_middle_avg * remaining_middle_days
        
        # 对于末尾3日，我们也用 max_day - 1（即 effective_max_day）来判断该天的数据是否完整
        # 如果 effective_max_day 已经覆盖了倒数第X天，说明那天数据完整了，可以作为真实基数
        
        last_day3_idx = total_presale_days - 3
        last_day2_idx = total_presale_days - 2
        last_day1_idx = total_presale_days - 1
        
        # 实际小订量
        actual_last_day3 = (ls8_retained_df['days_to_end'] == 2).sum()
        actual_last_day2 = (ls8_retained_df['days_to_end'] == 1).sum()
        actual_last_day1 = (ls8_retained_df['days_to_end'] == 0).sum()
        
        # 如果当天不完整（effective_max_day 还未覆盖到它），我们需要结合历史比例进行“当天未完成的推演”
        # 所以这里仅保留“完整跑完”的标记
        is_last_day3_complete = ls8_effective_max_day >= last_day3_idx
        is_last_day2_complete = ls8_effective_max_day >= last_day2_idx
        is_last_day1_complete = ls8_effective_max_day >= last_day1_idx
        
        date_last_day3 = ls8_end.normalize() - pd.Timedelta(days=2)
        date_last_day2 = ls8_end.normalize() - pd.Timedelta(days=1)
        date_last_day1 = ls8_end.normalize()
        
        # 提取 LS8 实际锁单数据 (如果已发生锁单)
        mask_lock_ls8 = df_ls8['lock_time'].notna()
        ls8_locked_orders = df_ls8.loc[mask_lock_ls8, ['order_number', 'lock_time']].dropna(subset=['order_number']).drop_duplicates(subset=['order_number'])
        ls8_locked_retained_df = ls8_locked_orders[ls8_locked_orders['order_number'].isin(ls8_retained_df['order_number'])].copy()
        
        if not ls8_locked_retained_df.empty:
            ls8_locked_retained_df = ls8_locked_retained_df.merge(ls8_retained_df[['order_number', 'days_from_start', 'days_to_end']], on='order_number', how='left')
            actual_lock_last_day3 = (ls8_locked_retained_df['days_to_end'] == 2).sum()
            actual_lock_last_day2 = (ls8_locked_retained_df['days_to_end'] == 1).sum()
            actual_lock_last_day1 = (ls8_locked_retained_df['days_to_end'] == 0).sum()
            actual_lock_top3 = (ls8_locked_retained_df['days_from_start'] < 3).sum()
            actual_lock_middle = len(ls8_locked_retained_df) - actual_lock_top3 - actual_lock_last_day3 - actual_lock_last_day2 - actual_lock_last_day1
        else:
            actual_lock_last_day3 = actual_lock_last_day2 = actual_lock_last_day1 = 0
            actual_lock_top3 = actual_lock_middle = 0
        
        projection_results = []
        for hist in retained_intention_dist_results:
            hist_model = hist['车型']
            hist_total = hist['总留存小订']
            if hist_total == 0:
                continue
                
            hist_top3_ratio = hist['前3日累计'] / hist_total
            hist_middle_ratio = hist['中间期'] / hist_total
            hist_last_day3_ratio = hist['倒数Day2'] / hist_total
            hist_last_day2_ratio = hist['倒数Day1'] / hist_total
            hist_last_day1_ratio = hist['倒数Day0(上市当天)'] / hist_total
            
            # 构建投影基数 (已知的前3天 + 完整或推演的中间期)
            base_count = ls8_top3_cnt + ls8_projected_middle
            base_ratio = hist_top3_ratio + hist_middle_ratio
            
            # 如果末尾某天数据完整，将其计入基数
            if is_last_day3_complete:
                base_count += actual_last_day3
                base_ratio += hist_last_day3_ratio
            if is_last_day2_complete:
                base_count += actual_last_day2
                base_ratio += hist_last_day2_ratio
            if is_last_day1_complete:
                base_count += actual_last_day1
                base_ratio += hist_last_day1_ratio
                
            proj_total_base = base_count / base_ratio if base_ratio > 0 else 0
            
            # 计算倒数3天的推演值
            # 如果数据完整 -> 用实际值 (已计入基数)
            # 如果不完整 -> 取实际值与推演值的较大者
            if is_last_day3_complete:
                proj_last_day3 = actual_last_day3
            else:
                proj_last_day3 = max(actual_last_day3, proj_total_base * hist_last_day3_ratio)
                
            if is_last_day2_complete:
                proj_last_day2 = actual_last_day2
            else:
                proj_last_day2 = max(actual_last_day2, proj_total_base * hist_last_day2_ratio)
                
            if is_last_day1_complete:
                proj_last_day1 = actual_last_day1
            else:
                proj_last_day1 = max(actual_last_day1, proj_total_base * hist_last_day1_ratio)
                
            proj_last3_total = proj_last_day3 + proj_last_day2 + proj_last_day1
            proj_combined = ls8_top3_cnt + ls8_projected_middle + proj_last3_total
            
            projection_results.append({
                "参考历史车型": hist_model,
                "推演末尾Day2": int(proj_last_day3),
                "推演末尾Day1": int(proj_last_day2),
                "推演末尾Day0": int(proj_last_day1),
                "综合推演末尾3日增量": int(proj_last3_total),
                "综合推演最终值": int(proj_combined),
                "_proj_last_day3": proj_last_day3,
                "_proj_last_day2": proj_last_day2,
                "_proj_last_day1": proj_last_day1
            })
            
        if projection_results:
            proj_df = pd.DataFrame(projection_results)
            display_cols = [
                "参考历史车型", "推演末尾Day2", "推演末尾Day1", "推演末尾Day0", 
                "综合推演末尾3日增量", "综合推演最终值"
            ]
            print(f"\n--- LS8 最终留存小订数推演 (结合已知数据 + 动态推演) ---")
            print(f"当前已知前3日累计: {ls8_top3_cnt}")
            print(f"中间期: 已过 {passed_middle_days} 天 (实际 {ls8_middle_actual_cnt}), 剩余 {remaining_middle_days} 天推演为 {int(ls8_projected_middle - ls8_middle_actual_cnt)}, 中间期合计: {int(ls8_projected_middle)}")
            print(f"末尾3日当前实际值 - Day2: {actual_last_day3}, Day1: {actual_last_day2}, Day0: {actual_last_day1}")
            print(proj_df[display_cols].to_string(index=False))
            
            conv_map = {d["车型"]: d for d in intention_group_conversion_results if "车型" in d}
            retained_map = {d["车型"]: d for d in retained_intention_dist_results if "车型" in d}
            
            def parse_pct(s):
                if s is None:
                    return 0.0
                if isinstance(s, (int, float)):
                    return float(s)
                s = str(s).strip()
                if not s:
                    return 0.0
                if s.endswith("%"):
                    s = s[:-1]
                try:
                    return float(s) / 100.0
                except Exception:
                    return 0.0
                
            lock_projection_rows = []
            for row in projection_results:
                hist_model = row["参考历史车型"]
                proj_total = int(row["综合推演最终值"])
                
                proj_last_day3 = row["_proj_last_day3"]
                proj_last_day2 = row["_proj_last_day2"]
                proj_last_day1 = row["_proj_last_day1"]
                
                conv = conv_map.get(hist_model, {})
                hist_dist = retained_map.get(hist_model, {})
                
                rate_top3 = parse_pct(conv.get("前3日转化率"))
                rate_middle = parse_pct(conv.get("中间期转化率"))
                
                rate_last_day3 = parse_pct(conv.get("倒数Day2转化率"))
                rate_last_day2 = parse_pct(conv.get("倒数Day1转化率"))
                rate_last_day1 = parse_pct(conv.get("倒数Day0转化率"))
                
                # 开始推演
                ls8_base_top3 = int(ls8_top3_cnt)
                ls8_base_middle = int(round(ls8_projected_middle))
                
                # 计算锁单推演值并与实际发生值取较大者 (避免推演低于实际)
                lock_top3 = max(actual_lock_top3, ls8_base_top3 * rate_top3)
                lock_middle = max(actual_lock_middle, ls8_base_middle * rate_middle)
                
                lock_last_day3 = max(actual_lock_last_day3, proj_last_day3 * rate_last_day3)
                lock_last_day2 = max(actual_lock_last_day2, proj_last_day2 * rate_last_day2)
                lock_last_day1 = max(actual_lock_last_day1, proj_last_day1 * rate_last_day1)
                
                lock_last3 = lock_last_day3 + lock_last_day2 + lock_last_day1
                proj_last3_total = proj_last_day3 + proj_last_day2 + proj_last_day1
                
                rate_last3_overall = lock_last3 / proj_last3_total if proj_last3_total > 0 else 0.0
                
                lock_total = lock_top3 + lock_middle + lock_last3
                
                lock_projection_rows.append({
                    "参考历史车型": hist_model,
                    "推演留存小订": proj_total,
                    "推演30日锁单": int(round(lock_total)),
                    "推演转化率": f"{(lock_total / proj_total * 100):.1f}%" if proj_total > 0 else "0.0%",
                    "历史前3日转化率": f"{rate_top3*100:.1f}%",
                    "前3日推演锁单": int(round(lock_top3)),
                    "历史中间期转化率": f"{rate_middle*100:.1f}%",
                    "中间期推演锁单": int(round(lock_middle)),
                    "综合末尾3日转化率": f"{rate_last3_overall*100:.1f}%",
                    "末尾3日推演锁单": int(round(lock_last3))
                })
                
            if lock_projection_rows:
                lock_df = pd.DataFrame(lock_projection_rows)
                cols = [
                    "参考历史车型", "推演留存小订", "推演30日锁单", "推演转化率",
                    "历史前3日转化率", "前3日推演锁单", 
                    "历史中间期转化率", "中间期推演锁单",
                    "综合末尾3日转化率", "末尾3日推演锁单"
                ]
                print(f"\n--- LS8 上市后30日锁单数推演 (前3日 + 中间期 + 末尾3日分别代入转化率) ---")
                print(f"前3日已知基数: {ls8_top3_cnt}, 中间期推演基数: {int(ls8_projected_middle)}")
                print(lock_df[cols].to_string(index=False))

    # === 新增：推演 LS8 上市后首日 Day1 锁单数 ===
    if 'lock_projection_rows' in locals() and 'summary_rows' in locals() and lock_30d_results:
        ls8_day1_projection_results = []
        
        lock_30d_map = {d["车型"]: d for d in lock_30d_results}
        summary_map = {d["车型"]: d for d in summary_rows}
        
        for row in lock_projection_rows:
            hist_model = row["参考历史车型"]
            proj_30d_lock = row["推演30日锁单"]
            
            lock_30d_info = lock_30d_map.get(hist_model)
            summary_info = summary_map.get(hist_model)
            
            if lock_30d_info and summary_info:
                day1_retained_ratio_str = lock_30d_info.get("首日留存小订转化占比", "0.0%")
                day1_retained_ratio = float(day1_retained_ratio_str.strip('%')) / 100.0 if day1_retained_ratio_str != "0.0%" else 0.0
                
                day1_ratio_str = summary_info.get("Day1_占比", "0.0%")
                day1_ratio = float(day1_ratio_str.strip('%')) / 100.0 if day1_ratio_str != "0.0%" else 0.0
                
                if day1_retained_ratio > 0:
                    ls8_day1_lock = proj_30d_lock * day1_ratio / day1_retained_ratio
                    
                    ls8_day1_projection_results.append({
                        "参考历史车型": hist_model,
                        "推演30日锁单(留存转化)": proj_30d_lock,
                        "首日留存小订转化占比": f"{day1_retained_ratio*100:.1f}%",
                        "Day1_占比": f"{day1_ratio*100:.1f}%",
                        "推演LS8首日Day1锁单数": int(round(ls8_day1_lock))
                    })
                    
        if ls8_day1_projection_results:
            day1_proj_df = pd.DataFrame(ls8_day1_projection_results)
            print("\n--- 推演 LS8 上市后首日 Day1 锁单数 ---")
            print("计算逻辑：推演LS8首日Day1锁单数 = 推演30日锁单(留存转化) * Day1_占比 / 首日留存小订转化占比")
            print(day1_proj_df.to_string(index=False))

    if top3_locked_intention_dist_results:
        top3_dist_df = pd.DataFrame(top3_locked_intention_dist_results)
        print("\n--- 各车型上市后【前3日】锁单的小订时间在预售周期内的分布 ---")
        
        cols_t3 = [
            "车型", "前3日总锁单", 
            "Day1", "Day1占比", "前3日累计", "前3日占比",
            "中间期", "中间占比",
            "倒数Day2", "倒数Day2占比",
            "倒数Day1", "倒数Day1占比",
            "倒数Day0(上市当天)", "倒数Day0占比"
        ]
        print(top3_dist_df[cols_t3].to_string(index=False))

    if day1_locked_intention_dist_results:
        day1_dist_df = pd.DataFrame(day1_locked_intention_dist_results)
        print("\n--- 各车型上市后【Day1】锁单的小订时间在预售周期内的分布 ---")
        
        cols_d1 = [
            "车型", "Day1总锁单", 
            "Day1", "Day1占比", "前3日累计", "前3日占比",
            "中间期", "中间占比",
            "倒数Day2", "倒数Day2占比",
            "倒数Day1", "倒数Day1占比",
            "倒数Day0(上市当天)", "倒数Day0占比"
        ]
        print(day1_dist_df[cols_d1].to_string(index=False))

if __name__ == "__main__":
    main()
