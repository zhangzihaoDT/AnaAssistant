# 业务指标计算口径 (Business Operators)

## 1. 预售期留存小订数 (Retained Intention Orders in Presale Period)

### 1.1 业务定义
统计在各车型指定“预售周期”内支付小订，并且在“预售周期结束时”尚未退订（依然保持小订状态）的独立订单数量。

### 1.2 数据源与依赖字段
- **订单数据表**：`order_data.parquet`
  - `model` / `series_group_logic`: 车型分类
  - `order_number`: 订单编号（用于去重）
  - `intention_payment_time`: 小订支付时间
  - `intention_refund_time`: 小订退款时间
- **业务定义表**：`business_definition.json` (`time_periods` 字典)
  - `start`: 预售开始日期
  - `end`: 预售结束日期

### 1.3 核心计算口径

1. **时间窗口定义 (Time Window)**
   - **开始时间 (`start_day`)**: `time_periods` 中定义的 `start` 日期（包含当天）。
   - **结束时间边界 (`window_end_excl`)**: `time_periods` 中定义的 `end` 日期的次日零点（Exclusive）。即时间窗口为 `[start_day, end_day + 1天)`。

2. **小订支付条件 (Intention Payment Condition)**
   - 订单的小订支付时间 (`intention_payment_time`) 必须非空。
   - 支付时间需落在预售时间窗口内：
     ```python
     (intention_payment_time >= start_day) & (intention_payment_time < window_end_excl)
     ```

3. **留存判定条件 (Retention Condition)**
   - 订单在预售期结束时未发生退款。满足以下任意一条即视为“留存”：
     - 小订退款时间 (`intention_refund_time`) 为空（即至今未退款）。
     - 或者，小订退款时间晚于预售时间窗口的结束边界（即在预售期结束后才发生的退款，在预售期内仍算作留存）。
     ```python
     intention_refund_time.isna() | (intention_refund_time > window_end_excl)
     ```

4. **订单去重计数 (Distinct Count)**
   - 满足上述条件的订单，基于订单编号 (`order_number`) 提取并去除重复项（`.drop_duplicates()`）。
   - 最终计算去重后的订单编号数量（`nunique()`）。

### 1.4 参考代码实现 (Python/Pandas)
```python
start_day = pd.to_datetime(time_periods[model]['start'])
end_day = pd.to_datetime(time_periods[model]['end'])

# 预售结束日期的次日0点作为时间窗口的排他性边界 (exclusive)
window_end_excl = end_day + pd.Timedelta(days=1)

# 条件1：小订支付时间在预售周期内
mask_time = (df_model['intention_payment_time'].notna()) & \
            (df_model['intention_payment_time'] >= start_day) & \
            (df_model['intention_payment_time'] < window_end_excl)

# 条件2：留存小订（未发生小订退款，或者退款时间晚于预售窗口期结束时间）
mask_retained = df_model['intention_refund_time'].isna() | \
                (df_model['intention_refund_time'] > window_end_excl)

# 应用条件并去重 order_number 计数
retained_orders = df_model.loc[mask_time & mask_retained, 'order_number'].dropna().drop_duplicates()
retained_count = int(retained_orders.nunique())
```