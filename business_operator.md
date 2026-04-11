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

## 2. 留存小订的上市后30日锁单数与转化率 (30-Day Lock Orders & Conversion Rate for Retained Intentions)

### 2.1 业务定义
基于上一阶段确定的“预售期留存小订”集合，统计这批订单在上市日期（含）起的 30 天内，发生锁单（Lock）行为的订单数量，并计算这部分订单占所有留存小订的转化率。

### 2.2 额外依赖字段
- **订单数据表**：`order_data.parquet`
  - `lock_time`: 订单锁单时间
- **业务定义表**：`business_definition.json`
  - `finish`: 数据统计截断日（部分车型存在，未存在则使用默认边界）

### 2.3 核心计算口径

1. **上市日期定义 (Listing Day)**
   - 一般取预售结束日期 (`end_day`)。
   - **特例：** 对于 `CM0` 车型，因历史原因，上市日期特殊处理为预售结束日期的次日（`end_day + 1天`）。

2. **30日锁单时间窗口 (30-Day Lock Window)**
   - **开始时间 (`listing_day`)**：上述定义的上市日期（包含当天 00:00:00）。
   - **结束边界 (`lock_30d_end_excl`)**：取“上市日向后推 31 天（即 +31 days，exclusive）” 与 “最终统计截断日的次日零点 (`finish_excl`)” 之间的**较小值**。
   - 这意味着有效锁单时间段为：`[listing_day, lock_30d_end_excl)`。

3. **锁单统计条件 (Lock Condition)**
   - 锁单时间 (`lock_time`) 必须非空。
   - 锁单时间落在 30日锁单时间窗口内：
     ```python
     (lock_time >= listing_day) & (lock_time < lock_30d_end_excl)
     ```
   - 此订单**必须**同时属于第一步筛选出的“预售期留存小订”订单池。

4. **指标计算公式**
   - **上市后30日锁单数 (`locked_count`)**：同时满足预售期留存且在锁单时间窗口内锁单的唯一订单总数。
   - **转化率 (`conversion_rate`)**：`上市后30日锁单数 / 预售期留存小订总数 * 100%`。

## 3. 上市后30日锁单数的日分布规律 (Daily Distribution of 30-Day Lock Orders)

### 3.1 业务定义
对于上述统计出的“上市后30日锁单”订单，按其锁单发生的日期距离上市日的天数进行分组，得出每一天的锁单数量和占比（占 30 天总锁单数的比例），用以观察不同车型的锁单节奏和转化高峰期。

### 3.2 核心计算口径
1. 提取发生了锁单行为的留存订单集 `locked_retained_df`，并获取对应的 `lock_time`。
2. 计算天数差：
   ```python
   days_since_listing = (lock_time - listing_day).dt.days
   # +1 转为常规业务语义上的 "第1天"（即上市当天）
   day_idx = days_since_listing + 1
   ```
3. 以 `day_idx` (1到31天) 为维度聚合统计订单数 (`count`)。
4. 占比计算公式：`当日锁单数 / 30日锁单总数 * 100%`。

## 4. 上市后30日锁单订单的小订支付时间分布 (Intention Payment Timing Distribution of Locked Orders)

### 4.1 业务定义
针对“上市后30日内锁单”的留存小订订单，分析这些订单当初支付小订的时间距离“上市日(T)”有多久，从而判断锁单用户的意向积累规律（是盲订期早早下订的死忠粉，还是临近上市/上市当天的冲动型用户）。

### 4.2 核心计算口径
1. 提取所有在上市后30日内发生锁单的留存小订，获取其 `intention_payment_time`。
2. 计算小订支付日距离上市日的天数差：
   ```python
   days_to_listing = (intention_payment_time.dt.normalize() - listing_day.dt.normalize()).dt.days
   ```
   *注：若在上市日当天支付小订，则差值为 0；若在上市前1天支付，则差值为 -1。*
3. 将天数差划分为不同的时间窗口（Bins）：
   - `T-22及以前` (<= -22)
   - `T-15至T-21` (-21 ~ -15)
   - `T-8至T-14` (-14 ~ -8)
   - `T-4至T-7` (-7 ~ -4)
   - `T-1至T-3` (-3 ~ -1)
   - `上市当天(T-0)` (0)
   - `上市后` (> 0)
4. 分组统计每个窗口内的订单数量，并计算占该车型30日锁单总数的比例。
