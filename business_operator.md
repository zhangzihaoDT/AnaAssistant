## 4. 上市后30日锁单的小订时间在预售周期内的分布规律 (Intention Time Distribution of 30-Day Lock Orders)

### 4.1 业务定义
提取那些在**上市后 30 日内发生了锁单**的留存小订订单，分析它们**当初下小订的时间**在整个预售期内的分布特征。借此观察高转化意向用户主要是“早鸟（预售前几天盲订）”、“中间观望者”还是“发布会/上市当天的冲动消费者”。

### 4.2 核心计算口径

1. **目标订单集：** 
   取在“上市后30日内发生锁单行为”的所有预售期留存订单 (`locked_retained_df`)。

2. **相对时间计算（距离预售起点和终点）：**
   提取目标订单的小订支付时间（`intention_payment_time`），分别计算其距离预售开始日 (`start_day`) 和预售结束日 (`end_day`) 的天数差（取整）：
   - `intention_days_from_start = (intention_payment_time - start_day).dt.days`
   - `intention_days_to_end = (end_day - intention_payment_time).dt.days`
   *(注：使用日期级别（normalize）的运算以消除具体小时和分钟的误差)*

3. **分段切分规则（Bucketing）：**
   - **Day1 (预售首日)**: `intention_days_from_start == 0`
   - **Day2 (预售次日)**: `intention_days_from_start == 1`
   - **Day3 (预售第三日)**: `intention_days_from_start == 2`
   - **前3日累计**: 以上三者之和
   - **倒数Day1 (预售最后一天/上市日)**: `intention_days_to_end == 0`
   - **倒数Day2 (上市前一日)**: `intention_days_to_end == 1`
   - **倒数Day3 (上市前两日)**: `intention_days_to_end == 2`
   - **中间期**: `总锁单数 - 前3日累计 - 倒数Day1 - 倒数Day2 - 倒数Day3`

4. **占比计算公式：**
   各分段锁单数 / 该车型“上市后30日锁单数”总和 * 100%。