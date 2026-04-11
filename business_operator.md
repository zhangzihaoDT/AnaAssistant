## 7. 上市后【Day1】锁单的小订时间在预售周期的分布规律 (Intention Time Distribution of Day-1 Lock Orders)

### 7.1 业务定义
提取在**上市后第一天（Day1）就完成了锁单**的最核心、行动最果断的顶尖买家，分析他们下小订（交意向金）的日期分布。此项指标是反映“盲订忠诚度”和“核心基盘粉丝转化力度”的最敏感探针。

### 7.2 核心计算口径

1. **目标订单集提取：**
   在 `locked_retained_df` 中，进一步过滤出 `days_since_listing == 0`（即上市当天）的订单：
   ```python
   day1_locked_df = locked_retained_df[locked_retained_df['days_since_listing'] == 0].copy()
   ```

2. **意向时间分组与相对天数计算：**
   逻辑与第 6 节完全一致，对 `day1_locked_df` 订单按照 `intention_days_from_start` 和 `intention_days_to_end` 分别划归到不同切片（Day1、前3日、中间期、倒数Day2、倒数Day1/上市日）。

3. **占比计算公式：**
   `各分段对应锁单数 / 目标车型Day1总锁单数 * 100%`