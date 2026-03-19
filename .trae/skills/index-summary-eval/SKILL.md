---
name: index-summary-eval
description: 对 index_summary.py 的单日或区间 JSON 输出执行状态评估。使用两因子（Volume/Conversion）做历史分位、状态离散化、regime 对比和诊断输出。适用于“判断当前高/中/低位”“对比活动期与常态期表现”“把 daily_metrics_matrix 或 days 结果转成可解释评估结论”的场景。
---

# index-summary-eval

使用 `scripts/evaluation_engine.py` 将 `index_summary.py` 结果转换为统一评估结果。

## 执行流程

1. 准备输入 JSON：
   - 单日模式：`index_summary.py --date ...` 的 JSON。
   - 区间模式：`index_summary.py --start ... --end ... --print-json` 的 JSON（支持 `days` 或 `daily_metrics_matrix`）。
2. 运行评估脚本，自动识别或显式指定 `--scope`。
3. 输出统一结构：
   - `factor_scores`：`Volume`、`Conversion`
   - `state`：`High/Mid/Low` 组合状态
   - `regime_eval`：`global` 与各 regime 对比分位
   - `trend`：区间内“后3天均值 - 前3天均值”的趋势方向与强度
   - `structure`：结构化因子（LeadQuality、CRConcentration）及状态
   - `peer_days`：相似历史日 TopK（date/distance/regime）
   - `diagnosis`：最小规则诊断结论

## 命令模板

```bash
python .trae/skills/index-summary-eval/scripts/evaluation_engine.py \
  --input-json /absolute/path/to/input.json \
  --scope auto
```

可选参数：

- `--scope day|interval|auto`：默认 `auto`
- `--history-csv`：历史分位基准矩阵，默认 `schema/index_summary_daily_matrix_2024-01-01_to_yesterday.csv`
- `--business-definition`：活动窗口定义，默认 `schema/business_definition.json`

## 输入要求

- 单日输入至少包含：
  - `date`
  - `订单表.锁单数`
  - `订单表.CR5门店销量集中度`
  - `订单表.CR5门店城市销量集中度`
  - `下发线索转化率.下发线索数`
  - `下发线索转化率.下发线索数 (门店)`
  - `下发线索转化率.门店线索占比`
  - `下发线索转化率.下发线索数（直播）`
  - `下发线索转化率.下发线索数（平台)`
  - `下发线索转化率.下发 (门店)线索当日锁单率`
  - `下发线索转化率.下发线索当日试驾率`
  - 若评估日期距今天 >7 天：需要 `下发线索转化率.下发线索当7日锁单率`
  - 若评估日期距今天 >30 天：需要 `下发线索转化率.下发线索当30日锁单率`
- 区间输入至少满足其一：
  - `days`（每天一条 index_summary 单日结构）
  - `daily_metrics_matrix`（矩阵结构）

## 评估口径

- 因子压缩：
  - `Volume = 0.6*P(lock_cnt) + 0.25*P(leads) + 0.15*P(leads_store)`
  - `Conversion` 依据评估日期距今天的滞后动态切换：
    - `lag <= 7d`：`Conversion(level) = 0.6*P(store_lock0_rate) + 0.4*P(td0_rate)`；`Conversion(end_state) = 0.7*P(store_lock0_rate) + 0.3*P(td0_rate)`
    - `7d < lag <= 30d`：加入 `P(conv7)`（下发线索当7日锁单率）
    - `lag > 30d`：加入 `P(conv30)`（下发线索当30日锁单率），并提高其权重（当前配置：`conv30≈0.70`）
- 分位函数：`P(x) = mean(history < x)`
- 状态阈值：
  - `>=0.7` 为 `High`
  - `<=0.3` 为 `Low`
  - 其余为 `Mid`
- 区间汇总：
  - `level` 使用每日分数中位数
  - `end_state` 使用最后 3 天均值（Conversion 使用 end_state 权重）
  - `trend` 使用前 3 天 vs 后 3 天的分位均值差（阈值 0.05）
  - `peer_days` 基于末端（end_state）在分位空间的相似度检索
- 诊断规则：
  - `Volume > 0.7` → `流量高位`
  - `Conversion < 0.3` → `转化偏低`
  - `activity_high_eff 且 Conversion < 0.3` → `活动期转化异常`
