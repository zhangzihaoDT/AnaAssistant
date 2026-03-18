---
name: index-summary-monitor
description: 评估日度业务指标（来自 index_summary.py 的 JSON 输出）并做分布监控解读。适用于：需要对某天 index_summary.py --date 的表现做统计分布评估（前7天周环比、近8周同星期几、以及按 business_definition.json 的车型预售/上市窗口形成活动基线）并输出可执行的业务判断与告警解读（可选调用 DeepSeek 生成总结）。
---

# 指标分布监控（index_summary）

## 评估口径（ABCD）

- 本技能按 ABCD 四组样本做分布对比，先拼接样本集，再逐指标评估：
  - A：目标样本
  - B：短期基线样本
  - C：同周期基线样本
  - D：活动基线样本
- 默认模式：`A vs B`、`A vs C`、`A vs D` 分别评估
- 可选模式：`A vs (B+C+D)`，即先合并 BCD，再与 A 对比

## 单日输入

- 输入：`--date YYYY-MM-DD`
- A：目标日期当天样本
- B：前 7 天样本（周环比视角）
- C：近 8 周同星期几样本
- D：`schema/business_definition.json` 的活动窗口样本
  - 预售期：`[start, end)`
  - 上市期：`[end, finish)`
  - 每个窗口仅形成一个活动样本点（窗口期代表值）
  - 因此仅支持单样本点对比，例如“2026-03-14 与 CM2 上市期对比”

## 周期输入（不做周期压缩）

- 输入：`--date A~B` 或 `--start A --end B`
- A：周期内每日样本点集合（不压缩为单个 mean 点）
- B：向前平移 7 天的同长度窗口对应每日样本点
- C：近 8 周同星期几同长度窗口对应每日样本点
- D：活动窗口样本集合（预售+上市），每个窗口仍是单样本点

## 输出解释

- `compare_mode`：当前评估模式（`abcd` 或 `a_vs_bcd`）
- `baselines.source_groups`：B/C/D 三组来源样本定义
- `baselines.evaluation_groups`：本次实际参与评估的分组
- `comparison_dataset`：ABCD 拼接后的样本清单（用于追踪样本来源）

## 默认输出

- 默认只输出总结文本（LLM 或规则摘要）
- 只有显式 `--output json` 才输出完整评估 JSON

## 在项目内运行

优先使用项目虚拟环境的 Python（若存在）：

```bash
.venv/bin/python .trae/skills/index-summary-monitor/scripts/evaluate_day.py --date 2026-03-14
```

若没有虚拟环境，则使用系统 `python3`：

```bash
python3 .trae/skills/index-summary-monitor/scripts/evaluate_day.py --date 2026-03-14
```

周期评估示例：

```bash
.venv/bin/python .trae/skills/index-summary-monitor/scripts/evaluate_day.py --date 2026-03-14~2026-03-15
```

等价写法（显式 start/end）：

```bash
.venv/bin/python .trae/skills/index-summary-monitor/scripts/evaluate_day.py --start 2026-03-14 --end 2026-03-15
```

多 CSV 集合评估示例（`evalute.py`）：

```bash
.venv/bin/python .trae/skills/index-summary-monitor/scripts/evalute.py \
  --a out/index_summary_daily_matrix_2026-03-13_2026-03-15.csv --a-column 2026-03-14 \
  --b out/index_summary_daily_matrix_2026-03-13_2026-03-15.csv --b-columns 2026-03-13 \
  --c out/index_summary_daily_matrix_2026-03-13_2026-03-15.csv --c-columns 2026-03-15 \
  --d out/index_summary_daily_matrix_2026-03-13_2026-03-15.csv --d-columns 2026-03-13 \
  --compare-mode abcd --output json
```

常用参数：

- `--index-summary`：指定 `index_summary.py` 路径（默认在 `skills/index_summary.py` 或 `scripts/index_summary.py` 自动查找）
- `--data-path-md`：指定数据路径配置（默认 `schema/data_path.md`）
- `--business-definition`：指定活动窗口定义（默认 `schema/business_definition.json`）
- `--output llm|json`：默认 `llm`
- `evalute.py` 参数（多 CSV 集合）：
  - `--a/--b/--c/--d`：分别输入 A/B/C/D 的 CSV 文件或 glob
  - `--a-column`：A 目标样本列
  - `--b-columns/--c-columns/--d-columns`：基线样本列集合
  - `--compare-mode abcd|a_vs_bcd`
  - `--output json|text`
- `--compare-mode abcd|a_vs_bcd`：
  - `abcd`（默认）：分别对比 A vs B、A vs C、A vs D
  - `a_vs_bcd`：将 B/C/D 合并后，对比 A vs (BCD)
- `--no-deepseek`：禁用 DeepSeek，仅输出规则化总结
- `--deepseek-model`：默认 `deepseek-chat`

注意：

- 活动基线依赖 `index_summary.py` 支持 `--start/--end` 的周期输出；推荐直接使用项目内的 `scripts/index_summary.py`。
