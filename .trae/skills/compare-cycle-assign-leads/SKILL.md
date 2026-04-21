---
name: compare-cycle-assign-leads
description: 调度 scripts/index_summary.py 的周期模式（按时间窗口切片，不带车系过滤），生成 JSON/CSV，并对比下发线索相关指标（日均/均值）在两个窗口间的差异；适用于对比 CM2 vs LS8 等不同时间窗口的下发线索规模结构。
---

# compare-cycle-assign-leads

使用仓库自带数据源与 `scripts/index_summary.py` 周期模式，跑出两个窗口的结果（JSON/CSV），并输出下发线索相关指标的均值对比表。

## 口径

- 传参窗口使用 `--start/--end`，其中 `end` 按“排除端点”理解（[start,end)）。
- 因为 `index_summary.py` 会包含 `end` 当天，本技能在消费 `days` 时会剔除 `date == end` 的那天。
- 采用 B 方案时：直接把 `end` 传成“你想要的最后一天 + 1 天”。

## 快速使用

直接用脚本调度并对比（推荐）：

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --a-name CM2上市后4天 --a-start 2025-09-10 --a-end 2025-09-14 \
  --b-name LS8上市后4天 --b-start 2026-04-16 --b-end 2026-04-20
```

仅消费已有 JSON（不重复跑 index_summary）：

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --a-name A --a-json out/index_summary_A.json \
  --b-name B --b-json out/index_summary_B.json
```

如需切换数据源配置（仍不做车系过滤），透传 `data_path.md`：

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --data-path-md schema/data_path.md \
  --a-name A --a-start 2025-09-10 --a-end 2025-09-14 \
  --b-name B --b-start 2026-04-16 --b-end 2026-04-20
```

## 多窗口（上市后 N 天）均值表

从 `schema/business_definition.json` 的 `time_periods.<SERIES>.end` 读取“上市日”，自动构造 `[上市日, 上市日+N)` 窗口，批量跑 `index_summary.py` 并输出多列均值 Markdown 表。

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --series LS8 LS9 CM2 \
  --listing-plus-days 5
```

如需把 Markdown 表格落盘：

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --series LS8 LS9 CM2 \
  --listing-plus-days 5 \
  --md-out out/上市后5天_均值表.md
```

如需切换 business_definition 文件：

```bash
python3 scripts/compare_cycle_assign_leads.py \
  --business-definition schema/business_definition.json \
  --series LS8 LS9 CM2 \
  --listing-plus-days 5
```
