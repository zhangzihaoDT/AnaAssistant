---
name: high-refund-lock-risk
description: 基于“高退订率锁单特征画像”对某日锁单进行高退订风险查验与反推诊断。用于排查某日锁单是否呈现活动期冲量导致的高风险形态（如锁后未推进付款/金融、用户信息不完善、锁单过快、风险门店聚集），并输出可操作的门店/城市/车型维度线索与风险信号清单。
---

# 高退订率锁单特征

## 运行

在项目根目录执行：

```bash
source .venv/bin/activate
python .trae/skills/high-refund-lock-risk/scripts/lock_refund_risk_check.py --date 2025-11-12
```

常用参数：

- `--date YYYY-MM-DD`
- `--order-parquet /abs/path/to/order_data.parquet`（不传则从 `schema/data_path.md` 读取“订单分析”路径）
- `--profile-json .trae/skills/high-refund-lock-risk/references/profile_2025-11-12.json`
- `--print-json`（输出 JSON，便于接入其它脚本/报表）

## 输出解读

- `metrics`：当日锁单 cohort 的可观测特征（不依赖未来退款）
- `retrospective`：若该日已发生退款，会补充“已观测到的锁后退订率”和“退款时间分布”（用于回溯验证）
- `risk.prospective`：仅基于锁单时可观测信息的风险信号
- `risk.retrospective`：基于“已发生退订”的回溯画像匹配度（用于反推查验）
- `suspicious`：需要优先复核的门店/城市/车型切片（按“锁单量”和“高风险占比”排序）

## 排查动作

- 先看 `risk_flags` 是否触发多条；再看 `suspicious.stores_top` 是否集中在画像门店或出现新的异常门店。
- 若 `final_payment_way_na_share` 很高，重点核查门店锁单是否存在“占位/冲量/未推进”的流程问题。
- 若 `intention_to_lock_median_hours` 显著偏低，重点核查是否存在活动期强刺激导致的冲动锁单（后续跟进不足）。

## 画像文件

默认画像为 2025-11-12 的“高退订率锁单特征”，存放于：

- [profile_2025-11-12.json](file:///Users/zihao_/Documents/github/26W06_Tool_calls/.trae/skills/high-refund-lock-risk/references/profile_2025-11-12.json)
