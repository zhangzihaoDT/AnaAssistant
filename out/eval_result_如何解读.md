## 举例：eval_2025-08-01.v2_volume_tuned.json

**1）先看总判定（这天到底好不好）**

- `state: "Mid Volume + Low Conversion"`
  - 规模（Volume）在历史里 **中位**
  - 转化（Conversion）在历史里 **低位**  
    这是一个很典型的“量不差，但接不住/转不动”的日子。

**2）看 factor_scores（为什么会是这个状态）**

- `factor_scores.Volume = 0.5166`
  - 对应口径：`Volume = 0.6*P(lock_cnt) + 0.4*P(leads)`
  - 0.5166 的意思：把“锁单数、下发线索数”放到历史分布里看，综合处在 **略高于中位**。
- `factor_scores.Conversion = 0.1732`
  - 对应口径：根据评估日期距今天的 `lag` 动态切换。
  - 2025-08-01 距今天（2026-03-19）明显 **>30 天**，所以 Conversion 会 **纳入 conv30（30日锁单率）并提高权重（≈0.70）**，短漏斗只占小头。
  - 0.1732 的意思：按“已跑完生命周期”的口径看，这天的转化在历史里依然偏差（低位），不是“因为 30 天没跑完所以被低估”。

**3）看 regime（这天属于什么类型的日子，拿谁比）**

- `regime: "weekday"`：工作日。  
  这保证你最该参考的是 `regime_eval.weekday`，而不是 weekend/activity 的子集。

**4）看 regime_eval（同一组表现，在不同历史子集里的相对位置）**

- `global: Volume 0.5166 / Conversion 0.1732`：全历史口径下就是“量中位、转化低位”。
- `weekday: Volume 0.6706 / Conversion 0.2042`：如果只跟工作日比，规模其实算 **偏高**，但转化仍然 **不高**（0.20 左右）。
- `weekend: ... Conversion 0.0327`、`activity_high_eff / activity_low_eff`：这些更多是“跨场景对照”，不作为主口径，但能提示你：在别的 regime 子集中，这个转化位置也不突出。

**5）看 structure（结构好不好看，解释“为什么接不住”的线索）**

- `structure.scores.LeadQuality = 0.7059`，且 `state` 写的是 `LeadQuality High`
  - 这表示：按你定义的结构因子（门店/直播/平台占比等组合）这天“结构看起来不错”。
- `CRConcentration = 0.3891 (Mid)`：集中度中位。
- 大白话：**线索来源结构不差，但最终转化仍偏低**，更像是“承接/转化链路的问题”，而不是“来源结构太烂”。

**6）看 peer_days（找历史相似日抄作业/找差异）**

- `peer_days` 给了 3 个“形态最像”的历史日（含 `distance` 越小越像、以及它们的 `regime`）。  
  用法：把这些日期的 `index_summary.py` 输出拿出来对照，看在“相似形态”下哪天转化更好、差在渠道/触达次数/试驾等哪个环节。

**7）diagnosis（一句话诊断）**

- 这里只有一个：`转化偏低`，与 `Conversion=0.1732` 完全一致。

如果你愿意，我可以把这段 JSON 里“lag>30 时到底用了哪些 Conversion 权重、分别贡献了多少分位”也打印成一个更直观的小表（目前输出里没直接展开权重贡献）。
