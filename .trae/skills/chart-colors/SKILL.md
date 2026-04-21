---
name: tableau-colorblind-chart-colors
description: 提供 Tableau 色盲友好调色板与图表配色落地规则（Plotly/Matplotlib/ECharts 等）。当用户要求“图表配色/色盲友好/统一视觉规范/给分类上色/指定调色板”时使用。
---

调色板（Tableau 色盲友好 10 色）

['#006BA4', '#FF800E', '#ABABAB', '#595959', '#5F9ED1', '#C85200', '#898989', '#A2C8EC', '#FFBC79', '#CFCFCF']

规则
- 分类色：按类别顺序依次取色；类别数 > 10 时循环使用（优先通过合并类别/分面/筛选减少同时出现的类别数）
- 强调色：优先 '#FF800E' 或 '#C85200'
- 弱化/背景：优先 '#CFCFCF'、'#ABABAB'
- 固定映射：同一业务维度在不同图中保持同色，避免因数据顺序变化导致颜色漂移

落地（Plotly）
- 建议：显式构建 `color_map = {category: palette[i]}`，每条 trace 的 `marker.color` 用 `color_map[category]`
- 使用 plotly.express：
  - `color_discrete_map=color_map`（推荐，稳定）
  - `color_discrete_sequence=palette`（依赖类别出现顺序，不稳定）

落地（ECharts）
- `option.color = palette` 作为默认分类色
- 或在 `series[i].itemStyle.color`/`data[j].itemStyle.color` 里按类目显式指定（推荐稳定）

