---
name: visualization-style
description: 定义项目统一的可视化配色与布局规范，并提供 Plotly 模板（layout/template）快速落地；适用于需要“统一图表风格/指定主色和对比色/统一背景与网格线/输出 Plotly 样式模板参数”的场景。
---

配色（核心 + 背景）

- 主指标（Main）：#3498DB
- 对比指标（Contrast）：#E67E22
- 深色强调（Dark）：#373f4a
- 背景（Background）：#FFFFFF
- 网格线/零线（Grid/Zero）：#ebedf0
- 文字/边框（Text/Border）：#7B848F

使用规则

- 主指标优先用于基准/核心走势；对比指标用于对照组/次要指标；深色用于强调（关键点/阈值/注释）。
- 辅助/衍生指标优先使用主色的变体（透明度、虚线、点划线、较细线宽），避免引入第三个强对比色。
- 背景保持白底；网格线与坐标轴线使用同一浅灰，减少视觉噪声；文字与边框统一使用 Text/Border 色。

落地（Plotly）

```python
import plotly.graph_objects as go

COLOR_MAIN = "#3498DB"
COLOR_CONTRAST = "#E67E22"
COLOR_DARK = "#373f4a"
COLOR_GRID = "#ebedf0"
COLOR_TEXT = "#7B848F"
COLOR_BG = "#FFFFFF"

LAYOUT_CONFIG = dict(
    plot_bgcolor=COLOR_BG,
    paper_bgcolor=COLOR_BG,
    font=dict(color=COLOR_TEXT),
    xaxis=dict(
        gridcolor=COLOR_GRID,
        zerolinecolor=COLOR_GRID,
        tickfont=dict(color=COLOR_TEXT),
        title_font=dict(color=COLOR_TEXT),
        showline=True,
        linecolor=COLOR_GRID,
        mirror=True,
    ),
    yaxis=dict(
        gridcolor=COLOR_GRID,
        zerolinecolor=COLOR_GRID,
        tickfont=dict(color=COLOR_TEXT),
        title_font=dict(color=COLOR_TEXT),
        showline=True,
        linecolor=COLOR_GRID,
        mirror=True,
    ),
    legend=dict(
        bordercolor=COLOR_TEXT,
        borderwidth=1,
        font=dict(color=COLOR_TEXT),
    ),
)

def apply_visualization_style(fig: go.Figure) -> go.Figure:
    fig.update_layout(**LAYOUT_CONFIG)
    return fig

fig = go.Figure()
fig.add_trace(go.Scatter(x=[1, 2], y=[1, 3], name="Main", line=dict(color=COLOR_MAIN)))
fig.add_trace(go.Scatter(x=[1, 2], y=[2, 2], name="Contrast", line=dict(color=COLOR_CONTRAST)))
apply_visualization_style(fig)
```

