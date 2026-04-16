---
name: competitor-events-updater
description: 使用 WebSearch 维护竞品车型关键时间点数据并生成 HTML 报告。适用于：从 schema/competitors.json 选择指定车系，联网检索上市/交付/到店/预售/测评/试驾等节点，更新 schema/events.json，并运行 scripts/gen_competitors_report.py 输出 scripts/reports/competitors_report.html，实现闭环更新。
---

# 竞品时间点闭环更新

## 输入与产出

- 输入
  - `schema/competitors.json`：车系到竞品车型列表映射。当前约定格式为 JSON 数组，每个元素为 `{ "<SERIES>": ["车型A", "车型B"] }`。
  - `schema/重点关注新能源品牌.md`：品牌 →（微博主页/官网主页）网址清单，用于 WebSearch 时优先限定官方/微博来源（例如用 `site:` 或直接带域名关键词）。
  - （可选）用户指定车系：例如 `LS8`、`CM1`。
- 产出
  - `schema/events.json`：指定车系的竞品事件时间点（精确到日）。
  - `scripts/reports/competitors_report.html`：基于 `events.json` 生成的 HTML 报告。

## 执行流程（闭环）

所有命令均在仓库根目录执行。

1. 确认车系
   - 若用户未指定车系，先询问用户要更新哪一个 `<SERIES>`。
   - 读取 `schema/competitors.json`，抽取该 `<SERIES>` 下的竞品车型列表。
   - 说明：`schema/competitors.json` 的车型列表应同时包含本品与竞品；对列表中每个车型一视同仁执行检索与落表。
   - 读取 `schema/重点关注新能源品牌.md`，获取品牌官网/微博主页链接；为每个车型确定“优先检索来源域名”：
     - 若车型名能明确映射品牌（如“问界→AITO”“智界→智界”“极氪→极氪”“阿维塔→阿维塔”），直接使用对应官网/微博域名做限定检索。
     - 若无法明确映射（如出现未在清单内的品牌/车型），先询问用户对应品牌；若用户不确定则跳过域名限定，改用通用检索 + 多媒体源交叉验证。

2. 生成/更新 `schema/events.json`（只包含该车系车型）
   - 可先用脚本生成空模板（推荐）：
     - `python3 scripts/gen_events_json.py --series <SERIES>`
   - `event_types` 固定为：
     - `static_review`（静态测评）
     - `dynamic_review`（动态测评）
     - `presale_release`（预售发布）
     - `test_drive_reservation`（预约试驾）
     - `delivery_start`（开启交付）
     - `launch_release`（上市发布）
     - `demo_car_arrival`（展车到店）
   - 对每个车型、每个事件类型做联网检索与落表：
     - 第一轮优先检索（官方/微博优先）：
       - 用 WebSearch 组合检索词：`车型名 + 事件关键词 + site:<官网域名>` 或 `车型名 + 事件关键词 + <微博主页域名>`。
    - 第二轮补充检索（指定站点补全，按用户要求）：
      - 易车新车频道：`site:news.yiche.com/xinche`（https://news.yiche.com/xinche/）
      - 易车号（内容号）：`site:news.yiche.com/hao`（https://news.yiche.com/hao/）
       - 汽车之家车家号：`site:chejiahao.autohome.com.cn`（https://chejiahao.autohome.com.cn/）
       - 用 WebSearch 组合检索词：`车型名 + 事件关键词 + site:<上述域名>`，将检索结果作为“媒体补充来源”写入到对应节点。
    - 第三轮补充检索（通用检索，媒体补充，避免只靠单一来源）：
      - 在官方口径缺失或日期不够精确时，用 WebSearch 做通用检索：`车型名 + 事件关键词 + 日期/时间`，并在结果中优先选择权威媒体/垂直媒体（如 IT之家/新出行/财经媒体等）作为补充来源。
     - 每个节点优先找“可精确到日”的来源；无法精确到日则该节点保留空数组 `[]`。
     - 每条事件项结构：
       - `date`: `YYYY-MM-DD`
       - `title`: 来源文章标题或可读摘要标题
       - `url`: 文章链接
       - `variant`（可选）: 若明确对应版本（Pro/Max/Ultra/增程/纯电等）
   - 处理原则：
     - 优先级：官方发布/官网 > 权威媒体转述发布会 > 垂直媒体/综合媒体。
     - 冲突处理：同一节点多来源日期不一致时，选择更接近官方/更明确写明日期的来源；不确定则不写入。
   - 更新 `generated_at` 为当天日期（`YYYY-MM-DD`）。

3. 生成 HTML 报告
   - 运行：
     - `python3 scripts/gen_competitors_report.py`
   - 默认从 `schema/events.json` 读取，并写到 `scripts/reports/competitors_report.html`。

4. 验证（必须做）
   - 验证 `schema/events.json` 为合法 JSON（可用 `python3 -m json.tool schema/events.json`）。
   - 验证 `scripts/reports/competitors_report.html` 文件存在且有内容（可读前 30 行确认标题与表格渲染）。

## WebSearch 查询模板（按节点）

- 关键词尽量用短词，不强制包含“正式/首批/开启”等限定词；优先用 `上市/交付/到店/预售/试驾/体验` 等核心词命中结果。
- 上市发布：`{车型} 上市` / `{车型} 发布会 上市` / `{车型} 售价 上市`
- 开启交付：`{车型} 交付` / `{车型} 开启交付` / `{车型} 首批交付`
- 展车到店：`{车型} 到店` / `{车型} 展车 到店` / `{车型} 试驾车 到店`
- 预售发布：`{车型} 预售` / `{车型} 开启预订` / `{车型} 预售价`
- 预约试驾：`{车型} 试驾` / `{车型} 预约试驾` / `{车型} 预约体验`
- 静态测评：`静态体验 {车型}` / `{车型} 静态体验`
- 动态测评：`试驾 {车型}` / `{车型} 动态体验` / `试乘试驾 {车型}`

## 与仓库脚本的衔接

- 报告生成脚本：`scripts/gen_competitors_report.py`
- 报告输出路径：`scripts/reports/competitors_report.html`
