import argparse
import html
import json
from pathlib import Path


EVENT_TYPE_MAP = {
    "static_review": "静态测评",
    "dynamic_review": "动态测评",
    "presale_release": "预售发布",
    "test_drive_reservation": "预约试驾",
    "delivery_start": "开启交付",
    "launch_release": "上市发布",
    "demo_car_arrival": "展车到店",
}


def _render_items(items):
    if not items:
        return "<span class=\"empty\">暂无数据</span>"

    blocks = []
    for item in items:
        date = html.escape(item.get("date") or "未知")
        title = html.escape(item.get("title") or "详情")
        url = html.escape(item.get("url") or "#", quote=True)
        variant = item.get("variant")
        variant_html = (
            f"<span class=\"variant\">{html.escape(variant)}</span>" if variant else ""
        )
        blocks.append(
            "<div class=\"item\">"
            f"{variant_html}"
            f"<span class=\"date\">{date}</span>"
            f"<a href=\"{url}\" target=\"_blank\" rel=\"noreferrer\">{title}</a>"
            "</div>"
        )
    return "".join(blocks)


def _truncate(text, max_len):
    text = text or ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _safe_json_for_html(value):
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def build_report(events):
    models = events.get("models", {})
    event_types = events.get("event_types", [])
    preferred_order = [
        "static_review",
        "presale_release",
        "dynamic_review",
        "test_drive_reservation",
        "demo_car_arrival",
        "launch_release",
        "delivery_start",
    ]
    event_types = [et for et in preferred_order if et in event_types] + [
        et for et in event_types if et not in preferred_order
    ]
    generated_at = html.escape(events.get("generated_at") or "")
    models_json = _safe_json_for_html(models)
    event_types_json = _safe_json_for_html(event_types)
    event_type_map_json = _safe_json_for_html(EVENT_TYPE_MAP)

    head = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>竞品车型关键时间点报告</title>
  <script>window.PlotlyConfig = {{MathJaxConfig: 'local'}};</script>
  <script charset="utf-8" src="https://cdn.plot.ly/plotly-3.4.0.min.js" integrity="sha256-KEmPoupLpFyGMyGAiOsiNDbKDKAvxXAn/W+oQa0ZAfk=" crossorigin="anonymous"></script>
  <style>
    body {{ font-family: 'PingFang SC', 'Microsoft YaHei', Arial, sans-serif; margin: 20px; background: #f4f7f6; color: #333; }}
    h1 {{ text-align: center; color: #2c3e50; margin-bottom: 8px; }}
    .meta {{ text-align: center; color: #7f8c8d; margin-bottom: 24px; font-size: 13px; }}
    .table-container {{ overflow-x: auto; background: #fff; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); padding: 20px; }}
    .diagram-container {{ background: #fff; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); padding: 20px; margin-bottom: 20px; }}
    .diagram-container h2 {{ margin: 0 0 12px; color: #2c3e50; font-size: 16px; }}
    .timeline-controls {{ display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 14px; }}
    .timeline-controls .group {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }}
    .timeline-controls label {{ display: inline-flex; gap: 6px; align-items: center; font-size: 13px; color: #2c3e50; }}
    .timeline-controls button {{ border: 1px solid #e0e6ed; background: #fff; padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 13px; }}
    .timeline-controls button:hover {{ background: #f8f9fa; }}
    #timeline-warnings {{ color: #7f8c8d; font-size: 12px; margin: 6px 0 14px; }}
    #timeline-warnings details {{ border: 1px solid #e0e6ed; border-radius: 6px; padding: 8px 10px; margin-top: 8px; background: #fafbfc; }}
    #timeline-warnings summary {{ cursor: pointer; color: #2c3e50; }}
    #timeline-diagram {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1000px; }}
    th, td {{ border: 1px solid #e0e6ed; padding: 14px; text-align: left; vertical-align: top; }}
    th {{ background: #34495e; color: #fff; font-weight: 600; white-space: nowrap; }}
    tr:nth-child(even) {{ background: #f8f9fa; }}
    tr:hover {{ background: #f1f4f8; }}
    a {{ color: #2980b9; text-decoration: none; font-size: 13px; line-height: 1.4; display: inline-block; }}
    a:hover {{ text-decoration: underline; color: #1abc9c; }}
    .date {{ font-weight: 700; color: #e74c3c; font-size: 13px; margin-bottom: 6px; display: block; }}
    .item {{ margin-bottom: 10px; padding-bottom: 10px; border-bottom: 1px dashed #eee; }}
    .item:last-child {{ margin-bottom: 0; padding-bottom: 0; border-bottom: none; }}
    .empty {{ color: #bdc3c7; font-style: italic; font-size: 13px; }}
    .model-name {{ font-weight: 700; color: #2c3e50; font-size: 15px; white-space: nowrap; }}
    .variant {{ background: #e8f4f8; color: #2c3e50; font-size: 12px; padding: 2px 6px; border-radius: 4px; margin-bottom: 6px; display: inline-block; }}
  </style>
</head>
<body>
  <h1>竞品车型关键时间点报告</h1>
  <div class="meta">数据生成日期：{generated_at}</div>
  <div class="diagram-container">
    <h2>时间轴</h2>
    <div class="timeline-controls">
      <div class="group" id="timeline-model-filters"></div>
      <div class="group">
        <button type="button" id="timeline-select-all">全选</button>
        <button type="button" id="timeline-select-none">全不选</button>
      </div>
    </div>
    <div id="timeline-warnings"></div>
    <div id="timeline-diagram"></div>
"""

    diagram_tail = """  </div>
  <div class="table-container">
    <table>
      <tr>
        <th>车型</th>
"""

    header_cells = []
    for et in event_types:
        header_cells.append(f"<th>{html.escape(EVENT_TYPE_MAP.get(et, et))}</th>")

    rows = []
    for model, model_events in models.items():
        row_cells = [f"<td class=\"model-name\">{html.escape(model)}</td>"]
        for et in event_types:
            row_cells.append(f"<td>{_render_items(model_events.get(et, []))}</td>")
        rows.append(
            "<tr data-model=\"" + html.escape(model, quote=True) + "\">" + "".join(row_cells) + "</tr>"
        )

    tail = """      </tr>
    </table>
  </div>
  <script>
    const timelineModels = """ + models_json + """;
    const timelineEventTypes = """ + event_types_json + """;
    const timelineEventTypeMap = """ + event_type_map_json + """;

    const timelineColors = {
      static_review: "#8ecae6",
      presale_release: "#ffb703",
      dynamic_review: "#219ebc",
      test_drive_reservation: "#90be6d",
      demo_car_arrival: "#bde0fe",
      launch_release: "#fb8500",
      delivery_start: "#ef476f",
    };

    function buildFilters() {
      const box = document.getElementById("timeline-model-filters");
      box.innerHTML = "";
      const models = Object.keys(timelineModels);
      for (const m of models) {
        const label = document.createElement("label");
        const input = document.createElement("input");
        input.type = "checkbox";
        input.value = m;
        input.checked = true;
        input.addEventListener("change", () => {
          updateTableVisibility();
          renderTimeline();
        });
        label.appendChild(input);
        label.appendChild(document.createTextNode(m));
        box.appendChild(label);
      }

      document.getElementById("timeline-select-all").addEventListener("click", () => {
        for (const el of box.querySelectorAll("input[type=checkbox]")) el.checked = true;
        updateTableVisibility();
        renderTimeline();
      });

      document.getElementById("timeline-select-none").addEventListener("click", () => {
        for (const el of box.querySelectorAll("input[type=checkbox]")) el.checked = false;
        updateTableVisibility();
        renderTimeline();
      });
    }

    function selectedModels() {
      const box = document.getElementById("timeline-model-filters");
      return Array.from(box.querySelectorAll("input[type=checkbox]"))
        .filter((el) => el.checked)
        .map((el) => el.value);
    }

    function updateTableVisibility() {
      const selected = new Set(selectedModels());
      for (const row of document.querySelectorAll("table tr[data-model]")) {
        const m = row.getAttribute("data-model");
        row.style.display = selected.has(m) ? "" : "none";
      }
    }

    function sanitizeId(text) {
      return "T_" + String(text).replace(/[^a-zA-Z0-9_]/g, "_");
    }

    function truncate(text, maxLen) {
      const s = String(text || "");
      if (s.length <= maxLen) return s;
      return s.slice(0, maxLen - 1) + "…";
    }

    function pad2(n) {
      return String(n).padStart(2, "0");
    }

    function parseDateFromString(raw) {
      const s = String(raw || "");
      let m = s.match(/(\\d{4})[-\\/\\.](\\d{1,2})[-\\/\\.](\\d{1,2})/);
      if (m) return `${m[1]}-${pad2(m[2])}-${pad2(m[3])}`;
      m = s.match(/(\\d{4})年(\\d{1,2})月(\\d{1,2})日/);
      if (m) return `${m[1]}-${pad2(m[2])}-${pad2(m[3])}`;
      m = s.match(/(\\d{4})(\\d{2})(\\d{2})/);
      if (m) return `${m[1]}-${m[2]}-${m[3]}`;
      return null;
    }

    function collectPoints(modelsToShow) {
      const byEventType = {};
      for (const et of timelineEventTypes) {
        byEventType[et] = { x: [], y: [], text: [], url: [] };
      }
      const skipped = [];

      for (const model of modelsToShow) {
        const modelEvents = timelineModels[model] || {};
        for (const et of timelineEventTypes) {
          const items = modelEvents[et] || [];
          for (const it of items) {
            const date = parseDateFromString(it.date) || parseDateFromString(it.title) || parseDateFromString(it.url);
            if (!date) {
              skipped.push({ model, et, title: it.title || "", url: it.url || "" });
              continue;
            }
            const title = it.title || "";
            const etName = timelineEventTypeMap[et] || et;
            byEventType[et].x.push(date);
            byEventType[et].y.push(model);
            byEventType[et].text.push(`${etName} | ${truncate(title, 80)}`);
            byEventType[et].url.push(it.url || "");
          }
        }
      }

      return { byEventType, skipped };
    }

    function renderTimeline() {
      const container = document.getElementById("timeline-diagram");
      const warnings = document.getElementById("timeline-warnings");
      const modelsToShow = selectedModels();
      const { byEventType, skipped } = collectPoints(modelsToShow);

      const traces = [];
      for (const et of timelineEventTypes) {
        const name = timelineEventTypeMap[et] || et;
        const color = timelineColors[et] || "#cccccc";
        const p = byEventType[et];
        traces.push({
          type: "scatter",
          mode: "markers",
          name,
          x: p.x,
          y: p.y,
          text: p.text,
          customdata: p.url,
          marker: { color, size: 10, line: { color: "#333", width: 1 } },
          hovertemplate: "%{y}<br>%{x}<br>%{text}<extra></extra>",
        });
      }

      const height = Math.max(320, 80 + modelsToShow.length * 40);
      const layout = {
        height,
        margin: { l: 140, r: 40, t: 20, b: 60 },
        xaxis: { type: "date", tickformat: "%Y-%m-%d", showgrid: true, gridcolor: "#ebedf0" },
        yaxis: { type: "category", categoryorder: "array", categoryarray: modelsToShow, automargin: true },
        legend: { orientation: "h" },
      };

      if (!skipped.length) {
        warnings.innerHTML = "日期解析：全部可用";
      } else {
        const head = `日期解析：跳过 ${skipped.length} 条（缺少可解析日期，未绘制）`;
        const lines = skipped.slice(0, 30).map((it) => {
          const etName = timelineEventTypeMap[it.et] || it.et;
          const title = truncate(it.title || "详情", 70);
          return `${it.model} | ${etName} | ${title} | ${it.url || ""}`;
        });
        const more = skipped.length > 30 ? `\\n... 还有 ${skipped.length - 30} 条` : "";
        warnings.innerHTML = `${head}<details><summary>查看详情</summary><pre style="white-space:pre-wrap;margin:8px 0 0;">${lines.join("\\n")}${more}</pre></details>`;
      }

      Plotly.react(container, traces, layout, { responsive: true, displaylogo: false });

      container.on("plotly_click", (ev) => {
        const pt = ev && ev.points && ev.points[0];
        const url = pt && pt.customdata;
        if (url) window.open(url, "_blank");
      });
    }

    buildFilters();
    updateTableVisibility();
    renderTimeline();
  </script>
</body>
</html>
"""

    return (
        head
        + diagram_tail
        + "".join(header_cells)
        + "</tr>"
        + "".join(rows)
        + tail
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(
            Path(__file__).resolve().parents[1].joinpath("schema").joinpath("events.json")
        ),
    )
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__)
            .resolve()
            .parents[1]
            .joinpath("scripts")
            .joinpath("reports")
            .joinpath("competitors_report.html")
        ),
    )
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        events = json.load(f)

    report = build_report(events)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)


if __name__ == "__main__":
    main()
