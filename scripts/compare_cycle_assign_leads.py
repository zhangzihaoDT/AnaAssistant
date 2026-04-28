"""
---
name: compare_cycle_assign_leads
type: script
path: scripts/compare_cycle_assign_leads.py
updated_at: "2026-04-21 20:13"
summary: 对比两个窗口（A/B）线索相关指标均值；内部调用 index_summary
inputs:
  - schema/data_path.md (optional, via --data-path-md)
  - scripts/index_summary.py
outputs:
  - stdout: 对比表（A/B 窗口）
  - out/index_summary_<name>.json
  - out/index_summary_<name>.csv
  - optional: markdown table (via --md-out)
cli:
  - python3 scripts/compare_cycle_assign_leads.py --a-start 2026-01-01 --a-end 2026-02-01 --b-start 2026-02-01 --b-end 2026-03-01
  - python3 scripts/compare_cycle_assign_leads.py --series LS8 --listing-plus-days 30 --md-out out/LS8上市后30天.md
---
"""

import argparse
import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


METRICS = [
    ("下发线索转化率", "下发线索数"),
    ("下发线索转化率", "下发线索数 (门店)"),
    ("下发线索转化率", "下发线索数（直播）"),
    ("下发线索转化率", "下发线索数（平台)"),
    ("下发线索转化率", "下发线索数（APP小程序)"),
    ("下发线索转化率", "下发线索数（快慢闪)"),
    ("下发线索转化率", "下发门店数"),
    ("订单分析", "店日均下发线索数"),
]


REF_MEANS = {
    "下发线索数": 21457,
    "下发线索数 (门店)": 4203,
    "下发线索数（直播）": 1719,
    "下发线索数（平台)": 6572,
    "下发线索数（APP小程序)": 2301,
    "下发线索数（快慢闪)": 1914,
    "下发门店数": 460,
    "店日均下发线索数": 46.65,
}


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_business_definition(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_date(s: str) -> date:
    return date.fromisoformat(str(s))


def _add_days(d: str, days: int) -> str:
    return (_parse_date(d) + timedelta(days=int(days))).isoformat()


def _filter_days_exclusive_end(data: dict) -> list[dict]:
    end = str(data.get("end"))
    days = list(data.get("days") or [])
    return [d for d in days if str(d.get("date")) < end]


def _calc_means(data: dict) -> dict[str, float | None]:
    days = _filter_days_exclusive_end(data)
    out: dict[str, float | None] = {"_n_days": float(len(days))}
    for section, metric in METRICS:
        vals: list[float] = []
        for d in days:
            block = d.get(section) or {}
            v = _to_float(block.get(metric))
            if v is not None:
                vals.append(v)
        out[metric] = _mean(vals)
    return out


def _fmt(v: float | None) -> str:
    if v is None:
        return "-"
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.2f}"


def _run_index_summary(
    index_summary_path: Path,
    start: str,
    end: str,
    csv_out: Path,
    json_out: Path,
    data_path_md: Path | None,
) -> None:
    cmd = [
        sys.executable,
        str(index_summary_path),
        "--start",
        start,
        "--end",
        end,
        "--csv-out",
        str(csv_out),
        "--print-json",
        "--include-days",
    ]
    if data_path_md is not None:
        cmd.extend(["--data-path-md", str(data_path_md)])
    json_out.parent.mkdir(parents=True, exist_ok=True)
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    with json_out.open("w", encoding="utf-8") as f:
        subprocess.run(cmd, check=True, stdout=f)


def _print_table(a_name: str, a_data: dict, a_means: dict, b_name: str, b_data: dict, b_means: dict) -> None:
    headers = [
        "指标",
        "参考均值",
        f"{a_name}均值",
        f"{b_name}均值",
        f"{b_name}-{a_name}",
        f"{a_name}-参考",
        f"{b_name}-参考",
    ]

    rows = []
    for _, metric in METRICS:
        ref = float(REF_MEANS.get(metric)) if metric in REF_MEANS else None
        a_v = a_means.get(metric)
        b_v = b_means.get(metric)
        rows.append(
            [
                metric,
                _fmt(ref),
                _fmt(a_v),
                _fmt(b_v),
                _fmt(None if (a_v is None or b_v is None) else (b_v - a_v)),
                _fmt(None if (a_v is None or ref is None) else (a_v - ref)),
                _fmt(None if (b_v is None or ref is None) else (b_v - ref)),
            ]
        )

    col_widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def fmt_row(r: list[str]) -> str:
        return " | ".join(
            str(c).rjust(col_widths[i]) if i else str(c).ljust(col_widths[i])
            for i, c in enumerate(r)
        )

    print(
        a_name + "窗口:",
        a_data.get("start"),
        "->",
        a_data.get("end"),
        "(按[start,end)排除end当日)",
        "n_days=",
        int(a_means["_n_days"]),
    )
    print(
        b_name + "窗口:",
        b_data.get("start"),
        "->",
        b_data.get("end"),
        "(按[start,end)排除end当日)",
        "n_days=",
        int(b_means["_n_days"]),
    )
    print(fmt_row(headers))
    print("-+-".join("-" * w for w in col_widths))
    for r in rows:
        print(fmt_row(r))


def _print_md_table(windows: list[tuple[str, dict, dict]], md_out: Path | None) -> None:
    if not windows:
        return
    cols = ["指标"] + [w[0] for w in windows]
    rows: list[list[str]] = []
    rows.append(["n_days"] + [str(int(w[2].get("_n_days") or 0)) for w in windows])
    for _, metric in METRICS:
        rows.append([metric] + [_fmt(w[2].get(metric)) for w in windows])

    lines: list[str] = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("|" + "|".join(["---"] * len(cols)) + "|")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")

    lines.append("")
    lines.append("窗口口径: [start,end) (已排除 end 当日)")
    for name, data, means in windows:
        lines.append(
            "- "
            + name
            + ": "
            + str(data.get("start"))
            + " -> "
            + str(data.get("end"))
            + " n_days="
            + str(int(means.get("_n_days") or 0))
        )

    text = "\n".join(lines)
    print(text)
    if md_out is not None:
        md_out.parent.mkdir(parents=True, exist_ok=True)
        md_out.write_text(text + "\n", encoding="utf-8")


def _run_window(
    *,
    index_summary_path: Path,
    out_dir: Path,
    name: str,
    start: str,
    end: str,
    data_path_md: Path | None,
) -> tuple[str, dict, dict]:
    json_out = out_dir / f"index_summary_{name}.json"
    csv_out = out_dir / f"index_summary_{name}.csv"
    _run_index_summary(
        index_summary_path=index_summary_path,
        start=start,
        end=end,
        csv_out=csv_out,
        json_out=json_out,
        data_path_md=data_path_md,
    )
    data = _load_json(json_out)
    means = _calc_means(data)
    return name, data, means


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--a-start", default=None)
    parser.add_argument("--a-end", default=None)
    parser.add_argument("--b-start", default=None)
    parser.add_argument("--b-end", default=None)
    parser.add_argument("--a-name", default="A")
    parser.add_argument("--b-name", default="B")

    parser.add_argument("--a-json", default=None)
    parser.add_argument("--b-json", default=None)

    parser.add_argument("--index-summary-path", default="scripts/index_summary.py")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--data-path-md", default=None)
    parser.add_argument("--series", nargs="*", default=None)
    parser.add_argument("--listing-plus-days", type=int, default=None)
    parser.add_argument("--business-definition", default="schema/business_definition.json")
    parser.add_argument("--md-out", default=None)

    args = parser.parse_args()

    index_summary_path = Path(str(args.index_summary_path)).expanduser().resolve()
    out_dir = Path(str(args.out_dir)).expanduser().resolve()
    data_path_md = None if args.data_path_md is None else Path(str(args.data_path_md)).expanduser().resolve()
    md_out = None if args.md_out is None else Path(str(args.md_out)).expanduser().resolve()

    if args.series and args.listing_plus_days is not None:
        bd_path = Path(str(args.business_definition)).expanduser().resolve()
        bd = _load_business_definition(bd_path)
        time_periods = dict(bd.get("time_periods") or {})
        windows: list[tuple[str, dict, dict]] = []
        for series in list(args.series):
            p = time_periods.get(str(series))
            if not isinstance(p, dict) or not p.get("end"):
                raise SystemExit(f"business_definition.time_periods 缺少 {series}.end")
            start = str(p["end"])
            end = _add_days(start, int(args.listing_plus_days))
            name = f"{series}上市后{int(args.listing_plus_days)}天"
            windows.append(
                _run_window(
                    index_summary_path=index_summary_path,
                    out_dir=out_dir,
                    name=name,
                    start=start,
                    end=end,
                    data_path_md=data_path_md,
                )
            )
        _print_md_table(windows, md_out=md_out)
        return

    if args.a_json is None and (args.a_start and args.a_end):
        a_json = out_dir / f"index_summary_{args.a_name}.json"
        a_csv = out_dir / f"index_summary_{args.a_name}.csv"
        _run_index_summary(
            index_summary_path=index_summary_path,
            start=str(args.a_start),
            end=str(args.a_end),
            csv_out=a_csv,
            json_out=a_json,
            data_path_md=data_path_md,
        )
        args.a_json = str(a_json)

    if args.b_json is None and (args.b_start and args.b_end):
        b_json = out_dir / f"index_summary_{args.b_name}.json"
        b_csv = out_dir / f"index_summary_{args.b_name}.csv"
        _run_index_summary(
            index_summary_path=index_summary_path,
            start=str(args.b_start),
            end=str(args.b_end),
            csv_out=b_csv,
            json_out=b_json,
            data_path_md=data_path_md,
        )
        args.b_json = str(b_json)

    if args.a_json is None or args.b_json is None:
        raise SystemExit("需要提供 (--a-json,--b-json) 或者分别提供 (--a-start,--a-end) 与 (--b-start,--b-end)")

    a_data = _load_json(Path(str(args.a_json)).expanduser().resolve())
    b_data = _load_json(Path(str(args.b_json)).expanduser().resolve())

    a_means = _calc_means(a_data)
    b_means = _calc_means(b_data)

    _print_table(
        a_name=str(args.a_name),
        a_data=a_data,
        a_means=a_means,
        b_name=str(args.b_name),
        b_data=b_data,
        b_means=b_means,
    )


if __name__ == "__main__":
    main()
