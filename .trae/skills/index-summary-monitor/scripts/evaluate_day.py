import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any


def _parse_ymd(value: str) -> date:
    value = str(value).strip()
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
    if not m:
        raise ValueError(f"无法解析日期: {value}")
    y, mo, d = map(int, m.groups())
    return date(y, mo, d)


def _daterange(start: date, end_inclusive: date) -> list[date]:
    if end_inclusive < start:
        return []
    out: list[date] = []
    cur = start
    while cur <= end_inclusive:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _same_weekday_history(target: date, weeks: int) -> list[date]:
    out: list[date] = []
    for k in range(1, max(int(weeks), 0) + 1):
        out.append(target - timedelta(days=7 * k))
    return out


def _read_env_file(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}
    out: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        if k:
            out[k] = v
    return out


def _load_deepseek_api_key(env_file: Path) -> str | None:
    k = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if k:
        return k
    env_map = _read_env_file(env_file)
    k2 = (env_map.get("DEEPSEEK_API_KEY") or "").strip()
    return (k2 or None)


def _deepseek_chat(
    *,
    api_key: str,
    base_url: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise RuntimeError(f"DeepSeek HTTPError: {e.code} {e.reason} {raw}".strip())
    except urllib.error.URLError as e:
        raise RuntimeError(f"DeepSeek URLError: {e}")


def _extract_deepseek_text(resp: dict[str, Any]) -> str:
    choices = resp.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict):
            content = msg.get("content")
            if isinstance(content, str):
                return content
    return ""


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        v = float(value)
        if math.isfinite(v):
            return v
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("%"):
            try:
                return float(s[:-1].strip()) / 100.0
            except ValueError:
                return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _is_percent_string(value: Any) -> bool:
    return isinstance(value, str) and value.strip().endswith("%")


def _flatten_metrics(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            k2 = str(k).strip()
            path = k2 if not prefix else f"{prefix}/{k2}"
            if isinstance(v, (dict, list)):
                out.update(_flatten_metrics(v, prefix=path))
            else:
                out[path] = v
        return out

    if isinstance(obj, list):
        for item in obj:
            if not isinstance(item, dict):
                continue
            if "pct" not in item:
                continue
            pct = item.get("pct")
            if pct is None:
                continue
            dim_key = None
            dim_val = None
            if "channel" in item:
                dim_key = "channel"
                dim_val = item.get("channel")
            elif "category" in item:
                dim_key = "category"
                dim_val = item.get("category")
            if not isinstance(dim_key, str) or dim_val is None:
                continue
            dim_val_str = str(dim_val).strip()
            if not dim_val_str:
                continue
            out[f"{prefix}/{dim_key}={dim_val_str}/pct"] = pct
        return out

    return out


def _median(values: list[float]) -> float | None:
    xs = sorted([x for x in values if x is not None and math.isfinite(x)])
    if not xs:
        return None
    n = len(xs)
    mid = n // 2
    if n % 2 == 1:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _quantile(values: list[float], q: float) -> float | None:
    xs = sorted([x for x in values if x is not None and math.isfinite(x)])
    if not xs:
        return None
    if q <= 0:
        return xs[0]
    if q >= 1:
        return xs[-1]
    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    w = pos - lo
    return xs[lo] * (1 - w) + xs[hi] * w


def _percentile_rank(values: list[float], x: float) -> float | None:
    xs = sorted([v for v in values if v is not None and math.isfinite(v)])
    if not xs:
        return None
    n = len(xs)
    le = 0
    for v in xs:
        if v <= x:
            le += 1
        else:
            break
    return le / n


def _mad(values: list[float], center: float) -> float | None:
    dev = [abs(v - center) for v in values if v is not None and math.isfinite(v)]
    return _median(dev)


def _robust_z(values: list[float], x: float, transform: str) -> float | None:
    xs = [v for v in values if v is not None and math.isfinite(v)]
    if not xs:
        return None

    if transform == "log1p":
        xs_t = [math.log1p(max(v, 0.0)) for v in xs]
        x_t = math.log1p(max(x, 0.0))
    elif transform == "logit":
        eps = 1e-6
        xs_t = []
        for v in xs:
            p = min(max(v, eps), 1.0 - eps)
            xs_t.append(math.log(p / (1.0 - p)))
        p_cur = min(max(x, eps), 1.0 - eps)
        x_t = math.log(p_cur / (1.0 - p_cur))
    else:
        xs_t = xs[:]
        x_t = x

    c = _median(xs_t)
    if c is None:
        return None
    m = _mad(xs_t, c)
    if m is None or m == 0.0:
        return None
    return (x_t - c) / (1.4826 * m)


def _normal_tail_p(z: float) -> float:
    z = abs(float(z))
    return math.erfc(z / math.sqrt(2.0))


def _severity(pctl: float | None) -> str | None:
    if pctl is None:
        return None
    if pctl <= 0.01 or pctl >= 0.99:
        return "red"
    if pctl <= 0.05 or pctl >= 0.95:
        return "yellow"
    return "green"


def _choose_transform(metric_key: str, raw_value: Any) -> str:
    if _is_percent_string(raw_value):
        return "logit"
    leaf = metric_key.split("/")[-1]
    if metric_key.endswith("转化率") or metric_key.endswith("占比") or leaf.startswith("share_"):
        return "logit"
    if isinstance(raw_value, int) or (isinstance(raw_value, float) and raw_value >= 0):
        return "log1p"
    return "none"


def _should_evaluate_metric(metric_key: str) -> bool:
    if metric_key in {
        "订单表/锁单数",
        "订单表/开票数",
        "下发线索转化率/下发线索数",
        "试驾分析/有效试驾数",
        "下发线索转化率/下发线索当日试驾率",
        "下发线索转化率/下发 (门店)线索当日锁单率",
        "下发线索转化率/下发线索当7日锁单率",
        "下发线索转化率/下发线索当30日锁单率",
        "订单表/share_l6",
        "订单表/share_ls6",
        "订单表/share_ls9",
        "订单表/share_reev",
        "订单表/CR5门店销量集中度",
        "订单表/CR5门店城市销量集中度",
        "订单表/整体ATP(用户车,万元)",
    }:
        return True

    if metric_key.startswith("归因分析/锁单用户主要渠道Top5/channel=") and metric_key.endswith("/pct"):
        return True

    if metric_key.startswith("归因分析/锁单用户分类占比（观察口径）/category=") and metric_key.endswith("/pct"):
        return True

    return False


@dataclass(frozen=True)
class BaselineSpec:
    name: str
    dates: list[date] | None
    ranges: list[tuple[date, date]] | None


def _run_index_summary(python_executable: str, index_summary_path: Path, target_date: date, data_path_md: Path) -> dict[str, Any]:
    cmd = [
        str(python_executable),
        str(index_summary_path),
        "--date",
        target_date.isoformat(),
        "--data-path-md",
        str(data_path_md),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip() or "index_summary 执行失败")
    return json.loads(p.stdout)


def _run_index_summary_range(
    python_executable: str,
    index_summary_path: Path,
    start_date: date,
    end_date_inclusive: date,
    data_path_md: Path,
) -> dict[str, Any]:
    cmd = [
        str(python_executable),
        str(index_summary_path),
        "--start",
        start_date.isoformat(),
        "--end",
        end_date_inclusive.isoformat(),
        "--print-json",
        "--data-path-md",
        str(data_path_md),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip() or "index_summary(范围) 执行失败")
    return json.loads(p.stdout)


def _collect_samples(
    python_executable: str,
    index_summary_path: Path,
    dates: list[date],
    data_path_md: Path,
    cache_dir: Path | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d in dates:
        cache_hit = None
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)
            fp = cache_dir / f"index_summary_{d.isoformat()}.json"
            if fp.exists():
                try:
                    cache_hit = json.loads(fp.read_text(encoding="utf-8"))
                except Exception:
                    cache_hit = None
        if cache_hit is not None:
            out.append(cache_hit)
            continue

        try:
            obj = _run_index_summary(python_executable, index_summary_path, d, data_path_md=data_path_md)
        except Exception:
            continue

        out.append(obj)
        if cache_dir is not None:
            fp = cache_dir / f"index_summary_{d.isoformat()}.json"
            try:
                fp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
    return out


def _collect_range_samples(
    python_executable: str,
    index_summary_path: Path,
    ranges: list[tuple[date, date]],
    data_path_md: Path,
    cache_dir: Path | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for start_d, end_d in ranges:
        cache_hit = None
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)
            fp = cache_dir / f"index_summary_{start_d.isoformat()}_{end_d.isoformat()}.json"
            if fp.exists():
                try:
                    cache_hit = json.loads(fp.read_text(encoding="utf-8"))
                except Exception:
                    cache_hit = None
        if cache_hit is not None:
            out.append(cache_hit)
            continue

        try:
            obj = _run_index_summary_range(
                python_executable=python_executable,
                index_summary_path=index_summary_path,
                start_date=start_d,
                end_date_inclusive=end_d,
                data_path_md=data_path_md,
            )
        except Exception:
            continue

        mean_obj = obj.get("mean")
        if not isinstance(mean_obj, dict):
            continue

        wrapped = {"date": f"{start_d.isoformat()}~{end_d.isoformat()}"}
        wrapped.update(mean_obj)
        out.append(wrapped)

        if cache_dir is not None:
            fp = cache_dir / f"index_summary_{start_d.isoformat()}_{end_d.isoformat()}.json"
            try:
                fp.write_text(json.dumps(wrapped, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
    return out


def _sample_tag(sample: dict[str, Any], fallback: str) -> str:
    raw = sample.get("date")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return fallback


def _evaluate_distribution(values: list[float], cur: float, transform: str) -> dict[str, Any]:
    xs = [v for v in values if v is not None and math.isfinite(v)]
    if not xs:
        return {"method": "distribution", "baseline_n": 0}

    pctl = _percentile_rank(xs, cur)
    z = _robust_z(xs, cur, transform=transform)
    tail_p = (None if z is None else _normal_tail_p(z))
    return {
        "method": "distribution",
        "baseline_n": len(xs),
        "baseline_median": _median(xs),
        "p10": _quantile(xs, 0.10),
        "p50": _quantile(xs, 0.50),
        "p90": _quantile(xs, 0.90),
        "pctl": pctl,
        "z": z,
        "tail_p": tail_p,
        "severity": _severity(pctl),
    }


def _evaluate_rate_with_denoms(baseline_pairs: list[tuple[int, int]], cur_pair: tuple[int, int]) -> dict[str, Any]:
    base = [(n, d) for (n, d) in baseline_pairs if d is not None and d > 0 and n is not None and n >= 0]
    n_cur, d_cur = cur_pair
    if not base or d_cur <= 0:
        return {"method": "rate_with_denoms", "baseline_n": len(base), "denom": d_cur}

    total_n = sum(n for n, _ in base)
    total_d = sum(d for _, d in base)
    alpha = total_n + 1.0
    beta = (total_d - total_n) + 1.0
    mu = alpha / (alpha + beta)
    var = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1.0))
    p_cur = n_cur / d_cur
    z = (p_cur - mu) / math.sqrt(max(var, 1e-12))
    tail_p = _normal_tail_p(z)

    base_rates = [n / d for n, d in base if d > 0]
    pctl = _percentile_rank(base_rates, p_cur)
    baseline_median = _median(base_rates)
    return {
        "method": "rate_with_denoms",
        "baseline_n": len(base),
        "denom": d_cur,
        "baseline_median": baseline_median,
        "pctl": pctl,
        "z": z,
        "tail_p": tail_p,
        "severity": _severity(pctl),
    }


def _resolve_project_file(candidates: list[Path]) -> Path | None:
    for p in candidates:
        try:
            if p.exists():
                return p.resolve()
        except Exception:
            continue
    return None


def _build_activity_baselines(
    business_definition_path: Path,
) -> tuple[list[tuple[date, date]], list[tuple[date, date]], dict[str, dict[str, Any]]]:
    obj = json.loads(business_definition_path.read_text(encoding="utf-8"))
    time_periods = obj.get("time_periods") or {}
    presale: list[tuple[date, date]] = []
    launch: list[tuple[date, date]] = []
    debug: dict[str, dict[str, Any]] = {}

    for name, win in time_periods.items():
        if not isinstance(name, str) or not isinstance(win, dict):
            continue
        s = win.get("start")
        e = win.get("end")
        f = win.get("finish")
        if not (isinstance(s, str) and isinstance(e, str) and isinstance(f, str)):
            continue
        try:
            d_s = _parse_ymd(s)
            d_e = _parse_ymd(e)
            d_f = _parse_ymd(f)
        except Exception:
            continue
        presale_end = d_e - timedelta(days=1)
        launch_end = d_f - timedelta(days=1)

        pre_range = None
        if presale_end >= d_s:
            pre_range = (d_s, presale_end)
            presale.append(pre_range)

        la_range = None
        if launch_end >= d_e:
            la_range = (d_e, launch_end)
            launch.append(la_range)

        debug[name] = {
            "start": s,
            "end": e,
            "finish": f,
            "presale_range": (None if pre_range is None else [pre_range[0].isoformat(), pre_range[1].isoformat()]),
            "launch_range": (None if la_range is None else [la_range[0].isoformat(), la_range[1].isoformat()]),
        }

    presale = sorted(set(presale), key=lambda x: (x[0], x[1]))
    launch = sorted(set(launch), key=lambda x: (x[0], x[1]))
    return presale, launch, debug


def _build_llm_context(output: dict[str, Any], metric_doc_text: str | None) -> dict[str, Any]:
    alerts = output.get("alerts") or []
    alerts_compact: list[dict[str, Any]] = []
    if isinstance(alerts, list):
        for a in alerts[:30]:
            if not isinstance(a, dict):
                continue
            metric = a.get("metric")
            if not isinstance(metric, str):
                continue
            node = None
            if isinstance(output.get("metrics"), dict):
                node = output["metrics"].get(metric)
            if node is None and isinstance(output.get("rates"), dict):
                node = output["rates"].get(metric)
            if not isinstance(node, dict):
                continue
            alerts_compact.append(
                {
                    "metric": metric,
                    "severity": a.get("severity"),
                    "value": node.get("value"),
                    "worst_baseline": node.get("worst_baseline"),
                    "worst_tail_p": node.get("worst_tail_p"),
                    "baselines": node.get("baselines"),
                }
            )

    def _pick(path: str) -> Any:
        if isinstance(output.get("metrics"), dict) and path in output["metrics"]:
            return output["metrics"][path].get("value")
        if isinstance(output.get("rates"), dict) and path in output["rates"]:
            return output["rates"][path].get("value")
        return None

    key_indicators = {
        "锁单数": _pick("订单表/锁单数"),
        "开票数": _pick("订单表/开票数"),
        "下发线索数": _pick("下发线索转化率/下发线索数"),
        "有效试驾数": _pick("试驾分析/有效试驾数"),
        "下发线索当日试驾率": _pick("下发线索转化率/下发线索当日试驾率"),
        "下发(门店)线索当日锁单率": _pick("下发线索转化率/下发 (门店)线索当日锁单率"),
        "下发线索当7日锁单率": _pick("下发线索转化率/下发线索当7日锁单率"),
        "下发线索当30日锁单率": _pick("下发线索转化率/下发线索当30日锁单率"),
        "整体ATP(用户车,万元)": _pick("订单表/整体ATP(用户车,万元)"),
    }
    key_indicators = {k: v for k, v in key_indicators.items() if v is not None}

    return {
        "date": output.get("date"),
        "baselines": output.get("baselines"),
        "min_baseline_samples": output.get("min_baseline_samples"),
        "alerts": alerts_compact,
        "key_indicators": key_indicators,
        "metric_doc": metric_doc_text,
    }


def _load_metric_doc_text(md_path: Path, max_chars: int) -> str | None:
    if not md_path.exists():
        return None
    text = md_path.read_text(encoding="utf-8").strip()
    if max_chars > 0 and len(text) > max_chars:
        return text[:max_chars].rstrip() + "\n..."
    return text or None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--output", choices=["llm", "json"], default="llm")

    parser.add_argument("--short-window-days", type=int, default=7)
    parser.add_argument("--dow-weeks", type=int, default=8)
    parser.add_argument("--min-baseline-samples", type=int, default=6)
    parser.add_argument("--compare-mode", choices=["abcd", "a_vs_bcd"], default="abcd")

    parser.add_argument("--index-summary", default="")
    parser.add_argument("--data-path-md", default="")
    parser.add_argument("--business-definition", default="")

    parser.add_argument("--no-deepseek", action="store_true")
    parser.add_argument("--deepseek-model", default="deepseek-chat")
    parser.add_argument("--deepseek-base-url", default="https://api.deepseek.com")
    parser.add_argument("--deepseek-timeout-s", type=int, default=60)
    parser.add_argument("--deepseek-max-tokens", type=int, default=900)
    parser.add_argument("--deepseek-temperature", type=float, default=0.2)
    parser.add_argument("--env-file", default=str(Path.cwd() / ".env"))

    parser.add_argument("--metric-doc-md", default=str(Path.cwd() / "scripts" / "指标体系梳理.md"))
    parser.add_argument("--metric-doc-max-chars", type=int, default=6000)

    parser.add_argument("--cache-dir", default="")
    args = parser.parse_args()

    start_arg = (str(args.start).strip() if args.start is not None else "") or None
    end_arg = (str(args.end).strip() if args.end is not None else "") or None
    date_arg = (str(args.date).strip() if args.date is not None else "") or None

    if date_arg and ("~" in date_arg) and (start_arg is None and end_arg is None):
        left, right = [x.strip() for x in date_arg.split("~", 1)]
        if left and right:
            start_arg = left
            end_arg = right
            date_arg = None

    is_range = start_arg is not None or end_arg is not None
    if is_range and (start_arg is None or end_arg is None):
        raise ValueError("周期评估必须同时提供 --start 与 --end，或使用 --date A~B")
    if (not is_range) and (not date_arg):
        raise ValueError("请提供 --date YYYY-MM-DD 或 --start/--end 或 --date A~B")

    target = (None if is_range else _parse_ymd(str(date_arg)))
    range_start = (None if not is_range else _parse_ymd(str(start_arg)))
    range_end = (None if not is_range else _parse_ymd(str(end_arg)))
    python_executable = str(args.python)

    if args.index_summary:
        index_summary_path = Path(args.index_summary).resolve()
    else:
        index_summary_path = _resolve_project_file(
            [
                Path.cwd() / "skills" / "index_summary.py",
                Path.cwd() / "scripts" / "index_summary.py",
            ]
        )
        if index_summary_path is None:
            raise RuntimeError("未找到 index_summary.py，请用 --index-summary 指定路径")

    if args.data_path_md:
        data_path_md = Path(args.data_path_md).resolve()
    else:
        data_path_md = _resolve_project_file([Path.cwd() / "schema" / "data_path.md"])
        if data_path_md is None:
            raise RuntimeError("未找到 schema/data_path.md，请用 --data-path-md 指定路径")

    if args.business_definition:
        business_definition_path = Path(args.business_definition).resolve()
    else:
        business_definition_path = _resolve_project_file([Path.cwd() / "schema" / "business_definition.json"])
        if business_definition_path is None:
            raise RuntimeError("未找到 schema/business_definition.json，请用 --business-definition 指定路径")

    cache_dir = (None if not args.cache_dir else Path(args.cache_dir).resolve())

    if not is_range:
        cur_obj = _run_index_summary(python_executable, index_summary_path, target, data_path_md=data_path_md)
        cur_flat = _flatten_metrics(cur_obj)

        short_dates = _daterange(target - timedelta(days=max(args.short_window_days, 1)), target - timedelta(days=1))
        dow_dates = _same_weekday_history(target, weeks=args.dow_weeks)
        wow_ranges = None
        same_weekday_ranges = None
        date_label = target.isoformat()
    else:
        cur_range_obj = _run_index_summary_range(
            python_executable=python_executable,
            index_summary_path=index_summary_path,
            start_date=range_start,
            end_date_inclusive=range_end,
            data_path_md=data_path_md,
        )
        mean_obj = cur_range_obj.get("mean")
        if not isinstance(mean_obj, dict):
            raise RuntimeError("周期输出缺少 mean 字段")
        cur_obj = {"date": f"{range_start.isoformat()}~{range_end.isoformat()}"}
        cur_obj.update(mean_obj)
        cur_flat = _flatten_metrics(cur_obj)

        short_dates = None
        dow_dates = None
        wow_ranges = [(range_start - timedelta(days=7), range_end - timedelta(days=7))]
        same_weekday_ranges = [
            (range_start - timedelta(days=7 * k), range_end - timedelta(days=7 * k))
            for k in range(1, max(int(args.dow_weeks), 0) + 1)
        ]
        date_label = f"{range_start.isoformat()}~{range_end.isoformat()}"

    presale_ranges, launch_ranges, activity_debug = _build_activity_baselines(business_definition_path)

    source_baselines: list[BaselineSpec] = []
    if not is_range:
        source_baselines.extend(
            [
                BaselineSpec("B_short_window", dates=short_dates, ranges=None),
                BaselineSpec("C_same_weekday", dates=dow_dates, ranges=None),
            ]
        )
    else:
        source_baselines.extend(
            [
                BaselineSpec("B_short_window", dates=None, ranges=wow_ranges),
                BaselineSpec("C_same_weekday", dates=None, ranges=same_weekday_ranges),
            ]
        )
    source_baselines.append(
        BaselineSpec("D_activity", dates=None, ranges=(presale_ranges + launch_ranges))
    )

    source_samples: dict[str, list[dict[str, Any]]] = {}
    for b in source_baselines:
        if b.dates is not None:
            source_samples[b.name] = _collect_samples(
                python_executable=python_executable,
                index_summary_path=index_summary_path,
                dates=b.dates,
                data_path_md=data_path_md,
                cache_dir=cache_dir,
            )
        elif b.ranges is not None:
            source_samples[b.name] = _collect_range_samples(
                python_executable=python_executable,
                index_summary_path=index_summary_path,
                ranges=b.ranges,
                data_path_md=data_path_md,
                cache_dir=cache_dir,
            )
        else:
            source_samples[b.name] = []

    if args.compare_mode == "a_vs_bcd":
        eval_samples = {
            "BCD_merged": (
                source_samples.get("B_short_window", [])
                + source_samples.get("C_same_weekday", [])
                + source_samples.get("D_activity", [])
            )
        }
    else:
        eval_samples = {
            "B_short_window": source_samples.get("B_short_window", []),
            "C_same_weekday": source_samples.get("C_same_weekday", []),
            "D_activity": source_samples.get("D_activity", []),
        }

    baseline_flats: dict[str, list[dict[str, Any]]] = {
        name: [_flatten_metrics(o) for o in objs] for name, objs in eval_samples.items()
    }

    rate_specs: dict[str, dict[str, str]] = (
        {}
        if is_range
        else {
            "下发线索转化率/下发线索当日试驾率": {
                "n": "下发线索转化率/下发线索当日试驾数",
                "d": "下发线索转化率/下发线索数",
            },
            "下发线索转化率/下发 (门店)线索当日锁单率": {
                "n": "下发线索转化率/下发 (门店)线索当日锁单数",
                "d": "下发线索转化率/下发线索数 (门店)",
                "d_fallback": "下发线索转化率/下发线索数",
            },
            "下发线索转化率/下发线索当7日锁单率": {
                "n": "下发线索转化率/下发线索 7 日锁单数",
                "d": "下发线索转化率/下发线索数",
            },
            "下发线索转化率/下发线索当30日锁单率": {
                "n": "下发线索转化率/下发线索 30 日锁单数",
                "d": "下发线索转化率/下发线索数",
            },
        }
    )
    rate_keys = set(rate_specs.keys())

    metrics_out: dict[str, Any] = {}
    rate_out: dict[str, Any] = {}
    alerts: list[dict[str, Any]] = []

    for metric_key, raw_val in cur_flat.items():
        if metric_key == "date":
            continue
        if not _should_evaluate_metric(metric_key):
            continue
        if metric_key in rate_keys:
            continue
        cur_num = _to_number(raw_val)
        if cur_num is None:
            continue

        per_baseline: dict[str, Any] = {}
        for baseline_name, rows in baseline_flats.items():
            xs: list[float] = []
            for r in rows:
                v = _to_number(r.get(metric_key))
                if v is not None:
                    xs.append(v)
            if len(xs) < int(args.min_baseline_samples):
                continue
            per_baseline[baseline_name] = _evaluate_distribution(
                xs, cur_num, transform=_choose_transform(metric_key, raw_val)
            )

        if not per_baseline:
            continue

        fused = min(
            ((k, v.get("tail_p")) for k, v in per_baseline.items() if isinstance(v, dict) and v.get("tail_p") is not None),
            key=lambda kv: float(kv[1]),
            default=(None, None),
        )

        metrics_out[metric_key] = {
            "value": raw_val,
            "value_num": cur_num,
            "baselines": per_baseline,
            "worst_baseline": fused[0],
            "worst_tail_p": fused[1],
        }

        sev_rank = {"red": 2, "yellow": 1, "green": 0, None: -1}
        worst_sev = max(
            ((bn, per_baseline[bn].get("severity"), per_baseline[bn].get("pctl")) for bn in per_baseline.keys()),
            key=lambda x: sev_rank.get(x[1], -1),
            default=(None, None, None),
        )
        if worst_sev[1] in ("red", "yellow"):
            alerts.append({"metric": metric_key, "severity": worst_sev[1], "baseline": worst_sev[0], "pctl": worst_sev[2], "value": raw_val})

    for rate_key, spec in rate_specs.items():
        raw_val = cur_flat.get(rate_key)
        n_raw = _to_number(cur_flat.get(spec["n"]))
        d_raw = _to_number(cur_flat.get(spec["d"]))
        if (d_raw is None or d_raw <= 0) and "d_fallback" in spec:
            d2 = _to_number(cur_flat.get(spec["d_fallback"]))
            if d2 is not None and d2 > 0:
                d_raw = d2
        if n_raw is None or d_raw is None or d_raw <= 0:
            continue

        cur_n = int(round(n_raw))
        cur_d = int(round(d_raw))

        per_baseline: dict[str, Any] = {}
        for baseline_name, rows in baseline_flats.items():
            pairs: list[tuple[int, int]] = []
            for r in rows:
                n = _to_number(r.get(spec["n"]))
                d = _to_number(r.get(spec["d"]))
                if (d is None or d <= 0) and "d_fallback" in spec:
                    d2 = _to_number(r.get(spec["d_fallback"]))
                    if d2 is not None and d2 > 0:
                        d = d2
                if n is None or d is None or d <= 0:
                    continue
                pairs.append((int(round(n)), int(round(d))))
            if len(pairs) < int(args.min_baseline_samples):
                continue
            per_baseline[baseline_name] = _evaluate_rate_with_denoms(pairs, (cur_n, cur_d))

        if not per_baseline:
            continue

        fused = min(
            ((k, v.get("tail_p")) for k, v in per_baseline.items() if isinstance(v, dict) and v.get("tail_p") is not None),
            key=lambda kv: float(kv[1]),
            default=(None, None),
        )

        value_show = raw_val
        if value_show is None:
            value_show = (cur_n / cur_d)
        rate_out[rate_key] = {
            "value": value_show,
            "value_num": _to_number(value_show),
            "numerator": cur_n,
            "denominator": cur_d,
            "baselines": per_baseline,
            "worst_baseline": fused[0],
            "worst_tail_p": fused[1],
        }

        sev_rank = {"red": 2, "yellow": 1, "green": 0, None: -1}
        worst_sev = max(
            ((bn, per_baseline[bn].get("severity"), per_baseline[bn].get("pctl")) for bn in per_baseline.keys()),
            key=lambda x: sev_rank.get(x[1], -1),
            default=(None, None, None),
        )
        if worst_sev[1] in ("red", "yellow"):
            alerts.append({"metric": rate_key, "severity": worst_sev[1], "baseline": worst_sev[0], "pctl": worst_sev[2], "value": value_show})

    alerts = sorted(alerts, key=lambda x: (0 if x["severity"] == "red" else 1, float(x.get("pctl") or 0.5)))

    comparison_dataset_rows = [
        {"group": "A_target", "sample": date_label},
    ]
    for group_name, samples in source_samples.items():
        for i, sample in enumerate(samples, start=1):
            comparison_dataset_rows.append(
                {
                    "group": group_name,
                    "sample": _sample_tag(sample, fallback=f"{group_name}#{i}"),
                }
            )

    output = {
        "date": date_label,
        "compare_mode": args.compare_mode,
        "baselines": {
            "source_groups": {
                "B_short_window": (
                    ([d.isoformat() for d in short_dates] if short_dates is not None else [[s.isoformat(), e.isoformat()] for (s, e) in wow_ranges])
                ),
                "C_same_weekday": (
                    ([d.isoformat() for d in dow_dates] if dow_dates is not None else [[s.isoformat(), e.isoformat()] for (s, e) in same_weekday_ranges])
                ),
                "D_activity_presale": [[s.isoformat(), e.isoformat()] for (s, e) in presale_ranges],
                "D_activity_launch": [[s.isoformat(), e.isoformat()] for (s, e) in launch_ranges],
            },
            "evaluation_groups": {
                k: len(v) for k, v in eval_samples.items()
            },
        },
        "comparison_dataset": {
            "description": "ABCD 拼接视角：A为目标样本，B短期基线，C同周期基线，D活动基线",
            "rows": comparison_dataset_rows,
        },
        "activity_sampling": {
            "periods": activity_debug,
        },
        "min_baseline_samples": int(args.min_baseline_samples),
        "alerts": alerts[:30],
        "metrics": metrics_out,
        "rates": rate_out,
    }

    if args.output == "json":
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    metric_doc_text = _load_metric_doc_text(Path(args.metric_doc_md).resolve(), max_chars=int(args.metric_doc_max_chars))
    if args.no_deepseek:
        lines = []
        lines.append(f"{output['date']} 监控总结")
        lines.append(f"- 告警数: {len(output['alerts'])}")
        for a in output["alerts"][:10]:
            lines.append(f"- {a['severity']} {a['metric']}: {a.get('value')} (baseline={a.get('baseline')}, pctl={a.get('pctl')})")
        print("\n".join(lines))
        return

    api_key = _load_deepseek_api_key(Path(args.env_file).resolve())
    if not api_key:
        print("未找到 DEEPSEEK_API_KEY（请设置环境变量或在 .env 中配置）")
        return

    context = _build_llm_context(output, metric_doc_text=metric_doc_text)
    messages = [
        {
            "role": "system",
            "content": (
                "你是业务监控分析助手。基于给定的监控评估结果，输出一段中文总结，要求：\n"
                "0) 优先使用 metric_doc（指标体系梳理）解释指标含义与分子分母；\n"
                "1) 先给 3-6 条结论要点（含关键数字）；\n"
                "2) 对每条告警给出“解释/可能原因/需要验证的分母或口径”；\n"
                "3) 给出可执行的业务动作建议（按优先级排序）；\n"
                "4) 区分“好异常/坏异常/口径异常”的可能性；\n"
                "5) 避免堆砌字段名，尽量把告警归纳到漏斗/结构/价格/集中度四类。\n"
            ),
        },
        {"role": "user", "content": json.dumps(context, ensure_ascii=False, indent=2)},
    ]

    t0 = time.time()
    resp = _deepseek_chat(
        api_key=api_key,
        base_url=str(args.deepseek_base_url),
        model=str(args.deepseek_model),
        messages=messages,
        temperature=float(args.deepseek_temperature),
        max_tokens=int(args.deepseek_max_tokens),
        timeout_s=int(args.deepseek_timeout_s),
    )
    content = _extract_deepseek_text(resp).strip()
    if content:
        print(content)
        return
    elapsed_ms = int((time.time() - t0) * 1000)
    print(f"DeepSeek 返回空内容（elapsed_ms={elapsed_ms}）")


if __name__ == "__main__":
    main()
