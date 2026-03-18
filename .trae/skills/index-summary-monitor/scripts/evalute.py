import argparse
import csv
import glob
import json
import math
from pathlib import Path
from typing import Any


def _split_patterns(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for v in values:
        for part in str(v).split(","):
            s = part.strip()
            if s:
                out.append(s)
    return out


def _expand_csv_paths(values: list[str] | None) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for pat in _split_patterns(values):
        matched = glob.glob(pat)
        if not matched:
            matched = [pat]
        for p in matched:
            rp = str(Path(p).expanduser().resolve())
            if rp in seen:
                continue
            seen.add(rp)
            paths.append(Path(rp))
    return paths


def _split_columns(value: str | None) -> set[str] | None:
    if value is None:
        return None
    cols = {x.strip() for x in str(value).split(",") if x.strip()}
    return cols or None


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        x = float(value)
        if math.isfinite(x):
            return x
        return None
    s = str(value).strip()
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


def _median(values: list[float]) -> float | None:
    xs = sorted([x for x in values if math.isfinite(x)])
    if not xs:
        return None
    n = len(xs)
    i = n // 2
    if n % 2 == 1:
        return xs[i]
    return (xs[i - 1] + xs[i]) / 2.0


def _quantile(values: list[float], q: float) -> float | None:
    xs = sorted([x for x in values if math.isfinite(x)])
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
    return xs[lo] * (1.0 - w) + xs[hi] * w


def _percentile_rank(values: list[float], x: float) -> float | None:
    xs = sorted([v for v in values if math.isfinite(v)])
    if not xs:
        return None
    le = 0
    for v in xs:
        if v <= x:
            le += 1
        else:
            break
    return le / len(xs)


def _mad(values: list[float], center: float) -> float | None:
    return _median([abs(v - center) for v in values if math.isfinite(v)])


def _robust_z(values: list[float], x: float, transform: str) -> float | None:
    xs = [v for v in values if math.isfinite(v)]
    if not xs:
        return None
    if transform == "log1p":
        xs_t = [math.log1p(max(v, 0.0)) for v in xs]
        x_t = math.log1p(max(x, 0.0))
    elif transform == "logit":
        eps = 1e-6
        xs_t = []
        for v in xs:
            p = min(max(v, eps), 1 - eps)
            xs_t.append(math.log(p / (1 - p)))
        p_cur = min(max(x, eps), 1 - eps)
        x_t = math.log(p_cur / (1 - p_cur))
    else:
        xs_t = xs
        x_t = x
    c = _median(xs_t)
    if c is None:
        return None
    m = _mad(xs_t, c)
    if m is None or m == 0:
        return None
    return (x_t - c) / (1.4826 * m)


def _normal_tail_p(z: float) -> float:
    return math.erfc(abs(float(z)) / math.sqrt(2.0))


def _severity(pctl: float | None) -> str | None:
    if pctl is None:
        return None
    if pctl <= 0.01 or pctl >= 0.99:
        return "red"
    if pctl <= 0.05 or pctl >= 0.95:
        return "yellow"
    return "green"


def _choose_transform(metric: str, raw: Any) -> str:
    s = str(raw).strip()
    if s.endswith("%") or metric.endswith("率") or "share_" in metric or metric.endswith("占比"):
        return "logit"
    v = _to_number(raw)
    if v is not None and v >= 0:
        return "log1p"
    return "none"


def _read_matrix_samples(csv_path: Path, only_columns: set[str] | None) -> list[dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header or len(header) < 2 or str(header[0]).strip() != "metric":
            raise ValueError(f"CSV 结构不合法: {csv_path}")
        columns = [str(c).strip() for c in header[1:]]
        selected_idx = [i for i, c in enumerate(columns) if (only_columns is None or c in only_columns)]
        samples = [
            {"sample": f"{csv_path.name}::{columns[i]}", "column": columns[i], "metrics": {}}
            for i in selected_idx
        ]
        for row in reader:
            if not row:
                continue
            metric = str(row[0]).strip()
            if not metric:
                continue
            vals = row[1:]
            for j, col_i in enumerate(selected_idx):
                val = vals[col_i] if col_i < len(vals) else ""
                samples[j]["metrics"][metric] = val
        return samples


def _collect_group(paths: list[Path], columns: set[str] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in paths:
        if not p.exists():
            continue
        out.extend(_read_matrix_samples(p, columns))
    return out


def _evaluate_distribution(values: list[float], cur: float, transform: str) -> dict[str, Any]:
    xs = [x for x in values if math.isfinite(x)]
    pctl = _percentile_rank(xs, cur)
    z = _robust_z(xs, cur, transform)
    return {
        "baseline_n": len(xs),
        "baseline_median": _median(xs),
        "p10": _quantile(xs, 0.10),
        "p50": _quantile(xs, 0.50),
        "p90": _quantile(xs, 0.90),
        "pctl": pctl,
        "z": z,
        "tail_p": (None if z is None else _normal_tail_p(z)),
        "severity": _severity(pctl),
    }


def _evaluate_rate(pairs: list[tuple[int, int]], cur_pair: tuple[int, int]) -> dict[str, Any]:
    base = [(n, d) for n, d in pairs if d > 0 and n >= 0]
    n_cur, d_cur = cur_pair
    if not base or d_cur <= 0:
        return {"baseline_n": len(base), "denominator": d_cur}
    total_n = sum(n for n, _ in base)
    total_d = sum(d for _, d in base)
    alpha = total_n + 1.0
    beta = (total_d - total_n) + 1.0
    mu = alpha / (alpha + beta)
    var = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1))
    p_cur = n_cur / d_cur
    z = (p_cur - mu) / math.sqrt(max(var, 1e-12))
    rates = [n / d for n, d in base if d > 0]
    pctl = _percentile_rank(rates, p_cur)
    return {
        "baseline_n": len(base),
        "denominator": d_cur,
        "baseline_median": _median(rates),
        "pctl": pctl,
        "z": z,
        "tail_p": _normal_tail_p(z),
        "severity": _severity(pctl),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", nargs="+", required=True)
    parser.add_argument("--b", nargs="*", default=[])
    parser.add_argument("--c", nargs="*", default=[])
    parser.add_argument("--d", nargs="*", default=[])
    parser.add_argument("--a-columns", default=None)
    parser.add_argument("--b-columns", default=None)
    parser.add_argument("--c-columns", default=None)
    parser.add_argument("--d-columns", default=None)
    parser.add_argument("--a-column", default=None)
    parser.add_argument("--compare-mode", choices=["abcd", "a_vs_bcd"], default="abcd")
    parser.add_argument("--min-baseline-samples", type=int, default=3)
    parser.add_argument("--top-alerts", type=int, default=20)
    parser.add_argument("--output", choices=["json", "text"], default="json")
    parser.add_argument("--output-file", default=None)
    args = parser.parse_args()

    group_paths = {
        "A_target": _expand_csv_paths(args.a),
        "B_short_window": _expand_csv_paths(args.b),
        "C_same_weekday": _expand_csv_paths(args.c),
        "D_activity": _expand_csv_paths(args.d),
    }
    group_columns = {
        "A_target": _split_columns(args.a_columns),
        "B_short_window": _split_columns(args.b_columns),
        "C_same_weekday": _split_columns(args.c_columns),
        "D_activity": _split_columns(args.d_columns),
    }

    source_samples = {k: _collect_group(group_paths[k], group_columns[k]) for k in group_paths.keys()}
    a_samples = source_samples["A_target"]
    if not a_samples:
        raise RuntimeError("A 组无可用样本")
    target_sample = None
    if args.a_column:
        for s in a_samples:
            if s.get("column") == args.a_column:
                target_sample = s
                break
        if target_sample is None:
            raise RuntimeError(f"A 组未找到列: {args.a_column}")
    else:
        target_sample = a_samples[0]

    if args.compare_mode == "a_vs_bcd":
        eval_groups = {
            "BCD_merged": source_samples["B_short_window"] + source_samples["C_same_weekday"] + source_samples["D_activity"]
        }
    else:
        eval_groups = {
            "B_short_window": source_samples["B_short_window"],
            "C_same_weekday": source_samples["C_same_weekday"],
            "D_activity": source_samples["D_activity"],
        }

    rate_specs = {
        "下发线索转化率.下发线索当日试驾率": {"n": "下发线索转化率.下发线索当日试驾数", "d": "下发线索转化率.下发线索数"},
        "下发线索转化率.下发 (门店)线索当日锁单率": {
            "n": "下发线索转化率.下发 (门店)线索当日锁单数",
            "d": "下发线索转化率.下发线索数 (门店)",
            "d_fallback": "下发线索转化率.下发线索数",
        },
        "下发线索转化率.下发线索当7日锁单率": {"n": "下发线索转化率.下发线索 7 日锁单数", "d": "下发线索转化率.下发线索数"},
        "下发线索转化率.下发线索当30日锁单率": {"n": "下发线索转化率.下发线索 30 日锁单数", "d": "下发线索转化率.下发线索数"},
    }

    metrics_out: dict[str, Any] = {}
    rates_out: dict[str, Any] = {}
    alerts: list[dict[str, Any]] = []

    cur_metrics = target_sample["metrics"]
    rate_keys = set(rate_specs.keys())
    for metric, raw in cur_metrics.items():
        if metric in rate_keys:
            continue
        cur_num = _to_number(raw)
        if cur_num is None:
            continue
        per_group: dict[str, Any] = {}
        for g, samples in eval_groups.items():
            xs = []
            for s in samples:
                v = _to_number(s["metrics"].get(metric))
                if v is not None:
                    xs.append(v)
            if len(xs) < int(args.min_baseline_samples):
                continue
            per_group[g] = _evaluate_distribution(xs, cur_num, _choose_transform(metric, raw))
        if not per_group:
            continue
        metrics_out[metric] = {"value": raw, "value_num": cur_num, "baselines": per_group}
        for g, ev in per_group.items():
            if ev.get("severity") in ("red", "yellow"):
                alerts.append({"metric": metric, "severity": ev.get("severity"), "baseline": g, "pctl": ev.get("pctl"), "value": raw})

    for metric, spec in rate_specs.items():
        n_cur = _to_number(cur_metrics.get(spec["n"]))
        d_cur = _to_number(cur_metrics.get(spec["d"]))
        if (d_cur is None or d_cur <= 0) and "d_fallback" in spec:
            d_cur = _to_number(cur_metrics.get(spec["d_fallback"]))
        if n_cur is None or d_cur is None or d_cur <= 0:
            continue
        per_group: dict[str, Any] = {}
        for g, samples in eval_groups.items():
            pairs: list[tuple[int, int]] = []
            for s in samples:
                n = _to_number(s["metrics"].get(spec["n"]))
                d = _to_number(s["metrics"].get(spec["d"]))
                if (d is None or d <= 0) and "d_fallback" in spec:
                    d = _to_number(s["metrics"].get(spec["d_fallback"]))
                if n is None or d is None or d <= 0:
                    continue
                pairs.append((int(round(n)), int(round(d))))
            if len(pairs) < int(args.min_baseline_samples):
                continue
            per_group[g] = _evaluate_rate(pairs, (int(round(n_cur)), int(round(d_cur))))
        if not per_group:
            continue
        rates_out[metric] = {
            "value": cur_metrics.get(metric),
            "value_num": _to_number(cur_metrics.get(metric)),
            "numerator": int(round(n_cur)),
            "denominator": int(round(d_cur)),
            "baselines": per_group,
        }
        for g, ev in per_group.items():
            if ev.get("severity") in ("red", "yellow"):
                alerts.append({"metric": metric, "severity": ev.get("severity"), "baseline": g, "pctl": ev.get("pctl"), "value": cur_metrics.get(metric)})

    alerts = sorted(alerts, key=lambda x: (0 if x["severity"] == "red" else 1, float(x.get("pctl") or 0.5)))[: int(args.top_alerts)]
    output = {
        "compare_mode": args.compare_mode,
        "target_sample": target_sample["sample"],
        "groups": {k: len(v) for k, v in source_samples.items()},
        "evaluation_groups": {k: len(v) for k, v in eval_groups.items()},
        "alerts": alerts,
        "metrics": metrics_out,
        "rates": rates_out,
    }

    text = json.dumps(output, ensure_ascii=False, indent=2) if args.output == "json" else (
        "\n".join(
            [f"target={output['target_sample']}", f"alerts={len(alerts)}"]
            + [f"{a['severity']} {a['metric']} value={a['value']} baseline={a['baseline']} pctl={a.get('pctl')}" for a in alerts]
        )
    )
    if args.output_file:
        Path(args.output_file).expanduser().resolve().write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
