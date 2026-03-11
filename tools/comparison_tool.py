import pandas as pd

from .query_tool import QueryTool


class ComparisonTool:
    def __init__(self, query_tool: QueryTool):
        self.query_tool = query_tool

    def perform_comparison(self, request: dict) -> str:
        dataset = request.get("dataset")
        metrics = request.get("metrics", [])
        dimensions = request.get("dimensions", [])
        filters = request.get("filters", [])
        time = request.get("time", {})
        comparison = request.get("comparison", {}) or {}

        time_field = time.get("field")
        current_start = time.get("start")
        current_end = time.get("end")
        comparison_type = comparison.get("type")

        if not dataset or not metrics or not time_field or not current_start or not current_end:
            return "对比分析缺少必要参数: dataset/metrics/time.field/time.start/time.end"

        if comparison_type not in {"yoy", "wow"}:
            return f"不支持的对比类型: {comparison_type}"

        current_start_ts = pd.Timestamp(current_start)
        current_end_ts = pd.Timestamp(current_end)

        if comparison_type == "yoy":
            compare_start_ts = current_start_ts - pd.DateOffset(years=1)
            compare_end_ts = current_end_ts - pd.DateOffset(years=1)
        else:
            compare_start_ts = current_start_ts - pd.Timedelta(days=7)
            compare_end_ts = current_end_ts - pd.Timedelta(days=7)

        current_plan = {
            "dataset": dataset,
            "metrics": metrics,
            "dimensions": dimensions,
            "filters": [
                *filters,
                {"field": time_field, "op": ">=", "value": current_start_ts.strftime("%Y-%m-%d")},
                {"field": time_field, "op": "<", "value": current_end_ts.strftime("%Y-%m-%d")},
            ],
        }

        compare_plan = {
            "dataset": dataset,
            "metrics": metrics,
            "dimensions": dimensions,
            "filters": [
                *filters,
                {"field": time_field, "op": ">=", "value": compare_start_ts.strftime("%Y-%m-%d")},
                {"field": time_field, "op": "<", "value": compare_end_ts.strftime("%Y-%m-%d")},
            ],
        }

        current_df = self.query_tool.execute_analysis_df(current_plan)
        compare_df = self.query_tool.execute_analysis_df(compare_plan)

        if isinstance(current_df, str):
            return current_df
        if isinstance(compare_df, str):
            return compare_df

        valid_dims = [d for d in dimensions if d in current_df.columns]
        metric_alias = metrics[0].get("alias") or "value"

        def _metric_column(df: pd.DataFrame) -> str | None:
            candidates = [c for c in df.columns if c not in valid_dims]
            if not candidates:
                return None
            return candidates[0]

        current_metric_col = _metric_column(current_df)
        compare_metric_col = _metric_column(compare_df)
        if not current_metric_col or not compare_metric_col:
            return "对比分析无法识别聚合结果列"

        if valid_dims:
            merged = pd.merge(
                current_df,
                compare_df,
                on=valid_dims,
                how="outer",
                suffixes=("_current", "_compare"),
            )
            merged_current_col = current_metric_col
            merged_compare_col = compare_metric_col
            if merged_current_col not in merged.columns and f"{current_metric_col}_current" in merged.columns:
                merged_current_col = f"{current_metric_col}_current"
            if merged_compare_col not in merged.columns and f"{compare_metric_col}_compare" in merged.columns:
                merged_compare_col = f"{compare_metric_col}_compare"

            if merged_current_col not in merged.columns or merged_compare_col not in merged.columns:
                return "对比分析合并结果列失败"

            merged[merged_current_col] = merged[merged_current_col].fillna(0)
            merged[merged_compare_col] = merged[merged_compare_col].fillna(0)
            merged.rename(
                columns={
                    merged_current_col: f"{metric_alias}_current",
                    merged_compare_col: f"{metric_alias}_compare",
                },
                inplace=True,
            )
            merged[f"{metric_alias}_diff"] = merged[f"{metric_alias}_current"] - merged[f"{metric_alias}_compare"]
            merged[f"{metric_alias}_diff_pct"] = merged.apply(
                lambda r: None
                if r[f"{metric_alias}_compare"] == 0
                else (r[f"{metric_alias}_diff"] / r[f"{metric_alias}_compare"]),
                axis=1,
            )
            result_df = merged[valid_dims + [f"{metric_alias}_current", f"{metric_alias}_compare", f"{metric_alias}_diff", f"{metric_alias}_diff_pct"]]
        else:
            current_value = float(current_df[current_metric_col].iloc[0]) if not current_df.empty else 0.0
            compare_value = float(compare_df[compare_metric_col].iloc[0]) if not compare_df.empty else 0.0
            diff = current_value - compare_value
            diff_pct = None if compare_value == 0 else diff / compare_value
            result_df = pd.DataFrame(
                [
                    {
                        f"{metric_alias}_current": current_value,
                        f"{metric_alias}_compare": compare_value,
                        f"{metric_alias}_diff": diff,
                        f"{metric_alias}_diff_pct": diff_pct,
                    }
                ]
            )

        return result_df.to_string(index=False)


COMPARISON_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "perform_comparison",
        "description": "执行派生指标对比分析（同比/环比），在同一口径下对比两个时间窗口。",
        "parameters": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string"},
                "metrics": {"type": "array"},
                "dimensions": {"type": "array"},
                "filters": {"type": "array"},
                "time": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"},
                        "start": {"type": "string"},
                        "end": {"type": "string"},
                    },
                    "required": ["field", "start", "end"],
                },
                "comparison": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["yoy", "wow"]},
                    },
                    "required": ["type"],
                },
            },
            "required": ["dataset", "metrics", "time", "comparison"],
        },
    },
}
