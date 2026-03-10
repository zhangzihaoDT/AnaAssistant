import contextlib
import io
import json
from pathlib import Path
from typing import Any, List, Optional, Union

import pandas as pd


def _safe_read_csv(file_path: Path) -> pd.DataFrame:
    candidates = [
        {"encoding": "utf-8", "sep": ","},
        {"encoding": "utf-16", "sep": "\t"},
        {"encoding": "utf-16le", "sep": "\t"},
        {"encoding": "gbk", "sep": ","},
        {"encoding": "gb18030", "sep": ","},
    ]
    for option in candidates:
        try:
            return pd.read_csv(file_path, low_memory=False, **option)
        except Exception:
            continue
    return pd.read_csv(file_path, low_memory=False)


class QueryTool:
    def __init__(self, data_path_file: str, schema_dir: str):
        self.data_path_file = Path(data_path_file)
        self.schema_dir = Path(schema_dir)
        self.datasets: dict[str, pd.DataFrame] = {}
        self._loaded = False

    def _parse_dataset_paths(self) -> list[Path]:
        if not self.data_path_file.exists():
            return []
        paths: list[Path] = []
        for raw in self.data_path_file.read_text(encoding="utf-8").splitlines():
            value = raw.strip()
            if not value:
                continue
            normalized = value.replace("\\_", "_")
            paths.append(Path(normalized))
        return paths

    def _load_datasets(self) -> None:
        if self._loaded:
            return
        for file_path in self._parse_dataset_paths():
            if not file_path.exists():
                continue
            suffix = file_path.suffix.lower()
            if suffix == ".parquet":
                df = pd.read_parquet(file_path)
            elif suffix == ".csv":
                df = _safe_read_csv(file_path)
            else:
                continue
            stem_key = file_path.stem
            name_key = file_path.name
            self.datasets[stem_key] = df
            self.datasets[name_key] = df
        self._loaded = True

    def _schema_context(self) -> dict[str, str]:
        schema: dict[str, str] = {}
        schema_file = self.schema_dir / "schema.md"
        business_file = self.schema_dir / "business_definition.json"
        schema["schema_md"] = schema_file.read_text(encoding="utf-8") if schema_file.exists() else ""
        schema["business_definition"] = business_file.read_text(encoding="utf-8") if business_file.exists() else ""
        return schema

    def answer_question(self, question: str) -> str:
        """Fallback simple keyword matching"""
        self._load_datasets()
        lowered = question.strip()
        if not lowered:
            return "问题为空，请提供要查询的指标问题。"
        for dataset_name, df in self.datasets.items():
            if "." in dataset_name:
                continue
            for column in df.columns:
                if column in lowered:
                    series = df[column]
                    if "平均" in lowered:
                        return f"{dataset_name}.{column} 平均值: {series.mean()}"
                    if "最大" in lowered:
                        return f"{dataset_name}.{column} 最大值: {series.max()}"
                    if "最小" in lowered:
                        return f"{dataset_name}.{column} 最小值: {series.min()}"
                    if "总和" in lowered or "合计" in lowered:
                        return f"{dataset_name}.{column} 总和: {series.sum()}"
                    if "中位" in lowered:
                        return f"{dataset_name}.{column} 中位数: {series.median()}"
                    if "多少" in lowered or "数量" in lowered or "总数" in lowered:
                        return f"{dataset_name}.{column} 非空数量: {series.count()}"
        return "未能从问题中匹配到字段，请在问题中包含明确字段名。"

    def execute_analysis(self, plan: dict) -> str:
        """
        Execute a BI DSL analysis plan.
        """
        # 记录开始执行
        print(f"      [QueryTool] 接收到 DSL 计划: {json.dumps(plan, ensure_ascii=False)}")
        
        self._load_datasets()
        dataset_name = plan.get("dataset")
        if not dataset_name or dataset_name not in self.datasets:
            # Try finding fuzzy match or default to first
            if not self.datasets:
                return "未加载到任何数据集。"
            if not dataset_name:
                 # Default to assign_data or order_full_data if available
                if "assign_data" in self.datasets:
                    dataset_name = "assign_data"
                elif "order_full_data" in self.datasets:
                    dataset_name = "order_full_data"
                else:
                    dataset_name = list(self.datasets.keys())[0]
            elif dataset_name not in self.datasets:
                return f"找不到数据集: {dataset_name}。可用数据集: {list(self.datasets.keys())}"

        df = self.datasets[dataset_name]
        
        # 1. Filters
        filters = plan.get("filters", [])
        for f in filters:
            field = f.get("field")
            op = f.get("op")
            value = f.get("value")
            if field not in df.columns:
                continue
            
            if op == "==":
                df = df[df[field] == value]
            elif op == "!=":
                df = df[df[field] != value]
            elif op == ">":
                df = df[df[field] > value]
            elif op == "<":
                df = df[df[field] < value]
            elif op == ">=":
                df = df[df[field] >= value]
            elif op == "<=":
                df = df[df[field] <= value]
            elif op == "in" and isinstance(value, list):
                df = df[df[field].isin(value)]

        # 2. Grouping & Aggregation
        dimensions = plan.get("dimensions", [])
        metrics = plan.get("metrics", [])
        
        result_df = df
        
        if metrics:
            agg_dict = {}
            rename_dict = {}
            for m in metrics:
                field = m.get("field")
                agg_func = m.get("agg", "count")
                alias = m.get("alias", f"{field}_{agg_func}")
                
                if field not in df.columns:
                    continue
                    
                # Handle special count case
                if agg_func == "count" and field == "*":
                     # Just use any column for counting
                     field = df.columns[0]
                
                if field in agg_dict:
                     # If same field multiple aggs, need complex handling, simplify for now
                     if isinstance(agg_dict[field], list):
                         agg_dict[field].append(agg_func)
                     else:
                         agg_dict[field] = [agg_dict[field], agg_func]
                else:
                    agg_dict[field] = agg_func
                
                # We can't easily rename in one go with simple agg, so we'll rename after
                # But if dimensions exist, we use groupby
            
            if dimensions:
                valid_dims = [d for d in dimensions if d in df.columns]
                if valid_dims:
                    try:
                        result_df = df.groupby(valid_dims).agg(agg_dict).reset_index()
                    except Exception as e:
                        return f"聚合计算失败: {e}"
                else:
                     # Dimensions invalid, fallback to no group
                     result_df = df.agg(agg_dict).to_frame().T
            else:
                # No dimensions, scalar aggregation
                try:
                    result_df = df.agg(agg_dict).to_frame().T
                except Exception as e:
                    return f"聚合计算失败: {e}"
        
        # 3. Sorting
        sort_opts = plan.get("sort", [])
        if sort_opts and not result_df.empty:
            # Support single sort for now or list
            if isinstance(sort_opts, dict):
                sort_opts = [sort_opts]
            
            by = []
            ascending = []
            for s in sort_opts:
                field = s.get("field")
                if field in result_df.columns:
                    by.append(field)
                    ascending.append(s.get("order", "asc") == "asc")
            
            if by:
                result_df = result_df.sort_values(by=by, ascending=ascending)

        # 4. Limit
        limit = plan.get("limit")
        if limit:
            result_df = result_df.head(int(limit))

        return result_df.to_string(index=False)


QUERY_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "perform_analysis",
        "description": "执行 BI 数据分析。根据 DSL 计划执行数据查询、聚合、排序等操作。",
        "parameters": {
            "type": "object",
            "properties": {
                "plan": {
                    "type": "object",
                    "description": "BI 分析计划 (DSL)",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "数据集名称 (e.g. 'assign_data', 'order_full_data')"
                        },
                        "metrics": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "agg": {"type": "string", "enum": ["sum", "mean", "count", "min", "max"]},
                                    "alias": {"type": "string"}
                                },
                                "required": ["field", "agg"]
                            }
                        },
                        "dimensions": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "filters": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "op": {"type": "string", "enum": ["==", "!=", ">", "<", ">=", "<=", "in"]},
                                    "value": {} 
                                },
                                "required": ["field", "op", "value"]
                            }
                        },
                        "sort": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "order": {"type": "string", "enum": ["asc", "desc"]}
                                }
                            }
                        },
                        "limit": {"type": "integer"}
                    },
                    "required": ["dataset", "metrics"]
                }
            },
            "required": ["plan"]
        }
    }
}
