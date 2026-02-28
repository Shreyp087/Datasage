from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import pandas as pd


class NotebookRunner:
    """
    Executes notebook cells against a dataset DataFrame.
    Each cell type has its own execution logic.
    Results are stored as JSON-serializable objects.
    """

    def run_all(self, notebook: Any, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for cell in notebook.cells or []:
            cell_id = str(cell.get("id") or "")
            if not cell_id:
                continue
            try:
                result = self.run_cell(cell, df)
                results[cell_id] = {
                    "status": "success",
                    "result": result,
                    "executed_at": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as exc:
                results[cell_id] = {
                    "status": "error",
                    "error": str(exc),
                    "executed_at": datetime.now(timezone.utc).isoformat(),
                }
        return results

    def run_cell(self, cell: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        cell_type = str(cell.get("type") or "analysis").strip().lower()
        analysis_type = str(cell.get("analysis_type") or "").strip().lower()
        if cell_type == "text" and not analysis_type:
            return {
                "type": "text",
                "title": cell.get("title"),
                "content": cell.get("content") or cell.get("description") or cell.get("config", {}).get("text"),
            }

        atype = analysis_type
        config = cell.get("config", {}) or {}
        filtered_df = self._apply_filters(df.copy(), config.get("filters", {}) or {})

        dispatch = {
            "trend": self._run_trend,
            "distribution": self._run_distribution,
            "comparison": self._run_comparison,
            "summary": self._run_summary,
            "correlation": self._run_correlation,
            "top_n": self._run_top_n,
            "heatmap": self._run_heatmap,
            "text_sample": self._run_text_sample,
        }

        fn = dispatch.get(atype)
        if not fn:
            raise ValueError(f"Unknown analysis type: {atype}")
        return fn(filtered_df, config)

    def annotate_cells_with_results(
        self,
        cells: list[dict[str, Any]],
        results: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        updated: list[dict[str, Any]] = []
        for cell in cells or []:
            item = dict(cell)
            cell_id = str(item.get("id") or "")
            outcome = results.get(cell_id)
            if outcome:
                item["result"] = outcome.get("result")
                item["executed_at"] = outcome.get("executed_at")
                item["status"] = outcome.get("status")
                if outcome.get("error"):
                    item["error"] = outcome.get("error")
            updated.append(item)
        return updated

    def _run_trend(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        x = config.get("x_field", "year")
        group_by = config.get("group_by")

        if x not in df.columns:
            raise ValueError(f"Column '{x}' not found")

        if group_by and group_by in df.columns:
            counts = df.groupby([x, group_by], dropna=False).size().reset_index(name="count")
        else:
            counts = df.groupby(x, dropna=False).size().reset_index(name="count")

        counts = counts.sort_values(x)
        return {
            "type": "chart",
            "chart_type": config.get("chart_type", "line"),
            "data": self._records(counts),
            "x_field": x,
            "y_field": "count",
            "group_by": group_by if group_by in df.columns else None,
            "insight": self._trend_insight(counts, x),
        }

    def _run_distribution(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        field = config.get("field")
        top_n = int(config.get("top_n", 15))
        if not field:
            raise ValueError("Missing required config: field")
        if field not in df.columns:
            raise ValueError(f"Column '{field}' not found")

        series = df[field]
        counts = (
            series.value_counts(dropna=False)
            .head(top_n)
            .rename_axis(field)
            .reset_index(name="count")
        )
        total = int(series.notna().sum())
        counts["percentage"] = ((counts["count"] / max(total, 1)) * 100).round(1)

        return {
            "type": "chart",
            "chart_type": config.get("chart_type", "bar"),
            "data": self._records(counts),
            "x_field": field,
            "y_field": "count",
            "total_records": total,
            "null_count": int(series.isna().sum()),
        }

    def _run_comparison(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        x_field = config.get("x_field")
        y_field = config.get("y_field")
        agg = str(config.get("agg", "count")).lower()
        top_n = int(config.get("top_n", 20))

        if not x_field:
            raise ValueError("Missing required config: x_field")
        if x_field not in df.columns:
            raise ValueError(f"Column '{x_field}' not found")

        if not y_field or y_field == "count":
            data = (
                df.groupby(x_field, dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
                .head(top_n)
            )
            metric = "count"
        else:
            if y_field not in df.columns:
                raise ValueError(f"Column '{y_field}' not found")
            numeric = pd.to_numeric(df[y_field], errors="coerce")
            metric = y_field
            if agg == "sum":
                data = (
                    pd.DataFrame({x_field: df[x_field], y_field: numeric})
                    .groupby(x_field, dropna=False)[y_field]
                    .sum(min_count=1)
                    .reset_index()
                )
            elif agg == "median":
                data = (
                    pd.DataFrame({x_field: df[x_field], y_field: numeric})
                    .groupby(x_field, dropna=False)[y_field]
                    .median()
                    .reset_index()
                )
            else:
                data = (
                    pd.DataFrame({x_field: df[x_field], y_field: numeric})
                    .groupby(x_field, dropna=False)[y_field]
                    .mean()
                    .reset_index()
                )
            data = data.sort_values(y_field, ascending=False).head(top_n)

        return {
            "type": "chart",
            "chart_type": config.get("chart_type", "bar"),
            "data": self._records(data),
            "x_field": x_field,
            "y_field": metric,
            "aggregation": agg,
        }

    def _run_summary(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        del config

        def top_value(col: str) -> Any:
            if col not in df.columns or df[col].dropna().empty:
                return None
            return df[col].value_counts(dropna=True).index[0]

        return {
            "type": "stats",
            "total_incidents": int(len(df)),
            "date_range": {
                "earliest": str(df["date"].min()) if "date" in df.columns else None,
                "latest": str(df["date"].max()) if "date" in df.columns else None,
            },
            "unique_deployers": (
                int(df["allegeddeployerofaisystem_primary"].nunique(dropna=True))
                if "allegeddeployerofaisystem_primary" in df.columns
                else None
            ),
            "unique_developers": (
                int(df["allegeddeveloperofaisystem_primary"].nunique(dropna=True))
                if "allegeddeveloperofaisystem_primary" in df.columns
                else None
            ),
            "top_harm_type": top_value("harm_type"),
            "top_sector": top_value("sector_of_deployment"),
        }

    def _run_correlation(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        x_field = config.get("x_field")
        y_field = config.get("y_field")

        if x_field and y_field:
            if x_field not in df.columns or y_field not in df.columns:
                raise ValueError("x_field or y_field not found")
            pair = df[[x_field, y_field]].copy()
            pair[x_field] = pd.to_numeric(pair[x_field], errors="coerce")
            pair[y_field] = pd.to_numeric(pair[y_field], errors="coerce")
            pair = pair.dropna()
            if len(pair) < 2:
                corr = None
            else:
                corr = pair[x_field].corr(pair[y_field])
                if corr is not None and math.isnan(corr):
                    corr = None
            return {
                "type": "stat",
                "metric": "pearson_correlation",
                "x_field": x_field,
                "y_field": y_field,
                "value": corr,
                "sample_size": int(len(pair)),
            }

        numeric_df = df.select_dtypes(include=["number"]).copy()
        if numeric_df.empty:
            return {"type": "table", "data": [], "message": "No numeric columns available for correlation"}
        corr = numeric_df.corr()
        return {
            "type": "table",
            "matrix": self._dict_safe(corr.to_dict()),
            "columns": list(corr.columns),
        }

    def _run_top_n(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        field = config.get("field")
        n = int(config.get("n", 10))
        if not field:
            raise ValueError("Missing required config: field")
        if field not in df.columns:
            raise ValueError(f"Column '{field}' not found")

        counts = (
            df[field]
            .value_counts(dropna=False)
            .head(n)
            .rename_axis("entity")
            .reset_index(name="count")
        )

        return {
            "type": "chart",
            "chart_type": "horizontal_bar",
            "data": self._records(counts),
            "x_field": "count",
            "y_field": "entity",
        }

    def _run_heatmap(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        row_field = config.get("row_field")
        col_field = config.get("col_field")
        top_n = int(config.get("top_n", 8))
        if not row_field or not col_field:
            raise ValueError("Missing required config: row_field/col_field")
        if row_field not in df.columns or col_field not in df.columns:
            raise ValueError("row_field or col_field not found")

        top_rows = df[row_field].value_counts(dropna=True).head(top_n).index.tolist()
        top_cols = df[col_field].value_counts(dropna=True).head(top_n).index.tolist()
        filtered = df[df[row_field].isin(top_rows) & df[col_field].isin(top_cols)]
        pivot = pd.crosstab(filtered[row_field], filtered[col_field])

        return {
            "type": "heatmap",
            "data": self._dict_safe(pivot.to_dict()),
            "rows": [self._scalar_safe(v) for v in top_rows],
            "cols": [self._scalar_safe(v) for v in top_cols],
        }

    def _run_text_sample(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        field = config.get("field", "title")
        n = int(config.get("n", 10))
        if field not in df.columns:
            raise ValueError(f"Column '{field}' not found")
        series = df[field].dropna()
        if series.empty:
            sample: list[str] = []
        else:
            sample = series.sample(min(n, len(series)), random_state=42).astype(str).tolist()
        return {
            "type": "text_list",
            "samples": sample,
            "field": field,
        }

    def _apply_filters(self, df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
        for col, val in filters.items():
            if col not in df.columns:
                continue

            if isinstance(val, list):
                df = df[df[col].isin(val)]
                continue

            if isinstance(val, dict):
                min_v = val.get("min")
                max_v = val.get("max")
                contains_v = val.get("contains")
                if min_v is not None:
                    df = df[pd.to_numeric(df[col], errors="coerce") >= min_v]
                if max_v is not None:
                    df = df[pd.to_numeric(df[col], errors="coerce") <= max_v]
                if contains_v is not None:
                    df = df[df[col].astype(str).str.contains(str(contains_v), case=False, na=False)]
                continue

            df = df[df[col] == val]
        return df

    def _trend_insight(self, counts_df: pd.DataFrame, x_field: str) -> str:
        if counts_df.empty:
            return "No data available for trend analysis."

        if "count" not in counts_df.columns:
            return "Trend output is missing count values."

        line = counts_df.groupby(x_field, dropna=False)["count"].sum().reset_index().sort_values(x_field)
        if len(line) < 2:
            return "Insufficient data for trend analysis."

        first = float(line.iloc[0]["count"])
        last = float(line.iloc[-1]["count"])
        peak_row = line.loc[line["count"].idxmax()]
        change = ((last - first) / first * 100.0) if first > 0 else 0.0
        direction = "increased" if change > 0 else "decreased"
        return (
            f"Incidents {direction} by {abs(change):.0f}% from {line.iloc[0][x_field]} "
            f"to {line.iloc[-1][x_field]}. Peak was in {peak_row[x_field]} "
            f"with {int(peak_row['count'])} incidents."
        )

    def _records(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        return [self._dict_safe(row) for row in frame.to_dict(orient="records")]

    def _dict_safe(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            return {self._scalar_safe(k): self._dict_safe(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._dict_safe(item) for item in payload]
        return self._scalar_safe(payload)

    def _scalar_safe(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return str(value)
        return value
