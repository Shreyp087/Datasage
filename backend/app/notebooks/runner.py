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
            "detailed_summary": self._run_detailed_summary,
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

    def _resolve_existing_column(self, df: pd.DataFrame, candidates: list[str | None]) -> str | None:
        for candidate in candidates:
            if candidate and candidate in df.columns:
                return str(candidate)
        return None

    def _date_range_from_field(self, df: pd.DataFrame, field: str | None) -> dict[str, Any]:
        if not field or field not in df.columns:
            return {"earliest": None, "latest": None, "field": None}
        parsed = pd.to_datetime(df[field], errors="coerce")
        if not parsed.notna().any():
            return {"earliest": None, "latest": None, "field": field}
        earliest = parsed.min()
        latest = parsed.max()
        return {
            "earliest": earliest.isoformat() if hasattr(earliest, "isoformat") else str(earliest),
            "latest": latest.isoformat() if hasattr(latest, "isoformat") else str(latest),
            "field": field,
        }

    def _run_summary(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        def top_value(col: str | None) -> Any:
            if not col or col not in df.columns or df[col].dropna().empty:
                return None
            return df[col].value_counts(dropna=True).index[0]

        date_field = self._resolve_existing_column(
            df,
            [
                config.get("date_field"),
                "date",
                "incident_date",
                "event_date",
                "created_at",
            ],
        )
        harm_field = self._resolve_existing_column(df, [config.get("harm_field"), "harm_type"])
        sector_field = self._resolve_existing_column(df, [config.get("sector_field"), "sector_of_deployment"])
        deployer_field = self._resolve_existing_column(
            df,
            [config.get("deployer_field"), "allegeddeployerofaisystem_primary"],
        )
        developer_field = self._resolve_existing_column(
            df,
            [config.get("developer_field"), "allegeddeveloperofaisystem_primary"],
        )

        top_fields = [str(field) for field in (config.get("top_fields") or []) if str(field) in df.columns]
        if not top_fields:
            top_fields = [field for field in [harm_field, sector_field] if field]

        unique_fields_cfg = config.get("unique_fields") or {}
        unique_counts: dict[str, int] = {}
        if isinstance(unique_fields_cfg, dict):
            for label, field in unique_fields_cfg.items():
                if field and field in df.columns:
                    unique_counts[str(label)] = int(df[str(field)].nunique(dropna=True))
        elif isinstance(unique_fields_cfg, list):
            for field in unique_fields_cfg:
                if field and str(field) in df.columns:
                    unique_counts[str(field)] = int(df[str(field)].nunique(dropna=True))

        if deployer_field:
            unique_counts.setdefault("deployer", int(df[deployer_field].nunique(dropna=True)))
        if developer_field:
            unique_counts.setdefault("developer", int(df[developer_field].nunique(dropna=True)))

        top_values = {
            field: top_value(field)
            for field in top_fields
        }

        return {
            "type": "stats",
            "total_incidents": int(len(df)),
            "total_rows": int(len(df)),
            "column_count": int(len(df.columns)),
            "date_range": self._date_range_from_field(df, date_field),
            "unique_counts": unique_counts,
            "top_values": top_values,
            "unique_deployers": int(df[deployer_field].nunique(dropna=True)) if deployer_field else None,
            "unique_developers": int(df[developer_field].nunique(dropna=True)) if developer_field else None,
            "top_harm_type": top_value(harm_field),
            "top_sector": top_value(sector_field),
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

    def _run_detailed_summary(self, df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
        top_n = int(config.get("top_n", 5))
        total = int(len(df))

        year_counts_df = pd.DataFrame(columns=["year", "count"])
        year_field = self._resolve_existing_column(df, [config.get("year_field"), "year"])
        date_field = self._resolve_existing_column(df, [config.get("date_field"), "date", "incident_date", "event_date"])
        if year_field:
            years = pd.to_numeric(df[year_field], errors="coerce")
            if years.notna().any():
                year_counts_df = (
                    years.dropna()
                    .astype(int)
                    .value_counts()
                    .sort_index()
                    .rename_axis("year")
                    .reset_index(name="count")
                )
        elif date_field:
            dates = pd.to_datetime(df[date_field], errors="coerce")
            if dates.notna().any():
                year_counts_df = (
                    dates.dt.year.dropna()
                    .astype(int)
                    .value_counts()
                    .sort_index()
                    .rename_axis("year")
                    .reset_index(name="count")
                )

        harm_field = self._resolve_existing_column(df, [config.get("harm_field"), "harm_type"])
        sector_field = self._resolve_existing_column(df, [config.get("sector_field"), "sector_of_deployment"])
        deployer_field = self._resolve_existing_column(
            df,
            [config.get("deployer_field"), "allegeddeployerofaisystem_primary"],
        )
        developer_field = self._resolve_existing_column(
            df,
            [config.get("developer_field"), "allegeddeveloperofaisystem_primary"],
        )

        top_harms = self._top_counts(df, harm_field, top_n)
        top_sectors = self._top_counts(df, sector_field, top_n)
        top_deployers = self._top_counts(df, deployer_field, top_n)
        top_developers = self._top_counts(df, developer_field, top_n)

        coverage = {
            "harm_type_pct": self._coverage_pct(df, harm_field),
            "sector_of_deployment_pct": self._coverage_pct(df, sector_field),
            "deployer_pct": self._coverage_pct(df, deployer_field),
            "developer_pct": self._coverage_pct(df, developer_field),
            "date_pct": self._coverage_pct(df, date_field),
        }
        coverage["field_coverage"] = {
            field: self._coverage_pct(df, field)
            for field in [harm_field, sector_field, deployer_field, developer_field, date_field]
            if field
        }

        primary_label = str(config.get("primary_label") or "Top categories")
        secondary_label = str(config.get("secondary_label") or "Secondary categories")
        deployer_label = str(config.get("deployer_label") or "Top entities")
        developer_label = str(config.get("developer_label") or "Secondary entities")

        trend_note = "Insufficient data for long-term trend analysis."
        yoy_change_pct: float | None = None
        if len(year_counts_df) >= 2:
            first_year = int(year_counts_df.iloc[0]["year"])
            first_count = float(year_counts_df.iloc[0]["count"])
            last_year = int(year_counts_df.iloc[-1]["year"])
            last_count = float(year_counts_df.iloc[-1]["count"])
            if first_count > 0:
                yoy_change_pct = ((last_count - first_count) / first_count) * 100.0
            peak = year_counts_df.loc[year_counts_df["count"].idxmax()]
            trend_note = (
                f"Incident volume shifted from {int(first_count)} in {first_year} "
                f"to {int(last_count)} in {last_year}. Peak year was "
                f"{int(peak['year'])} with {int(peak['count'])} incidents."
            )

        highlights: list[str] = [f"Total documented incidents analyzed: {total:,}."]
        if trend_note:
            highlights.append(trend_note)
        if top_harms:
            top = top_harms[0]
            highlights.append(
                f"Most common value for {primary_label.lower()}: {top.get('value', 'Unknown')} "
                f"({int(top.get('count', 0)):,} incidents, {float(top.get('percentage', 0.0)):.1f}%)."
            )
        if top_sectors:
            top = top_sectors[0]
            highlights.append(
                f"Top value for {secondary_label.lower()}: {top.get('value', 'Unknown')} "
                f"({int(top.get('count', 0)):,} incidents)."
            )
        if top_deployers:
            top = top_deployers[0]
            highlights.append(
                f"Leading value for {deployer_label.lower()}: {top.get('value', 'Unknown')}."
            )
        if top_developers:
            top = top_developers[0]
            highlights.append(
                f"Leading value for {developer_label.lower()}: {top.get('value', 'Unknown')}."
            )

        markdown = self._build_detailed_summary_markdown(
            total=total,
            year_counts=year_counts_df,
            top_harms=top_harms,
            top_sectors=top_sectors,
            top_deployers=top_deployers,
            top_developers=top_developers,
            highlights=highlights,
            coverage=coverage,
            labels={
                "top_harms": primary_label,
                "top_sectors": secondary_label,
                "top_deployers": deployer_label,
                "top_developers": developer_label,
                "harm_coverage": f"{primary_label} coverage",
                "sector_coverage": f"{secondary_label} coverage",
                "deployer_coverage": f"{deployer_label} coverage",
                "developer_coverage": f"{developer_label} coverage",
                "date_coverage": "Date coverage",
            },
        )

        return {
            "type": "narrative",
            "summary_markdown": markdown,
            "highlights": highlights,
            "yearly_incidents": self._records(year_counts_df),
            "top_harm_types": top_harms,
            "top_sectors": top_sectors,
            "top_deployers": top_deployers,
            "top_developers": top_developers,
            "coverage": coverage,
            "trend_change_pct": yoy_change_pct,
            "field_mapping": {
                "year_field": year_field,
                "date_field": date_field,
                "primary_field": harm_field,
                "secondary_field": sector_field,
                "deployer_field": deployer_field,
                "developer_field": developer_field,
            },
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

    def _top_counts(self, df: pd.DataFrame, field: str | None, n: int) -> list[dict[str, Any]]:
        if not field or field not in df.columns:
            return []
        series = df[field].dropna().astype(str)
        if series.empty:
            return []
        counts = (
            series.value_counts()
            .head(n)
            .rename_axis("value")
            .reset_index(name="count")
        )
        counts["percentage"] = ((counts["count"] / max(len(series), 1)) * 100.0).round(1)
        return self._records(counts)

    def _coverage_pct(self, df: pd.DataFrame, field: str | None) -> float:
        if not field or field not in df.columns or len(df) == 0:
            return 0.0
        return round(float(df[field].notna().mean() * 100.0), 1)

    def _build_detailed_summary_markdown(
        self,
        *,
        total: int,
        year_counts: pd.DataFrame,
        top_harms: list[dict[str, Any]],
        top_sectors: list[dict[str, Any]],
        top_deployers: list[dict[str, Any]],
        top_developers: list[dict[str, Any]],
        highlights: list[str],
        coverage: dict[str, float],
        labels: dict[str, str] | None = None,
    ) -> str:
        labels = labels or {}

        def _fmt_list(items: list[dict[str, Any]], label: str) -> str:
            if not items:
                return f"- {label}: N/A"
            top = items[:3]
            bits = [
                f"{entry.get('value', 'Unknown')} ({int(entry.get('count', 0)):,})"
                for entry in top
            ]
            return f"- {label}: " + ", ".join(bits)

        lines: list[str] = [
            "### Detailed Snapshot Summary",
            "",
            f"- Total incidents analyzed: **{total:,}**",
            _fmt_list(top_harms, labels.get("top_harms", "Top harm types")),
            _fmt_list(top_sectors, labels.get("top_sectors", "Top sectors")),
            _fmt_list(top_deployers, labels.get("top_deployers", "Top deployers")),
            _fmt_list(top_developers, labels.get("top_developers", "Top developers")),
            "",
            "#### Coverage",
            f"- {labels.get('harm_coverage', 'Harm type coverage')}: **{coverage.get('harm_type_pct', 0.0):.1f}%**",
            f"- {labels.get('sector_coverage', 'Sector coverage')}: **{coverage.get('sector_of_deployment_pct', 0.0):.1f}%**",
            f"- {labels.get('deployer_coverage', 'Deployer coverage')}: **{coverage.get('deployer_pct', 0.0):.1f}%**",
            f"- {labels.get('developer_coverage', 'Developer coverage')}: **{coverage.get('developer_pct', 0.0):.1f}%**",
            f"- {labels.get('date_coverage', 'Incident date coverage')}: **{coverage.get('date_pct', 0.0):.1f}%**",
            "",
            "#### Key Highlights",
        ]
        lines.extend([f"- {msg}" for msg in highlights])

        if not year_counts.empty:
            lines.append("")
            lines.append("#### Yearly Trend")
            for row in year_counts.to_dict(orient="records"):
                lines.append(f"- {int(row.get('year'))}: {int(row.get('count', 0)):,} incidents")

        return "\n".join(lines)

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
