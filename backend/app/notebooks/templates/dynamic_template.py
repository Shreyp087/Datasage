from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _canonical_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _human_label(column: str | None, default: str) -> str:
    if not column:
        return default
    text = str(column).strip().replace("_", " ")
    return text[:1].upper() + text[1:]


def _dedupe(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _match_column(columns: list[str], aliases: list[str]) -> str | None:
    if not columns:
        return None

    normalized = {_canonical_name(col): col for col in columns}
    alias_tokens = [_canonical_name(alias) for alias in aliases if alias]

    for alias in alias_tokens:
        if alias in normalized:
            return normalized[alias]

    best: tuple[float, str] | None = None
    for col in columns:
        c_norm = _canonical_name(col)
        if not c_norm:
            continue
        for alias in alias_tokens:
            if len(alias) < 4:
                continue
            if alias in c_norm or c_norm in alias:
                score = min(len(alias), len(c_norm)) / max(len(alias), len(c_norm))
                if best is None or score > best[0]:
                    best = (score, col)

    if best and best[0] >= 0.55:
        return best[1]
    return None


def _candidate_categorical_columns(
    sample_df: pd.DataFrame,
    *,
    exclude: set[str],
    limit: int = 6,
) -> list[str]:
    ranked: list[tuple[float, str]] = []
    for col in sample_df.columns:
        if col in exclude:
            continue

        series = sample_df[col]
        if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_timedelta64_dtype(series):
            continue

        if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
            continue

        non_null = series.dropna()
        if non_null.empty:
            continue

        unique_count = int(non_null.nunique())
        if unique_count <= 1 or unique_count > 80:
            continue

        fill_ratio = float(series.notna().mean())
        keyword_bonus = 0.0
        c_norm = _canonical_name(col)
        if any(token in c_norm for token in ["type", "category", "sector", "harm", "status", "source", "region"]):
            keyword_bonus = 0.2
        score = fill_ratio + keyword_bonus - (unique_count / 200.0)
        ranked.append((score, str(col)))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [col for _, col in ranked[:limit]]


def _candidate_numeric_columns(sample_df: pd.DataFrame, *, limit: int = 4) -> list[str]:
    ranked: list[tuple[float, str]] = []
    for col in sample_df.columns:
        series = sample_df[col]
        if pd.api.types.is_bool_dtype(series):
            continue
        if not pd.api.types.is_numeric_dtype(series):
            continue
        non_null = series.dropna()
        if non_null.empty:
            continue
        unique_count = int(non_null.nunique())
        if unique_count <= 1:
            continue
        score = float(non_null.notna().mean()) + min(unique_count, 500) / 1000.0
        ranked.append((score, str(col)))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [col for _, col in ranked[:limit]]


def _candidate_target_columns(
    sample_df: pd.DataFrame,
    *,
    exclude: set[str],
) -> tuple[list[str], list[str]]:
    classification_targets: list[str] = []
    regression_targets: list[str] = []

    for col in sample_df.columns:
        if col in exclude:
            continue

        series = sample_df[col]
        non_null = series.dropna()
        if non_null.empty:
            continue
        unique_count = int(non_null.nunique())
        if unique_count <= 1:
            continue

        if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
            if unique_count >= 12:
                regression_targets.append(str(col))
        else:
            if 2 <= unique_count <= 20:
                classification_targets.append(str(col))

    return classification_targets[:3], regression_targets[:3]


def _build_eda_snippets_markdown(
    *,
    primary_fields: list[str],
    numeric_fields: list[str],
    date_field: str | None,
) -> str:
    primary_field = primary_fields[0] if primary_fields else None
    numeric_field = numeric_fields[0] if numeric_fields else None
    secondary_numeric_field = numeric_fields[1] if len(numeric_fields) > 1 else None

    lines: list[str] = [
        "## EDA Snippets",
        "",
        "Use these starter commands in Jupyter to profile the uploaded dataset quickly.",
        "",
        "```python",
        "# shape, schema, and quick profile",
        "df.shape",
        "df.info()",
        "df.describe(include='all').T.head(25)",
        "",
        "# missing values",
        "missing_pct = (df.isna().mean() * 100).sort_values(ascending=False)",
        "missing_pct.head(20)",
    ]

    if primary_field:
        lines.extend(
            [
                "",
                f"# value distribution for {primary_field}",
                f"df['{primary_field}'].value_counts(dropna=False).head(15)",
            ]
        )

    if numeric_field:
        lines.extend(
            [
                "",
                f"# numeric distribution for {numeric_field}",
                f"df['{numeric_field}'].plot(kind='hist', bins=30, figsize=(8, 4))",
            ]
        )

    if numeric_field and secondary_numeric_field:
        lines.extend(
            [
                "",
                "# numeric correlation",
                "df.select_dtypes(include=['number']).corr(numeric_only=True)",
                f"df.plot(kind='scatter', x='{numeric_field}', y='{secondary_numeric_field}', figsize=(7, 5))",
            ]
        )

    if date_field:
        lines.extend(
            [
                "",
                f"# time distribution from {date_field}",
                f"dt = pd.to_datetime(df['{date_field}'], errors='coerce')",
                "dt.dt.year.value_counts(dropna=True).sort_index()",
            ]
        )

    lines.extend(["```"])
    return "\n".join(lines)


def _build_model_suggestions_markdown(
    *,
    date_field: str | None,
    classification_targets: list[str],
    regression_targets: list[str],
    candidate_cats: list[str],
    numeric_fields: list[str],
) -> str:
    lines: list[str] = [
        "## Suggested Modeling Paths",
        "",
        "Choose one of these starter paths based on your current objective.",
    ]

    if classification_targets:
        target = classification_targets[0]
        feature_candidates = [col for col in [*candidate_cats, *numeric_fields] if col != target][:8]
        lines.extend(
            [
                "",
                f"### 1) Classification Starter (target: `{target}`)",
                "```python",
                "from sklearn.model_selection import train_test_split",
                "from sklearn.compose import ColumnTransformer",
                "from sklearn.pipeline import Pipeline",
                "from sklearn.preprocessing import OneHotEncoder, StandardScaler",
                "from sklearn.impute import SimpleImputer",
                "from sklearn.ensemble import RandomForestClassifier",
                "from sklearn.metrics import classification_report",
                "",
                f"target_col = '{target}'",
                f"feature_cols = {feature_candidates}",
                "train = df[feature_cols + [target_col]].dropna(subset=[target_col]).copy()",
                "X = train[feature_cols]",
                "y = train[target_col].astype(str)",
                "",
                "num_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]",
                "cat_cols = [c for c in X.columns if c not in num_cols]",
                "",
                "pre = ColumnTransformer([",
                "    ('num', Pipeline([('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), num_cols),",
                "    ('cat', Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('ohe', OneHotEncoder(handle_unknown='ignore'))]), cat_cols),",
                "])",
                "model = Pipeline([('pre', pre), ('clf', RandomForestClassifier(n_estimators=300, random_state=42))])",
                "X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)",
                "model.fit(X_train, y_train)",
                "pred = model.predict(X_test)",
                "print(classification_report(y_test, pred))",
                "```",
            ]
        )

    if regression_targets:
        target = regression_targets[0]
        feature_candidates = [col for col in [*candidate_cats, *numeric_fields] if col != target][:8]
        lines.extend(
            [
                "",
                f"### 2) Regression Starter (target: `{target}`)",
                "```python",
                "from sklearn.model_selection import train_test_split",
                "from sklearn.compose import ColumnTransformer",
                "from sklearn.pipeline import Pipeline",
                "from sklearn.preprocessing import OneHotEncoder",
                "from sklearn.impute import SimpleImputer",
                "from sklearn.ensemble import RandomForestRegressor",
                "from sklearn.metrics import mean_absolute_error, r2_score",
                "",
                f"target_col = '{target}'",
                f"feature_cols = {feature_candidates}",
                "train = df[feature_cols + [target_col]].dropna(subset=[target_col]).copy()",
                "X = train[feature_cols]",
                "y = pd.to_numeric(train[target_col], errors='coerce')",
                "mask = y.notna()",
                "X, y = X.loc[mask], y.loc[mask]",
                "",
                "num_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]",
                "cat_cols = [c for c in X.columns if c not in num_cols]",
                "",
                "pre = ColumnTransformer([",
                "    ('num', Pipeline([('imputer', SimpleImputer(strategy='median'))]), num_cols),",
                "    ('cat', Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('ohe', OneHotEncoder(handle_unknown='ignore'))]), cat_cols),",
                "])",
                "model = Pipeline([('pre', pre), ('reg', RandomForestRegressor(n_estimators=400, random_state=42))])",
                "X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)",
                "model.fit(X_train, y_train)",
                "pred = model.predict(X_test)",
                "print('MAE:', mean_absolute_error(y_test, pred))",
                "print('R2 :', r2_score(y_test, pred))",
                "```",
            ]
        )

    if date_field:
        lines.extend(
            [
                "",
                f"### 3) Time Pattern Starter (time field: `{date_field}`)",
                "```python",
                f"dt = pd.to_datetime(df['{date_field}'], errors='coerce')",
                "daily = dt.dt.date.value_counts().sort_index()",
                "daily.plot(figsize=(11, 4), title='Record Volume Over Time')",
                "```",
            ]
        )

    lines.extend(
        [
            "",
            "### 4) Unsupervised Baseline (when no clear target)",
            "```python",
            "from sklearn.cluster import KMeans",
            "num_df = df.select_dtypes(include=['number']).copy().dropna()",
            "if not num_df.empty:",
            "    km = KMeans(n_clusters=4, random_state=42, n_init='auto')",
            "    labels = km.fit_predict(num_df)",
            "    num_df.assign(cluster=labels).head()",
            "```",
        ]
    )
    return "\n".join(lines)


def _make_cell(cell_number: int, payload: dict[str, Any]) -> dict[str, Any]:
    item = dict(payload)
    item["id"] = f"cell_{cell_number:03d}"
    return item


def build_dynamic_notebook_template(
    *,
    dataset_name: str,
    domain: str,
    df: pd.DataFrame,
    snapshot_date: str | None = None,
    snapshot_url: str | None = None,
) -> dict[str, Any]:
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Expected pandas DataFrame for dynamic notebook template generation")

    sample_df = df.head(min(20000, len(df))).copy()
    columns = [str(col) for col in sample_df.columns]

    year_field = _match_column(
        columns,
        ["year", "incident_year", "event_year", "calendar_year", "fiscal_year"],
    )
    date_field = _match_column(
        columns,
        ["date", "incident_date", "event_date", "timestamp", "created_at", "reported_at", "datetime"],
    )
    title_field = _match_column(
        columns,
        ["title", "incident_title", "name", "headline", "summary", "description", "text"],
    )
    harm_field = _match_column(
        columns,
        ["harm_type", "harm", "risk_type", "risk_domain", "incident_type", "category"],
    )
    sector_field = _match_column(
        columns,
        ["sector_of_deployment", "sector", "industry", "business_unit", "deployment_sector"],
    )
    deployer_field = _match_column(
        columns,
        [
            "allegeddeployerofaisystem_primary",
            "alleged_deployer_of_ai_system_primary",
            "deployer",
            "deployed_by",
            "organization",
            "company",
        ],
    )
    developer_field = _match_column(
        columns,
        [
            "allegeddeveloperofaisystem_primary",
            "alleged_developer_of_ai_system_primary",
            "developer",
            "vendor",
            "model_provider",
            "provider",
        ],
    )
    source_field = _match_column(
        columns,
        ["report_sources", "source", "source_domain", "publisher", "news_source"],
    )

    excluded = set(_dedupe([year_field, date_field, title_field]))
    candidate_cats = _candidate_categorical_columns(sample_df, exclude=excluded, limit=6)
    candidate_numeric = _candidate_numeric_columns(sample_df, limit=4)
    classification_targets, regression_targets = _candidate_target_columns(
        sample_df,
        exclude=excluded,
    )

    primary_fields = _dedupe(
        [harm_field, sector_field, deployer_field, developer_field, source_field, *candidate_cats]
    )
    entity_fields = _dedupe(
        [
            deployer_field,
            developer_field,
            *[
                col
                for col in candidate_cats
                if any(token in _canonical_name(col) for token in ["org", "company", "vendor", "provider", "team"])
            ],
        ]
    )
    if len(entity_fields) < 2:
        entity_fields = _dedupe([*entity_fields, *candidate_cats])

    trend_field = year_field or date_field
    heatmap_fields = primary_fields[:2] if len(primary_fields) >= 2 else []

    summary_unique_fields = {
        key: value
        for key, value in {
            "deployer": deployer_field,
            "developer": developer_field,
            "source": source_field,
        }.items()
        if value
    }

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dataset_label = dataset_name.strip() or "Uploaded Dataset"
    domain_label = (domain or "general").strip().lower() or "general"
    snapshot_label = snapshot_date or "latest"
    snapshot_link = snapshot_url or ""

    cells: list[dict[str, Any]] = []
    cell_no = 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "text",
                "title": "Dataset Notebook Starter",
                "content": (
                    f"## {dataset_label} - Dynamic Notebook Template\n\n"
                    f"This notebook was auto-generated from the uploaded dataset schema.\n\n"
                    f"- Domain: `{domain_label}`\n"
                    f"- Rows: **{len(df):,}**\n"
                    f"- Columns: **{len(columns)}**\n"
                    f"- Generated: **{generated_at}**\n\n"
                    "### What this template includes\n"
                    "- Snapshot and coverage overview\n"
                    "- Time trend (when a date/year field is available)\n"
                    "- Top category and entity breakdowns using detected columns\n"
                    "- Cross-field relationship heatmap (when compatible fields exist)\n"
                    "- Narrative summary and reproducibility notes\n\n"
                    "### Notebook usage\n"
                    "Run all cells first, then edit the generated configs to refine the analysis.\n"
                ),
            },
        )
    )
    cell_no += 1

    starter_fields = {
        "date_field": date_field,
        "year_field": year_field,
        "title_field": title_field,
        "primary_category_field": harm_field or (primary_fields[0] if primary_fields else None),
        "secondary_category_field": sector_field or (primary_fields[1] if len(primary_fields) > 1 else None),
        "entity_field": deployer_field or (entity_fields[0] if entity_fields else None),
    }
    starter_lines = [f"- `{k}`: `{v}`" for k, v in starter_fields.items() if v]
    starter_field_block = "\n".join(starter_lines) if starter_lines else "- No high-confidence field mapping detected."

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "text",
                "title": "Jupyter Starter Code",
                "content": (
                    "## Reproducible Jupyter Starter\n\n"
                    "```python\n"
                    "import pandas as pd\n\n"
                    "DATASET_PATH = \"path/to/your_uploaded_dataset.csv\"\n"
                    "if DATASET_PATH.endswith('.parquet'):\n"
                    "    df = pd.read_parquet(DATASET_PATH)\n"
                    "elif DATASET_PATH.endswith('.json'):\n"
                    "    df = pd.read_json(DATASET_PATH, lines=True)\n"
                    "else:\n"
                    "    df = pd.read_csv(DATASET_PATH, low_memory=False)\n\n"
                    "print(df.shape)\n"
                    "df.head()\n"
                    "```\n\n"
                    "### Suggested starter fields from your upload\n"
                    f"{starter_field_block}\n"
                ),
            },
        )
    )
    cell_no += 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "text",
                "title": "EDA Template Snippets",
                "content": _build_eda_snippets_markdown(
                    primary_fields=primary_fields,
                    numeric_fields=candidate_numeric,
                    date_field=date_field,
                ),
            },
        )
    )
    cell_no += 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "text",
                "title": "Suggested Model Execution",
                "content": _build_model_suggestions_markdown(
                    date_field=date_field,
                    classification_targets=classification_targets,
                    regression_targets=regression_targets,
                    candidate_cats=candidate_cats,
                    numeric_fields=candidate_numeric,
                ),
            },
        )
    )
    cell_no += 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "analysis",
                "title": "Snapshot Overview",
                "description": "Core counts, top values, and coverage for detected key fields.",
                "analysis_type": "summary",
                "config": {
                    "date_field": date_field,
                    "top_fields": primary_fields[:3],
                    "unique_fields": summary_unique_fields,
                    "harm_field": harm_field,
                    "sector_field": sector_field,
                    "deployer_field": deployer_field,
                    "developer_field": developer_field,
                },
            },
        )
    )
    cell_no += 1

    if trend_field:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "analysis",
                    "title": "Trend Over Time",
                    "description": "Trend view over the detected temporal field.",
                    "analysis_type": "trend",
                    "config": {"x_field": trend_field, "chart_type": "line"},
                },
            )
        )
        cell_no += 1

    for field in primary_fields[:4]:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "analysis",
                    "title": f"Distribution: {_human_label(field, field)}",
                    "description": "Most frequent values in this detected category field.",
                    "analysis_type": "distribution",
                    "config": {"field": field, "chart_type": "bar", "top_n": 12},
                },
            )
        )
        cell_no += 1

    for field in entity_fields[:2]:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "analysis",
                    "title": f"Top Values: {_human_label(field, field)}",
                    "description": "Top entities for accountability or concentration analysis.",
                    "analysis_type": "top_n",
                    "config": {"field": field, "n": 15, "chart_type": "horizontal_bar"},
                },
            )
        )
        cell_no += 1

    if len(heatmap_fields) == 2:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "analysis",
                    "title": "Cross-Field Heatmap",
                    "description": "Cross-tab of the top two detected categorical fields.",
                    "analysis_type": "heatmap",
                    "config": {
                        "row_field": heatmap_fields[0],
                        "col_field": heatmap_fields[1],
                        "top_n": 10,
                    },
                },
            )
        )
        cell_no += 1

    if len(candidate_numeric) >= 2:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "analysis",
                    "title": "Numeric Correlation Check",
                    "description": "Correlation matrix for detected numeric columns.",
                    "analysis_type": "correlation",
                    "config": {},
                },
            )
        )
        cell_no += 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "analysis",
                "title": "Narrative Summary",
                "description": "Consolidated summary for trend, concentrations, and coverage.",
                "analysis_type": "detailed_summary",
                "config": {
                    "top_n": 10,
                    "year_field": year_field,
                    "date_field": date_field,
                    "harm_field": harm_field or (primary_fields[0] if primary_fields else None),
                    "sector_field": sector_field or (primary_fields[1] if len(primary_fields) > 1 else None),
                    "deployer_field": deployer_field or (entity_fields[0] if entity_fields else None),
                    "developer_field": developer_field or (entity_fields[1] if len(entity_fields) > 1 else None),
                    "primary_label": _human_label(harm_field or (primary_fields[0] if primary_fields else None), "Primary category"),
                    "secondary_label": _human_label(
                        sector_field or (primary_fields[1] if len(primary_fields) > 1 else None),
                        "Secondary category",
                    ),
                    "deployer_label": _human_label(
                        deployer_field or (entity_fields[0] if entity_fields else None),
                        "Primary entity",
                    ),
                    "developer_label": _human_label(
                        developer_field or (entity_fields[1] if len(entity_fields) > 1 else None),
                        "Secondary entity",
                    ),
                },
            },
        )
    )
    cell_no += 1

    if title_field:
        cells.append(
            _make_cell(
                cell_no,
                {
                    "type": "text",
                    "title": "Sample Text Records",
                    "description": "Random sample from the main text/title field for qualitative review.",
                    "analysis_type": "text_sample",
                    "config": {"field": title_field, "n": 10},
                },
            )
        )
        cell_no += 1

    cells.append(
        _make_cell(
            cell_no,
            {
                "type": "text",
                "title": "Reproducibility",
                "content": (
                    "## Reproducibility Notes\n\n"
                    "1. Export this notebook as `.ipynb` from DataSage.\n"
                    "2. Update `DATASET_PATH` in the setup cell to your local dataset file.\n"
                    "3. Re-run all cells in Jupyter and extend sections with custom logic.\n\n"
                    "### Dataset metadata\n"
                    f"- Snapshot date: `{snapshot_label}`\n"
                    f"- Snapshot source: `{snapshot_link}`\n"
                    f"- Generated at: `{generated_at}`\n"
                ),
            },
        )
    )

    title = f"Auto Notebook - {dataset_label}"
    description = (
        "Auto-generated dynamic notebook boilerplate for this uploaded dataset. "
        "Sections and commands are customized to detected columns."
    )

    tags = _dedupe(
        [
            "auto-generated",
            "dynamic-template",
            "reproducible",
            "jupyter",
            domain_label,
        ]
    )

    return {
        "title": title,
        "description": description,
        "domain": domain_label,
        "tags": tags,
        "cells": cells,
    }
