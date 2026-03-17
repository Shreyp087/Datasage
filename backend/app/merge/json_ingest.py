"""
DataSage JSON Structural Analyzer and Ingestion Engine.

Public API:
  load_json(source) -> tuple[pd.DataFrame, JsonReport]
  analyze_json(source) -> JsonReport
  json_report_to_text(report) -> str

This module replaces naive pd.read_json() with robust structural handling for:
  - array-of-objects
  - wrapped arrays
  - object-of-objects
  - array-of-arrays
  - NDJSON/JSONL
  - sparse/mixed layouts
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union


class JsonLayout(str, Enum):
    ARRAY_OF_OBJECTS = "array_of_objects"
    ARRAY_OF_ARRAYS = "array_of_arrays"
    ARRAY_OF_SCALARS = "array_of_scalars"
    WRAPPED_ARRAY = "wrapped_array"
    WRAPPED_MULTI = "wrapped_multi"
    OBJECT_OF_OBJECTS = "object_of_objects"
    SINGLE_OBJECT = "single_object"
    NDJSON = "ndjson"
    MIXED_ARRAY = "mixed_array"
    EMPTY = "empty"
    UNKNOWN = "unknown"


class FieldKind(str, Enum):
    SCALAR_STR = "string"
    SCALAR_NUM = "number"
    SCALAR_BOOL = "boolean"
    SCALAR_NULL = "null"
    ARRAY = "array"
    OBJECT = "object"
    MIXED = "mixed"


@dataclass
class FieldProfile:
    path: str
    flat_col: str
    kind: FieldKind
    count: int
    null_count: int
    null_rate: float
    type_counts: dict
    n_unique: int
    sample_values: list
    is_nested: bool
    array_depth: int
    child_paths: list[str]
    notes: list[str]


@dataclass
class JsonReport:
    source_label: str
    layout: JsonLayout
    root_records: int
    total_fields: int
    max_depth: int
    fields: list[FieldProfile]
    array_keys: list[str]
    warnings: list[str]
    errors: list[str]
    raw_sample: Any
    flatten_strategy: str


_DATE_PAT = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?Z?)?$|"
    r"^\d{2}[/\-]\d{2}[/\-]\d{4}$|"
    r"^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$"
)
_EMAIL_PAT = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_PAT = re.compile(r"^https?://")
_UUID_PAT = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


def _safe_col(path: str) -> str:
    s = path.replace(".", "__")
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    return s


def _python_type(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def _value_hints(samples: list) -> list[str]:
    strs = [v for v in samples if isinstance(v, str)]
    if not strs:
        return []
    hints = []
    date_hits = sum(1 for v in strs if _DATE_PAT.match(v.strip()))
    email_hits = sum(1 for v in strs if _EMAIL_PAT.match(v.strip()))
    url_hits = sum(1 for v in strs if _URL_PAT.match(v.strip()))
    uuid_hits = sum(1 for v in strs if _UUID_PAT.match(v.strip()))
    n = len(strs)
    if date_hits / n >= 0.7:
        hints.append("looks like datetime")
    if email_hits / n >= 0.7:
        hints.append("looks like email")
    if url_hits / n >= 0.7:
        hints.append("looks like URL")
    if uuid_hits / n >= 0.7:
        hints.append("looks like UUID")
    return hints


def _extract_paths(records: list[dict], max_records: int = 500) -> dict[str, list]:
    probe = records[:max_records]
    path_vals: dict[str, list] = {}

    def walk(obj: Any, prefix: str, depth: int) -> None:
        if depth > 8:
            return
        if obj is None or isinstance(obj, (bool, int, float, str)):
            path_vals.setdefault(prefix, []).append(obj)
            return
        if isinstance(obj, dict):
            if not obj:
                path_vals.setdefault(prefix, []).append(obj)
                return
            for k, v in obj.items():
                child = f"{prefix}.{k}" if prefix else k
                walk(v, child, depth + 1)
            return
        if isinstance(obj, list):
            if not obj:
                path_vals.setdefault(prefix, []).append(None)
                return
            if all(isinstance(x, dict) for x in obj):
                for item in obj:
                    walk(item, f"{prefix}[*]", depth + 1)
            else:
                path_vals.setdefault(prefix, []).append(json.dumps(obj))
            return
        path_vals.setdefault(prefix, []).append(str(obj))

    for rec in probe:
        walk(rec, "", 0)

    path_vals.pop("", None)
    return path_vals


def _max_depth(obj: Any, d: int = 0) -> int:
    if isinstance(obj, dict):
        if not obj:
            return d
        return max(_max_depth(v, d + 1) for v in obj.values())
    if isinstance(obj, list):
        if not obj:
            return d
        return max(_max_depth(v, d + 1) for v in obj)
    return d


def _detect_layout(raw: Any) -> tuple[JsonLayout, list[str]]:
    if raw is None:
        return JsonLayout.EMPTY, []

    if isinstance(raw, list):
        if len(raw) == 0:
            return JsonLayout.EMPTY, []
        types = Counter(_python_type(v) for v in raw)
        if types.get("dict", 0) / len(raw) >= 0.8:
            return JsonLayout.ARRAY_OF_OBJECTS, []
        if types.get("list", 0) / len(raw) >= 0.8:
            return JsonLayout.ARRAY_OF_ARRAYS, []
        if "dict" not in types and "list" not in types:
            return JsonLayout.ARRAY_OF_SCALARS, []
        return JsonLayout.MIXED_ARRAY, []

    if isinstance(raw, dict):
        array_keys = [
            k
            for k, v in raw.items()
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict)
        ]
        if array_keys:
            if len(array_keys) == 1:
                return JsonLayout.WRAPPED_ARRAY, array_keys
            return JsonLayout.WRAPPED_MULTI, array_keys
        dict_vals = [v for v in raw.values() if isinstance(v, dict)]
        if len(dict_vals) / max(len(raw), 1) >= 0.8:
            return JsonLayout.OBJECT_OF_OBJECTS, []
        return JsonLayout.SINGLE_OBJECT, []

    return JsonLayout.UNKNOWN, []


def _try_ndjson(text: str) -> Optional[list[dict]]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return None
    parsed = []
    for ln in lines:
        try:
            obj = json.loads(ln)
        except json.JSONDecodeError:
            return None
        if not isinstance(obj, dict):
            return None
        parsed.append(obj)
    return parsed


def _load_raw(source: Union[str, Path, bytes, dict, list]) -> tuple[Any, str]:
    if isinstance(source, (dict, list)):
        return source, "in-memory"

    if isinstance(source, bytes):
        text = source.decode("utf-8", errors="replace")
        try:
            return json.loads(text), "bytes"
        except json.JSONDecodeError:
            nd = _try_ndjson(text)
            if nd:
                return nd, "bytes[ndjson]"
            raise ValueError("Bytes input is not valid JSON or NDJSON")

    path = str(source)
    if path.startswith("{") or path.startswith("["):
        try:
            return json.loads(path), "string"
        except json.JSONDecodeError:
            nd = _try_ndjson(path)
            if nd:
                return nd, "string[ndjson]"
            raise ValueError("String input is not valid JSON")

    label = Path(path).name
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        text = fh.read().strip()

    if "\n" in text:
        nd = _try_ndjson(text)
        if nd:
            return nd, f"{label}[ndjson]"

    return json.loads(text), label


def _extract_records(
    raw: Any,
    layout: JsonLayout,
    array_keys: list[str],
) -> tuple[list[dict], str]:
    if layout == JsonLayout.ARRAY_OF_OBJECTS:
        return list(raw), "Direct array of objects - no transformation needed."

    if layout == JsonLayout.NDJSON:
        return list(raw), "Newline-delimited JSON - each line parsed as one record."

    if layout == JsonLayout.WRAPPED_ARRAY:
        key = array_keys[0]
        return list(raw[key]), f"Extracted data from wrapper key '{key}'."

    if layout == JsonLayout.WRAPPED_MULTI:
        largest_key = max(array_keys, key=lambda k: len(raw[k]))
        other_keys = [k for k in array_keys if k != largest_key]
        records = list(raw[largest_key])
        strategy = (
            f"Multiple array keys found: {array_keys}. "
            f"Used largest: '{largest_key}' ({len(records)} records). "
            f"Other arrays ({other_keys}) available as separate sources."
        )
        return records, strategy

    if layout == JsonLayout.OBJECT_OF_OBJECTS:
        records = []
        for k, v in raw.items():
            row = dict(v) if isinstance(v, dict) else {"value": v}
            row.setdefault("_key", k)
            records.append(row)
        return records, "Object-of-objects - outer keys injected as '_key' column."

    if layout == JsonLayout.SINGLE_OBJECT:
        return [raw], "Single object - wrapped in a 1-row DataFrame."

    if layout == JsonLayout.ARRAY_OF_ARRAYS:
        if raw and isinstance(raw[0], list):
            first = raw[0]
            if all(isinstance(v, str) for v in first):
                headers = first
                records = [dict(zip(headers, row)) for row in raw[1:]]
                return records, f"Array-of-arrays with header row: {headers}."
            headers = [f"col_{i}" for i in range(len(first))]
            records = [dict(zip(headers, row)) for row in raw]
            return records, (
                "Array-of-arrays without headers - assigned "
                f"col_0..col_{len(first)-1}."
            )
        return [], "Array-of-arrays - no rows found."

    if layout == JsonLayout.ARRAY_OF_SCALARS:
        return [{"value": v} for v in raw], "Array of scalars - wrapped in 'value' column."

    if layout == JsonLayout.MIXED_ARRAY:
        records = []
        for v in raw:
            records.append(v if isinstance(v, dict) else {"value": v})
        return records, "Mixed-type array - non-dict elements wrapped in 'value'."

    return [], f"Layout '{layout.value}' - no records extracted."


def _flatten_records(records: list[dict]) -> tuple[list[dict], bool]:
    import pandas as pd

    if not records:
        return records, False

    try:
        flat = pd.json_normalize(records, sep="__").to_dict(orient="records")
        was_nested = any("__" in k for k in flat[0]) if flat else False
        return flat, was_nested
    except Exception:
        flat = []
        was_nested = False
        for rec in records:
            row = {}
            for k, v in rec.items():
                if isinstance(v, dict):
                    was_nested = True
                    for ck, cv in v.items():
                        row[f"{k}__{ck}"] = (
                            cv if not isinstance(cv, (dict, list)) else json.dumps(cv)
                        )
                elif isinstance(v, list):
                    row[k] = json.dumps(v)
                else:
                    row[k] = v
            flat.append(row)
        return flat, was_nested


def _profile_fields(path_vals: dict[str, list], total_records: int) -> list[FieldProfile]:
    _ = total_records
    profiles: list[FieldProfile] = []

    for path, values in path_vals.items():
        count = len(values)
        null_count = sum(1 for v in values if v is None)
        null_rate = null_count / count if count > 0 else 1.0
        notnull = [v for v in values if v is not None]
        type_counts = dict(Counter(_python_type(v) for v in values))

        if not notnull:
            kind = FieldKind.SCALAR_NULL
        else:
            dominant = Counter(_python_type(v) for v in notnull).most_common(1)[0][0]
            kind_map = {
                "str": FieldKind.SCALAR_STR,
                "int": FieldKind.SCALAR_NUM,
                "float": FieldKind.SCALAR_NUM,
                "bool": FieldKind.SCALAR_BOOL,
                "dict": FieldKind.OBJECT,
                "list": FieldKind.ARRAY,
                "null": FieldKind.SCALAR_NULL,
            }
            kind = kind_map.get(dominant, FieldKind.MIXED)
            structural = {"dict", "list"}
            scalar = {"str", "int", "float", "bool"}
            types_seen = {_python_type(v) for v in notnull}
            if len(types_seen - {"null"}) > 1 and (types_seen & structural) and (types_seen & scalar):
                kind = FieldKind.MIXED

        array_depth = path.count("[*]") if "[*]" in path else 0

        try:
            hashable = [v for v in notnull if not isinstance(v, (dict, list))]
            n_unique = len(set(map(str, hashable[:10000])))
        except Exception:
            n_unique = 0

        sample_values = []
        seen_s: set[str] = set()
        for v in notnull:
            sv = str(v)[:120]
            if sv not in seen_s:
                seen_s.add(sv)
                sample_values.append(v)
            if len(sample_values) >= 8:
                break

        child_paths: list[str] = []
        if kind == FieldKind.OBJECT:
            all_keys: set[str] = set()
            for v in notnull:
                if isinstance(v, dict):
                    all_keys.update(v.keys())
            child_paths = sorted(all_keys)

        profiles.append(
            FieldProfile(
                path=path,
                flat_col=_safe_col(path),
                kind=kind,
                count=count,
                null_count=null_count,
                null_rate=round(null_rate, 4),
                type_counts=type_counts,
                n_unique=n_unique,
                sample_values=sample_values,
                is_nested=(kind in (FieldKind.OBJECT, FieldKind.MIXED) or array_depth > 0),
                array_depth=array_depth,
                child_paths=child_paths,
                notes=_value_hints(sample_values),
            )
        )

    profiles.sort(key=lambda p: (p.array_depth, p.path.count("."), p.path))
    return profiles


def analyze_json(
    source: Union[str, Path, bytes, dict, list],
    label: Optional[str] = None,
) -> JsonReport:
    warnings: list[str] = []
    errors: list[str] = []

    try:
        raw, auto_label = _load_raw(source)
    except Exception as exc:
        return JsonReport(
            source_label=label or str(source)[:60],
            layout=JsonLayout.UNKNOWN,
            root_records=0,
            total_fields=0,
            max_depth=0,
            fields=[],
            array_keys=[],
            warnings=[],
            errors=[str(exc)],
            raw_sample=None,
            flatten_strategy="Failed to load source.",
        )

    source_label = label or auto_label
    layout, array_keys = _detect_layout(raw)
    if isinstance(raw, list) and all(isinstance(x, dict) for x in raw) and "ndjson" in source_label:
        layout = JsonLayout.NDJSON

    records, flatten_strategy = _extract_records(raw, layout, array_keys)
    if not records:
        return JsonReport(
            source_label=source_label,
            layout=layout,
            root_records=0,
            total_fields=0,
            max_depth=0,
            fields=[],
            array_keys=array_keys,
            warnings=["No records extracted."],
            errors=errors,
            raw_sample=None,
            flatten_strategy=flatten_strategy,
        )

    raw_sample = records[:3]
    max_d = max((_max_depth(r) for r in records[:200]), default=0)
    if max_d > 3:
        warnings.append(
            f"Deep nesting detected (depth={max_d}). "
            "Deeply nested fields are flattened with '__' separators."
        )

    path_vals = _extract_paths(records, max_records=500)
    field_profiles = _profile_fields(path_vals, len(records))

    nested_fields = [p for p in field_profiles if p.is_nested]
    if nested_fields:
        warnings.append(
            f"{len(nested_fields)} nested field(s) found - "
            "will be flattened or serialised as JSON strings."
        )

    mixed_fields = [p for p in field_profiles if p.kind == FieldKind.MIXED]
    if mixed_fields:
        warnings.append(
            f"{len(mixed_fields)} field(s) have inconsistent types across records: "
            f"{[p.path for p in mixed_fields[:5]]}"
        )

    high_null = [p for p in field_profiles if p.null_rate > 0.5]
    if high_null:
        warnings.append(
            f"{len(high_null)} field(s) are >50% null: "
            f"{[p.path for p in high_null[:5]]}"
        )

    if layout == JsonLayout.WRAPPED_MULTI:
        warnings.append(
            f"Multiple data arrays found under keys {array_keys}. "
            "Only the largest was used. Pass others separately to AutoJoiner."
        )

    return JsonReport(
        source_label=source_label,
        layout=layout,
        root_records=len(records),
        total_fields=len(field_profiles),
        max_depth=max_d,
        fields=field_profiles,
        array_keys=array_keys,
        warnings=warnings,
        errors=errors,
        raw_sample=raw_sample,
        flatten_strategy=flatten_strategy,
    )


def load_json(
    source: Union[str, Path, bytes, dict, list],
    label: Optional[str] = None,
    max_array_depth: int = 4,
    stringify_nested: bool = True,
) -> tuple["pd.DataFrame", JsonReport]:  # noqa: F821
    _ = max_array_depth
    import pandas as pd

    report = analyze_json(source, label=label)
    if report.errors:
        return pd.DataFrame(), report

    try:
        raw, _ = _load_raw(source)
    except Exception:
        return pd.DataFrame(), report

    records, _ = _extract_records(raw, report.layout, report.array_keys)
    if not records:
        return pd.DataFrame(), report

    flat_records, _was_nested = _flatten_records(records)
    df = pd.DataFrame(flat_records)

    if stringify_nested:
        for col in df.columns:
            mask = df[col].map(lambda v: isinstance(v, (dict, list)))
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].map(json.dumps)

    df.columns = [_safe_col(str(c)) for c in df.columns]
    return df, report


def json_report_to_text(report: JsonReport) -> str:
    lines: list[str] = []
    width = 70

    def rule(ch: str = "-") -> None:
        lines.append(ch * width)

    def h1(text: str) -> None:
        rule("=")
        lines.append(f"  {text}")
        rule("=")

    def h2(text: str) -> None:
        rule("-")
        lines.append(f"  {text}")
        rule("-")

    h1(f"JSON STRUCTURAL REPORT - {report.source_label}")
    lines.append("")
    lines.append(f"  Layout          : {report.layout.value}")
    lines.append(f"  Records         : {report.root_records:,}")
    lines.append(f"  Fields (flat)   : {report.total_fields}")
    lines.append(f"  Max nesting     : {report.max_depth}")
    if report.array_keys:
        lines.append(f"  Data array keys : {report.array_keys}")
    lines.append(f"  Flatten strategy: {report.flatten_strategy}")
    lines.append("")

    if report.warnings:
        h2("WARNINGS")
        for warning in report.warnings:
            lines.append(f"  * {warning}")
        lines.append("")

    if report.errors:
        h2("ERRORS")
        for err in report.errors:
            lines.append(f"  x {err}")
        lines.append("")

    h2(f"FIELD PROFILES ({report.total_fields} fields)")
    kind_icon = {
        FieldKind.SCALAR_STR: "Aa ",
        FieldKind.SCALAR_NUM: "123",
        FieldKind.SCALAR_BOOL: "T/F",
        FieldKind.SCALAR_NULL: "NUL",
        FieldKind.ARRAY: "[ ]",
        FieldKind.OBJECT: "{ }",
        FieldKind.MIXED: "MIX",
    }

    for fp in report.fields:
        icon = kind_icon.get(fp.kind, "   ")
        null_blocks = int(fp.null_rate * 10)
        null_bar = ("#" * null_blocks) + ("." * (10 - null_blocks))
        depth_tag = f" [depth={fp.array_depth}]" if fp.array_depth > 0 else ""

        lines.append("")
        lines.append(f"  [{icon}] {fp.path}{depth_tag}")
        lines.append(f"         flat col   : {fp.flat_col}")
        lines.append(
            f"         count      : {fp.count:,}  |  nulls: {fp.null_count:,} "
            f"({fp.null_rate:.0%})  {null_bar}"
        )
        lines.append(f"         unique     : {fp.n_unique:,}")
        if fp.type_counts:
            tc = ", ".join(f"{k}:{v}" for k, v in sorted(fp.type_counts.items()))
            lines.append(f"         types      : {tc}")
        if fp.sample_values:
            sv = [repr(v)[:40] for v in fp.sample_values[:5]]
            lines.append(f"         samples    : {', '.join(sv)}")
        if fp.child_paths:
            lines.append(f"         children   : {fp.child_paths[:8]}")
        if fp.notes:
            lines.append(f"         hints      : {', '.join(fp.notes)}")

    lines.append("")
    h2("RAW SAMPLE (first 2 records before flattening)")
    if report.raw_sample:
        for i, rec in enumerate(report.raw_sample[:2], 1):
            lines.append("")
            lines.append(f"  Record {i}:")
            txt = json.dumps(rec, indent=4, default=str)
            txt_lines = txt.splitlines()
            for ln in txt_lines[:20]:
                lines.append(f"    {ln}")
            if len(txt_lines) > 20:
                lines.append("    ... (truncated)")
    else:
        lines.append("  (no sample available)")

    lines.append("")
    rule("=")
    return "\n".join(lines)


def load_json_as_df(source, **kwargs):
    _ = kwargs
    df, report = load_json(source)
    if report.errors:
        raise ValueError(f"JSON load failed: {report.errors}")

    print(f"\n[JSON] {report.source_label}")
    print(
        f"       layout={report.layout.value}  records={report.root_records:,}"
        f"  fields={report.total_fields}  depth={report.max_depth}"
    )
    print(f"       strategy: {report.flatten_strategy}")
    for warning in report.warnings:
        print(f"       * {warning}")
    print()
    return df

