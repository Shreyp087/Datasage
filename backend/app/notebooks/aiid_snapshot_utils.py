from __future__ import annotations

import os
import re
import tarfile
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from app.pipeline.aiid_ingestor import AIIDIngestor


def safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    root = dest.resolve()
    for member in tar.getmembers():
        target = (dest / member.name).resolve()
        if not str(target).startswith(str(root)):
            raise ValueError(f"Unsafe path in archive: {member.name}")
    tar.extractall(dest)


def download_file(url: str, dest: Path, chunk_size: int = 2 * 1024 * 1024) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=(20, 600)) as response:
        response.raise_for_status()
        with open(dest, "wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                handle.write(chunk)
    return dest


def extract_archive(archive_path: Path, dest_dir: Path) -> dict[str, str]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:*") as tar:
        safe_extract(tar, dest_dir)
    return build_file_inventory(dest_dir)


def build_file_inventory(root: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        full = str(p)
        files[rel] = full
        files.setdefault(p.name, full)
    return files


def resolve_snapshot(
    *,
    snapshot_url: str | None,
    local_archive: str | None,
    extracted_dir: str | None,
    cache_dir: str | Path,
) -> tuple[Path, dict[str, str]]:
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)

    if extracted_dir:
        root = Path(extracted_dir)
        if not root.exists():
            raise FileNotFoundError(f"EXTRACTED_DIR not found: {root}")
        return root, build_file_inventory(root)

    if local_archive:
        archive = Path(local_archive)
        if not archive.exists():
            raise FileNotFoundError(f"LOCAL_ARCHIVE not found: {archive}")
    elif snapshot_url:
        archive_name = re.sub(r"[^a-zA-Z0-9._-]+", "_", Path(snapshot_url).name or "aiid_snapshot.tar.bz2")
        archive = cache / archive_name
        if not archive.exists() or archive.stat().st_size == 0:
            download_file(snapshot_url, archive)
    else:
        raise ValueError("Provide one of extracted_dir, local_archive, or snapshot_url.")

    extracted_root = cache / f"extracted_{archive.stem.replace('.', '_')}"
    files = extract_archive(archive, extracted_root)
    return extracted_root, files


def _table_key_from_path(path: Path, root: Path) -> str:
    rel = str(path.relative_to(root)).replace("\\", "/")
    stem = rel.rsplit(".", 1)[0]
    key = re.sub(r"[^a-zA-Z0-9]+", "_", stem).strip("_").lower()
    return key or path.stem.lower()


def load_tabular_tables(root: Path, max_file_mb: float = 120.0) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []

    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".csv", ".json"}:
            continue

        size_mb = p.stat().st_size / (1024 * 1024)
        key = _table_key_from_path(p, root)
        uniq = key
        suffix = 2
        while uniq in tables:
            uniq = f"{key}_{suffix}"
            suffix += 1

        if size_mb > max_file_mb:
            rows.append(
                {
                    "table": uniq,
                    "path": str(p.relative_to(root)),
                    "rows": None,
                    "columns": None,
                    "size_mb": round(size_mb, 2),
                    "status": f"skipped_size_gt_{max_file_mb}MB",
                }
            )
            continue

        try:
            if p.suffix.lower() == ".csv":
                try:
                    df = pd.read_csv(p, low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(p, low_memory=False, encoding="latin-1")
            else:
                try:
                    df = pd.read_json(p, lines=True)
                except ValueError:
                    df = pd.read_json(p)
            tables[uniq] = df
            rows.append(
                {
                    "table": uniq,
                    "path": str(p.relative_to(root)),
                    "rows": int(len(df)),
                    "columns": int(len(df.columns)),
                    "size_mb": round(size_mb, 2),
                    "status": "loaded",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "table": uniq,
                    "path": str(p.relative_to(root)),
                    "rows": None,
                    "columns": None,
                    "size_mb": round(size_mb, 2),
                    "status": f"error:{type(exc).__name__}",
                }
            )

    catalog = pd.DataFrame(rows).sort_values(by=["status", "rows"], ascending=[True, False], na_position="last")
    return tables, catalog


def infer_relationships(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    edges: list[dict[str, Any]] = []
    pk_map: dict[str, tuple[str, set[str]]] = {}

    def _norm(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        text = str(value).strip()
        if not text:
            return None
        return re.sub(r"\.0+$", "", text)

    id_candidates = ("incident_id", "Incident ID", "report_number", "report_id", "_id", "id", "ref_number")

    for table_name, frame in tables.items():
        for col in frame.columns:
            if str(col) not in id_candidates:
                continue
            values = [_norm(v) for v in frame[col].dropna().head(50000).tolist()]
            keys = {v for v in values if v}
            if not keys:
                continue
            if table_name.startswith("incidents") and str(col) in {"incident_id", "Incident ID"}:
                pk_map[table_name] = (str(col), keys)
                break
            if table_name.startswith("reports") and str(col) in {"report_number", "report_id", "ref_number", "id"}:
                pk_map[table_name] = (str(col), keys)
                break
            ratio = len(keys) / max(len(values), 1)
            if ratio >= 0.95:
                pk_map[table_name] = (str(col), keys)
                break

    for src_name, src_df in tables.items():
        for tgt_name, (pk_col, pk_keys) in pk_map.items():
            if src_name == tgt_name:
                continue
            for src_col in src_df.columns:
                src_values = [_norm(v) for v in src_df[src_col].dropna().head(50000).tolist()]
                src_keys = {v for v in src_values if v}
                if not src_keys:
                    continue
                matched = len(src_keys & pk_keys)
                if matched == 0:
                    continue
                overlap_pct = round((matched / max(len(src_keys), 1)) * 100.0, 2)
                if overlap_pct < 5.0:
                    continue
                edges.append(
                    {
                        "source_table": src_name,
                        "source_column": str(src_col),
                        "target_table": tgt_name,
                        "target_column": pk_col,
                        "matched_values": matched,
                        "overlap_pct": overlap_pct,
                    }
                )

    if not edges:
        return pd.DataFrame(columns=["source_table", "source_column", "target_table", "target_column", "matched_values", "overlap_pct"])
    out = pd.DataFrame(edges).sort_values(by=["matched_values", "overlap_pct"], ascending=False)
    return out.drop_duplicates(subset=["source_table", "source_column", "target_table", "target_column"])


def build_canonical_incident_df(files: dict[str, str]) -> pd.DataFrame:
    ingestor = AIIDIngestor()
    return ingestor.normalize(ingestor.load_incidents_csv(files))
