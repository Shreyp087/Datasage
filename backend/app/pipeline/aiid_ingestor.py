from __future__ import annotations

import ast
import os
import tarfile
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm


class AIIDIngestor:
    """
    Downloads, extracts, and normalizes AIID snapshot data
    into a clean pandas DataFrame ready for DataSage processing.
    """

    LATEST_SNAPSHOT = "https://pub-72b2b2fc36ec423189843747af98f80e.r2.dev/backup-20260223102103.tar.bz2"

    def fetch_and_extract(self, url: str, dest_dir: str) -> dict[str, str]:
        """
        Download tar.bz2, extract to dest_dir.
        Returns dict of {filename: filepath} for extracted files.
        Stream download - do NOT load entire file into memory.
        Shows download progress via tqdm.
        """
        os.makedirs(dest_dir, exist_ok=True)
        local_tar = os.path.join(dest_dir, "aiid_snapshot.tar.bz2")

        with requests.get(url, stream=True, timeout=(20, 600)) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))

            with open(local_tar, "wb") as handle, tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                desc="Downloading AIID snapshot",
            ) as progress:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    progress.update(len(chunk))

        with tarfile.open(local_tar, "r:bz2") as tar:
            self._safe_extract(tar, dest_dir)

        files: dict[str, str] = {}
        for root, _, filenames in os.walk(dest_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                if os.path.abspath(full_path) == os.path.abspath(local_tar):
                    continue

                rel_path = os.path.relpath(full_path, dest_dir)
                files[rel_path] = full_path
                files.setdefault(filename, full_path)

        return files

    def load_incidents_csv(self, files: dict[str, str]) -> pd.DataFrame:
        """
        Load incidents.csv as the primary DataFrame and enrich with related files.
        """
        incidents_path = self._find_file(files, "incidents.csv")
        if not incidents_path:
            raise FileNotFoundError(
                "incidents.csv not found in snapshot. Check snapshot format."
            )

        df_incidents = pd.read_csv(incidents_path, low_memory=False)
        self._stringify_incident_id(df_incidents)

        class_path = self._find_file(files, "classifications.csv")
        if class_path:
            df_class = pd.read_csv(class_path, low_memory=False)
            self._stringify_incident_id(df_class)
            if "incident_id" in df_class.columns:
                df_incidents = df_incidents.merge(
                    df_class,
                    on="incident_id",
                    how="left",
                    suffixes=("", "_classification"),
                )

        entities_path = self._find_file(files, "entities.csv")
        if entities_path:
            df_entities = pd.read_csv(entities_path, low_memory=False)
            self._stringify_incident_id(df_entities)
            if "incident_id" in df_entities.columns:
                entity_col = next(
                    (
                        candidate
                        for candidate in ("name", "entity_name", "entity", "title")
                        if candidate in df_entities.columns
                    ),
                    None,
                )
                if entity_col:
                    entity_summary = (
                        df_entities.groupby("incident_id")[entity_col]
                        .apply(lambda values: ", ".join(values.dropna().astype(str).unique()))
                        .reset_index()
                        .rename(columns={entity_col: "entities_involved"})
                    )
                    df_incidents = df_incidents.merge(
                        entity_summary, on="incident_id", how="left"
                    )

        return df_incidents

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize AIID-specific fields for DataSage processing.
        """
        normalized = df.copy()

        date_col = self._resolve_column(
            normalized, ["date", "incident_date", "Date", "IncidentDate"]
        )
        if date_col:
            normalized[date_col] = pd.to_datetime(normalized[date_col], errors="coerce")
            normalized["year"] = normalized[date_col].dt.year
            normalized["month"] = normalized[date_col].dt.month

        list_cols = [
            "AllegedDeployerOfAISystem",
            "AllegedDeveloperOfAISystem",
            "AllegedHarmedOrNearlyHarmedParties",
        ]
        for candidate in list_cols:
            col = self._resolve_column(normalized, [candidate, candidate.lower()])
            if col:
                normalized[f"{col}_primary"] = normalized[col].apply(self._extract_first)

        normalized.columns = [self._normalize_column_name(column) for column in normalized.columns]
        return normalized

    def _extract_first(self, val: Any) -> str | None:
        """Extract first item from AIID's stringified list fields."""
        if pd.isna(val):
            return None
        if isinstance(val, (list, tuple)):
            return str(val[0]) if val else None

        text = str(val).strip()
        if text.startswith("["):
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, (list, tuple)) and parsed:
                    return str(parsed[0])
            except (SyntaxError, ValueError):
                return text
        return text

    def _find_file(self, files: dict[str, str], expected_name: str) -> str | None:
        if expected_name in files:
            return files[expected_name]
        for key, path in files.items():
            if key.lower().endswith(expected_name.lower()):
                return path
        return None

    def _resolve_column(self, df: pd.DataFrame, candidates: list[str]) -> str | None:
        lowered = {column.lower(): column for column in df.columns}
        for candidate in candidates:
            found = lowered.get(candidate.lower())
            if found:
                return found
        return None

    def _normalize_column_name(self, column: str) -> str:
        return (
            str(column)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace(".", "_")
        )

    def _safe_extract(self, tar: tarfile.TarFile, dest_dir: str) -> None:
        target_root = os.path.abspath(dest_dir)
        for member in tar.getmembers():
            member_path = os.path.abspath(os.path.join(dest_dir, member.name))
            if not member_path.startswith(target_root + os.sep) and member_path != target_root:
                raise ValueError(f"Unsafe path in archive: {member.name}")
        tar.extractall(dest_dir)

    def _stringify_incident_id(self, df: pd.DataFrame) -> None:
        if "incident_id" in df.columns:
            df["incident_id"] = df["incident_id"].astype("string")
