from __future__ import annotations

import ast
import os
import re
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

    CLASSIFICATION_FIELD_CANDIDATES: dict[str, list[str]] = {
        "harm_type": [
            "Risk Domain",
            "Tangible Harm",
            "Harm Type",
            "Special Interest Intangible Harm",
            "Harm Domain",
            "Harm.Type",
        ],
        "harm_subtype": [
            "Risk Subdomain",
            "Harm Distribution Basis",
            "Protected Characteristic",
            "Harmed Class of Entities",
        ],
        "sector_of_deployment": [
            "Sector of Deployment",
            "Infrastructure Sectors",
        ],
        "technology_purveyor": [
            "Technology Purveyor",
            "Known AI Technology",
            "Potential AI Technology",
            "System Developer",
        ],
        "ai_system": [
            "AI System Description",
            "AI System",
            "Known AI Goal",
            "Potential AI Goal",
            "AI Task",
            "Relevant AI functions",
            "AI Applications",
        ],
        "harm_distribution_basis": [
            "Harm Distribution Basis",
            "Protected Characteristic",
            "Harmed Class of Entities",
        ],
        "intentional_harm": [
            "Intentional Harm",
            "Intent",
        ],
        "location_region": [
            "Location Region",
            "Location",
        ],
    }

    INCIDENT_ID_CANDIDATES = [
        "incident_id",
        "Incident ID",
        "incident id",
        "IncidentId",
        "incidentId",
    ]

    REPORT_ID_CANDIDATES = [
        "report_number",
        "report id",
        "report_id",
        "ref_number",
    ]

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

        return self.extract_archive(local_tar, dest_dir)

    def extract_archive(self, archive_path: str, dest_dir: str) -> dict[str, str]:
        """
        Extract a local AIID snapshot archive and return inventory of extracted files.
        Supports .tar.bz2 tarball snapshots that include CSV/JSON/Mongo exports.
        """
        os.makedirs(dest_dir, exist_ok=True)
        with tarfile.open(archive_path, "r:*") as tar:
            self._safe_extract(tar, dest_dir)

        files: dict[str, str] = {}
        for root, _, filenames in os.walk(dest_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                if os.path.abspath(full_path) == os.path.abspath(archive_path):
                    continue

                rel_path = os.path.relpath(full_path, dest_dir)
                files[rel_path] = full_path
                files.setdefault(filename, full_path)

        return files

    def load_incidents_csv(self, files: dict[str, str]) -> pd.DataFrame:
        """
        Load incidents.csv as the primary DataFrame and enrich with related files.
        """
        df_incidents = self._load_primary_incidents(files)

        if df_incidents.empty:
            raise FileNotFoundError(
                "No incident records found in AIID snapshot."
            )
        self._ensure_incident_id(df_incidents)

        classification_summary = self._build_classification_summary(files)
        if classification_summary is not None and not classification_summary.empty:
            df_incidents = df_incidents.merge(
                classification_summary,
                on="incident_id",
                how="left",
            )

        entity_summary = self._build_entity_summary(files)
        if entity_summary is not None and not entity_summary.empty:
            df_incidents = df_incidents.merge(entity_summary, on="incident_id", how="left")

        report_summary = self._build_report_summary(files, df_incidents)
        if report_summary is not None and not report_summary.empty:
            df_incidents = df_incidents.merge(report_summary, on="incident_id", how="left")

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

        canonical_aliases: dict[str, list[str]] = {
            "harm_type": ["Harm.Type", "Harm Type", "Harm Domain", "Risk Domain"],
            "sector_of_deployment": ["Sector of Deployment", "Infrastructure Sectors"],
            "technology_purveyor": ["Technology Purveyor", "Known AI Technology", "Potential AI Technology"],
            "ai_system": ["AI System Description", "AI System", "Known AI Goal", "Potential AI Goal"],
        }
        for canonical, candidates in canonical_aliases.items():
            source = self._resolve_column(normalized, [canonical, *candidates])
            if source:
                normalized[canonical] = normalized[source]

        list_aliases: dict[str, list[str]] = {
            "allegeddeployerofaisystem": [
                "AllegedDeployerOfAISystem",
                "Alleged deployer of AI system",
            ],
            "allegeddeveloperofaisystem": [
                "AllegedDeveloperOfAISystem",
                "Alleged developer of AI system",
            ],
            "allegedharmedornearlyharmedparties": [
                "AllegedHarmedOrNearlyHarmedParties",
                "Alleged harmed or nearly harmed parties",
            ],
        }
        for canonical, candidates in list_aliases.items():
            source = self._resolve_column(normalized, [canonical, *candidates])
            if source:
                normalized[canonical] = normalized[source]
                normalized[f"{canonical}_primary"] = normalized[source].apply(self._extract_first)

        normalized.columns = [self._normalize_column_name(column) for column in normalized.columns]
        return normalized

    def _extract_first(self, val: Any) -> str | None:
        """Extract first item from AIID's stringified list fields."""
        if isinstance(val, (list, tuple)):
            return str(val[0]) if val else None
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass

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

    def _find_files_by_prefix(self, files: dict[str, str], prefix: str) -> list[str]:
        matched: list[str] = []
        seen: set[str] = set()
        normalized_prefix = prefix.strip().lower()
        for key, path in files.items():
            base = os.path.basename(key).lower()
            candidate = os.path.basename(path).lower()
            if not (
                base.startswith(normalized_prefix)
                or candidate.startswith(normalized_prefix)
            ):
                continue
            if not (candidate.endswith(".csv") or candidate.endswith(".json")):
                continue
            if path in seen:
                continue
            seen.add(path)
            matched.append(path)
        return matched

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
            df["incident_id"] = (
                df["incident_id"]
                .astype("string")
                .str.strip()
                .str.replace(r"\.0+$", "", regex=True)
            )

    def _ensure_incident_id(self, df: pd.DataFrame) -> None:
        if "incident_id" not in df.columns:
            source = self._resolve_column(df, self.INCIDENT_ID_CANDIDATES)
            if source:
                df["incident_id"] = df[source]
        self._stringify_incident_id(df)

    def _read_table(self, path: str) -> pd.DataFrame:
        lower = path.lower()
        if lower.endswith(".csv"):
            return pd.read_csv(path, low_memory=False)
        if lower.endswith(".json"):
            try:
                return pd.read_json(path, lines=True)
            except ValueError:
                return pd.read_json(path)
        raise ValueError(f"Unsupported tabular file format: {path}")

    def _load_primary_incidents(self, files: dict[str, str]) -> pd.DataFrame:
        incidents_path = self._find_file(files, "incidents.csv")
        if incidents_path:
            return self._read_table(incidents_path)

        incidents_json_path = self._find_file(files, "incidents.json")
        if incidents_json_path:
            return self._read_table(incidents_json_path)

        candidates = self._find_files_by_prefix(files, "incidents")
        if candidates:
            return self._read_table(candidates[0])

        raise FileNotFoundError(
            "incidents.csv/incidents.json not found in snapshot. Check snapshot format."
        )

    def _classification_priority(self, source_name: str) -> int:
        name = source_name.lower()
        if name == "classifications_csetv1":
            return 0
        if name == "classifications_csetv0":
            return 1
        if name == "classifications_mit":
            return 2
        if name == "classifications_gmf":
            return 3
        if "annotator" in name:
            return 5
        return 4

    def _canonicalize_header(self, value: str) -> str:
        text = str(value).strip().lower()
        text = re.sub(r"\.\d+$", "", text)
        return re.sub(r"[^a-z0-9]+", "", text)

    def _matching_columns(self, df: pd.DataFrame, candidate: str) -> list[str]:
        expected = self._canonicalize_header(candidate)
        return [
            column
            for column in df.columns
            if self._canonicalize_header(column) == expected
        ]

    def _tokenize_value(self, value: Any) -> list[str]:
        if isinstance(value, (list, tuple, set)):
            tokens: list[str] = []
            for item in value:
                tokens.extend(self._tokenize_value(item))
            return tokens

        try:
            if pd.isna(value):
                return []
        except Exception:
            pass

        if isinstance(value, bool):
            return [str(value)]

        text = str(value).strip()
        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, (list, tuple, set)):
                    return self._tokenize_value(list(parsed))
            except (SyntaxError, ValueError):
                pass

        return [text]

    def _is_placeholder(self, token: str) -> bool:
        return token.strip().lower() in {
            "",
            "--",
            "-",
            "na",
            "n/a",
            "nan",
            "none",
            "null",
            "false",
            "yes",
            "no",
            "maybe",
            "[]",
            "{}",
        }

    def _clean_value(self, value: Any) -> str | pd._libs.missing.NAType:
        for token in self._tokenize_value(value):
            text = token.strip()
            if not text or self._is_placeholder(text):
                continue
            return text
        return pd.NA

    def _coalesce_first(self, df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        out = pd.Series(pd.NA, index=df.index, dtype="string")
        for candidate in candidates:
            for column in self._matching_columns(df, candidate):
                cleaned = df[column].apply(self._clean_value).astype("string")
                out = out.fillna(cleaned)
        return out

    def _join_unique(self, values: Any) -> str | pd._libs.missing.NAType:
        ordered: dict[str, None] = {}
        for value in values:
            for token in self._tokenize_value(value):
                normalized = token.strip()
                if not normalized or self._is_placeholder(normalized):
                    continue
                ordered.setdefault(normalized, None)
        if not ordered:
            return pd.NA
        return ", ".join(ordered.keys())

    def _first_nonempty(self, values: Any) -> str | pd._libs.missing.NAType:
        for value in values:
            cleaned = self._clean_value(value)
            if isinstance(cleaned, str):
                return cleaned
        return pd.NA

    def _build_classification_summary(self, files: dict[str, str]) -> pd.DataFrame | None:
        paths = self._find_files_by_prefix(files, "classifications")
        if not paths:
            return None

        canonical_fields = list(self.CLASSIFICATION_FIELD_CANDIDATES.keys())
        frames: list[pd.DataFrame] = []
        for path in paths:
            try:
                frame = self._read_table(path)
            except Exception:
                continue
            if frame.empty:
                continue

            self._ensure_incident_id(frame)
            if "incident_id" not in frame.columns:
                continue

            source_name = os.path.splitext(os.path.basename(path))[0]
            scoped = pd.DataFrame({"incident_id": frame["incident_id"]})
            scoped["classification_source"] = source_name
            scoped["classification_priority"] = self._classification_priority(source_name)

            for canonical, candidates in self.CLASSIFICATION_FIELD_CANDIDATES.items():
                scoped[canonical] = self._coalesce_first(frame, candidates)

            frames.append(scoped)

        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined = combined.sort_values(
            by=["incident_id", "classification_priority", "classification_source"]
        )

        rows: list[dict[str, Any]] = []
        for incident_id, group in combined.groupby("incident_id", sort=False):
            row: dict[str, Any] = {
                "incident_id": incident_id,
                "classification_sources": self._join_unique(group["classification_source"]),
            }
            for field in canonical_fields:
                row[field] = self._first_nonempty(group[field])

            row["harm_type_all"] = self._join_unique(group["harm_type"])
            row["sector_of_deployment_all"] = self._join_unique(group["sector_of_deployment"])
            row["technology_purveyor_all"] = self._join_unique(group["technology_purveyor"])
            row["ai_system_all"] = self._join_unique(group["ai_system"])
            rows.append(row)

        summary = pd.DataFrame(rows)
        self._stringify_incident_id(summary)
        return summary

    def _build_entity_summary(self, files: dict[str, str]) -> pd.DataFrame | None:
        paths = self._find_files_by_prefix(files, "entities")
        if not paths:
            legacy = self._find_file(files, "entities.csv") or self._find_file(files, "entities.json")
            if legacy:
                paths = [legacy]

        if not paths:
            return None

        rows: list[pd.DataFrame] = []
        for path in paths:
            try:
                frame = self._read_table(path)
            except Exception:
                continue

            self._ensure_incident_id(frame)
            if "incident_id" not in frame.columns:
                continue

            entity_col = self._resolve_column(
                frame,
                ["name", "entity_name", "entity", "title", "organization"],
            )
            if not entity_col:
                continue

            scoped = frame[["incident_id", entity_col]].copy()
            scoped = scoped.rename(columns={entity_col: "entity_name"})
            rows.append(scoped)

        if not rows:
            return None

        combined = pd.concat(rows, ignore_index=True, sort=False)
        summary = (
            combined.groupby("incident_id", dropna=False)["entity_name"]
            .apply(self._join_unique)
            .reset_index()
            .rename(columns={"entity_name": "entities_involved"})
        )
        self._stringify_incident_id(summary)
        return summary

    def _parse_list_like(self, value: Any) -> list[Any]:
        if isinstance(value, (list, tuple, set)):
            return list(value)
        if value is None:
            return []

        try:
            if pd.isna(value):
                return []
        except Exception:
            pass

        text = str(value).strip()
        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, (list, tuple, set)):
                    return list(parsed)
            except (SyntaxError, ValueError):
                return []

        return [text]

    def _normalize_report_key(self, value: Any) -> str | None:
        token = self._clean_value(value)
        if not isinstance(token, str):
            return None
        normalized = token.strip()
        if not normalized:
            return None
        return normalized.replace(".0", "")

    def _latest_date(self, values: Any) -> str | pd._libs.missing.NAType:
        parsed = pd.to_datetime(pd.Series(list(values), dtype="string"), errors="coerce")
        if parsed.notna().any():
            return parsed.max().date().isoformat()
        return pd.NA

    def _build_report_lookup(self, files: dict[str, str]) -> dict[str, dict[str, Any]]:
        paths = self._find_files_by_prefix(files, "reports")
        if not paths:
            report_file = self._find_file(files, "reports.csv") or self._find_file(files, "reports.json")
            if report_file:
                paths = [report_file]
        if not paths:
            return {}

        frames: list[pd.DataFrame] = []
        for path in paths:
            try:
                frame = self._read_table(path)
            except Exception:
                continue
            if frame.empty:
                continue
            frames.append(frame)

        if not frames:
            return {}

        reports = pd.concat(frames, ignore_index=True, sort=False)
        report_id_col = self._resolve_column(reports, self.REPORT_ID_CANDIDATES)
        if not report_id_col:
            return {}

        reports = reports.copy()
        reports["report_key"] = reports[report_id_col].apply(self._normalize_report_key).astype("string")
        reports = reports[reports["report_key"].notna()]
        if reports.empty:
            return {}

        source_col = self._resolve_column(reports, ["source_domain", "source", "domain"])
        title_col = self._resolve_column(reports, ["title"])
        url_col = self._resolve_column(reports, ["url"])
        date_col = self._resolve_column(
            reports,
            ["date_published", "date_submitted", "date_modified", "date_downloaded"],
        )

        agg_spec: dict[str, Any] = {}
        if source_col:
            agg_spec["report_sources"] = (source_col, self._join_unique)
        if title_col:
            agg_spec["report_titles"] = (title_col, self._join_unique)
        if url_col:
            agg_spec["report_urls"] = (url_col, self._join_unique)
        if date_col:
            agg_spec["latest_report_date"] = (date_col, self._latest_date)

        if not agg_spec:
            return {}

        grouped = reports.groupby("report_key", dropna=False).agg(**agg_spec).reset_index()
        lookup: dict[str, dict[str, Any]] = {}
        for row in grouped.to_dict(orient="records"):
            report_key = str(row.pop("report_key"))
            lookup[report_key] = row
        return lookup

    def _build_report_summary(self, files: dict[str, str], incidents: pd.DataFrame) -> pd.DataFrame | None:
        reports_col = self._resolve_column(incidents, ["reports", "report_ids", "report_numbers"])
        if not reports_col:
            return None

        report_lookup = self._build_report_lookup(files)
        summary_rows: list[dict[str, Any]] = []

        for _, record in incidents[["incident_id", reports_col]].iterrows():
            incident_id = record.get("incident_id")
            keys = [
                key
                for key in (self._normalize_report_key(item) for item in self._parse_list_like(record[reports_col]))
                if key
            ]
            if not keys:
                continue

            dedup_keys = list(dict.fromkeys(keys))
            sources: list[Any] = []
            titles: list[Any] = []
            urls: list[Any] = []
            latest_dates: list[Any] = []
            for key in dedup_keys:
                details = report_lookup.get(key, {})
                if details.get("report_sources") is not None:
                    sources.append(details["report_sources"])
                if details.get("report_titles") is not None:
                    titles.append(details["report_titles"])
                if details.get("report_urls") is not None:
                    urls.append(details["report_urls"])
                if details.get("latest_report_date") is not None:
                    latest_dates.append(details["latest_report_date"])

            summary_rows.append(
                {
                    "incident_id": incident_id,
                    "report_count": len(dedup_keys),
                    "report_sources": self._join_unique(sources),
                    "report_titles": self._join_unique(titles),
                    "report_urls": self._join_unique(urls),
                    "latest_report_date": self._latest_date(latest_dates),
                }
            )

        if not summary_rows:
            return None

        summary = pd.DataFrame(summary_rows)
        self._stringify_incident_id(summary)
        return summary
