"""
DataSage Merge Studio — Universal Auto-Join Engine
===================================================
Completely dataset-agnostic. Zero hardcoded column names, keywords,
or domain assumptions. Join detection works purely by:

  1. Normalising actual cell values (not column names)
  2. Measuring value-set overlap between every column pair
  3. Detecting structural patterns in the data itself
  4. Scoring key quality (uniqueness, null rate, cardinality)
  5. Column-name similarity is only a weak 2-point tiebreaker

Philosophy
----------
  Column NAMES are unreliable across datasets.
    "emp_id" vs "employee_identifier" vs "staff_no" vs "id" — all the
    same concept, but name matching would miss most of them.

  Column VALUES are ground truth.
    If 90% of values in col A appear in col B after normalisation,
    that IS the join key — regardless of what either column is called.

Normalisation strategies (auto-selected per column pair)
---------------------------------------------------------
  EXACT      — raw string equality (fast baseline)
  CASEFOLD   — lower + strip + unicode normalise
  NUMERIC    — parse to int/float ("42" == 42.0 == "42.0")
  DATE       — parse any date format → YYYY-MM-DD
  ID_STRIP   — strip all non-numeric chars, keep numeric core
               "EMP-007" → "7",  "user_42" → "42",  "#100" → "100"
  SLUG       — remove all non-alphanumeric, casefold
               "New York" → "newyork",  "new-york" → "newyork"

The engine tries ALL applicable strategies and keeps the one with
highest bidirectional value overlap.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import pandas as pd


# ── Enums ──────────────────────────────────────────────────────────────────────

class JoinType(str, Enum):
    INNER = "inner"
    LEFT  = "left"
    RIGHT = "right"
    OUTER = "outer"


class MatchStrategy(str, Enum):
    EXACT    = "exact"
    CASEFOLD = "casefold"
    NUMERIC  = "numeric"
    DATE     = "date"
    ID_STRIP = "id_strip"
    SLUG     = "slug"


class SignalType(str, Enum):
    VALUE_OVERLAP   = "value_overlap"
    UNIQUENESS      = "uniqueness"
    NULL_RATE       = "null_rate"
    CARDINALITY_SYM = "cardinality_sym"
    DTYPE_COMPAT    = "dtype_compat"
    NAME_HINT       = "name_hint"


# ── Core data structures ───────────────────────────────────────────────────────

@dataclass
class JoinSignal:
    type:        SignalType
    description: str
    score_pts:   float
    detail:      dict = field(default_factory=dict)


@dataclass
class JoinCandidate:
    left_col:    str
    right_col:   str
    join_type:   JoinType
    strategy:    MatchStrategy
    confidence:  float
    signals:          list[JoinSignal] = field(default_factory=list)
    match_count:      int   = 0
    left_total:       int   = 0
    right_total:      int   = 0
    left_match_pct:   float = 0.0
    right_match_pct:  float = 0.0
    est_output_rows:  int   = 0
    sample_matches:   list  = field(default_factory=list)
    sample_no_match:  list  = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.left_col} <-> {self.right_col}"

    def to_dict(self) -> dict:
        return {
            "left_col":        self.left_col,
            "right_col":       self.right_col,
            "join_type":       self.join_type.value,
            "strategy":        self.strategy.value,
            "confidence":      round(self.confidence, 1),
            "label":           self.label,
            "match_count":     self.match_count,
            "left_total":      self.left_total,
            "right_total":     self.right_total,
            "left_match_pct":  round(self.left_match_pct,  1),
            "right_match_pct": round(self.right_match_pct, 1),
            "est_output_rows": self.est_output_rows,
            "sample_matches":  self.sample_matches[:5],
            "sample_no_match": self.sample_no_match[:5],
            "signals": [
                {
                    "type":        s.type.value,
                    "description": s.description,
                    "score_pts":   round(s.score_pts, 2),
                    **s.detail,
                }
                for s in self.signals
            ],
        }


@dataclass
class MergeResult:
    df:              pd.DataFrame
    left_rows:       int
    right_rows:      int
    merged_rows:     int
    matched_rows:    int
    left_only_rows:  int
    right_only_rows: int
    duplicate_keys:  int
    col_conflicts:   list[str]
    warnings:        list[str]

    def summary(self) -> dict:
        return {
            "left_rows":       self.left_rows,
            "right_rows":      self.right_rows,
            "merged_rows":     self.merged_rows,
            "matched_rows":    self.matched_rows,
            "left_only_rows":  self.left_only_rows,
            "right_only_rows": self.right_only_rows,
            "duplicate_keys":  self.duplicate_keys,
            "col_conflicts":   self.col_conflicts,
            "warnings":        self.warnings,
        }


# ── Normalisation functions ────────────────────────────────────────────────────

def _norm_exact(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v)


def _norm_casefold(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s or None


def _norm_numeric(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(str(v).strip().replace(",", ""))
        return str(int(f)) if f == int(f) else str(f)
    except (ValueError, OverflowError):
        return None


_DATE_FMTS = [
    "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
    "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d",
    "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
]


def _norm_date(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    for fmt in _DATE_FMTS:
        try:
            return pd.to_datetime(s, format=fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        return pd.to_datetime(s, infer_datetime_format=True).strftime("%Y-%m-%d")
    except Exception:
        return None


_ID_STRIP_RE = re.compile(r"[^0-9]+")


def _norm_id_strip(v: Any) -> Optional[str]:
    """Strip all non-numeric chars. 'EMP-007' -> '7', 'user_42' -> '42'."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    digits = _ID_STRIP_RE.sub("", str(v))
    if not digits:
        return None
    return digits.lstrip("0") or "0"


_SLUG_RE = re.compile(r"[^a-z0-9]")


def _norm_slug(v: Any) -> Optional[str]:
    """Remove all non-alphanumeric, casefold. 'New York' -> 'newyork'."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = _SLUG_RE.sub("", s.lower())
    return s or None


_NORM_FN = {
    MatchStrategy.EXACT:    _norm_exact,
    MatchStrategy.CASEFOLD: _norm_casefold,
    MatchStrategy.NUMERIC:  _norm_numeric,
    MatchStrategy.DATE:     _norm_date,
    MatchStrategy.ID_STRIP: _norm_id_strip,
    MatchStrategy.SLUG:     _norm_slug,
}


def _apply_strategy(series: pd.Series, strategy: MatchStrategy) -> pd.Series:
    return series.map(_NORM_FN[strategy])


# ── Column profiler ────────────────────────────────────────────────────────────

@dataclass
class ColProfile:
    name:            str
    n_total:         int
    n_notnull:       int
    null_rate:       float
    n_unique:        float
    dtype_str:       str
    looks_numeric:   bool
    looks_date:      bool
    looks_id_prefix: bool
    looks_free_text: bool
    median_str_len:  float
    sample_vals:     list


def _profile_column(series: pd.Series, name: str, sample_n: int = 3000) -> ColProfile:
    s         = series.dropna()
    n_total   = len(series)
    n_notnull = len(s)
    null_rate = 1 - (n_notnull / n_total) if n_total > 0 else 1.0
    n_unique  = s.nunique() / n_notnull if n_notnull > 0 else 0.0

    if n_notnull == 0:
        return ColProfile(
            name=name, n_total=n_total, n_notnull=0, null_rate=1.0,
            n_unique=0.0, dtype_str=str(series.dtype),
            looks_numeric=False, looks_date=False, looks_id_prefix=False,
            looks_free_text=False, median_str_len=0.0, sample_vals=[],
        )

    probe     = s if len(s) <= sample_n else s.sample(sample_n, random_state=0)
    str_probe = probe.astype(str)

    numeric_rate   = str_probe.str.match(r"^\s*-?[\d,]+(\.\d+)?\s*$").mean()
    id_prefix_rate = str_probe.str.match(r"^[A-Za-z_#\-/]+\d+[A-Za-z0-9_\-/]*$").mean()
    median_len     = str_probe.str.len().median()
    median_words   = str_probe.str.split().str.len().fillna(0).median()

    date_sample = str_probe.sample(min(40, len(str_probe)), random_state=1)
    date_hits   = sum(1 for v in date_sample if _norm_date(v) is not None)
    date_rate   = date_hits / len(date_sample) if len(date_sample) > 0 else 0.0

    return ColProfile(
        name=name,
        n_total=n_total,
        n_notnull=n_notnull,
        null_rate=null_rate,
        n_unique=n_unique,
        dtype_str=str(series.dtype),
        looks_numeric=bool(numeric_rate >= 0.80),
        looks_date=bool(date_rate >= 0.75),
        looks_id_prefix=bool(id_prefix_rate >= 0.60),
        looks_free_text=bool(median_words >= 4),
        median_str_len=float(median_len),
        sample_vals=[_norm_casefold(v) for v in s.head(20).tolist()],
    )


# ── Strategy selection (data-pattern based, no column names) ──────────────────

def _candidate_strategies(lp: ColProfile, rp: ColProfile) -> list[MatchStrategy]:
    strategies: list[MatchStrategy] = []
    if lp.looks_date and rp.looks_date:
        strategies.append(MatchStrategy.DATE)
    if lp.looks_numeric and rp.looks_numeric:
        strategies.append(MatchStrategy.NUMERIC)
    if lp.looks_id_prefix or rp.looks_id_prefix:
        strategies.append(MatchStrategy.ID_STRIP)
    if not lp.looks_free_text and not rp.looks_free_text:
        if not lp.looks_numeric and not rp.looks_numeric:
            strategies.append(MatchStrategy.SLUG)
    strategies.append(MatchStrategy.CASEFOLD)
    strategies.append(MatchStrategy.EXACT)
    seen: set = set()
    result: list[MatchStrategy] = []
    for s in strategies:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


# ── Value overlap ──────────────────────────────────────────────────────────────

def _compute_overlap(
    left_keys: pd.Series,
    right_keys: pd.Series,
    sample_n: int = 8000,
) -> tuple[float, float, int]:
    lk = left_keys.dropna()
    rk = right_keys.dropna()
    if len(lk) == 0 or len(rk) == 0:
        return 0.0, 0.0, 0
    if len(lk) > sample_n:
        lk = lk.sample(sample_n, random_state=42)
    if len(rk) > sample_n:
        rk = rk.sample(sample_n, random_state=42)
    lk_set = set(lk.astype(str))
    rk_set = set(rk.astype(str))
    l_in_r = sum(1 for v in lk if str(v) in rk_set)
    r_in_l = sum(1 for v in rk if str(v) in lk_set)
    return (
        float(l_in_r / len(lk)) if len(lk) > 0 else 0.0,
        float(r_in_l / len(rk)) if len(rk) > 0 else 0.0,
        int(l_in_r),
    )


def _best_strategy_overlap(
    left_series: pd.Series,
    right_series: pd.Series,
    strategies: list[MatchStrategy],
    sample_n: int = 8000,
) -> tuple[MatchStrategy, float, float, int, pd.Series, pd.Series]:
    best_strat = MatchStrategy.CASEFOLD
    best_score = -1.0
    best = (0.0, 0.0, 0)
    best_lkeys = pd.Series(dtype=str)
    best_rkeys = pd.Series(dtype=str)

    for strat in strategies:
        lk = _apply_strategy(left_series,  strat)
        rk = _apply_strategy(right_series, strat)
        lk_valid_rate = len(lk.dropna()) / max(len(left_series), 1)
        rk_valid_rate = len(rk.dropna()) / max(len(right_series), 1)
        if lk_valid_rate < 0.3 or rk_valid_rate < 0.3:
            continue
        lf, rf, mc = _compute_overlap(lk, rk, sample_n)
        score = (lf * rf) ** 0.5  # geometric mean: both sides must overlap
        if score > best_score:
            best_score = score
            best_strat = strat
            best       = (lf, rf, mc)
            best_lkeys = lk
            best_rkeys = rk

    return (best_strat, best[0], best[1], best[2], best_lkeys, best_rkeys)


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_value_overlap(lf: float, rf: float) -> float:
    return ((lf * rf) ** 0.5) * 55.0


def _score_uniqueness(lp: ColProfile, rp: ColProfile) -> float:
    m = min(lp.n_unique, rp.n_unique)
    if m >= 0.95: return 20.0
    if m >= 0.80: return 15.0
    if m >= 0.60: return 10.0
    if m >= 0.30: return  5.0
    if m >= 0.10: return  2.0
    return 0.0


def _score_null_rate(lp: ColProfile, rp: ColProfile) -> float:
    m = max(lp.null_rate, rp.null_rate)
    if m <= 0.01: return 10.0
    if m <= 0.05: return  8.0
    if m <= 0.15: return  5.0
    if m <= 0.30: return  2.0
    return 0.0


def _score_cardinality_compat(lp: ColProfile, rp: ColProfile) -> float:
    lu = lp.n_unique * lp.n_notnull
    ru = rp.n_unique * rp.n_notnull
    if lu == 0 or ru == 0:
        return 0.0
    ratio = min(lu, ru) / max(lu, ru)
    if ratio >= 0.70: return 8.0
    if ratio >= 0.40: return 5.0
    if ratio >= 0.15: return 2.0
    return 0.0


def _score_dtype_compat(lp: ColProfile, rp: ColProfile) -> float:
    if (lp.looks_numeric and rp.looks_numeric) or (lp.looks_date and rp.looks_date):
        return 5.0
    if (not lp.looks_numeric and not lp.looks_date and
        not rp.looks_numeric and not rp.looks_date):
        return 3.0
    return 1.0


def _score_name_hint(left_col: str, right_col: str) -> float:
    """Weak tiebreaker only — max 2 points. Never drives the decision."""
    def tokens(s: str) -> set[str]:
        return set(re.sub(r"[^a-z0-9]", " ", s.lower()).split())
    l_tok = tokens(left_col)
    r_tok = tokens(right_col)
    if not l_tok or not r_tok:
        return 0.0
    jaccard = len(l_tok & r_tok) / len(l_tok | r_tok)
    return jaccard * 2.0


# ── Main engine ────────────────────────────────────────────────────────────────

class AutoJoiner:
    """
    Universal dataset-agnostic join detector.
    Works with financial, medical, e-commerce, sensor, or any other data.
    No hardcoded column names, keywords, or domain assumptions.
    All decisions derived from actual cell values.
    """

    def __init__(
        self,
        df_left:        pd.DataFrame,
        df_right:       pd.DataFrame,
        left_name:      str   = "left",
        right_name:     str   = "right",
        min_confidence: float = 5.0,
        sample_n:       int   = 8000,
    ):
        self.left       = df_left.copy()
        self.right      = df_right.copy()
        self.left_name  = left_name
        self.right_name = right_name
        self.min_conf   = min_confidence
        self.sample_n   = sample_n

        self._lprofiles = {c: _profile_column(df_left[c],  c, sample_n) for c in df_left.columns}
        self._rprofiles = {c: _profile_column(df_right[c], c, sample_n) for c in df_right.columns}

    def detect(self, top_n: int = 8) -> list[JoinCandidate]:
        candidates: list[JoinCandidate] = []

        for lc in self.left.columns:
            lp = self._lprofiles[lc]
            if lp.null_rate >= 0.95 or lp.looks_free_text:
                continue

            for rc in self.right.columns:
                rp = self._rprofiles[rc]
                if rp.null_rate >= 0.95 or rp.looks_free_text:
                    continue

                cand = self._score_pair(lc, rc, lp, rp)
                if cand and cand.confidence >= self.min_conf:
                    candidates.append(cand)

        candidates.sort(key=lambda c: c.confidence, reverse=True)

        seen_pairs: set[tuple] = set()
        enriched: list[JoinCandidate] = []
        for c in candidates:
            pair = (c.left_col, c.right_col)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                self._enrich(c)
                enriched.append(c)
            if len(enriched) >= top_n:
                break

        return enriched

    def apply(
        self,
        candidate:  JoinCandidate,
        join_type:  Optional[JoinType] = None,
        suffixes:   tuple[str, str] = ("_left", "_right"),
    ) -> MergeResult:
        jt    = join_type or candidate.join_type
        lc    = candidate.left_col
        rc    = candidate.right_col
        strat = candidate.strategy

        _lkey = "__merge_key_left__"
        _rkey = "__merge_key_right__"

        df_l = self.left.copy()
        df_r = self.right.copy()

        df_l[_lkey] = _apply_strategy(df_l[lc], strat)
        df_r[_rkey] = _apply_strategy(df_r[rc], strat)

        merged = pd.merge(
            df_l, df_r,
            left_on=_lkey, right_on=_rkey,
            how=jt.value, suffixes=suffixes, indicator=True,
        )

        matched   = int((merged["_merge"] == "both").sum())
        left_only = int((merged["_merge"] == "left_only").sum())
        right_only = int((merged["_merge"] == "right_only").sum())

        l_cols    = set(df_l.columns) - {_lkey}
        r_cols    = set(df_r.columns) - {_rkey}
        conflicts = sorted(l_cols & r_cols - {lc, rc})

        dup_l = int(df_l[_lkey].dropna().duplicated().sum())
        dup_r = int(df_r[_rkey].dropna().duplicated().sum())
        warnings: list[str] = []
        if dup_l > 0:
            warnings.append(f"Left '{lc}' has {dup_l:,} duplicate keys — row fan-out possible.")
        if dup_r > 0:
            warnings.append(f"Right '{rc}' has {dup_r:,} duplicate keys — row fan-out possible.")
        if matched == 0 and jt == JoinType.INNER:
            warnings.append("INNER join: 0 matched rows. Try LEFT join or another strategy.")
        if conflicts:
            warnings.append(f"Column conflicts renamed with suffixes {suffixes}: {conflicts}")

        drop = [c for c in merged.columns if c in (_lkey, _rkey, "_merge")]
        merged.drop(columns=drop, inplace=True, errors="ignore")

        return MergeResult(
            df=merged,
            left_rows=len(df_l),
            right_rows=len(df_r),
            merged_rows=len(merged),
            matched_rows=matched,
            left_only_rows=left_only,
            right_only_rows=right_only,
            duplicate_keys=dup_l + dup_r,
            col_conflicts=conflicts,
            warnings=warnings,
        )

    def _score_pair(self, lc, rc, lp, rp) -> Optional[JoinCandidate]:
        strategies = _candidate_strategies(lp, rp)
        (strat, lf, rf, mc, lkeys, rkeys) = _best_strategy_overlap(
            self.left[lc], self.right[rc], strategies, self.sample_n
        )

        if lf == 0.0 and rf == 0.0:
            return None

        signals: list[JoinSignal] = []
        score = 0.0

        # Signal 1: Value overlap — PRIMARY (max 55 pts)
        op = _score_value_overlap(lf, rf)
        score += op
        signals.append(JoinSignal(
            type=SignalType.VALUE_OVERLAP,
            description=(f"{lf:.0%} of left values found in right, "
                         f"{rf:.0%} of right found in left (strategy: {strat.value})"),
            score_pts=op,
            detail={"left_overlap": round(lf, 3), "right_overlap": round(rf, 3),
                    "match_count": mc, "strategy": strat.value},
        ))

        # Signal 2: Uniqueness (max 20 pts)
        up = _score_uniqueness(lp, rp)
        score += up
        signals.append(JoinSignal(
            type=SignalType.UNIQUENESS,
            description=(f"Key uniqueness: left {lp.n_unique:.0%}, right {rp.n_unique:.0%}"),
            score_pts=up,
            detail={"left_uniqueness": round(lp.n_unique, 3),
                    "right_uniqueness": round(rp.n_unique, 3)},
        ))

        # Signal 3: Null rate (max 10 pts)
        np_ = _score_null_rate(lp, rp)
        score += np_
        signals.append(JoinSignal(
            type=SignalType.NULL_RATE,
            description=(f"Null rates: left {lp.null_rate:.0%}, right {rp.null_rate:.0%}"),
            score_pts=np_,
            detail={"left_null_rate": round(lp.null_rate, 3),
                    "right_null_rate": round(rp.null_rate, 3)},
        ))

        # Signal 4: Cardinality symmetry (max 8 pts)
        cp = _score_cardinality_compat(lp, rp)
        score += cp
        if cp > 0:
            signals.append(JoinSignal(
                type=SignalType.CARDINALITY_SYM,
                description=(f"Cardinality: ~{int(lp.n_unique*lp.n_notnull):,} left unique, "
                              f"~{int(rp.n_unique*rp.n_notnull):,} right unique"),
                score_pts=cp,
            ))

        # Signal 5: Structural pattern compat (max 5 pts)
        dp = _score_dtype_compat(lp, rp)
        score += dp
        signals.append(JoinSignal(
            type=SignalType.DTYPE_COMPAT,
            description=(f"Data pattern: left={'numeric' if lp.looks_numeric else 'date' if lp.looks_date else 'text'}, "
                         f"right={'numeric' if rp.looks_numeric else 'date' if rp.looks_date else 'text'}"),
            score_pts=dp,
        ))

        # Signal 6: Column name hint — tiebreaker only (max 2 pts)
        nh = _score_name_hint(lc, rc)
        score += nh
        if nh > 0:
            signals.append(JoinSignal(
                type=SignalType.NAME_HINT,
                description=f"Column name hint: '{lc}' vs '{rc}' (tiebreaker only)",
                score_pts=nh,
            ))

        confidence = min(100.0, max(0.0, score))

        if lp.n_unique >= 0.95 and rp.n_unique >= 0.95 and lf >= 0.85:
            join_type = JoinType.INNER
        elif lf >= rf:
            join_type = JoinType.LEFT
        else:
            join_type = JoinType.RIGHT

        return JoinCandidate(
            left_col=lc, right_col=rc,
            join_type=join_type, strategy=strat,
            confidence=confidence, signals=signals,
        )

    def _enrich(self, cand: JoinCandidate) -> None:
        lc, rc, strat = cand.left_col, cand.right_col, cand.strategy
        lkeys = _apply_strategy(self.left[lc],  strat)
        rkeys = _apply_strategy(self.right[rc], strat)

        rk_set = set(rkeys.dropna().astype(str))
        lk_set = set(lkeys.dropna().astype(str))

        matched_mask       = lkeys.notna() & lkeys.astype(str).isin(rk_set)
        cand.left_total    = len(self.left)
        cand.right_total   = len(self.right)
        cand.match_count   = int(matched_mask.sum())
        cand.left_match_pct  = cand.match_count / cand.left_total * 100 if cand.left_total else 0
        cand.right_match_pct = len(rk_set & lk_set) / cand.right_total * 100 if cand.right_total else 0

        matched_idx = self.left.index[matched_mask]
        cand.sample_matches = [
            {"left_raw": str(self.left.at[i, lc]), "normalised_key": str(lkeys.at[i])}
            for i in matched_idx[:5]
        ]
        no_match_idx = self.left.index[~matched_mask]
        cand.sample_no_match = [str(self.left.at[i, lc]) for i in no_match_idx[:5]]

        lk_counts = lkeys.dropna().astype(str).value_counts()
        rk_counts = rkeys.dropna().astype(str).value_counts()
        common    = set(lk_counts.index) & set(rk_counts.index)
        cand.est_output_rows = int(sum(lk_counts[k] * rk_counts[k] for k in common))


def load_df(source, **kwargs) -> pd.DataFrame:
    """
    Load a DataFrame from any supported source.

    Supported formats:
      - pd.DataFrame  - returned as-is (copy)
      - .csv          - pandas read_csv with low_memory=False
      - .parquet      - pandas read_parquet
      - .json/.jsonl/.ndjson and JSON strings - structural ingestion via
                         json_ingest.load_json_as_df (layout detection +
                         flattening + NDJSON handling)
      - .xlsx/.xls    - pandas read_excel
    """
    if isinstance(source, pd.DataFrame):
        return source.copy()

    p = str(source).lower()

    is_json_path = any(p.endswith(ext) for ext in (".json", ".jsonl", ".ndjson"))
    is_json_string = (
        not hasattr(source, "read")
        and isinstance(source, str)
        and not p.endswith((".csv", ".parquet", ".xlsx", ".xls"))
        and (p.lstrip().startswith("{") or p.lstrip().startswith("["))
    )
    if is_json_path or is_json_string:
        try:
            from .json_ingest import load_json_as_df
        except ImportError:
            from json_ingest import load_json_as_df

        return load_json_as_df(source, **kwargs)

    if p.endswith(".csv"):
        return pd.read_csv(source, low_memory=False, **kwargs)
    if p.endswith(".parquet"):
        return pd.read_parquet(source, **kwargs)
    if p.endswith((".xlsx", ".xls")):
        return pd.read_excel(source, **kwargs)
    raise ValueError(f"Unsupported format: {source}")


if __name__ == "__main__":
    # Demo: two DataFrames with completely different column names,
    # mixed ID formats, and different date formats. No hints at all.

    df_a = pd.DataFrame({
        "col_a": ["EMP-001", "EMP-002", "EMP-003", "EMP-004", "EMP-005"],
        "col_b": ["Alice",   "Bob",     "Carol",   "Dave",    "Eve"],
        "col_c": ["2021-01-10", "2020-06-15", "2022-03-01", "2019-11-20", "2023-07-04"],
        "col_d": [85000, 92000, 78000, 110000, 67000],
    })

    df_b = pd.DataFrame({
        "ref":       [1, 2, 3, 5, 6],
        "full_name": ["Alice Smith", "Bob Jones", "Carol White", "Eve Brown", "Frank Black"],
        "joined":    ["Jan 10 2021", "Jun 15 2020", "Mar 01 2022", "Jul 04 2023", "Feb 28 2024"],
        "gross_pay": [85000, 92000, 78000, 67000, 55000],
    })

    print("=" * 60)
    print(" DataSage Universal Auto-Joiner — Demo")
    print("=" * 60)
    print(f" Left  cols: {list(df_a.columns)}")
    print(f" Right cols: {list(df_b.columns)}")
    print(" (No shared column names, mixed formats, no hints)\n")

    aj = AutoJoiner(df_a, df_b)
    candidates = aj.detect(top_n=5)

    for i, c in enumerate(candidates, 1):
        print(f" #{i} {c.label} | confidence={c.confidence:.1f} | strategy={c.strategy.value}")
        print(f"    matches={c.match_count}/{c.left_total} left ({c.left_match_pct:.0f}%)")
        for s in c.signals:
            bar = "=" * int(s.score_pts / 2)
            print(f"    [{bar:<28}] {s.score_pts:4.1f}  {s.description}")
        print()

    result = aj.apply(candidates[0], join_type=JoinType.LEFT)
    print(f" Merged: {result.merged_rows} rows | matched={result.matched_rows}")
    print(result.df.to_string())
