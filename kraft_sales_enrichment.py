#!/usr/bin/env python3
"""
Kraft Heinz Sales Intelligence Enrichment
----------------------------------------

Ingests internal ERP sell-in/sell-out plus Circana/Nielsen syndicated files from
GCS (or local), harmonizes schemas, joins product/customer masters, aligns retail
week to fiscal week, validates/deduplicates records, computes KPIs & market share,
and writes partitioned Parquet + dead-letter JSONL back to GCS/local.

Inputs (wildcards allowed):
  --internal "gs://bucket/internal/2025-10/*.parquet"
  --circana  "gs://bucket/circana/2025-10/*.csv"
  --nielsen  "gs://bucket/nielsen/2025-10/*.csv"
  --product_master "gs://bucket/masters/product_master.parquet"
  --customer_master "gs://bucket/masters/customer_master.parquet"

Outputs:
  Curated row-level and aggregates written to:
    --output_prefix "gs://bucket/curated/kraft_enriched"
    # partitioned by fiscal_week=YYYYWW

Bad rows:
  --badrows_prefix "gs://bucket/badrows/kraft_enriched"

Run (local):
  python kraft_sales_enrichment.py \
    --internal "./data/internal/*.csv" \
    --circana "./data/circana/*.csv" \
    --nielsen "./data/nielsen/*.csv" \
    --product_master "./data/masters/product_master.csv" \
    --customer_master "./data/masters/customer_master.csv" \
    --output_prefix "./data/curated/kraft_enriched" \
    --badrows_prefix "./data/badrows/kraft_enriched" \
    --aggregate_dims "retailer,channel,region,brand"

Auth:
  For GCS, ensure Application Default Credentials are set
  (e.g., `gcloud auth application-default login`) or run in Composer.
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
import gcsfs  # noqa: F401  # registers gs:// with fsspec

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("kraft-sales-enrichment")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Config:
    internal_glob: Optional[str]
    circana_glob: Optional[str]
    nielsen_glob: Optional[str]
    product_master_path: str
    customer_master_path: str
    output_prefix: str
    badrows_prefix: str
    partition_col: str = "fiscal_week"  # e.g., 202545 (YYYYWW)
    write_compression: str = "snappy"
    aggregate_dims: Tuple[str, ...] = ("retailer", "channel", "region", "brand")
    # Required canonical columns after harmonization (pre-master joins OK)
    required_fields: Tuple[str, ...] = (
        "retail_week_start", "upc", "retailer", "channel",
        "units", "net_sales"
    )
    # If retail→fiscal week offset is constant (simple case), e.g., +0 weeks
    retail_to_fiscal_week_offset: int = 0


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------
def _is_gcs(path: str) -> bool:
    return path and path.startswith("gs://")


def _fs_for_path(path: str):
    return fsspec.filesystem("gcs") if _is_gcs(path) else fsspec.filesystem("file")


def _glob(glob_uri: Optional[str]) -> List[str]:
    if not glob_uri:
        return []
    fs = _fs_for_path(glob_uri)
    return fs.glob(glob_uri)


def _read_any(paths: List[str]) -> pd.DataFrame:
    """Read CSV/Parquet files (local or GCS) and tag with __source_file."""
    frames: List[pd.DataFrame] = []
    for p in paths:
        fs = _fs_for_path(p)
        with fs.open(p, "rb") as f:
            if p.lower().endswith(".parquet"):
                df = pd.read_parquet(f)
            elif p.lower().endswith(".csv") or p.lower().endswith(".csv.gz"):
                df = pd.read_csv(f, low_memory=False)
            else:
                log.warning("Skipping unsupported file type: %s", p)
                continue
            df["__source_file"] = p
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d rows from %d files", len(out), len(frames))
    return out


# ---------------------------------------------------------------------------
# Canonical schema & harmonization
# ---------------------------------------------------------------------------
_CANON = [
    "retail_week_start",  # canonical daily start of retail week (date)
    "fiscal_week",        # YYYYWW string
    "upc", "gtin",
    "retailer", "banner", "region", "channel",
    "category", "segment", "brand", "packsize",
    "units", "net_sales", "promo_flag",
    "__source", "__source_file"
]

# Provider-specific alias maps -> canonical names
_INTERNAL_ALIASES = {
    "week_start_date": "retail_week_start",
    "upc_code": "upc",
    "gtin_code": "gtin",
    "customer_name": "retailer",
    "retailer_banner": "banner",
    "area": "region",
    "sales_channel": "channel",
    "prod_category": "category",
    "prod_segment": "segment",
    "prod_brand": "brand",
    "pack_size": "packsize",
    "quantity_units": "units",
    "net_revenue": "net_sales",
    "on_promo": "promo_flag",
}

_CIRCANA_ALIASES = {
    "week_start": "retail_week_start",
    "upc": "upc",
    "retailer": "retailer",
    "banner": "banner",
    "market": "region",
    "channel": "channel",
    "category": "category",
    "segment": "segment",
    "brand": "brand",
    "pack": "packsize",
    "units": "units",
    "dollar_sales": "net_sales",
    "promo": "promo_flag",
}

_NIELSEN_ALIASES = {
    "week_beg_dt": "retail_week_start",
    "upc": "upc",
    "retlr": "retailer",
    "bnr": "banner",
    "geo": "region",
    "chnl": "channel",
    "cat": "category",
    "seg": "segment",
    "brd": "brand",
    "pack_sz": "packsize",
    "unit_sls": "units",
    "net_sls": "net_sales",
    "promo_ind": "promo_flag",
}

def _rename_apply_aliases(df: pd.DataFrame, aliases: dict, source_label: str) -> pd.DataFrame:
    df = df.rename(columns={k: v for k, v in aliases.items() if k in df.columns}).copy()
    df["__source"] = source_label
    return df


def _coerce_and_trim(df: pd.DataFrame) -> pd.DataFrame:
    """Trim strings, coerce types for key fields."""
    def norm_str(x):
        if pd.isna(x): return None
        s = str(x)
        s = re.sub(r"[\x00-\x1F\x7F]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s or None

    for c in ("upc", "gtin", "retailer", "banner", "region", "channel",
              "category", "segment", "brand", "packsize"):
        if c in df.columns:
            df[c] = df[c].map(norm_str)

    # dates & numerics
    if "retail_week_start" in df.columns:
        df["retail_week_start"] = pd.to_datetime(df["retail_week_start"], errors="coerce").dt.date.astype("string")

    for c in ("units",):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    if "net_sales" in df.columns:
        df["net_sales"] = pd.to_numeric(df["net_sales"], errors="coerce").astype("float64")

    # promo_flag -> boolish
    if "promo_flag" in df.columns:
        def to_boolish(x):
            if pd.isna(x): return None
            s = str(x).strip().lower()
            return s in ("1", "t", "true", "y", "yes")
        df["promo_flag"] = df["promo_flag"].map(to_boolish)

    return df


# ---------------------------------------------------------------------------
# Week alignment (retail -> fiscal)
# ---------------------------------------------------------------------------
def _infer_fiscal_week_from_date_str(date_str: Optional[str], week_offset: int = 0) -> Optional[str]:
    """
    Simplified fiscal week: use ISO week (YYYYWW) and apply optional offset.
    For real-world S/4HANA fiscal calendars, replace with a proper calendar map.
    """
    if not date_str or date_str == "NaT":
        return None
    try:
        dt = pd.to_datetime(date_str).to_pydatetime()
        # ISO calendar
        year, week, _ = dt.isocalendar()
        week += week_offset
        # roll across year boundaries if offset pushes out of 1..52/53
        if week < 1:
            year -= 1
            week = max(1, week + 52)
        elif week > 53:
            year += 1
            week = ((week - 1) % 53) + 1
        return f"{year}{week:02d}"
    except Exception:
        return None


def _add_fiscal_week(df: pd.DataFrame, week_offset: int) -> pd.DataFrame:
    if "fiscal_week" not in df.columns or df["fiscal_week"].isna().all():
        df["fiscal_week"] = df["retail_week_start"].map(lambda s: _infer_fiscal_week_from_date_str(s, week_offset))
    return df


# ---------------------------------------------------------------------------
# Join masters
# ---------------------------------------------------------------------------
def _join_product_master(df: pd.DataFrame, product_master: pd.DataFrame) -> pd.DataFrame:
    """
    product_master expected columns (any subset): upc, brand, segment, category, packsize
    Left-join on UPC; fill gaps conservatively (don't overwrite if already present).
    """
    pm = product_master.copy()
    # Normalize master col names
    pm = pm.rename(columns={
        "upc_code": "upc",
        "prod_brand": "brand",
        "prod_segment": "segment",
        "prod_category": "category",
        "pack_size": "packsize",
    })

    if "upc" not in pm.columns:
        log.warning("Product master missing 'upc' column; skipping join.")
        return df

    merged = df.merge(pm, on="upc", how="left", suffixes=("", "_pm"))
    for c in ("brand", "segment", "category", "packsize"):
        src = f"{c}_pm"
        if c in merged.columns and src in merged.columns:
            merged[c] = merged[c].fillna(merged[src])
            merged.drop(columns=[src], inplace=True)
    return merged


def _join_customer_master(df: pd.DataFrame, customer_master: pd.DataFrame) -> pd.DataFrame:
    """
    customer_master expected columns (any subset): retailer, banner, region, channel
    Left-join on retailer; prefer row values, then master fallback.
    """
    cm = customer_master.copy()
    cm = cm.rename(columns={
        "customer_name": "retailer",
        "retailer_banner": "banner",
        "area": "region",
        "sales_channel": "channel",
    })

    if "retailer" not in cm.columns:
        log.warning("Customer master missing 'retailer' column; skipping join.")
        return df

    merged = df.merge(cm, on="retailer", how="left", suffixes=("", "_cm"))
    for c in ("banner", "region", "channel"):
        src = f"{c}_cm"
        if c in merged.columns and src in merged.columns:
            merged[c] = merged[c].fillna(merged[src])
            merged.drop(columns=[src], inplace=True)
    return merged


# ---------------------------------------------------------------------------
# Deduplicate & Validation
# ---------------------------------------------------------------------------
def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate by (fiscal_week, upc, retailer, banner, region, channel, __source_file)
    Keep record with highest units (heuristic) or last if tie.
    """
    keys = [c for c in ("fiscal_week", "upc", "retailer", "banner", "region", "channel", "__source_file") if c in df.columns]
    if not keys:
        return df.drop_duplicates()

    if "units" in df.columns:
        return (df.sort_values(keys + ["units"], ascending=[True]*len(keys) + [False])
                  .drop_duplicates(subset=keys, keep="first"))
    return df.drop_duplicates(subset=keys, keep="last")


def _validate_and_split(df: pd.DataFrame, required: Tuple[str, ...]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Basic checks:
      - required fields present and non-empty
      - units, net_sales non-negative
    """
    work = df.copy()
    mask_req = np.ones(len(work), dtype=bool)
    for col in required:
        if col not in work.columns:
            work[col] = np.nan
        mask_req &= work[col].notna() & (work[col] != "")

    def nonneg(col: str):
        return work[col].fillna(0) >= 0 if col in work.columns else True

    mask_nonneg = nonneg("units") & nonneg("net_sales")
    bad_mask = ~(mask_req & mask_nonneg)

    good = work[~bad_mask].copy()
    bad = work[bad_mask].copy()

    if not bad.empty:
        reasons = []
        for i in bad.index:
            r = []
            if not mask_req.loc[i]: r.append("missing_required")
            if not mask_nonneg.loc[i]: r.append("negative_values")
            reasons.append(",".join(r) if r else "invalid")
        bad["rejection_reason"] = reasons

    return good, bad


# ---------------------------------------------------------------------------
# KPI & Market share
# ---------------------------------------------------------------------------
def _derive_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute price per unit, promo splits, etc.
    """
    if {"net_sales", "units"}.issubset(df.columns):
        df["avg_price_per_unit"] = np.where(df["units"] > 0, df["net_sales"] / df["units"], np.nan)
    return df


def _compute_market_share(df: pd.DataFrame, dims: Tuple[str, ...]) -> pd.DataFrame:
    """
    Compute share within each market bucket (group without 'brand'), then add brand share.
    Market bucket = [fiscal_week] + (dims except 'brand'); share = net_sales / total_sales_in_bucket.
    """
    dims = tuple(d for d in dims if d in df.columns)
    bucket_dims = ["fiscal_week"] + [d for d in dims if d != "brand"]
    if "net_sales" not in df.columns or not bucket_dims:
        return df

    totals = (df.groupby(bucket_dims, dropna=False)["net_sales"]
                .sum()
                .reset_index()
                .rename(columns={"net_sales": "bucket_total_sales"}))
    out = df.merge(totals, on=bucket_dims, how="left")
    out["market_share"] = np.where(out["bucket_total_sales"] > 0,
                                   out["net_sales"] / out["bucket_total_sales"], np.nan)
    return out


def _aggregate(df: pd.DataFrame, dims: Tuple[str, ...]) -> pd.DataFrame:
    """
    Aggregate by fiscal_week + dims, recompute KPIs and market share.
    """
    group_cols = ["fiscal_week"] + [d for d in dims if d in df.columns]
    agg = (df.groupby(group_cols, dropna=False)
             .agg(units=("units", "sum"),
                  net_sales=("net_sales", "sum"))
             .reset_index())
    agg = _derive_kpis(agg)
    agg = _compute_market_share(agg, dims)
    return agg


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------
def _write_parquet_partitioned(df: pd.DataFrame, prefix: str, partition_col: str, compression: str = "snappy"):
    if df.empty:
        log.info("No rows to write for %s", prefix)
        return
    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' missing.")

    fs = _fs_for_path(prefix)
    for part_val, grp in df.groupby(partition_col):
        part_str = str(part_val)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        out_path = f"{prefix}/{partition_col}={part_str}/part-{ts}.parquet"
        table = pa.Table.from_pandas(grp, preserve_index=False)
        with fs.open(out_path, "wb") as f:
            pq.write_table(table, f, compression=compression)
        log.info("Wrote %d rows -> %s", len(grp), out_path)


def _write_badrows_jsonl(bad: pd.DataFrame, prefix: str):
    if bad.empty:
        return
    fs = _fs_for_path(prefix)
    out_path = f"{prefix}/bad-{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
    with fs.open(out_path, "wb") as f:
        for _, row in bad.iterrows():
            rec = {k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v))
                   for k, v in row.to_dict().items()}
            f.write((json.dumps(rec, default=str) + "\n").encode("utf-8"))
    log.warning("Wrote %d bad rows -> %s", len(bad), out_path)


# ---------------------------------------------------------------------------
# End-to-end driver
# ---------------------------------------------------------------------------
def transform(cfg: Config):
    # 1) Read inputs
    internal = _read_any(_glob(cfg.internal_glob)) if cfg.internal_glob else pd.DataFrame()
    circana = _read_any(_glob(cfg.circana_glob)) if cfg.circana_glob else pd.DataFrame()
    nielsen = _read_any(_glob(cfg.nielsen_glob)) if cfg.nielsen_glob else pd.DataFrame()

    if internal.empty and circana.empty and nielsen.empty:
        log.warning("No input rows found across internal/circana/nielsen.")
        return

    # 2) Harmonize schemas & tag sources
    frames = []
    if not internal.empty:
        frames.append(_coerce_and_trim(_rename_apply_aliases(internal, _INTERNAL_ALIASES, "internal")))
    if not circana.empty:
        frames.append(_coerce_and_trim(_rename_apply_aliases(circana, _CIRCANA_ALIASES, "circana")))
    if not nielsen.empty:
        frames.append(_coerce_and_trim(_rename_apply_aliases(nielsen, _NIELSEN_ALIASES, "nielsen")))

    data = pd.concat(frames, ignore_index=True)

    # 3) Add fiscal week if needed
    data = _add_fiscal_week(data, cfg.retail_to_fiscal_week_offset)

    # 4) Join masters
    prod_master = _read_any([cfg.product_master_path]) if cfg.product_master_path else pd.DataFrame()
    cust_master = _read_any([cfg.customer_master_path]) if cfg.customer_master_path else pd.DataFrame()
    data = _join_product_master(data, prod_master) if not prod_master.empty else data
    data = _join_customer_master(data, cust_master) if not cust_master.empty else data

    # 5) Deduplicate
    data = _dedupe(data)

    # 6) Validate & split
    good, bad = _validate_and_split(data, cfg.required_fields)

    # 7) KPIs & market share (row-level share computed after aggregates too)
    good = _derive_kpis(good)
    good = _compute_market_share(good, cfg.aggregate_dims)

    # 8) Aggregates
    agg = _aggregate(good, cfg.aggregate_dims)

    # 9) Curated selection
    keep_cols = [
        "fiscal_week", "retail_week_start",
        "upc", "gtin", "brand", "segment", "category", "packsize",
        "retailer", "banner", "region", "channel",
        "units", "net_sales", "avg_price_per_unit", "promo_flag",
        "market_share",
        "__source", "__source_file",
    ]
    curated = good[[c for c in keep_cols if c in good.columns]]

    # 10) Writes
    _write_parquet_partitioned(curated, cfg.output_prefix, cfg.partition_col, cfg.write_compression)
    _write_parquet_partitioned(agg, f"{cfg.output_prefix}_agg", cfg.partition_col, cfg.write_compression)
    _write_badrows_jsonl(bad, cfg.badrows_prefix)

    log.info("Done. curated=%d, aggregated=%d, bad=%d", len(curated), len(agg), len(bad))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> Config:
    ap = argparse.ArgumentParser(description="Kraft Heinz sales enrichment: internal + Circana/Nielsen to curated Parquet.")
    ap.add_argument("--internal", dest="internal_glob", help="Internal ERP glob (csv/parquet).")
    ap.add_argument("--circana", dest="circana_glob", help="Circana glob (csv/parquet).")
    ap.add_argument("--nielsen", dest="nielsen_glob", help="Nielsen glob (csv/parquet).")
    ap.add_argument("--product_master", required=True, help="Product master path (csv/parquet).")
    ap.add_argument("--customer_master", required=True, help="Customer master path (csv/parquet).")
    ap.add_argument("--output_prefix", required=True, help="Output prefix dir (local or gs://).")
    ap.add_argument("--badrows_prefix", required=True, help="Dead-letter prefix dir (local or gs://).")
    ap.add_argument("--partition_col", default="fiscal_week", help="Partition column (default: fiscal_week).")
    ap.add_argument("--write_compression", default="snappy", choices=["snappy", "gzip", "zstd"])
    ap.add_argument("--aggregate_dims", default="retailer,channel,region,brand",
                    help="Comma-separated dims for aggregation and market share.")
    ap.add_argument("--retail_to_fiscal_week_offset", type=int, default=0,
                    help="Constant offset to map retail week to fiscal week (simple calendars).")
    args = ap.parse_args()

    dims = tuple([d.strip() for d in args.aggregate_dims.split(",") if d.strip()])
    return Config(
        internal_glob=args.internal_glob,
        circana_glob=args.circana_glob,
        nielsen_glob=args.nielsen_glob,
        product_master_path=args.product_master,
        customer_master_path=args.customer_master,
        output_prefix=args.output_prefix.rstrip("/"),
        badrows_prefix=args.badrows_prefix.rstrip("/"),
        partition_col=args.partition_col,
        write_compression=args.write_compression,
        aggregate_dims=dims or ("retailer", "channel", "region", "brand"),
        retail_to_fiscal_week_offset=args.retail_to_fiscal_week_offset,
    )


if __name__ == "__main__":
    cfg = parse_args()
    transform(cfg)
