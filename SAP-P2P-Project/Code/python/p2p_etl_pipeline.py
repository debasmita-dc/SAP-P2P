"""
p2p_etl_pipeline.py
===================
SAP P2P ETL Pipeline — SAP Data Analytics Engineering
KIIT Capstone Project | April 2026

This pipeline processes raw SAP P2P extracts through three layers:
  1. STAGING   — raw CSVs loaded as-is with basic type casting
  2. CLEANSED  — deduplication, null handling, standardisation
  3. ANALYTICS — joined, enriched, KPI-ready tables

Architecture:
  Raw CSVs (data/raw/)
      └─► Staging Layer  (pandas DataFrames / data/staging/)
              └─► Cleansed Layer  (data/cleansed/)
                      └─► Analytics Layer  (data/analytics/)
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR      = "data/raw"
STAGING_DIR  = "data/staging"
CLEANSED_DIR = "data/cleansed"
ANALYTICS_DIR= "data/analytics"

# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1: STAGING — load raw with type coercion
# ─────────────────────────────────────────────────────────────────────────────

def load_staging(raw_dir: str = RAW_DIR) -> dict:
    """Load all raw CSVs into staging DataFrames with correct dtypes."""
    logger.info("LAYER 1: Loading raw data into STAGING...")

    date_cols = {
        "EKKO_EKPO": ["BEDAT", "EINDT"],
        "EBAN":      ["BADAT"],
        "MSEG":      ["BUDAT"],
        "RBKP_RSEG": ["BLDAT", "BUDAT"],
    }

    staging = {}
    for name, cols in date_cols.items():
        path = os.path.join(raw_dir, f"{name}.csv")
        if not os.path.exists(path):
            logger.warning(f"  File not found: {path} — run p2p_data_extractor.py first")
            continue
        df = pd.read_csv(path, parse_dates=cols)
        staging[name] = df
        logger.info(f"  {name}: {len(df):,} rows, {len(df.columns)} cols")

    return staging


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2: CLEANSED — deduplicate, validate, standardise
# ─────────────────────────────────────────────────────────────────────────────

def cleanse_po(df: pd.DataFrame) -> pd.DataFrame:
    """Cleanse PO (EKKO/EKPO) data."""
    logger.info("  Cleansing PO data (EKKO/EKPO)...")
    before = len(df)

    # Drop deleted POs
    df = df[df["LOEKZ"] != "X"].copy()

    # Remove exact duplicates
    df = df.drop_duplicates(subset=["EBELN", "MATNR"])

    # Fill nulls
    df["VENDOR_NAME"] = df["VENDOR_NAME"].fillna("Unknown Vendor")
    df["MATKL"] = df["MATKL"].fillna("General")

    # Ensure positive values
    df = df[(df["MENGE"] > 0) & (df["NETPR"] > 0)]

    # Derived columns
    df["NETWR_RECALC"] = df["MENGE"] * df["NETPR"]
    df["VARIANCE_FROM_ORIGINAL"] = abs(df["NETWR"] - df["NETWR_RECALC"]) / df["NETWR_RECALC"]

    logger.info(f"    {before} -> {len(df)} rows after cleansing PO data")
    return df


def cleanse_gr(df: pd.DataFrame) -> pd.DataFrame:
    """Cleanse GR (MSEG) data."""
    logger.info("  Cleansing GR data (MSEG)...")
    before = len(df)

    df = df.drop_duplicates(subset=["MBLNR", "ZEILE"])
    df = df[df["MENGE"] > 0]
    df["BUDAT"] = pd.to_datetime(df["BUDAT"])

    logger.info(f"    {before} -> {len(df)} rows after cleansing GR data")
    return df


def cleanse_invoices(df: pd.DataFrame) -> pd.DataFrame:
    """Cleanse Invoice (RBKP/RSEG) data. Flag duplicates."""
    logger.info("  Cleansing Invoice data (RBKP/RSEG)...")
    before = len(df)

    # Flag duplicate invoices (same invoice number + vendor)
    df["IS_DUPLICATE"] = df.duplicated(subset=["BELNR", "LIFNR"], keep="first")

    # Remove exact duplicates (keep first)
    df = df.drop_duplicates(subset=["BELNR", "LIFNR"], keep="first")
    df = df[df["MENGE"] > 0]

    logger.info(f"    {before} -> {len(df)} rows. Duplicates removed: {before - len(df)}")
    return df


def cleanse_pr(df: pd.DataFrame) -> pd.DataFrame:
    """Cleanse PR (EBAN) data."""
    logger.info("  Cleansing PR data (EBAN)...")
    df = df.drop_duplicates(subset=["BANFN", "BNFPO"])
    df["BADAT"] = pd.to_datetime(df["BADAT"])
    return df


def build_cleansed_layer(staging: dict, output_dir: str = CLEANSED_DIR) -> dict:
    """Run all cleanse functions and save to cleansed layer."""
    logger.info("LAYER 2: Building CLEANSED layer...")
    os.makedirs(output_dir, exist_ok=True)

    cleansed = {}
    if "EKKO_EKPO" in staging:
        cleansed["po"] = cleanse_po(staging["EKKO_EKPO"])
    if "MSEG" in staging:
        cleansed["gr"] = cleanse_gr(staging["MSEG"])
    if "RBKP_RSEG" in staging:
        cleansed["inv"] = cleanse_invoices(staging["RBKP_RSEG"])
    if "EBAN" in staging:
        cleansed["pr"] = cleanse_pr(staging["EBAN"])

    for name, df in cleansed.items():
        path = os.path.join(output_dir, f"{name}_cleansed.csv")
        df.to_csv(path, index=False)
        logger.info(f"  Saved cleansed: {path}")

    return cleansed


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 3: ANALYTICS — joins, enrichment, KPI tables
# ─────────────────────────────────────────────────────────────────────────────

def build_p2p_fact_table(cleansed: dict) -> pd.DataFrame:
    """
    Build a unified P2P fact table joining PO, GR, Invoice, and PR.
    This is the single source of truth for all downstream KPI queries.
    """
    logger.info("  Building P2P Fact Table (unified join)...")

    po  = cleansed.get("po")
    gr  = cleansed.get("gr")
    inv = cleansed.get("inv")
    pr  = cleansed.get("pr")

    if po is None:
        raise ValueError("PO data missing — cannot build fact table")

    # PO is the anchor
    fact = po[["EBELN", "LIFNR", "VENDOR_NAME", "MATNR", "TXZ01", "MATKL",
               "BEDAT", "EINDT", "MENGE", "NETPR", "NETWR", "EKGRP"]].copy()

    # Join PR (for PR -> PO cycle time)
    if pr is not None:
        pr_min = pr.groupby("EBELN").agg(PR_DATE=("BADAT", "min")).reset_index()
        fact = fact.merge(pr_min, on="EBELN", how="left")
        fact["BEDAT"] = pd.to_datetime(fact["BEDAT"])
        fact["PR_DATE"] = pd.to_datetime(fact["PR_DATE"])
        fact["PR_TO_PO_DAYS"] = (fact["BEDAT"] - fact["PR_DATE"]).dt.days
    else:
        fact["PR_DATE"] = pd.NaT
        fact["PR_TO_PO_DAYS"] = np.nan

    # Join GR (for delivery performance)
    if gr is not None:
        gr_agg = gr.groupby("EBELN").agg(
            GR_DATE=("BUDAT", "min"),
            GR_QTY=("MENGE", "sum"),
            GR_AMOUNT=("DMBTR", "sum"),
        ).reset_index()
        fact = fact.merge(gr_agg, on="EBELN", how="left")
        fact["EINDT"] = pd.to_datetime(fact["EINDT"])
        fact["GR_DATE"] = pd.to_datetime(fact["GR_DATE"])
        fact["DELIVERY_DELAY_DAYS"] = (fact["GR_DATE"] - fact["EINDT"]).dt.days
        fact["ON_TIME_DELIVERY"] = fact["DELIVERY_DELAY_DAYS"] <= 0
        fact["GR_COVERAGE_PCT"] = (fact["GR_QTY"] / fact["MENGE"] * 100).clip(0, 100)
        fact["GR_POSTED"] = ~fact["GR_DATE"].isna()
    else:
        for col in ["GR_DATE", "GR_QTY", "GR_AMOUNT", "DELIVERY_DELAY_DAYS",
                    "ON_TIME_DELIVERY", "GR_COVERAGE_PCT", "GR_POSTED"]:
            fact[col] = np.nan

    # Join Invoice
    if inv is not None:
        inv_agg = inv.groupby("EBELN").agg(
            INV_DATE=("BLDAT", "min"),
            INV_AMOUNT=("DMBTR", "sum"),
            INV_COUNT=("BELNR", "count"),
            BLOCKED_COUNT=("IS_BLOCKED", "sum"),
        ).reset_index()
        fact = fact.merge(inv_agg, on="EBELN", how="left")
        fact["INV_DATE"] = pd.to_datetime(fact["INV_DATE"])
        fact["GR_DATE"]  = pd.to_datetime(fact["GR_DATE"])
        fact["GR_TO_INV_DAYS"] = (fact["INV_DATE"] - fact["GR_DATE"]).dt.days
        fact["PRICE_MATCH"] = abs(fact["INV_AMOUNT"] - fact["NETWR"]) / fact["NETWR"] < 0.05
        fact["INV_POSTED"] = ~fact["INV_DATE"].isna()
        fact["HAS_BLOCKED_INV"] = fact["BLOCKED_COUNT"] > 0
    else:
        for col in ["INV_DATE", "INV_AMOUNT", "INV_COUNT", "GR_TO_INV_DAYS",
                    "PRICE_MATCH", "INV_POSTED", "HAS_BLOCKED_INV"]:
            fact[col] = np.nan

    # P2P Completion Status
    def p2p_status(row):
        if not row.get("GR_POSTED", False):
            return "PO Open — Awaiting GR"
        elif not row.get("INV_POSTED", False):
            return "GR Done — Awaiting Invoice"
        elif row.get("HAS_BLOCKED_INV", False):
            return "Invoice Blocked"
        else:
            return "P2P Complete"

    fact["P2P_STATUS"] = fact.apply(p2p_status, axis=1)

    logger.info(f"  -> Fact table built: {len(fact):,} rows, {len(fact.columns)} columns")
    return fact


def build_vendor_scorecard(fact: pd.DataFrame) -> pd.DataFrame:
    """Aggregate vendor-level KPIs from the fact table."""
    logger.info("  Building Vendor Scorecard...")

    scorecard = fact.groupby(["LIFNR", "VENDOR_NAME"]).agg(
        TOTAL_POS=("EBELN", "count"),
        TOTAL_SPEND_INR=("NETWR", "sum"),
        AVG_PO_VALUE=("NETWR", "mean"),
        ON_TIME_DELIVERY_RATE=("ON_TIME_DELIVERY", lambda x: x.mean() * 100 if x.notna().any() else np.nan),
        AVG_DELIVERY_DELAY=("DELIVERY_DELAY_DAYS", "mean"),
        INVOICE_MATCH_RATE=("PRICE_MATCH", lambda x: x.mean() * 100 if x.notna().any() else np.nan),
        BLOCKED_INVOICE_RATE=("HAS_BLOCKED_INV", lambda x: x.mean() * 100 if x.notna().any() else np.nan),
        AVG_PR_TO_PO_DAYS=("PR_TO_PO_DAYS", "mean"),
    ).reset_index()

    scorecard["TOTAL_SPEND_INR"] = scorecard["TOTAL_SPEND_INR"].round(2)
    scorecard["AVG_PO_VALUE"] = scorecard["AVG_PO_VALUE"].round(2)
    scorecard["ON_TIME_DELIVERY_RATE"] = scorecard["ON_TIME_DELIVERY_RATE"].round(1)
    scorecard["INVOICE_MATCH_RATE"] = scorecard["INVOICE_MATCH_RATE"].round(1)
    scorecard = scorecard.sort_values("TOTAL_SPEND_INR", ascending=False)

    logger.info(f"  -> Vendor scorecard: {len(scorecard)} vendors")
    return scorecard


def build_analytics_layer(cleansed: dict, output_dir: str = ANALYTICS_DIR) -> dict:
    """Build all analytics tables and save."""
    logger.info("LAYER 3: Building ANALYTICS layer...")
    os.makedirs(output_dir, exist_ok=True)

    fact = build_p2p_fact_table(cleansed)
    vendor_scorecard = build_vendor_scorecard(fact)

    # Spend by material group
    spend_by_group = fact.groupby("MATKL").agg(
        TOTAL_SPEND=("NETWR", "sum"),
        PO_COUNT=("EBELN", "count"),
    ).sort_values("TOTAL_SPEND", ascending=False).reset_index()

    analytics = {
        "p2p_fact": fact,
        "vendor_scorecard": vendor_scorecard,
        "spend_by_material_group": spend_by_group,
    }

    for name, df in analytics.items():
        path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"  Saved analytics: {path} ({len(df):,} rows)")

    return analytics


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline():
    """Execute the full ETL pipeline: Raw → Staging → Cleansed → Analytics."""
    logger.info("=" * 60)
    logger.info("SAP P2P ETL PIPELINE STARTED")
    logger.info(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    staging  = load_staging()
    cleansed = build_cleansed_layer(staging)
    analytics= build_analytics_layer(cleansed)

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE — All layers written successfully")
    logger.info("=" * 60)

    # Quick summary
    fact = analytics["p2p_fact"]
    vs   = analytics["vendor_scorecard"]
    logger.info("\n📊 PIPELINE SUMMARY")
    logger.info(f"  Total POs processed : {len(fact):,}")
    logger.info(f"  Total Spend (INR)   : {fact['NETWR'].sum():,.2f}")
    logger.info(f"  GR Posted Rate      : {fact['GR_POSTED'].mean()*100:.1f}%")
    logger.info(f"  Invoice Posted Rate : {fact['INV_POSTED'].mean()*100:.1f}%")
    logger.info(f"  On-Time Delivery    : {fact['ON_TIME_DELIVERY'].mean()*100:.1f}%")
    logger.info(f"  Avg PR→PO Days      : {fact['PR_TO_PO_DAYS'].mean():.1f} days")
    logger.info(f"  Top Vendor by Spend : {vs.iloc[0]['VENDOR_NAME']} (INR {vs.iloc[0]['TOTAL_SPEND_INR']:,.0f})")

    return analytics


if __name__ == "__main__":
    run_pipeline()
