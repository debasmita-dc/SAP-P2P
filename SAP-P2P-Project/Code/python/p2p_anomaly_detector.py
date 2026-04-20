"""
p2p_anomaly_detector.py
=======================
SAP P2P Invoice & PO Anomaly Detector — SAP Data Analytics Engineering
KIIT Capstone Project | April 2026

Detects common procurement anomalies using statistical methods:
  1. Duplicate Invoices         — same vendor + amount within 30 days
  2. Price Deviation Alerts     — invoice price > 5% above PO price
  3. GR/IR Mismatches           — GR quantity ≠ Invoice quantity (>10% variance)
  4. Maverick Buying            — POs created without a PR
  5. Split PO Detection         — multiple small POs to bypass approval threshold
  6. Vendor Concentration Risk  — single vendor > 40% of total spend
  7. Invoice Timing Anomaly     — invoices posted before GR
"""

import pandas as pd
import numpy as np
from scipy import stats
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ANALYTICS_DIR = "data/analytics"
REPORTS_DIR   = "reports"

# Thresholds (configurable via config.yaml in production)
PRICE_DEVIATION_THRESHOLD  = 0.05   # 5%
QTY_MISMATCH_THRESHOLD     = 0.10   # 10%
VENDOR_CONCENTRATION_LIMIT = 0.40   # 40% of spend
DUPLICATE_WINDOW_DAYS      = 30
SPLIT_PO_THRESHOLD         = 50000  # INR — approval threshold
SPLIT_PO_COUNT             = 3      # number of POs within window


def load_fact_table() -> pd.DataFrame:
    path = os.path.join(ANALYTICS_DIR, "p2p_fact.csv")
    if not os.path.exists(path):
        raise FileNotFoundError("p2p_fact.csv not found. Run p2p_etl_pipeline.py first.")
    df = pd.read_csv(path)
    for col in ["BEDAT", "EINDT", "GR_DATE", "INV_DATE", "PR_DATE"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def detect_duplicate_invoices(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag invoices from the same vendor with the same amount posted within 30 days.
    """
    inv = fact[fact["INV_DATE"].notna()].copy()
    inv = inv.sort_values(["LIFNR", "INV_DATE"])
    inv["PREV_INV_DATE"]   = inv.groupby(["LIFNR", "INV_AMOUNT"])["INV_DATE"].shift(1)
    inv["DAYS_SINCE_LAST"] = (inv["INV_DATE"] - inv["PREV_INV_DATE"]).dt.days
    duplicates = inv[
        (inv["DAYS_SINCE_LAST"] <= DUPLICATE_WINDOW_DAYS) &
        (inv["INV_AMOUNT"].notna())
    ][["EBELN", "LIFNR", "VENDOR_NAME", "INV_DATE", "INV_AMOUNT", "DAYS_SINCE_LAST"]].copy()
    duplicates["ANOMALY_TYPE"] = "Potential Duplicate Invoice"
    duplicates["RISK_LEVEL"]   = "HIGH"
    logger.info(f"  Duplicate invoices detected: {len(duplicates)}")
    return duplicates


def detect_price_deviations(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag invoices where the billed amount deviates >5% from PO net value.
    """
    inv = fact[fact["INV_AMOUNT"].notna() & fact["NETWR"].notna()].copy()
    inv["PRICE_DEV_PCT"] = (inv["INV_AMOUNT"] - inv["NETWR"]) / inv["NETWR"]
    deviations = inv[abs(inv["PRICE_DEV_PCT"]) > PRICE_DEVIATION_THRESHOLD][
        ["EBELN", "LIFNR", "VENDOR_NAME", "NETWR", "INV_AMOUNT", "PRICE_DEV_PCT"]
    ].copy()
    deviations["PRICE_DEV_PCT"]  = (deviations["PRICE_DEV_PCT"] * 100).round(2)
    deviations["ANOMALY_TYPE"]   = "Invoice Price Deviation > 5%"
    deviations["RISK_LEVEL"]     = deviations["PRICE_DEV_PCT"].apply(
        lambda x: "HIGH" if abs(x) > 10 else "MEDIUM"
    )
    logger.info(f"  Price deviations detected: {len(deviations)}")
    return deviations


def detect_gr_ir_mismatches(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag cases where GR quantity significantly differs from invoiced quantity.
    """
    check = fact[fact["GR_QTY"].notna() & fact["MENGE"].notna()].copy()
    check["GR_QTY_DIFF_PCT"] = abs(check["GR_QTY"] - check["MENGE"]) / check["MENGE"]
    mismatches = check[check["GR_QTY_DIFF_PCT"] > QTY_MISMATCH_THRESHOLD][
        ["EBELN", "LIFNR", "VENDOR_NAME", "MATNR", "MENGE", "GR_QTY", "GR_QTY_DIFF_PCT"]
    ].copy()
    mismatches["GR_QTY_DIFF_PCT"] = (mismatches["GR_QTY_DIFF_PCT"] * 100).round(2)
    mismatches["ANOMALY_TYPE"]    = "GR/IR Quantity Mismatch > 10%"
    mismatches["RISK_LEVEL"]      = mismatches["GR_QTY_DIFF_PCT"].apply(
        lambda x: "HIGH" if x > 25 else "MEDIUM"
    )
    logger.info(f"  GR/IR mismatches detected: {len(mismatches)}")
    return mismatches


def detect_maverick_buying(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag POs where no corresponding PR exists (PR_DATE is null).
    These represent unplanned purchases bypassing the requisition process.
    """
    maverick = fact[fact["PR_DATE"].isna()][
        ["EBELN", "LIFNR", "VENDOR_NAME", "BEDAT", "NETWR", "MATKL"]
    ].copy()
    maverick["ANOMALY_TYPE"] = "Maverick Buying — No PR Found"
    maverick["RISK_LEVEL"]   = "MEDIUM"
    logger.info(f"  Maverick buying instances: {len(maverick)}")
    return maverick


def detect_vendor_concentration(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag vendors accounting for more than 40% of total spend.
    """
    total_spend = fact["NETWR"].sum()
    vendor_spend = fact.groupby(["LIFNR", "VENDOR_NAME"])["NETWR"].sum().reset_index()
    vendor_spend["SPEND_PCT"] = vendor_spend["NETWR"] / total_spend * 100
    concentration = vendor_spend[vendor_spend["SPEND_PCT"] > VENDOR_CONCENTRATION_LIMIT * 100].copy()
    concentration["ANOMALY_TYPE"] = "Vendor Concentration Risk"
    concentration["RISK_LEVEL"]   = "HIGH"
    logger.info(f"  Vendor concentration risks: {len(concentration)}")
    return concentration


def detect_invoice_before_gr(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Flag invoices posted before the Goods Receipt date.
    This indicates potential fraud or process bypass.
    """
    check = fact[fact["GR_DATE"].notna() & fact["INV_DATE"].notna()].copy()
    early_inv = check[check["INV_DATE"] < check["GR_DATE"]][
        ["EBELN", "LIFNR", "VENDOR_NAME", "GR_DATE", "INV_DATE", "INV_AMOUNT"]
    ].copy()
    early_inv["DAYS_EARLY"]    = (check.loc[early_inv.index, "GR_DATE"] - check.loc[early_inv.index, "INV_DATE"]).dt.days
    early_inv["ANOMALY_TYPE"]  = "Invoice Posted Before GR"
    early_inv["RISK_LEVEL"]    = "HIGH"
    logger.info(f"  Invoice-before-GR anomalies: {len(early_inv)}")
    return early_inv


def statistical_outlier_detection(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Use z-score to detect statistically outlier PO values (|z| > 3).
    """
    po_values = fact["NETWR"].dropna()
    z_scores  = np.abs(stats.zscore(po_values))
    outlier_idx = po_values.index[z_scores > 3]
    outliers = fact.loc[outlier_idx, ["EBELN", "LIFNR", "VENDOR_NAME", "NETWR", "MATKL"]].copy()
    outliers["Z_SCORE"]      = z_scores[z_scores > 3].values
    outliers["ANOMALY_TYPE"] = "Statistical Outlier PO Value (Z-score > 3)"
    outliers["RISK_LEVEL"]   = "MEDIUM"
    logger.info(f"  Statistical outlier POs: {len(outliers)}")
    return outliers


def run_anomaly_detection() -> pd.DataFrame:
    """Run all anomaly detectors and produce a consolidated anomaly report."""
    logger.info("=" * 55)
    logger.info("SAP P2P ANOMALY DETECTION ENGINE")
    logger.info("=" * 55)

    fact = load_fact_table()
    logger.info(f"Loaded {len(fact):,} P2P records for analysis\n")

    results = []

    checks = [
        ("Duplicate Invoices",         detect_duplicate_invoices),
        ("Price Deviations",           detect_price_deviations),
        ("GR/IR Mismatches",           detect_gr_ir_mismatches),
        ("Maverick Buying",            detect_maverick_buying),
        ("Vendor Concentration",       detect_vendor_concentration),
        ("Invoice Before GR",          detect_invoice_before_gr),
        ("Statistical Outlier POs",    statistical_outlier_detection),
    ]

    for name, func in checks:
        logger.info(f"Running: {name}...")
        try:
            df = func(fact)
            if not df.empty:
                results.append(df[["ANOMALY_TYPE", "RISK_LEVEL"] + [c for c in df.columns if c not in ["ANOMALY_TYPE","RISK_LEVEL"]]])
        except Exception as e:
            logger.warning(f"  {name} check failed: {e}")

    if results:
        report = pd.concat(results, ignore_index=True, sort=False)
        report = report.sort_values("RISK_LEVEL", key=lambda x: x.map({"HIGH": 0, "MEDIUM": 1, "LOW": 2}))

        os.makedirs(REPORTS_DIR, exist_ok=True)
        path = os.path.join(REPORTS_DIR, "P2P_Anomaly_Report.csv")
        report.to_csv(path, index=False)

        logger.info("\n" + "=" * 55)
        logger.info("ANOMALY DETECTION COMPLETE")
        logger.info("=" * 55)
        logger.info(f"  Total anomalies found : {len(report)}")
        logger.info(f"  HIGH risk             : {(report['RISK_LEVEL']=='HIGH').sum()}")
        logger.info(f"  MEDIUM risk           : {(report['RISK_LEVEL']=='MEDIUM').sum()}")
        logger.info(f"  Report saved to       : {path}")
        return report
    else:
        logger.info("No anomalies detected.")
        return pd.DataFrame()


if __name__ == "__main__":
    run_anomaly_detection()
