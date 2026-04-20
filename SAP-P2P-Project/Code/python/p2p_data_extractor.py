"""
p2p_data_extractor.py
=====================
SAP P2P Data Extractor — SAP Data Analytics Engineering
KIIT Capstone Project | April 2026

This module simulates extraction of SAP P2P transactional data from key SAP tables
(EKKO, EKPO, EBAN, MSEG, RBKP) using either:
  - pyrfc (SAP RFC/BAPI calls) in a live SAP environment
  - Simulated/synthetic data generation for analytics development & testing

In production, replace the simulate_* functions with actual pyrfc BAPI calls.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SAP_TABLES = {
    "EKKO": "Purchase Order Header",
    "EKPO": "Purchase Order Item",
    "EBAN": "Purchase Requisition",
    "MSEG": "Material Document Segment (GR)",
    "RBKP": "Invoice Header",
    "RSEG": "Invoice Item",
    "LFA1": "Vendor Master",
    "MARA": "Material Master",
}

OUTPUT_DIR = "data/raw"
COMPANY_CODE = "KIIT"
PLANT = "KT01"
PURCH_ORG = "KP01"

VENDORS = [
    ("V001", "Tata Steel Ltd", "IN01", "NT30"),
    ("V002", "Larsen & Toubro", "IN01", "NT45"),
    ("V003", "Mahindra Logistics", "IN01", "NT30"),
    ("V004", "Reliance Industries", "IN01", "NT60"),
    ("V005", "Infosys BPM", "IN01", "NT30"),
    ("V006", "Wipro Infrastructure", "IN01", "NT45"),
    ("V007", "BHEL Supplies", "IN01", "NT30"),
    ("V008", "HCL Technologies", "IN01", "NT30"),
]

MATERIALS = [
    ("MAT001", "Steel Sheets 5mm", "ROH", "KG", "Raw Materials"),
    ("MAT002", "Copper Wire 2.5sqmm", "ROH", "M", "Electrical"),
    ("MAT003", "Hydraulic Pump Assembly", "HALB", "EA", "Mechanical"),
    ("MAT004", "Office Stationery Pack", "FERT", "EA", "Office Supplies"),
    ("MAT005", "Network Switch 24-Port", "FERT", "EA", "IT Hardware"),
    ("MAT006", "Industrial Lubricant 5L", "ROH", "L", "Consumables"),
    ("MAT007", "Safety Helmets", "FERT", "EA", "Safety Equipment"),
    ("MAT008", "PVC Pipes 50mm", "ROH", "M", "Plumbing"),
]

# ─────────────────────────────────────────────────────────────────────────────
# SIMULATION FUNCTIONS (replace with pyrfc calls in production)
# ─────────────────────────────────────────────────────────────────────────────

def simulate_purchase_orders(n: int = 200) -> pd.DataFrame:
    """
    Simulates EKKO (PO Header) + EKPO (PO Item) combined extract.
    In SAP production: use BAPI_PO_GETDETAIL or SE16 RFC on EKKO/EKPO.
    """
    logger.info(f"Extracting {n} Purchase Orders (EKKO/EKPO)...")
    records = []
    base_date = datetime(2025, 1, 1)

    for i in range(1, n + 1):
        vendor = random.choice(VENDORS)
        material = random.choice(MATERIALS)
        po_date = base_date + timedelta(days=random.randint(0, 470))
        delivery_date = po_date + timedelta(days=random.randint(7, 30))
        quantity = round(random.uniform(10, 500), 2)
        unit_price = round(random.uniform(50, 5000), 2)
        net_value = round(quantity * unit_price, 2)

        records.append({
            "EBELN": f"45{str(i).zfill(8)}",          # PO Number
            "BUKRS": COMPANY_CODE,                      # Company Code
            "BSART": "NB",                              # PO Type: Standard
            "LIFNR": vendor[0],                         # Vendor Number
            "VENDOR_NAME": vendor[1],                   # Vendor Name
            "EKORG": PURCH_ORG,                         # Purchasing Org
            "EKGRP": "K01",                             # Purchasing Group
            "BEDAT": po_date.strftime("%Y-%m-%d"),      # PO Date
            "WERKS": PLANT,                             # Plant
            "MATNR": material[0],                       # Material Number
            "TXZ01": material[1],                       # Material Description
            "MATKL": material[4],                       # Material Group
            "MENGE": quantity,                          # PO Quantity
            "MEINS": material[3],                       # Unit of Measure
            "NETPR": unit_price,                        # Net Price
            "NETWR": net_value,                         # Net Value
            "EINDT": delivery_date.strftime("%Y-%m-%d"),# Delivery Date
            "ELIKZ": random.choice(["", "", "", "X"]), # Delivery Complete Flag
            "REPOS": random.choice(["", "", "X"]),     # Invoice Receipt Flag
            "LOEKZ": "",                                # Deletion Indicator
        })

    df = pd.DataFrame(records)
    logger.info(f"  -> {len(df)} PO records extracted. Total spend: INR {df['NETWR'].sum():,.2f}")
    return df


def simulate_goods_receipts(po_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates MSEG (Material Document) GR records against POs.
    Movement Type 101 = GR against Purchase Order.
    In SAP production: RFC on MSEG WHERE BEWTP = 'E' AND BWART = '101'.
    """
    logger.info("Extracting Goods Receipts (MSEG - Movement Type 101)...")
    records = []

    # ~85% of POs get a GR
    gr_pos = po_df.sample(frac=0.85).copy()

    for _, po in gr_pos.iterrows():
        po_date = datetime.strptime(po["BEDAT"], "%Y-%m-%d")
        delivery_date = datetime.strptime(po["EINDT"], "%Y-%m-%d")
        # GR happens on or after delivery date, sometimes late
        days_late = random.choices([0, 0, 0, random.randint(1, 10)], weights=[6, 2, 1, 1])[0]
        gr_date = delivery_date + timedelta(days=days_late)

        # Partial or full GR
        gr_qty = po["MENGE"] if random.random() > 0.15 else round(po["MENGE"] * random.uniform(0.5, 0.9), 2)

        records.append({
            "MBLNR": f"50{str(len(records)+1).zfill(8)}",  # Material Doc Number
            "MJAHR": gr_date.year,
            "ZEILE": "0001",
            "BWART": "101",                                   # Movement Type: GR vs PO
            "EBELN": po["EBELN"],                             # PO Number reference
            "MATNR": po["MATNR"],
            "WERKS": po["WERKS"],
            "LGORT": "KS01",                                  # Storage Location
            "MENGE": gr_qty,
            "MEINS": po["MEINS"],
            "BUDAT": gr_date.strftime("%Y-%m-%d"),            # Posting Date
            "LIFNR": po["LIFNR"],
            "DMBTR": round(gr_qty * po["NETPR"], 2),          # Amount in local currency
            "WAERS": "INR",
            "ON_TIME": days_late == 0,
        })

    df = pd.DataFrame(records)
    logger.info(f"  -> {len(df)} GR records. On-time delivery rate: {df['ON_TIME'].mean()*100:.1f}%")
    return df


def simulate_invoices(gr_df: pd.DataFrame, po_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates RBKP/RSEG (Invoice Documents from MIRO).
    In SAP production: RFC on RBKP WHERE RBSTAT IN ('A','B') and join RSEG.
    """
    logger.info("Extracting Invoice Documents (RBKP/RSEG)...")
    records = []

    # ~90% of GRs have a corresponding invoice
    inv_grs = gr_df.sample(frac=0.90).copy()
    po_lookup = po_df.set_index("EBELN")

    for _, gr in inv_grs.iterrows():
        gr_date = datetime.strptime(gr["BUDAT"], "%Y-%m-%d")
        inv_date = gr_date + timedelta(days=random.randint(1, 14))

        # Price variance: 95% match exactly, 5% have discrepancy
        po_price = po_lookup.loc[gr["EBELN"], "NETPR"] if gr["EBELN"] in po_lookup.index else 100
        price_variance_pct = random.choices(
            [0, 0, 0, random.uniform(-0.10, 0.10)], weights=[7, 2, 0.5, 0.5]
        )[0]
        inv_unit_price = round(po_price * (1 + price_variance_pct), 2)
        inv_amount = round(gr["MENGE"] * inv_unit_price, 2)

        is_blocked = abs(price_variance_pct) > 0.05
        # Duplicate invoice simulation (2% of invoices)
        inv_number = f"51{str(len(records)+1).zfill(8)}" if random.random() > 0.02 else f"51{str(len(records)).zfill(8)}"

        records.append({
            "BELNR": inv_number,                              # Invoice Doc Number
            "BUKRS": COMPANY_CODE,
            "GJAHR": inv_date.year,
            "BLDAT": inv_date.strftime("%Y-%m-%d"),           # Invoice Date
            "BUDAT": inv_date.strftime("%Y-%m-%d"),           # Posting Date
            "LIFNR": gr["LIFNR"],
            "EBELN": gr["EBELN"],                             # PO Reference
            "MBLNR": gr["MBLNR"],                             # GR Reference
            "MATNR": gr["MATNR"],
            "MENGE": gr["MENGE"],
            "DMBTR": inv_amount,                              # Invoice Amount
            "WAERS": "INR",
            "RBSTAT": "B" if is_blocked else "A",            # A=Posted, B=Blocked
            "PRICE_VARIANCE_PCT": round(price_variance_pct * 100, 2),
            "IS_BLOCKED": is_blocked,
        })

    df = pd.DataFrame(records)
    blocked_rate = df["IS_BLOCKED"].mean() * 100
    duplicate_count = df.duplicated(subset=["BELNR", "LIFNR"]).sum()
    logger.info(f"  -> {len(df)} invoices. Blocked: {blocked_rate:.1f}%. Potential duplicates: {duplicate_count}")
    return df


def simulate_purchase_requisitions(po_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates EBAN (Purchase Requisition) table.
    PRs are created before POs — used for cycle time calculation.
    """
    logger.info("Extracting Purchase Requisitions (EBAN)...")
    records = []

    for _, po in po_df.iterrows():
        po_date = datetime.strptime(po["BEDAT"], "%Y-%m-%d")
        # PR is created 1–10 days before PO
        pr_date = po_date - timedelta(days=random.randint(1, 10))

        records.append({
            "BANFN": f"10{str(len(records)+1).zfill(8)}",   # PR Number
            "BNFPO": "00010",
            "MATNR": po["MATNR"],
            "TXZ01": po["TXZ01"],
            "MENGE": po["MENGE"],
            "MEINS": po["MEINS"],
            "BADAT": pr_date.strftime("%Y-%m-%d"),           # PR Creation Date
            "EBELN": po["EBELN"],                            # Converted to PO
            "WERKS": po["WERKS"],
            "EKGRP": po["EKGRP"],
            "LIFNR": po["LIFNR"],
            "AFNAM": f"USER{random.randint(1,10):02d}",     # Requestor
        })

    df = pd.DataFrame(records)
    logger.info(f"  -> {len(df)} PR records extracted.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACTION RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def extract_all(output_dir: str = OUTPUT_DIR, n_pos: int = 200) -> dict:
    """
    Run full P2P data extraction and save raw CSVs to output directory.
    Returns a dict of DataFrames for downstream ETL.
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info("=" * 60)
    logger.info("SAP P2P DATA EXTRACTION STARTED")
    logger.info(f"Company Code: {COMPANY_CODE} | Plant: {PLANT}")
    logger.info("=" * 60)

    po_df   = simulate_purchase_orders(n_pos)
    pr_df   = simulate_purchase_requisitions(po_df)
    gr_df   = simulate_goods_receipts(po_df)
    inv_df  = simulate_invoices(gr_df, po_df)

    datasets = {
        "EKKO_EKPO": po_df,
        "EBAN":      pr_df,
        "MSEG":      gr_df,
        "RBKP_RSEG": inv_df,
    }

    for name, df in datasets.items():
        path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"  Saved: {path} ({len(df)} rows)")

    logger.info("=" * 60)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 60)
    return datasets


if __name__ == "__main__":
    extract_all()
