# SAP-P2P
# SAP Procure-to-Pay (P2P) — Data Analytics Engineering Project

> **Domain:** SAP Data Analytics Engineering  
> **Module:** SAP MM (Materials Management)  
> **Institution:** KIIT  
> **Submission Deadline:** April 21, 2026

---

## 📌 Project Overview

This project implements the complete **Procure-to-Pay (P2P)** purchasing cycle in SAP MM and extends it with a **Data Analytics Engineering layer** — extracting, transforming, and visualizing procurement data using Python, SQL, and SAP analytics tools.

The project bridges SAP functional knowledge with modern data engineering practices, demonstrating how raw SAP transactional data (PRs, POs, GRs, Invoices) can be turned into actionable procurement insights.

---

## 🗂️ Repository Structure

```
SAP-P2P-Project/
│
├── README.md                          ← This file
│
├── Documentation/
│   └── SAP_P2P_Project_Report.docx   ← Full project report (A4, Arial)
│
├── Process_Flow/
│   └── P2P_Process_Flow.docx         ← Step-by-step P2P process flow document
│
├── Configuration_Document/
│   └── SAP_MM_Configuration_Steps.docx ← SAP MM org structure & config guide
│
├── Screenshots/
│   ├── P2P_Flow_Diagram.png          ← End-to-end P2P flow diagram
│   ├── ME51N_PR.png                  ← Purchase Requisition screen
│   ├── ME21N_PO.png                  ← Purchase Order screen
│   ├── MIGO_GR.png                   ← Goods Receipt (MIGO) screen
│   └── MIRO_Invoice.png              ← Invoice Verification (MIRO) screen
│
└── Code/
    ├── python/
    │   ├── p2p_data_extractor.py     ← Simulates SAP BAPI/RFC data extraction
    │   ├── p2p_etl_pipeline.py       ← ETL pipeline: raw → clean → analytics layer
    │   ├── p2p_kpi_dashboard.py      ← Procurement KPI calculations & reporting
    │   └── p2p_anomaly_detector.py   ← Invoice anomaly detection using statistical methods
    ├── sql/
    │   ├── p2p_schema.sql            ← SAP P2P staging schema (EKKO, EKPO, MSEG, RBKP)
    │   ├── p2p_analytics_queries.sql ← Procurement analytics SQL queries
    │   └── p2p_kpi_views.sql         ← KPI views: cycle time, spend analysis, vendor perf
    └── config/
        └── config.yaml               ← Project configuration (connection params, thresholds)
```

---

## 🔄 P2P Process Flow

```
Need Identified
     │
     ▼
Purchase Requisition (ME51N)
     │
     ▼
RFQ to Vendors (ME41) ──► Quotation Comparison (ME49)
     │
     ▼
Purchase Order (ME21N)
     │
     ▼
Goods Receipt (MIGO / Mvt 101)
     │
     ▼
Invoice Verification (MIRO) ──► 3-Way Match (PO + GR + Invoice)
     │
     ▼
Payment (F110 / F-53)
     │
     ▼
[DATA LAYER] Extract → Transform → Load → Analyse → Dashboard
```

---

## 💻 Code Components (Data Analytics Engineering)

### Python
| File | Purpose |
|------|---------|
| `p2p_data_extractor.py` | Simulates SAP RFC/BAPI calls to extract P2P tables (EKKO, EKPO, MSEG, RBKP) |
| `p2p_etl_pipeline.py` | Full ETL: raw CSV → staging → cleaned → analytics-ready DataFrames |
| `p2p_kpi_dashboard.py` | Calculates KPIs: PO cycle time, invoice match rate, vendor on-time delivery |
| `p2p_anomaly_detector.py` | Detects duplicate invoices, price deviations, and GR/IR mismatches |

### SQL
| File | Purpose |
|------|---------|
| `p2p_schema.sql` | DDL for SAP P2P staging tables mirroring EKKO, EKPO, MSEG, RBKP |
| `p2p_analytics_queries.sql` | Spend by vendor, cycle time analysis, open PO aging, blocked invoices |
| `p2p_kpi_views.sql` | Reusable SQL views for KPI dashboards |

---

## 📊 Key KPIs Tracked

- **PO Cycle Time** — PR creation to PO release (target: ≤ 3 days)
- **Invoice Match Rate** — % invoices matched on first pass (target: ≥ 95%)
- **Vendor On-Time Delivery Rate** — GR date vs. PO delivery date
- **Spend by Vendor / Material Group** — Top 10 vendors by spend
- **Blocked Invoice Rate** — % invoices blocked due to price/qty variance
- **GR/IR Clearing Backlog** — Uncleared GR/IR items older than 30 days

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| ERP | SAP ECC 6.0 / SAP S/4HANA |
| SAP Module | MM (Materials Management), FI (Accounts Payable) |
| Data Extraction | Python (pyrfc / simulated BAPI), CSV exports |
| Data Processing | Python 3.11, pandas, numpy |
| Anomaly Detection | scipy (z-score), IQR method |
| Database / SQL | PostgreSQL / SQLite (staging layer) |
| Visualization | matplotlib, seaborn |
| Configuration | YAML |

---

## 📋 SAP Tables Referenced

| SAP Table | Description |
|-----------|-------------|
| `EKKO` | Purchase Order Header |
| `EKPO` | Purchase Order Item |
| `EBAN` | Purchase Requisition |
| `MSEG` | Material Document Segment (GR) |
| `MKPF` | Material Document Header |
| `RBKP` | Invoice Document Header (MIRO) |
| `RSEG` | Invoice Document Item |
| `LFA1` | Vendor Master |
| `MARA` | Material Master |


