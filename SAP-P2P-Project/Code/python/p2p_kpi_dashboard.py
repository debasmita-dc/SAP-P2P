"""
p2p_kpi_dashboard.py
====================
SAP P2P Procurement KPI Dashboard — SAP Data Analytics Engineering
KIIT Capstone Project | April 2026

Generates procurement KPI reports and visualisations from the P2P analytics layer.
KPIs tracked:
  - PO Cycle Time (PR → PO, PO → GR, GR → Invoice)
  - Invoice First-Pass Match Rate
  - Vendor On-Time Delivery Rate
  - Spend Analysis (by vendor, material group, month)
  - Blocked Invoice Rate
  - GR/IR Clearing Backlog
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import os
import warnings
warnings.filterwarnings("ignore")

ANALYTICS_DIR = "data/analytics"
REPORTS_DIR   = "reports"
COLORS = {
    "primary":   "#1F4E79",
    "secondary": "#2E75B6",
    "accent":    "#F4B942",
    "success":   "#2E8B57",
    "danger":    "#C0392B",
    "light":     "#EAF2FB",
    "grey":      "#7F8C8D",
}


def load_analytics() -> dict:
    """Load analytics layer tables."""
    tables = {}
    for name in ["p2p_fact", "vendor_scorecard", "spend_by_material_group"]:
        path = os.path.join(ANALYTICS_DIR, f"{name}.csv")
        if os.path.exists(path):
            tables[name] = pd.read_csv(path, parse_dates=True)
    return tables


def compute_kpis(fact: pd.DataFrame) -> dict:
    """Compute all headline KPIs from the P2P fact table."""
    kpis = {}

    kpis["total_pos"]          = len(fact)
    kpis["total_spend_inr"]    = fact["NETWR"].sum()
    kpis["avg_po_value"]       = fact["NETWR"].mean()

    # Cycle times
    kpis["avg_pr_to_po_days"]  = fact["PR_TO_PO_DAYS"].dropna().mean()
    kpis["median_pr_to_po"]    = fact["PR_TO_PO_DAYS"].dropna().median()
    kpis["avg_gr_to_inv_days"] = fact["GR_TO_INV_DAYS"].dropna().mean() if "GR_TO_INV_DAYS" in fact.columns else np.nan

    # Delivery
    kpis["on_time_delivery_rate"]  = fact["ON_TIME_DELIVERY"].mean() * 100 if "ON_TIME_DELIVERY" in fact.columns else np.nan
    kpis["avg_delivery_delay"]     = fact["DELIVERY_DELAY_DAYS"].dropna().mean() if "DELIVERY_DELAY_DAYS" in fact.columns else np.nan

    # Invoice
    kpis["invoice_match_rate"]     = fact["PRICE_MATCH"].mean() * 100 if "PRICE_MATCH" in fact.columns else np.nan
    kpis["blocked_invoice_rate"]   = fact["HAS_BLOCKED_INV"].mean() * 100 if "HAS_BLOCKED_INV" in fact.columns else np.nan
    kpis["gr_posted_rate"]         = fact["GR_POSTED"].mean() * 100 if "GR_POSTED" in fact.columns else np.nan

    return kpis


def print_kpi_summary(kpis: dict):
    """Print a formatted KPI summary to console."""
    print("\n" + "=" * 55)
    print("   SAP P2P PROCUREMENT KPI DASHBOARD")
    print("   KIIT | SAP Data Analytics Engineering")
    print("=" * 55)

    print(f"\n{'VOLUME & SPEND':─<45}")
    print(f"  Total Purchase Orders     : {kpis['total_pos']:,}")
    print(f"  Total Spend (INR)         : ₹{kpis['total_spend_inr']:>15,.2f}")
    print(f"  Average PO Value (INR)    : ₹{kpis['avg_po_value']:>15,.2f}")

    print(f"\n{'CYCLE TIME':─<45}")
    print(f"  Avg PR → PO (days)        : {kpis['avg_pr_to_po_days']:.1f}")
    print(f"  Median PR → PO (days)     : {kpis['median_pr_to_po']:.1f}")
    print(f"  Avg GR → Invoice (days)   : {kpis['avg_gr_to_inv_days']:.1f}")

    print(f"\n{'DELIVERY PERFORMANCE':─<45}")
    otd = kpis['on_time_delivery_rate']
    otd_status = "✅" if otd >= 90 else "⚠️" if otd >= 75 else "❌"
    print(f"  On-Time Delivery Rate     : {otd:.1f}% {otd_status}")
    print(f"  Avg Delivery Delay (days) : {kpis['avg_delivery_delay']:.1f}")

    print(f"\n{'INVOICE QUALITY':─<45}")
    match = kpis['invoice_match_rate']
    match_status = "✅" if match >= 95 else "⚠️" if match >= 85 else "❌"
    blocked = kpis['blocked_invoice_rate']
    blocked_status = "✅" if blocked <= 5 else "⚠️" if blocked <= 10 else "❌"
    print(f"  Invoice Match Rate        : {match:.1f}% {match_status}  (target ≥95%)")
    print(f"  Blocked Invoice Rate      : {blocked:.1f}% {blocked_status}  (target ≤5%)")
    print(f"  GR Posting Rate           : {kpis['gr_posted_rate']:.1f}%")
    print("=" * 55)


def plot_kpi_dashboard(fact: pd.DataFrame, vendor_sc: pd.DataFrame, spend_grp: pd.DataFrame):
    """Generate a multi-chart KPI dashboard PNG."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    kpis = compute_kpis(fact)

    fig = plt.figure(figsize=(18, 14))
    fig.patch.set_facecolor("#F0F4F8")
    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    # ── Title ────────────────────────────────────────────────────
    fig.suptitle(
        "SAP P2P Procurement Analytics Dashboard\nKIIT | SAP Data Analytics Engineering Capstone Project",
        fontsize=16, fontweight="bold", color=COLORS["primary"], y=0.98
    )

    # ── KPI Cards (top row, 3 cols) ───────────────────────────────
    kpi_cards = [
        ("Total POs", f"{kpis['total_pos']:,}", "#1F4E79"),
        ("Total Spend", f"₹{kpis['total_spend_inr']/1e6:.1f}M", "#2E75B6"),
        ("On-Time Delivery", f"{kpis['on_time_delivery_rate']:.1f}%", "#2E8B57"),
    ]
    for i, (label, value, color) in enumerate(kpi_cards):
        ax = fig.add_subplot(gs[0, i])
        ax.set_facecolor(color)
        ax.set_xticks([]); ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.text(0.5, 0.65, value, ha="center", va="center", fontsize=26,
                fontweight="bold", color="white", transform=ax.transAxes)
        ax.text(0.5, 0.22, label, ha="center", va="center", fontsize=11,
                color="white", alpha=0.9, transform=ax.transAxes)

    # ── Spend by Material Group (bar) ────────────────────────────
    ax2 = fig.add_subplot(gs[1, :2])
    ax2.set_facecolor("white")
    spend_grp_sorted = spend_grp.sort_values("TOTAL_SPEND", ascending=True).tail(6)
    bars = ax2.barh(spend_grp_sorted["MATKL"], spend_grp_sorted["TOTAL_SPEND"] / 1e6,
                    color=COLORS["secondary"], edgecolor="white", height=0.6)
    for bar in bars:
        ax2.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                 f"₹{bar.get_width():.1f}M", va="center", fontsize=9, color=COLORS["primary"])
    ax2.set_xlabel("Total Spend (INR Millions)", fontsize=10)
    ax2.set_title("Spend by Material Group", fontweight="bold", color=COLORS["primary"], pad=8)
    ax2.grid(axis="x", alpha=0.3)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    # ── P2P Status Donut ─────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 2])
    ax3.set_facecolor("white")
    status_counts = fact["P2P_STATUS"].value_counts()
    donut_colors = [COLORS["success"], COLORS["secondary"], COLORS["accent"], COLORS["danger"]][:len(status_counts)]
    wedges, texts, autotexts = ax3.pie(
        status_counts.values, labels=None, autopct="%1.0f%%",
        colors=donut_colors, startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "white", "linewidth": 2},
        pctdistance=0.75
    )
    for at in autotexts:
        at.set_fontsize(8); at.set_color("white"); at.set_fontweight("bold")
    ax3.legend(status_counts.index, loc="lower center", fontsize=7, ncol=2,
               bbox_to_anchor=(0.5, -0.15))
    ax3.set_title("P2P Completion Status", fontweight="bold", color=COLORS["primary"], pad=8)

    # ── Vendor On-Time Delivery ───────────────────────────────────
    ax4 = fig.add_subplot(gs[2, :2])
    ax4.set_facecolor("white")
    vs_sorted = vendor_sc.sort_values("ON_TIME_DELIVERY_RATE", ascending=True)
    colors_bar = [COLORS["success"] if v >= 90 else COLORS["accent"] if v >= 75 else COLORS["danger"]
                  for v in vs_sorted["ON_TIME_DELIVERY_RATE"].fillna(0)]
    ax4.barh(vs_sorted["VENDOR_NAME"], vs_sorted["ON_TIME_DELIVERY_RATE"].fillna(0),
             color=colors_bar, edgecolor="white", height=0.6)
    ax4.axvline(x=90, color=COLORS["primary"], linestyle="--", linewidth=1.5, alpha=0.7, label="Target 90%")
    ax4.set_xlim(0, 115)
    ax4.set_xlabel("On-Time Delivery Rate (%)", fontsize=10)
    ax4.set_title("Vendor On-Time Delivery Rate", fontweight="bold", color=COLORS["primary"], pad=8)
    ax4.legend(fontsize=9)
    ax4.grid(axis="x", alpha=0.3)
    ax4.spines["top"].set_visible(False)
    ax4.spines["right"].set_visible(False)

    # ── Cycle Time KPI Card ───────────────────────────────────────
    ax5 = fig.add_subplot(gs[2, 2])
    ax5.set_facecolor("white")
    ax5.set_xticks([]); ax5.set_yticks([])
    for spine in ax5.spines.values():
        spine.set_color("#DDDDDD")
    metrics = [
        ("PR → PO", f"{kpis['avg_pr_to_po_days']:.1f} days"),
        ("GR → Invoice", f"{kpis['avg_gr_to_inv_days']:.1f} days"),
        ("Invoice Match", f"{kpis['invoice_match_rate']:.1f}%"),
        ("Blocked Inv.", f"{kpis['blocked_invoice_rate']:.1f}%"),
    ]
    ax5.set_title("Key Metrics", fontweight="bold", color=COLORS["primary"], pad=8)
    for j, (label, val) in enumerate(metrics):
        y = 0.82 - j * 0.22
        ax5.text(0.05, y, label, fontsize=10, color=COLORS["grey"], transform=ax5.transAxes)
        ax5.text(0.95, y, val, fontsize=11, fontweight="bold", color=COLORS["primary"],
                 ha="right", transform=ax5.transAxes)
        if j < len(metrics) - 1:
            ax5.axhline(y=y - 0.07, xmin=0.03, xmax=0.97, color="#EEEEEE", linewidth=1,
                        transform=ax5.transAxes)

    path = os.path.join(REPORTS_DIR, "P2P_KPI_Dashboard.png")
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"\n  Dashboard saved: {path}")
    return path


def run_dashboard():
    """Load data, compute KPIs, print summary, generate charts."""
    print("Loading analytics layer...")
    tables = load_analytics()

    if not tables:
        print("No analytics data found. Run p2p_etl_pipeline.py first.")
        return

    fact = tables.get("p2p_fact", pd.DataFrame())
    vs   = tables.get("vendor_scorecard", pd.DataFrame())
    sg   = tables.get("spend_by_material_group", pd.DataFrame())

    kpis = compute_kpis(fact)
    print_kpi_summary(kpis)

    if not fact.empty and not vs.empty and not sg.empty:
        plot_kpi_dashboard(fact, vs, sg)

    return kpis


if __name__ == "__main__":
    run_dashboard()
