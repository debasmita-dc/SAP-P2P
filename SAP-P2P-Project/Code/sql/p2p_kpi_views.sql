-- ============================================================
-- p2p_kpi_views.sql
-- Reusable KPI Views — SAP Data Analytics Engineering
-- KIIT Capstone Project | April 2026
-- ============================================================

SET search_path TO sap_p2p;

-- ────────────────────────────────────────────────────────────
-- VIEW 1: vw_p2p_fact — Unified P2P fact view
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_p2p_fact AS
SELECT
    h.ebeln                             AS po_number,
    h.bukrs                             AS company_code,
    h.lifnr                             AS vendor_number,
    v.name1                             AS vendor_name,
    h.bedat                             AS po_date,
    h.ekorg                             AS purch_org,
    h.ekgrp                             AS purch_group,
    i.ebelp                             AS po_item,
    i.matnr                             AS material_number,
    i.txz01                             AS material_description,
    m.mtart                             AS material_type,
    i.matkl                             AS material_group,
    i.werks                             AS plant,
    i.menge                             AS po_quantity,
    i.meins                             AS uom,
    i.netpr                             AS unit_price,
    i.netwr                             AS po_value,
    i.eindt                             AS planned_delivery_date,

    -- PR info
    pr.banfn                            AS pr_number,
    pr.badat                            AS pr_date,
    h.bedat - pr.badat                  AS pr_to_po_days,

    -- GR info
    gr.mblnr                            AS gr_document,
    gr.budat                            AS gr_posting_date,
    gr.menge                            AS gr_quantity,
    gr.dmbtr                            AS gr_amount,
    gr.budat - i.eindt                  AS delivery_delay_days,
    CASE WHEN gr.budat <= i.eindt
         THEN TRUE ELSE FALSE END       AS on_time_delivery,

    -- Invoice info
    rb.belnr                            AS invoice_number,
    rb.bldat                            AS invoice_date,
    rb.dmbtr                            AS invoice_amount,
    rb.rbstat                           AS invoice_status,
    rb.budat - gr.budat                 AS gr_to_invoice_days,
    CASE WHEN rb.rbstat = 'B'
         THEN TRUE ELSE FALSE END       AS invoice_blocked,
    CASE WHEN ABS(rb.dmbtr - i.netwr) / NULLIF(i.netwr, 0) <= 0.05
         THEN TRUE ELSE FALSE END       AS price_matched,

    -- P2P status
    CASE
        WHEN gr.mblnr IS NULL           THEN 'PO Open — Awaiting GR'
        WHEN rb.belnr IS NULL           THEN 'GR Done — Awaiting Invoice'
        WHEN rb.rbstat = 'B'            THEN 'Invoice Blocked'
        ELSE                                 'P2P Complete'
    END                                 AS p2p_status

FROM ekko h
JOIN ekpo i         ON h.ebeln = i.ebeln
LEFT JOIN lfa1 v    ON h.lifnr = v.lifnr
LEFT JOIN mara m    ON i.matnr = m.matnr
LEFT JOIN eban pr   ON h.ebeln = pr.ebeln
LEFT JOIN LATERAL (
    SELECT * FROM mseg
    WHERE ebeln = h.ebeln AND bwart = '101'
    ORDER BY budat ASC LIMIT 1
) gr ON TRUE
LEFT JOIN LATERAL (
    SELECT rb2.* FROM rbkp rb2
    JOIN rseg rs2 ON rb2.belnr = rs2.belnr AND rb2.gjahr = rs2.gjahr
    WHERE rs2.ebeln = h.ebeln
    ORDER BY rb2.budat ASC LIMIT 1
) rb ON TRUE
WHERE h.loekz != 'X';


-- ────────────────────────────────────────────────────────────
-- VIEW 2: vw_vendor_kpi — Vendor performance KPIs
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_vendor_kpi AS
SELECT
    vendor_number,
    vendor_name,
    COUNT(DISTINCT po_number)                               AS total_pos,
    ROUND(SUM(po_value)::NUMERIC, 2)                        AS total_spend,
    ROUND(AVG(po_value)::NUMERIC, 2)                        AS avg_po_value,
    ROUND(
        SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                       AS on_time_delivery_pct,
    ROUND(AVG(delivery_delay_days)::NUMERIC, 1)             AS avg_delay_days,
    ROUND(
        SUM(CASE WHEN price_matched THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(CASE WHEN invoice_number IS NOT NULL THEN 1 END), 0), 1
    )                                                       AS invoice_match_pct,
    ROUND(
        SUM(CASE WHEN invoice_blocked THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(CASE WHEN invoice_number IS NOT NULL THEN 1 END), 0), 1
    )                                                       AS blocked_invoice_pct,
    ROUND(AVG(pr_to_po_days)::NUMERIC, 1)                   AS avg_pr_to_po_days,
    ROUND(AVG(gr_to_invoice_days)::NUMERIC, 1)              AS avg_gr_to_inv_days
FROM vw_p2p_fact
GROUP BY vendor_number, vendor_name
ORDER BY total_spend DESC;


-- ────────────────────────────────────────────────────────────
-- VIEW 3: vw_monthly_kpi — Monthly procurement KPIs
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_monthly_kpi AS
SELECT
    TO_CHAR(po_date, 'YYYY-MM')         AS year_month,
    COUNT(DISTINCT po_number)           AS po_count,
    ROUND(SUM(po_value)::NUMERIC, 2)    AS total_spend,
    ROUND(AVG(po_value)::NUMERIC, 2)    AS avg_po_value,
    COUNT(DISTINCT vendor_number)       AS unique_vendors,
    ROUND(AVG(pr_to_po_days)::NUMERIC, 1)  AS avg_pr_to_po_days,
    ROUND(
        SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(CASE WHEN gr_posting_date IS NOT NULL THEN 1 END), 0), 1
    )                                   AS on_time_delivery_pct,
    ROUND(
        SUM(CASE WHEN price_matched THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(CASE WHEN invoice_number IS NOT NULL THEN 1 END), 0), 1
    )                                   AS invoice_match_pct,
    SUM(CASE WHEN p2p_status = 'P2P Complete' THEN 1 ELSE 0 END)   AS completed_cycles,
    SUM(CASE WHEN p2p_status = 'PO Open — Awaiting GR' THEN 1 ELSE 0 END) AS open_pos,
    SUM(CASE WHEN p2p_status = 'Invoice Blocked' THEN 1 ELSE 0 END) AS blocked_invoices
FROM vw_p2p_fact
GROUP BY TO_CHAR(po_date, 'YYYY-MM')
ORDER BY year_month;


-- ────────────────────────────────────────────────────────────
-- VIEW 4: vw_spend_analysis — Spend by material group
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_spend_analysis AS
SELECT
    material_group,
    material_type,
    COUNT(DISTINCT po_number)           AS po_count,
    COUNT(DISTINCT material_number)     AS unique_materials,
    COUNT(DISTINCT vendor_number)       AS unique_vendors,
    ROUND(SUM(po_value)::NUMERIC, 2)    AS total_spend,
    ROUND(AVG(po_value)::NUMERIC, 2)    AS avg_po_value,
    ROUND(
        SUM(po_value) * 100.0 / SUM(SUM(po_value)) OVER (), 2
    )                                   AS spend_pct
FROM vw_p2p_fact
GROUP BY material_group, material_type
ORDER BY total_spend DESC;
