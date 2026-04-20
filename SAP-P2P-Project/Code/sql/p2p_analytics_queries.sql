-- ============================================================
-- p2p_analytics_queries.sql
-- SAP P2P Analytics Queries — SAP Data Analytics Engineering
-- KIIT Capstone Project | April 2026
-- ============================================================

SET search_path TO sap_p2p;

-- ────────────────────────────────────────────────────────────
-- 1. TOTAL SPEND BY VENDOR (Top 10)
-- ────────────────────────────────────────────────────────────
SELECT
    h.lifnr                          AS vendor_number,
    v.name1                          AS vendor_name,
    COUNT(DISTINCT h.ebeln)          AS total_pos,
    SUM(i.netwr)                     AS total_spend_inr,
    ROUND(AVG(i.netwr), 2)           AS avg_po_value,
    ROUND(SUM(i.netwr) * 100.0 /
        SUM(SUM(i.netwr)) OVER (), 2) AS spend_pct
FROM ekko h
JOIN ekpo i  ON h.ebeln = i.ebeln
JOIN lfa1 v  ON h.lifnr = v.lifnr
WHERE h.loekz != 'X'
  AND i.loekz != 'X'
GROUP BY h.lifnr, v.name1
ORDER BY total_spend_inr DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 2. P2P CYCLE TIME ANALYSIS (PR → PO → GR → Invoice)
-- ────────────────────────────────────────────────────────────
SELECT
    h.ebeln                                              AS po_number,
    h.lifnr                                              AS vendor,
    pr.badat                                             AS pr_date,
    h.bedat                                              AS po_date,
    MIN(g.budat)                                         AS first_gr_date,
    MIN(r.budat)                                         AS first_inv_date,

    -- Cycle time components (days)
    h.bedat - pr.badat                                   AS pr_to_po_days,
    MIN(g.budat) - h.bedat                               AS po_to_gr_days,
    MIN(r.budat) - MIN(g.budat)                          AS gr_to_inv_days,
    MIN(r.budat) - pr.badat                              AS total_p2p_days

FROM ekko h
JOIN ekpo i         ON h.ebeln = i.ebeln
LEFT JOIN eban pr   ON h.ebeln = pr.ebeln
LEFT JOIN mseg g    ON h.ebeln = g.ebeln  AND g.bwart = '101'
LEFT JOIN rseg rs   ON h.ebeln = rs.ebeln
LEFT JOIN rbkp r    ON rs.belnr = r.belnr AND rs.gjahr = r.gjahr
WHERE h.loekz != 'X'
GROUP BY h.ebeln, h.lifnr, pr.badat, h.bedat
HAVING MIN(g.budat) IS NOT NULL
ORDER BY total_p2p_days DESC NULLS LAST;


-- ────────────────────────────────────────────────────────────
-- 3. VENDOR ON-TIME DELIVERY SCORECARD
-- ────────────────────────────────────────────────────────────
SELECT
    h.lifnr                                              AS vendor_number,
    v.name1                                              AS vendor_name,
    COUNT(DISTINCT h.ebeln)                              AS total_pos,
    SUM(CASE WHEN MIN(g.budat) <= i.eindt THEN 1 ELSE 0 END) AS on_time_count,
    ROUND(
        SUM(CASE WHEN MIN(g.budat) <= i.eindt THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(DISTINCT h.ebeln), 0), 1
    )                                                    AS on_time_pct,
    ROUND(AVG(MIN(g.budat) - i.eindt), 1)               AS avg_delay_days
FROM ekko h
JOIN ekpo i    ON h.ebeln = i.ebeln
JOIN lfa1 v    ON h.lifnr = v.lifnr
LEFT JOIN mseg g ON h.ebeln = g.ebeln AND g.bwart = '101'
WHERE h.loekz != 'X'
GROUP BY h.lifnr, v.name1, i.ebeln, i.eindt
ORDER BY on_time_pct DESC;


-- ────────────────────────────────────────────────────────────
-- 4. OPEN PO AGING REPORT (GR not yet done)
-- ────────────────────────────────────────────────────────────
SELECT
    h.ebeln                          AS po_number,
    h.lifnr                          AS vendor_number,
    v.name1                          AS vendor_name,
    i.matnr                          AS material,
    i.txz01                          AS description,
    i.eindt                          AS expected_delivery,
    CURRENT_DATE - i.eindt           AS days_overdue,
    i.menge                          AS ordered_qty,
    i.netwr                          AS po_value_inr,
    CASE
        WHEN CURRENT_DATE - i.eindt > 60 THEN 'CRITICAL (>60 days)'
        WHEN CURRENT_DATE - i.eindt > 30 THEN 'HIGH (31-60 days)'
        WHEN CURRENT_DATE - i.eindt > 0  THEN 'MEDIUM (1-30 days)'
        ELSE 'Not Yet Due'
    END                              AS aging_bucket
FROM ekko h
JOIN ekpo i    ON h.ebeln = i.ebeln
JOIN lfa1 v    ON h.lifnr = v.lifnr
WHERE h.loekz != 'X'
  AND i.loekz != 'X'
  AND i.elikz != 'X'       -- Not delivery complete
  AND NOT EXISTS (
      SELECT 1 FROM mseg g
      WHERE g.ebeln = h.ebeln AND g.bwart = '101'
  )
ORDER BY days_overdue DESC NULLS LAST;


-- ────────────────────────────────────────────────────────────
-- 5. BLOCKED INVOICE ANALYSIS
-- ────────────────────────────────────────────────────────────
SELECT
    r.belnr                          AS invoice_number,
    r.lifnr                          AS vendor_number,
    v.name1                          AS vendor_name,
    r.bldat                          AS invoice_date,
    r.dmbtr                          AS invoice_amount,
    i.netwr                          AS po_value,
    ROUND((r.dmbtr - i.netwr) / NULLIF(i.netwr, 0) * 100, 2)
                                     AS price_variance_pct,
    CURRENT_DATE - r.budat           AS days_blocked,
    CASE
        WHEN ABS(r.dmbtr - i.netwr) / NULLIF(i.netwr, 0) > 0.10
            THEN 'Price > 10% Variance'
        WHEN ABS(r.dmbtr - i.netwr) / NULLIF(i.netwr, 0) > 0.05
            THEN 'Price > 5% Variance'
        ELSE 'Quantity Mismatch / Other'
    END                              AS block_reason
FROM rbkp r
JOIN lfa1 v    ON r.lifnr = v.lifnr
JOIN rseg rs   ON r.belnr = rs.belnr AND r.gjahr = rs.gjahr
JOIN ekpo i    ON rs.ebeln = i.ebeln AND rs.ebelp = i.ebelp
WHERE r.rbstat = 'B'       -- Blocked
ORDER BY days_blocked DESC;


-- ────────────────────────────────────────────────────────────
-- 6. MONTHLY SPEND TREND
-- ────────────────────────────────────────────────────────────
SELECT
    TO_CHAR(h.bedat, 'YYYY-MM')       AS year_month,
    COUNT(DISTINCT h.ebeln)           AS po_count,
    SUM(i.netwr)                      AS total_spend_inr,
    ROUND(AVG(i.netwr), 2)            AS avg_po_value,
    COUNT(DISTINCT h.lifnr)           AS unique_vendors
FROM ekko h
JOIN ekpo i ON h.ebeln = i.ebeln
WHERE h.loekz != 'X'
  AND h.bedat >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY TO_CHAR(h.bedat, 'YYYY-MM')
ORDER BY year_month;


-- ────────────────────────────────────────────────────────────
-- 7. GR/IR CLEARING BACKLOG (uncleared items > 30 days)
-- ────────────────────────────────────────────────────────────
SELECT
    g.ebeln                          AS po_number,
    h.lifnr                          AS vendor,
    v.name1                          AS vendor_name,
    g.matnr                          AS material,
    g.budat                          AS gr_posting_date,
    g.dmbtr                          AS gr_amount,
    CURRENT_DATE - g.budat           AS days_uncleared,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM rseg rs
            JOIN rbkp r ON rs.belnr = r.belnr AND rs.gjahr = r.gjahr
            WHERE rs.ebeln = g.ebeln AND r.rbstat = 'A'
        ) THEN 'No Invoice Posted'
        ELSE 'Invoice Blocked / Mismatch'
    END                              AS clearing_issue
FROM mseg g
JOIN ekko h    ON g.ebeln = h.ebeln
JOIN lfa1 v    ON h.lifnr = v.lifnr
WHERE g.bwart = '101'
  AND CURRENT_DATE - g.budat > 30
  AND NOT EXISTS (
      SELECT 1 FROM rseg rs
      JOIN rbkp r ON rs.belnr = r.belnr AND rs.gjahr = r.gjahr
      WHERE rs.ebeln = g.ebeln AND r.rbstat = 'A'
  )
ORDER BY days_uncleared DESC;
