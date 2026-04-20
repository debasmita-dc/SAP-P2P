-- ============================================================
-- p2p_schema.sql
-- SAP P2P Staging Schema — SAP Data Analytics Engineering
-- KIIT Capstone Project | April 2026
--
-- Mirrors key SAP P2P tables in a PostgreSQL/SQLite staging DB.
-- Tables: EKKO, EKPO, EBAN, MSEG, MKPF, RBKP, RSEG, LFA1, MARA
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- SCHEMA
-- ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS sap_p2p;
SET search_path TO sap_p2p;

-- ────────────────────────────────────────────────────────────
-- LFA1 — Vendor Master
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lfa1 (
    lifnr       VARCHAR(10)  NOT NULL,   -- Vendor Number
    name1       VARCHAR(40),             -- Vendor Name
    land1       VARCHAR(3),              -- Country
    ort01       VARCHAR(25),             -- City
    stras       VARCHAR(30),             -- Street
    telf1       VARCHAR(16),             -- Phone
    ktokk       VARCHAR(4),              -- Account Group
    sperr       CHAR(1)      DEFAULT '', -- Central Posting Block
    loevm       CHAR(1)      DEFAULT '', -- Central Deletion Flag
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (lifnr)
);

-- ────────────────────────────────────────────────────────────
-- MARA — Material Master (General)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mara (
    matnr       VARCHAR(18)  NOT NULL,   -- Material Number
    mtart       VARCHAR(4),              -- Material Type (ROH, HALB, FERT)
    matkl       VARCHAR(9),              -- Material Group
    meins       VARCHAR(3),              -- Base Unit of Measure
    mbrsh       CHAR(1),                 -- Industry Sector
    bismt       VARCHAR(18),             -- Old Material Number
    maktx       VARCHAR(40),             -- Material Description
    erdat       DATE,                    -- Creation Date
    PRIMARY KEY (matnr)
);

-- ────────────────────────────────────────────────────────────
-- EBAN — Purchase Requisition
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eban (
    banfn       VARCHAR(10)  NOT NULL,   -- PR Number
    bnfpo       VARCHAR(5)   NOT NULL,   -- PR Item
    matnr       VARCHAR(18),             -- Material
    txz01       VARCHAR(40),             -- Short Text
    menge       DECIMAL(13,3),           -- Requested Quantity
    meins       VARCHAR(3),              -- Unit of Measure
    badat       DATE,                    -- PR Creation Date
    lfdat       DATE,                    -- Desired Delivery Date
    afnam       VARCHAR(12),             -- Requestor
    werks       VARCHAR(4),              -- Plant
    ekgrp       VARCHAR(3),              -- Purchasing Group
    lifnr       VARCHAR(10),             -- Preferred Vendor
    ebeln       VARCHAR(10),             -- Assigned PO Number
    ebelp       VARCHAR(5),              -- Assigned PO Item
    loekz       CHAR(1)      DEFAULT '', -- Deletion Indicator
    PRIMARY KEY (banfn, bnfpo)
);

-- ────────────────────────────────────────────────────────────
-- EKKO — Purchase Order Header
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ekko (
    ebeln       VARCHAR(10)  NOT NULL,   -- PO Number
    bukrs       VARCHAR(4),              -- Company Code
    bstyp       CHAR(1)      DEFAULT 'F',-- Purchasing Document Category
    bsart       VARCHAR(4),              -- PO Type (NB=Standard, FO=Framework)
    lifnr       VARCHAR(10),             -- Vendor Number
    ekorg       VARCHAR(4),              -- Purchasing Organisation
    ekgrp       VARCHAR(3),              -- Purchasing Group
    bedat       DATE,                    -- PO Date
    kdatb       DATE,                    -- Validity Start Date
    kdate       DATE,                    -- Validity End Date
    zterm       VARCHAR(4),              -- Payment Terms
    inco1       VARCHAR(3),              -- Incoterms 1
    inco2       VARCHAR(28),             -- Incoterms 2
    waers       VARCHAR(5),              -- Currency
    wkurs       DECIMAL(9,5),            -- Exchange Rate
    loekz       CHAR(1)      DEFAULT '', -- Deletion Flag
    statu       CHAR(1),                 -- Status
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ebeln),
    FOREIGN KEY (lifnr) REFERENCES lfa1(lifnr)
);

-- ────────────────────────────────────────────────────────────
-- EKPO — Purchase Order Item
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ekpo (
    ebeln       VARCHAR(10)  NOT NULL,   -- PO Number
    ebelp       VARCHAR(5)   NOT NULL,   -- PO Item
    matnr       VARCHAR(18),             -- Material
    txz01       VARCHAR(40),             -- Item Description
    werks       VARCHAR(4),              -- Plant
    lgort       VARCHAR(4),              -- Storage Location
    matkl       VARCHAR(9),              -- Material Group
    menge       DECIMAL(13,3),           -- PO Quantity
    meins       VARCHAR(3),              -- Unit of Measure
    netpr       DECIMAL(11,2),           -- Net Price
    peinh       DECIMAL(9,3),            -- Price Unit
    netwr       DECIMAL(13,2),           -- Net Value
    brtwr       DECIMAL(13,2),           -- Gross Value
    waers       VARCHAR(5),              -- Currency
    eindt       DATE,                    -- Delivery Date
    infnr       VARCHAR(10),             -- Purchasing Info Record
    loekz       CHAR(1)      DEFAULT '', -- Deletion Indicator
    elikz       CHAR(1)      DEFAULT '', -- Delivery Completed Flag
    repos       CHAR(1)      DEFAULT '', -- Invoice Receipt Indicator
    webre       CHAR(1)      DEFAULT 'X',-- GR-Based Invoice Verification
    PRIMARY KEY (ebeln, ebelp),
    FOREIGN KEY (ebeln) REFERENCES ekko(ebeln),
    FOREIGN KEY (matnr) REFERENCES mara(matnr)
);

-- ────────────────────────────────────────────────────────────
-- MKPF — Material Document Header
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mkpf (
    mblnr       VARCHAR(10)  NOT NULL,   -- Material Document Number
    mjahr       SMALLINT     NOT NULL,   -- Fiscal Year
    bldat       DATE,                    -- Document Date
    budat       DATE,                    -- Posting Date
    usnam       VARCHAR(12),             -- Created By
    PRIMARY KEY (mblnr, mjahr)
);

-- ────────────────────────────────────────────────────────────
-- MSEG — Material Document Segment (GR lines)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mseg (
    mblnr       VARCHAR(10)  NOT NULL,   -- Material Document Number
    mjahr       SMALLINT     NOT NULL,   -- Fiscal Year
    zeile       VARCHAR(4)   NOT NULL,   -- Line Item
    bwart       VARCHAR(3),              -- Movement Type (101=GR vs PO)
    matnr       VARCHAR(18),             -- Material
    werks       VARCHAR(4),              -- Plant
    lgort       VARCHAR(4),              -- Storage Location
    ebeln       VARCHAR(10),             -- PO Number
    ebelp       VARCHAR(5),              -- PO Item
    lifnr       VARCHAR(10),             -- Vendor
    menge       DECIMAL(13,3),           -- Quantity
    meins       VARCHAR(3),              -- Unit of Measure
    dmbtr       DECIMAL(13,2),           -- Amount in Local Currency
    waers       VARCHAR(5),              -- Currency
    budat       DATE,                    -- Posting Date
    charg       VARCHAR(10),             -- Batch Number
    PRIMARY KEY (mblnr, mjahr, zeile),
    FOREIGN KEY (ebeln, ebelp) REFERENCES ekpo(ebeln, ebelp)
);

-- ────────────────────────────────────────────────────────────
-- RBKP — Logistics Invoice Verification Header
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rbkp (
    belnr       VARCHAR(10)  NOT NULL,   -- Invoice Document Number
    gjahr       SMALLINT     NOT NULL,   -- Fiscal Year
    bukrs       VARCHAR(4),              -- Company Code
    bldat       DATE,                    -- Invoice Date
    budat       DATE,                    -- Posting Date
    lifnr       VARCHAR(10),             -- Vendor
    dmbtr       DECIMAL(13,2),           -- Invoice Amount (Gross)
    wmwst       DECIMAL(13,2),           -- Tax Amount
    waers       VARCHAR(5),              -- Currency
    zterm       VARCHAR(4),              -- Payment Terms
    rbstat      CHAR(1),                 -- Status (A=Posted, B=Blocked, R=Reversed)
    stblg       VARCHAR(10),             -- Reversal Document
    PRIMARY KEY (belnr, gjahr),
    FOREIGN KEY (lifnr) REFERENCES lfa1(lifnr)
);

-- ────────────────────────────────────────────────────────────
-- RSEG — Logistics Invoice Verification Item
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rseg (
    belnr       VARCHAR(10)  NOT NULL,   -- Invoice Document Number
    gjahr       SMALLINT     NOT NULL,   -- Fiscal Year
    buzei       VARCHAR(4)   NOT NULL,   -- Item Number
    ebeln       VARCHAR(10),             -- PO Number
    ebelp       VARCHAR(5),              -- PO Item
    matnr       VARCHAR(18),             -- Material
    menge       DECIMAL(13,3),           -- Quantity
    meins       VARCHAR(3),              -- Unit of Measure
    dmbtr       DECIMAL(13,2),           -- Item Amount
    mwskz       VARCHAR(2),              -- Tax Code
    PRIMARY KEY (belnr, gjahr, buzei),
    FOREIGN KEY (belnr, gjahr) REFERENCES rbkp(belnr, gjahr),
    FOREIGN KEY (ebeln, ebelp) REFERENCES ekpo(ebeln, ebelp)
);

-- ────────────────────────────────────────────────────────────
-- INDEXES for performance
-- ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ekko_lifnr  ON ekko(lifnr);
CREATE INDEX IF NOT EXISTS idx_ekko_bedat  ON ekko(bedat);
CREATE INDEX IF NOT EXISTS idx_ekpo_matnr  ON ekpo(matnr);
CREATE INDEX IF NOT EXISTS idx_ekpo_eindt  ON ekpo(eindt);
CREATE INDEX IF NOT EXISTS idx_mseg_ebeln  ON mseg(ebeln);
CREATE INDEX IF NOT EXISTS idx_mseg_budat  ON mseg(budat);
CREATE INDEX IF NOT EXISTS idx_mseg_bwart  ON mseg(bwart);
CREATE INDEX IF NOT EXISTS idx_rbkp_lifnr  ON rbkp(lifnr);
CREATE INDEX IF NOT EXISTS idx_rbkp_budat  ON rbkp(budat);
CREATE INDEX IF NOT EXISTS idx_rseg_ebeln  ON rseg(ebeln);
CREATE INDEX IF NOT EXISTS idx_eban_ebeln  ON eban(ebeln);
