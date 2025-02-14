{{ config(
    unique_key=['company_sk', 'tag_sk', 'adsh', 'filing_date', 'fiscal_year', 'fiscal_period', 'period_end_date', 'qtrs', 'uom', 'value', 'line_item', 'report']
) }}

WITH staged_results AS (
    SELECT 
        a.adsh, 
        a.tag, 
        a.version, 
        a.value, 
        a.qtrs, 
        a.uom, 
        b.line, 
        b.report, 
        c.cik, 
        c.name, 
        c.fy, 
        c.fp, 
        c.filed, 
        c.period  
    FROM {{source('raw_source', 'num')}} a 
    LEFT JOIN {{source('raw_source', 'pre')}} b 
    JOIN {{source('raw_source', 'sub')}} c 
        ON a.adsh = b.adsh 
        AND a.tag = b.tag 
        AND a.version = b.version 
        AND a.adsh = c.adsh
    WHERE b.stmt = 'CF'
)
SELECT distinct
    dc.company_sk,               -- Foreign Key to DIM_COMPANY
    dc.name AS COMPANY_NAME,
    dt.tag_sk,                   -- Foreign Key to DIM_TAG
    dt.tag,
    dt.version,
    s.adsh,                      -- EDGAR Accession Number
    s.filed AS filing_date,      -- Filing Date from raw.sub (maps to FILING_DATE)
    s.fy AS fiscal_year,         -- Fiscal Year from raw.sub
    s.fp AS fiscal_period,       -- Fiscal Period from raw.sub
    s.period AS period_end_date,  -- Period End Date from raw.sub
    s.qtrs,                      -- Number of Quarters
    s.uom,                       -- Unit of Measure (USD, EUR, etc.)
    s.value,                     -- Financial Value (Numeric Field)
    s.line AS line_item,         -- Line item for the balance sheet (from PRE table)
    s.report AS report,          -- Report number (from PRE table)
    CURRENT_TIMESTAMP AS CREATED_DT,  -- Record creation timestamp
    CURRENT_USER() AS CREATED_BY      -- Record created by
FROM staged_results s
JOIN {{ref('DIM_COMPANY')}} dc 
    ON s.cik = dc.cik 
    and s.name = dc.name
JOIN {{ref('DIM_TAG')}} dt 
    ON s.tag = dt.tag
    AND s.version = dt.version