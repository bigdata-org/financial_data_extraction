{{ config(
    unique_key=['CIK', 'NAME']
) }}

WITH deduplicated AS (
    SELECT 
        CIK,
        NULL AS TICKER,
        NAME,
        COUNTRYBA,
        STPRBA,
        CITYBA,
        ZIPBA,
        BAS1,
        BAS2,
        BAPH,
        COUNTRYMA,
        STPRMA,
        CITYMA,
        ZIPMA,
        MAS1,
        MAS2,
        ACIKS,
        NCIKS,
        CURRENT_TIMESTAMP AS CREATED_DT,
        CURRENT_USER() AS CREATED_BY,
        ROW_NUMBER() OVER (PARTITION BY CIK, NAME ORDER BY CURRENT_TIMESTAMP) AS row_num
    FROM {{source('raw_source', 'sub')}}
)
SELECT 
    CIK,
    TICKER,
    NAME,
    COUNTRYBA,
    STPRBA,
    CITYBA,
    ZIPBA,
    BAS1,
    BAS2,
    BAPH,
    COUNTRYMA,
    STPRMA,
    CITYMA,
    ZIPMA,
    MAS1,
    MAS2,
    ACIKS,
    NCIKS,
    CREATED_DT,
    CREATED_BY
FROM deduplicated
WHERE row_num = 1


    