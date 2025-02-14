{{ config(
    unique_key=['TAG', 'VERSION']
) }}

WITH deduplicated AS (
    SELECT 
        TAG,
        VERSION,
        CURRENT_TIMESTAMP AS CREATED_DT,
        CURRENT_USER() AS CREATED_BY,
        ROW_NUMBER() OVER (PARTITION BY TAG, VERSION ORDER BY CURRENT_TIMESTAMP) AS rn
    FROM {{source('raw_source', 'tag')}}
)
SELECT 
    TAG,
    VERSION,
    CREATED_DT,
    CREATED_BY
FROM deduplicated
WHERE rn = 1
