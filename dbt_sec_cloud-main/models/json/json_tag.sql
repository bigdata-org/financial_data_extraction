{{ config(
    alias='TAG',
    materialized='incremental',
    unique_key='DATA_HASH'
) }}

WITH staged_data AS (
    SELECT 
        OBJECT_CONSTRUCT(*) AS DATA, 
        SHA2(TO_JSON(OBJECT_CONSTRUCT(*)), 256) AS DATA_HASH,
        CURRENT_TIMESTAMP AS CREATED_DT,
        CURRENT_USER() AS CREATED_BY
    FROM {{source('raw_source', 'tag')}}
)
SELECT *
FROM staged_data
WHERE DATA_HASH NOT IN (SELECT DATA_HASH FROM JSON.TAG)
