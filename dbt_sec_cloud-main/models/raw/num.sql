{{ config(
    pre_hook=["TRUNCATE TABLE SEC.RAW.NUM", "alter external table {{ source('stage_source', 'sec_ext_table').render() }} refresh"]
) }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/num.tsv' %}

SELECT 
    GET(VALUE, 'c1')::STRING AS adsh,         
    GET(VALUE, 'c2')::STRING AS tag,          
    GET(VALUE, 'c3')::STRING AS version,      
    TRY_TO_DATE(GET(VALUE, 'c4')::STRING, 'YYYYMMDD') AS ddate,                   
    TRY_CAST(GET(VALUE, 'c5')::STRING AS INT) AS qtrs,                  
    GET(VALUE, 'c6')::STRING AS uom,           
    GET(VALUE, 'c7')::STRING AS segments,     
    GET(VALUE, 'c8')::STRING AS coreg,       
    TRY_CAST(GET(VALUE, 'c9')::STRING AS INT) AS value,      
    GET(VALUE, 'c10')::STRING AS footnote      
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'
