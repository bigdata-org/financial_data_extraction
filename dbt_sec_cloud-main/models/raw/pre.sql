{{ config(
    pre_hook=["TRUNCATE TABLE SEC.RAW.PRE", "alter external table {{ source('stage_source', 'sec_ext_table').render() }} refresh"]
) }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/pre.tsv' %}

SELECT 
    VALUE:c1::STRING AS adsh,
    TRY_CAST(VALUE:c2::STRING AS INT) AS report,
    TRY_CAST(VALUE:c3::STRING AS INT) AS line,
    VALUE:c4::STRING AS stmt,
    TRY_CAST(VALUE:c5::STRING AS INT) AS inpth,
    VALUE:c6::STRING AS rfile,
    VALUE:c7::STRING AS tag,
    VALUE:c8::STRING AS version,
    VALUE:c9::STRING AS plabel,
    TRY_CAST(VALUE:c10::STRING AS INT) AS negating
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'