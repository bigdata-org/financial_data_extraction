{{ config(
    pre_hook=["TRUNCATE TABLE SEC.RAW.TAG", "alter external table {{ source('stage_source', 'sec_ext_table').render() }} refresh"]
) }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/tag.tsv' %}

SELECT 
    VALUE:c1::STRING AS tag,
    VALUE:c2::STRING AS version,
    TRY_CAST(VALUE:c3::STRING AS INT) AS custom,
    TRY_CAST(VALUE:c4::STRING AS INT) AS abstract,
    VALUE:c5::STRING AS datatype,
    VALUE:c6::STRING AS iord,
    VALUE:c7::STRING AS crdr,
    VALUE:c8::STRING AS tlabel,
    VALUE:c9::STRING AS doc
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'