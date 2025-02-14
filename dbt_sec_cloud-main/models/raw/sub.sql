{{ config(
    pre_hook=["TRUNCATE TABLE SEC.RAW.SUB", "alter external table {{ source('stage_source', 'sec_ext_table').render() }} refresh"]
) }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/sub.tsv' %}

SELECT
    value:c1::STRING AS adsh,
    TRY_CAST(value:c2::STRING AS INT) AS cik,
    value:c3::STRING AS name,
    TRY_CAST(value:c4::STRING AS INT) AS sic,
    value:c5::STRING AS countryba,
    value:c6::STRING AS stprba,
    value:c7::STRING AS cityba,
    value:c8::STRING AS zipba,
    value:c9::STRING AS bas1,
    value:c10::STRING AS bas2,
    value:c11::STRING AS baph,
    value:c12::STRING AS countryma,
    value:c13::STRING AS stprma,
    value:c14::STRING AS cityma,
    value:c15::STRING AS zipma,
    value:c16::STRING AS mas1,
    value:c17::STRING AS mas2,
    value:c18::STRING AS countryinc,
    value:c19::STRING AS stprinc,
    TRY_CAST(value:c20::STRING AS INT) AS ein,
    value:c21::STRING AS former,
    TRY_CAST(value:c22::STRING AS INT) AS changed,
    value:c23::STRING AS afs,
    TRY_CAST(value:c24::STRING AS INT) AS wksi,
    TRY_CAST(value:c25::STRING AS INT) AS fye,
    value:c26::STRING AS form,
    TRY_TO_DATE(value:c27::STRING, 'YYYYMMDD') AS period,
    TRY_CAST(value:c28::STRING AS INT) AS fy,
    value:c29::STRING AS fp,
    TRY_TO_DATE(value:c30::STRING, 'YYYYMMDD') AS filed,
    value:c31::STRING AS accepted,
    TRY_CAST(value:c32::STRING AS INT) AS prevrpt,
    TRY_CAST(value:c33::STRING AS INT) AS detail,
    value:c34::STRING AS instance,
    TRY_CAST(value:c35::STRING AS INT) AS nciks,
    value:c36::STRING AS aciks
FROM {{ source('stage_source', 'sec_ext_table') }}
WHERE metadata$filename LIKE '{{ pattern }}'

