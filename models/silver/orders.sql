{{ config(
        materialized='incremental',
        unique_key = 'SalesOrderID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM {{ source('bronze','orders') }} t1
)

select * from source_data