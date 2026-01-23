{{ config(
        materialized='incremental',
        unique_key = 'DepartmentID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM {{ ref('departments') }} t1
)

select * from source_data