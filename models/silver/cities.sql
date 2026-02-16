{{ config(
        materialized='incremental',
        unique_key = 'CityID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select
  CityID,
  CityName,
  StateProvinceID,
  LatestRecordedPopulation,
  LastEditedBy,
  ValidFrom,
  ValidTo
from {{ source('bronze','cities') }} t1
)

select
  CityID,
  CityName,
  StateProvinceID,
  LatestRecordedPopulation,
  LastEditedBy,
  ValidFrom,
  ValidTo
from source_data
