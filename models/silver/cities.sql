{{ config(
        materialized='incremental',
        unique_key = 'CountryID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select
  CountryID,
  CountryName,
  FormalName,
  IsoAlpha3Code,
  IsoNumericCode,
  CountryType,
  LatestRecordedPopulation,
  Continent,
  Region,
  Subregion,
  LastEditedBy,
  ValidFrom,
  ValidTo,
  HappinessIndex
from {{ source('bronze','countries') }} t1
)

select
  CountryID,
  CountryName,
  FormalName,
  IsoAlpha3Code,
  IsoNumericCode,
  CountryType,
  LatestRecordedPopulation,
  Continent,
  Region,
  Subregion,
  LastEditedBy,
  ValidFrom,
  ValidTo,
  HappinessIndex
from   source_data
