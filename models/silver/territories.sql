{{ config(
        materialized='incremental',
        unique_key = 'TerritoryID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
select
  TerritoryID,
  Name,
  CountryRegionCode,
  "Group",
  SalesYTD,
  SalesLastYear,
  CostYTD,
  CostLastYear,
  rowguid,
  ModifiedDate
from {{ source('bronze','territories') }} t1
)
select
  TerritoryID,
  Name,
  CountryRegionCode,
  "Group",
  SalesYTD,
  SalesLastYear,
  CostYTD,
  CostLastYear,
  rowguid,
  ModifiedDate
from   source_data
