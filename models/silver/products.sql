{{ config(
        materialized='incremental',
        unique_key = 'ProductID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
select
  ProductID,
  ProductName,
  Brand,
  Size,
  RecommendedRetailPrice,
  TypicalWeightPerUnit,
  LastEditedBy,
  ValidFrom,
  ValidTo
from {{ source('bronze','products') }} t1
)

select
  ProductID,
  ProductName,
  Brand,
  Size,
  RecommendedRetailPrice,
  TypicalWeightPerUnit,
  LastEditedBy,
  ValidFrom,
  ValidTo
from   source_data
