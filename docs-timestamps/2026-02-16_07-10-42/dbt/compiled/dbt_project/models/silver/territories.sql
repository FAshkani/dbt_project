

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
from "ci"."lh_bronze"."territories" t1
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