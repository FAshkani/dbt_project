

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
from "ci"."lh_bronze"."cities" t1
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