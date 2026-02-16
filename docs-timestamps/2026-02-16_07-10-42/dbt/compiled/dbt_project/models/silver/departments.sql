

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select
  DepartmentID,
  DepartmentName,
  LastEditedBy,
  ValidFrom,
  ValidTo
from "ci"."lh_bronze"."departments" t1
)

select
  DepartmentID,
  DepartmentName,
  LastEditedBy,
  ValidFrom,
  ValidTo
from  source_data