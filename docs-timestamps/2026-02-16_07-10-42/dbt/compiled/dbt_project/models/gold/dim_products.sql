

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
select
  ProductID,
  Name,
  ProductNumber,
  MakeFlag,
  FinishedGoodsFlag,
  Color,
  SafetyStockLevel,
  ReorderPoint,
  StandardCost,
  ListPrice,
  Size,
  SizeUnitMeasureCode,
  WeightUnitMeasureCode,
  Weight,
  DaysToManufacture,
  ProductLine,
  Class,
  Style,
  ProductSubcategoryID,
  ProductModelID,
  SellStartDate,
  SellEndDate,
  DiscontinuedDate,
  rowguid,
  ModifiedDate
from "ci"."lh_silver"."products" t1
)

select
  ProductID,
  Name,
  ProductNumber,
  MakeFlag,
  FinishedGoodsFlag,
  Color,
  SafetyStockLevel,
  ReorderPoint,
  StandardCost,
  ListPrice,
  Size,
  SizeUnitMeasureCode,
  WeightUnitMeasureCode,
  Weight,
  DaysToManufacture,
  ProductLine,
  Class,
  Style,
  ProductSubcategoryID,
  ProductModelID,
  SellStartDate,
  SellEndDate,
  DiscontinuedDate,
  rowguid,
  ModifiedDate
from  source_data