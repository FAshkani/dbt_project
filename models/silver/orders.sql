{{ config(
        materialized='incremental',
        unique_key = 'SalesOrderID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
select
  SalesOrderID,
  RevisionNumber,
  OrderDate,
  DueDate,
  ShipDate,
  Status,
  OnlineOrderFlag,
  SalesOrderNumber,
  PurchaseOrderNumber,
  AccountNumber,
  CustomerID,
  SalesPersonID,
  TerritoryID,
  BillToAddressID,
  ShipToAddressID,
  ShipMethodID,
  CreditCardID,
  CreditCardApprovalCode,
  CurrencyRateID,
  SubTotal,
  TaxAmt,
  Freight,
  TotalDue,
  Comment,
  rowguid,
  ModifiedDate
from {{ source('bronze','orders') }} t1
)

select
  SalesOrderID,
  RevisionNumber,
  OrderDate,
  DueDate,
  ShipDate,
  Status,
  OnlineOrderFlag,
  SalesOrderNumber,
  PurchaseOrderNumber,
  AccountNumber,
  CustomerID,
  SalesPersonID,
  TerritoryID,
  BillToAddressID,
  ShipToAddressID,
  ShipMethodID,
  CreditCardID,
  CreditCardApprovalCode,
  CurrencyRateID,
  SubTotal,
  TaxAmt,
  Freight,
  TotalDue,
  Comment,
  rowguid,
  ModifiedDate
 
from  source_data
