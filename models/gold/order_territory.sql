{{ config(
        materialized='incremental',
        unique_key = 'SalesOrderID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (

SELECT 		
			ord.SalesOrderID as SalesOrderID,
			ord.TerritoryID,
			tr.Name as TerritoryName,
			tr.Group as TerritoryGroup,
			ord.TaxAmt,
			ord.Freight,
			ord.TotalDue,
			ord.Comment,
			ord.ModifiedDate
FROM {{ ref('orders') }}  ord
LEFT JOIN {{ ref('territories') }}  tr
ON ord.TerritoryID = tr.TerritoryID

)

select * from source_data