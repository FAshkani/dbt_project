{{ config(
        materialized='incremental',
        unique_key = 'order_sk',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (

SELECT 	CASE WHEN ord.SalesOrderID < 50000 THEN	{{surrogate_key(['ord.TerritoryID', 'tr.Group' , 'ord.SalesOrderID']) }} ELSE 'NA' END AS order_sk,
			--{{  dbt_utils.generate_surrogate_key(['ord.TerritoryID', 'tr.Group' , 'ord.SalesOrderID']) }} AS order_sk,
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