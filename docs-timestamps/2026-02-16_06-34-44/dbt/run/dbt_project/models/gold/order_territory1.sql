
  
    
        create or replace table lh_gold.order_territory1
      
      
      
      
      
      
      
      

      as
      

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (

SELECT 	CASE WHEN ord.SalesOrderID < 50000 THEN	
 
      MD5(
        CONCAT_WS(
          '||',COALESCE(CAST(ord.TerritoryID AS string), '__NULL__'),COALESCE(CAST(tr.Group AS string), '__NULL__'),COALESCE(CAST(ord.SalesOrderID AS string), '__NULL__'))
      ) ELSE 'NA' END AS order_sk,
			--md5(cast(concat(coalesce(cast(ord.TerritoryID as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(tr.Group as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(ord.SalesOrderID as string), '_dbt_utils_surrogate_key_null_')) as string)) AS order_sk,
			ord.SalesOrderID as SalesOrderID,
			ord.TerritoryID,
			tr.Name as TerritoryName,
			tr.Group as TerritoryGroup,
			ord.TaxAmt,
			ord.Freight,
			ord.TotalDue,
			ord.Comment,
			ord.ModifiedDate
FROM lh_silver.orders  ord
LEFT JOIN lh_silver.territories  tr
ON ord.TerritoryID = tr.TerritoryID

)

select * from source_data
  