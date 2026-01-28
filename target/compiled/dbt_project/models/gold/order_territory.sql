

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
FROM lh_silver.orders  ord
LEFT JOIN lh_silver.territories  tr
ON ord.TerritoryID = tr.TerritoryID

)

select * from source_data