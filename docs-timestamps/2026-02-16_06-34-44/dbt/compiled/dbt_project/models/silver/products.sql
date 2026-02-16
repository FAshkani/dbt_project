

-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM "ci"."lh_bronze"."products" t1
)

select * from source_data