# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# [comment]: # (Attach Default Lakehouse Markdown Cell)
# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "lh_bronze",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📦 Pip
# Pip installs reqired specifically for this template should occur here

# CELL ********************

# No pip installs needed for this notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

from notebookutils import mssparkutils # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# CELL ********************

def pre_execute_notebook(notebook_file):

    try:
        mssparkutils.notebook.run(notebook_file, 1800)
        status = 'PreExecute Notebook Executed'
        error = None
    except Exception as e:
        status = 'No PreExecute Notebook Found'
        error = str(e)

    return status

def post_execute_notebook(notebook_file):

    try:
        mssparkutils.notebook.run(notebook_file, 1800)
        status = 'PostExecute Notebook Executed'
        error = None
    except Exception as e:
        status = 'No PostExecute Notebook Found'
        error = str(e)

    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pre-Execution Python Script

# MARKDOWN ********************

# # Declare and Execute Pre-Execution Notebook

# CELL ********************

#Read the context of the notebook
notebook_info = mssparkutils.runtime.context

# Extract the currentNotebookName from the dictionary
current_notebook_name = notebook_info.get("currentNotebookName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Pre-Execute Notebook
preexecute_notebook_name  = current_notebook_name+ ".preexecute"
pre_execute_notebook(preexecute_notebook_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Declare and Execute SQL Statements

# CELL ********************


sql = '''
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
select * from source_data'''

for s in sql.split(';\n'):
    spark.sql(s)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🛑 Execution Stop

# CELL ********************

# Execute Post-Execute Notebook
postexecute_notebook_name  = current_notebook_name + ".postexecute"
post_execute_notebook(postexecute_notebook_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Exit to prevent spark sql debug cell running 
mssparkutils.notebook.exit("value string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # SPARK SQL Cells for Debugging

# CELL ********************

# MAGIC %%sql
# MAGIC         create or replace table lh_gold.order_territory1
# MAGIC       as
# MAGIC -- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
# MAGIC WITH source_data AS (
# MAGIC SELECT 	CASE WHEN ord.SalesOrderID < 50000 THEN	
# MAGIC       MD5(
# MAGIC         CONCAT_WS(
# MAGIC           '||',COALESCE(CAST(ord.TerritoryID AS string), '__NULL__'),COALESCE(CAST(tr.Group AS string), '__NULL__'),COALESCE(CAST(ord.SalesOrderID AS string), '__NULL__'))
# MAGIC       ) ELSE 'NA' END AS order_sk,
# MAGIC 			--md5(cast(concat(coalesce(cast(ord.TerritoryID as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(tr.Group as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(ord.SalesOrderID as string), '_dbt_utils_surrogate_key_null_')) as string)) AS order_sk,
# MAGIC 			ord.SalesOrderID as SalesOrderID,
# MAGIC 			ord.TerritoryID,
# MAGIC 			tr.Name as TerritoryName,
# MAGIC 			tr.Group as TerritoryGroup,
# MAGIC 			ord.TaxAmt,
# MAGIC 			ord.Freight,
# MAGIC 			ord.TotalDue,
# MAGIC 			ord.Comment,
# MAGIC 			ord.ModifiedDate
# MAGIC FROM lh_silver.orders  ord
# MAGIC LEFT JOIN lh_silver.territories  tr
# MAGIC ON ord.TerritoryID = tr.TerritoryID
# MAGIC )
# MAGIC select * from source_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
