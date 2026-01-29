# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "config"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sempy import fabric
import requests
import pandas as pd
from pyspark.sql.functions import explode, from_json, col, datediff, from_utc_timestamp, lit, concat_ws
from pyspark.sql.types import StructType, StructField, StringType

pipelines_list = fabric.list_items(type="DataPipeline")

if len(pipelines_list) > 0:
    
    workspace_id = fabric.get_workspace_id()
    workspace_name = fabric.resolve_workspace_name()
    base_url = f"https://api.fabric.microsoft.com/v1/workspaces"

    access_token = notebookutils.credentials.getToken('pbi')  # Set this securely

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    all_instances = []

    df = pd.DataFrame(pipelines_list)
    for index, row in df.iterrows():
        # print(row)
        
        pipeline_id = row['Id']
        pipeline_name = row['Display Name']

        job_instance_url = f"{base_url}/{workspace_id}/items/{pipeline_id}/jobs/instances/"

        continuation_token = None

        while True:
            params = {}
            if continuation_token:
                params['continuationToken'] = continuation_token

            response = requests.get(job_instance_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Append job instances
            if 'value' in data:
                for instance in data['value']:
                    instance['pipeline_id'] = pipeline_id
                    instance['pipeline_name'] = pipeline_name
            all_instances.extend(data['value'])

            # Check for continuation token
            continuation_token = data.get('continuationToken')
            if not continuation_token:
                break


        # print(all_instances)
    df = pd.DataFrame(all_instances)
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    spark_df = spark_df.select(
        col("id").alias("pipeline_run_id"),
        col("itemId").alias("pipeline_id"),
        col("pipeline_name"),
        lit(workspace_id).alias("workspace_id"),
        lit(workspace_name).alias("workspace_name"),
        col("invokeType").alias("invoke_type"),
        col("jobType").alias("job_type"),
        col("status").alias("job_status"),
        col("startTimeUtc").alias("start_time"),
        col("endTimeUtc").alias("end_time"),
        col("rootActivityId").alias("root_activity_id"),
        col("failureReason")
    )

    spark_df = spark_df.\
        withColumn("error_message", col("failureReason.message")).\
        withColumn("request_id", col("failureReason.requestId")).\
        withColumn("start_time", from_utc_timestamp(col("start_time").cast("timestamp"), 'Australia/Sydney')).\
        withColumn("end_time", from_utc_timestamp(col("end_time").cast("timestamp"), 'Australia/Sydney')).\
        withColumn("duration_in_seconds", col("end_time").cast("long") - col("start_time").cast("long")).\
        withColumn("duration_in_minutes", ((col("end_time").cast("long") - col("start_time").cast("long"))/ 60).cast("decimal(18,2)")).\
        withColumn("monitoring_url", concat_ws("/", lit("https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces"), col("workspace_id"), lit("pipelines"), col("pipeline_id"), col("pipeline_run_id"))).\
        drop("failureReason")

    spark_df = spark_df.createOrReplaceTempView("src_data_pipeline_logs_temp")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *
# MAGIC from src_data_pipeline_logs_temp

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS data_pipeline_logs(
# MAGIC     pipeline_run_id STRING,
# MAGIC     pipeline_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     workspace_id STRING,
# MAGIC     workspace_name STRING,
# MAGIC     invoke_type STRING,
# MAGIC     job_type STRING,
# MAGIC     job_status STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     root_activity_id STRING,
# MAGIC     error_message STRING,
# MAGIC     request_id STRING,
# MAGIC     duration_in_seconds INT,
# MAGIC     duration_in_minutes DECIMAL(10,2),
# MAGIC     monitoring_url STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO data_pipeline_logs AS TARGET
# MAGIC USING src_data_pipeline_logs_temp AS SOURCE
# MAGIC ON SOURCE.pipeline_run_id = TARGET.pipeline_run_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     pipeline_id = SOURCE.pipeline_id,
# MAGIC     pipeline_name = SOURCE.pipeline_name,
# MAGIC     workspace_id = SOURCE.workspace_id,
# MAGIC     workspace_name = SOURCE.workspace_name,
# MAGIC     invoke_type = SOURCE.invoke_type,
# MAGIC     job_type = SOURCE.job_type,
# MAGIC     job_status = SOURCE.job_status,
# MAGIC     start_time = SOURCE.start_time,
# MAGIC     end_time = SOURCE.end_time,
# MAGIC     root_activity_id = SOURCE.root_activity_id,
# MAGIC     error_message = SOURCE.error_message,
# MAGIC     request_id = SOURCE.request_id,
# MAGIC     duration_in_seconds = SOURCE.duration_in_seconds,
# MAGIC     duration_in_minutes = SOURCE.duration_in_minutes,
# MAGIC     monitoring_url = SOURCE.monitoring_url
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
