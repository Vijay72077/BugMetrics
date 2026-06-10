# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6178d909-4119-4eef-8b54-258a3cee7ca4",
# META       "default_lakehouse_name": "lh_BugMetrics_Silver",
# META       "default_lakehouse_workspace_id": "918d733d-ee77-49cb-a30f-4e5628da90c1",
# META       "known_lakehouses": [
# META         {
# META           "id": "6178d909-4119-4eef-8b54-258a3cee7ca4"
# META         },
# META         {
# META           "id": "eed63a94-f905-4be7-ae03-7bfc8272d4a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Load Product Dimension - Bronze to Silver

# CELL ********************

%run /nb_DQCheck

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp,lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PipelineID = "Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --DROP TABLE BugMetricsSilver.lh_BugMetrics_Silver.dbo.Product_Dim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS BugMetricsSilver.lh_BugMetrics_Silver.dbo.Product_Dim
# MAGIC (
# MAGIC ProductID VARCHAR(255), ProductName VARCHAR(255), CreateDateTime timestamp,ModifiedDateTime timestamp, PipelineRunID VARCHAR(255)
# MAGIC )


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

srcdf  = spark.sql("SELECT DISTINCT product_id AS ProductID, product_name AS ProductName FROM BugMetricsBronze.lh_BugMetrics_Bronze.dbo.BugMetrics")

srcdf = srcdf.withColumn("PipelineRunID",lit(f'{PipelineID}')).withColumn("CreateDateTime", current_timestamp()).withColumn("ModifiedDateTime", current_timestamp())

srcdf = validate_data(srcdf,'ProductID','')

#display(srcdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

update_dict = {col: f"s.{col}" for col in srcdf.columns if col not in ["CreateDateTime"] + ['ProductID']}

target_table = DeltaTable.forName(spark, "BugMetricsSilver.lh_BugMetrics_Silver.dbo.product_dim")

target_table.alias("t").merge(
            srcdf.alias("s"),
            "t.ProductID = s.ProductID"
        ).whenMatchedUpdate(set = update_dict).whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --select * FROM BugMetricsSilver.lh_BugMetrics_Silver.dbo.Product_Dim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
