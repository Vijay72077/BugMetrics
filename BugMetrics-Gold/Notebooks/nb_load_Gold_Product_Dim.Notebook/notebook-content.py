# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# Load Product Dimension - Silver to Gold

# CELL ********************

from pyspark.sql.functions import *

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
# MAGIC CREATE TABLE IF NOT EXISTS BugMetricsGold.lh_BugMetrics_Gold.dbo.Product_Dim
# MAGIC (
# MAGIC ProductSK STRING,ProductID VARCHAR(255), ProductName VARCHAR(255), CreateDateTime timestamp,ModifiedDateTime timestamp, PipelineRunID VARCHAR(255)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

srcdf  = spark.sql("SELECT ProductID,ProductName FROM BugMetricsSilver.lh_BugMetrics_Silver.dbo.Product_Dim UNION SELECT 'Unknown' AS ProductID,'Unknown' AS ProductName")

srcdf = srcdf.withColumn("ProductSK",sha2(concat_ws("||",coalesce((srcdf.ProductID), lit(""))),
        256
    )
).withColumn("PipelineRunID",lit(f'{PipelineID}')).withColumn("CreateDateTime", current_timestamp()).withColumn("ModifiedDateTime", current_timestamp())
#display(srcdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

update_dict = {col: f"s.{col}" for col in srcdf.columns if col not in ["CreateDateTime"] + ['ProductID']}

target_table = DeltaTable.forName(spark, "BugMetricsGold.lh_BugMetrics_Gold.dbo.Product_Dim")

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
# MAGIC 
# MAGIC select * FROM BugMetricsGold.lh_BugMetrics_Gold.dbo.Product_Dim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
