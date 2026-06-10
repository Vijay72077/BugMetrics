# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eed63a94-f905-4be7-ae03-7bfc8272d4a1",
# META       "default_lakehouse_name": "lh_BugMetrics_Bronze",
# META       "default_lakehouse_workspace_id": "a03811b6-1df1-4d4b-9a29-fa9b9eb435b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "eed63a94-f905-4be7-ae03-7bfc8272d4a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Load Fact - Bronze to Silver

# CELL ********************

%run /nb_DQCheck

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

srcdf = spark.sql("SELECT organization_id,project_id,work_item_id,area_path,assigned_to,changed_date,created_by,created_date,iteration_path,priority,resolved_date,severity,state,completed_work,work_item_type,product_id FROM BugMetricsBronze.lh_BugMetrics_Bronze.dbo.BugMetrics")

srcdf = srcdf.withColumn("PipelineRunID",lit(f'{PipelineID}')).withColumn("CreateDateTime", current_timestamp()).withColumn("ModifiedDateTime", current_timestamp())

srcdf = validate_data(srcdf,'organization_id,project_id,work_item_id','changed_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

if spark.catalog.tableExists("BugMetricsSilver.lh_BugMetrics_Silver.dbo.BugMetrics_Fact"):

    update_dict = {col: f"s.{col}" for col in srcdf.columns if col not in ["CreateDateTime"] + ['organization_id','project_id','work_item_id']}

    target_table = DeltaTable.forName(spark, "BugMetricsSilver.lh_BugMetrics_Silver.dbo.BugMetrics_Fact")

    target_table.alias("t").merge(
            srcdf.alias("s"),
            "t.organization_id = s.organization_id AND t.project_id = s.project_id AND t.work_item_id = s.work_item_id"
        ).whenMatchedUpdate(set = update_dict).whenNotMatchedInsertAll().execute()
else:
    srcdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable('BugMetricsSilver.lh_BugMetrics_Silver.dbo.BugMetrics_Fact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) FROM BugMetricsSilver.lh_BugMetrics_Silver.dbo.BugMetrics_Fact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
