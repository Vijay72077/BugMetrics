# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Bugs Fact - Silver to Gold

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

srcdf = spark.sql("""SELECT sha2(cast(concat(organization_id,project_id,work_item_id) AS string), 256) AS BugMetricSK,organization_id,project_id,work_item_id,COALESCE(assigned_to,'') AS assigned_to
,sha2(cast(date_format(changed_date, "yyyyMMdd") AS string), 256) ChangeDateSK
,sha2(cast(date_format(created_date, "yyyyMMdd") AS string), 256) CreateDateSK
,priority,severity,state,CASE WHEN dim.ProductID IS NOT NULL THEN sha2(cast(product_id AS string), 256) ELSE sha2(cast('Unknown' AS string), 256) END AS ProductSK,count(*) AS TotalBugs
FROM BugMetricsSilver.lh_BugMetrics_Silver.dbo.BugMetrics_Fact fact 
LEFT JOIN BugMetricsGold.lh_BugMetrics_Gold.dbo.Product_Dim dim ON fact.product_id = dim.ProductID 
where work_item_type = 'Bug'
group by sha2(cast(concat(organization_id,project_id,work_item_id) AS string), 256),organization_id,project_id,work_item_id,assigned_to,changed_date,created_date
,priority,severity,state,CASE WHEN dim.ProductID IS NOT NULL THEN sha2(cast(product_id AS string), 256) ELSE sha2(cast('Unknown' AS string), 256) END
""")

srcdf = srcdf.withColumn("PipelineRunID",lit(f'{PipelineID}')).withColumn("CreateDateTime", current_timestamp()).withColumn("ModifiedDateTime", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

if spark.catalog.tableExists("BugMetricsGold.lh_BugMetrics_Gold.dbo.BugMetrics_Fact"):

    update_dict = {col: f"s.{col}" for col in srcdf.columns if col not in ["CreateDateTime"] + ['organization_id','project_id','work_item_id']}

    target_table = DeltaTable.forName(spark, "BugMetricsGold.lh_BugMetrics_Gold.dbo.BugMetrics_Fact")

    target_table.alias("t").merge(
            srcdf.alias("s"),
            "t.organization_id = s.organization_id AND t.project_id = s.project_id AND t.work_item_id = s.work_item_id"
        ).whenMatchedUpdate(set = update_dict).whenNotMatchedInsertAll().execute()
else:
    srcdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable('BugMetricsGold.lh_BugMetrics_Gold.dbo.BugMetrics_Fact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
