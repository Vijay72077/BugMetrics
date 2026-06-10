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
# META         },
# META         {
# META           "id": "6178d909-4119-4eef-8b54-258a3cee7ca4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Build Merge Condition:

def build_merge_condition(primary_keys,srcdb,tgtdb):
    primary_keys = primary_keys.split(",")
    return " AND ".join([f"COALESCE({srcdb}.{key},'DefaultNull') = COALESCE({tgtdb}.{key},'DefaultNull')" for key in primary_keys])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Dups Check:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Helper function to validate and preprocess data
def validate_data(sourcedf,primary_keys,order_byfield):
    #sourcedf = spark.read.table(f"{srctableName}")
    duplicate_count = sourcedf.groupBy(primary_keys.split(",")).count().filter(col("count") > 1).count()
    print(duplicate_count)
    if duplicate_count >1:
        print("Duplicates Found")
        if order_byfield == '':
            w = Window.partitionBy(primary_keys.split(","))
        else:
            w = Window.partitionBy(primary_keys.split(",")).orderBy(col(order_byfield).desc())
        df_dedup = (sourcedf.withColumn("rn", row_number().over(w))
              .filter("rn = 1")
              .drop("rn"))
        return df_dedup
        
    else:
        
        print("No Duplicates")
        return sourcedf
    
    print("Data validation passed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col


def check_pattern(df, column, pattern):
    df_checked = df.withColumn(
        "IsValid",
        col(column).rlike(pattern)
    )

    invalid_count = df_checked.filter("IsValid IS NULL OR IsValid = false").count()

    if invalid_count > 0:
        print(f"Wrong pattern records available: {invalid_count} row(s) in column '{column}'")
    else:
        print("No bad records")

    return df_checked.filter("IsValid = true").drop("IsValid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
