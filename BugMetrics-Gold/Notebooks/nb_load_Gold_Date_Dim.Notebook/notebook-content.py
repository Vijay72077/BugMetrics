# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Generate Date Dim table Gold layer

# CELL ********************

start_date       = "2020-01-01"          
end_date         = "2026-12-31" 
PipelineRunID  = 'Manual' 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dates = (
    spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS Date")
)
print(f"Generated {df_dates.count():,} dates from {start_date} to {end_date}")

#display(df_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim = (
    df_dates
    # ── Surrogate & natural keys ────────────────────────────────────────
    .withColumn(
    "DateSK",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.date_format("Date", "yyyyMMdd"), F.lit(""))
        ),
        256
    )
)
    .withColumn("DateKey",         F.col("Date"))
    .withColumn("DateString",      F.date_format("Date", "yyyy-MM-dd"))

    # ── Calendar parts ───────────────────────────────────────────────────
    .withColumn("Year",            F.year("Date"))
    .withColumn("Quarter",         F.quarter("Date"))
    .withColumn("Month",           F.month("Date"))
    .withColumn("MonthName",       F.date_format("Date", "MMMM"))
    .withColumn("MonthShortName",  F.date_format("Date", "MMM"))
    .withColumn("Day",             F.dayofmonth("Date"))
    .withColumn("DayOfYear",       F.dayofyear("Date"))
    .withColumn("WeekOfYear",      F.weekofyear("Date"))
    .withColumn("DayOfWeek",       F.dayofweek("Date"))             
    .withColumn("DayOfWeekIso",    (((F.dayofweek("Date") + 5) % 7) + 1))  
    .withColumn("DayName",         F.date_format("Date", "EEEE"))
    .withColumn("DayShortName",    F.date_format("Date", "EEE"))

    # ── Period labels (handy for filters / slicers) ─────────────────────
    .withColumn("YearMonth",       F.date_format("Date", "yyyy-MM"))
    .withColumn("YearMonthInt",    F.date_format("Date", "yyyyMM").cast("int"))
    .withColumn("YearQuarter",     F.concat(F.year("Date"), F.lit("-Q"), F.quarter("Date")))
    .withColumn("YearWeek",        F.concat(F.year("Date"), F.lit("-W"),
                                            F.lpad(F.weekofyear("Date"), 2, "0")))

    # ── Start / end markers ─────────────────────────────────────────────
    .withColumn("FirstDayOfMonth", F.trunc("Date", "month"))
    .withColumn("LastDayOfMonth",  F.last_day("Date"))
    .withColumn("FirstDayOfYear",  F.trunc("Date", "year"))
    .withColumn("LastDayOfYear",   F.expr("add_months(trunc(Date,'year'), 12) - interval 1 day").cast("date"))

    # ── Flags ───────────────────────────────────────────────────────────
    .withColumn("IsWeekend",       F.dayofweek("Date").isin(1, 7))
    .withColumn("IsMonthStart",    F.col("Date") == F.trunc("Date", "month"))
    .withColumn("IsMonthEnd",      F.col("Date") == F.last_day("Date"))
    .withColumn("IsYearStart",     F.col("Date") == F.trunc("Date", "year"))
    .withColumn("IsYearEnd",       (F.month("Date") == 12) & (F.dayofmonth("Date") == 31))

 

    # ── Audit columns ───────────────────────────────────────────────────
    .withColumn("CreateDateTime",   F.current_timestamp())
    .withColumn("ModifiedDateTime", F.current_timestamp())
    .withColumn("PipelineRunID",    F.lit(PipelineRunID))
)

display(df_dim.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df_dim.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable("BugMetricsGold.lh_BugMetrics_Gold.dbo.Date_Dim"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * FROM BugMetricsGold.lh_BugMetrics_Gold.dbo.Product_Dim LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
