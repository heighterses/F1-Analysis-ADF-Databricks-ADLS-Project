# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df  = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

from pyspark.sql.functions import count , countDistinct,sum

# COMMAND ----------

demo_df.filter("driver_name = 'DanielRicciardo'").select(count("points"), countDistinct("constructor_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Group By

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races") )   \
.show()

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2022)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Group by with two Columns Race_Year and Driver_Name

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "constructor_name") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Windows Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))
