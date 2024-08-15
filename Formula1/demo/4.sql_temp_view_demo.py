# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Local Temp View

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SQL in PySpark

# COMMAND ----------

race_results_2020 = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2020")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Global Temp View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year = 1999

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results")
