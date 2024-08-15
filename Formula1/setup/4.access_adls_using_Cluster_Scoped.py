# Databricks notebook source
# MAGIC %md
# MAGIC # Access Aure Data Lake using access keys
# MAGIC ### (Currently detach the code from in spark config)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula001adls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula001adls.dfs.core.windows.net/002 circuits.csv"))
