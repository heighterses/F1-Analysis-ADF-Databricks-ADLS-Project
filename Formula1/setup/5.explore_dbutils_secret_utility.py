# Databricks notebook source
# MAGIC %md
# MAGIC # Explore Databricks Secret Utility

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-account-key' )

