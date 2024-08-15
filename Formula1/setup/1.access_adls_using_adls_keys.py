# Databricks notebook source
# MAGIC %md
# MAGIC # Access Aure Data Lake using access keys

# COMMAND ----------


formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-account-key')


# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.formula001adls.dfs.core.windows.net",
formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula001adls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula001adls.dfs.core.windows.net/002 circuits.csv"))
