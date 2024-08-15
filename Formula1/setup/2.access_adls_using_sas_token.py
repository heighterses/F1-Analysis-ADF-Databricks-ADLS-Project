# Databricks notebook source
# MAGIC %md
# MAGIC # Access Aure Data Lake using SAS Token

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-1-raw-sas-token')


# COMMAND ----------

storage_account_name = "formula001adls"  # Replace with your actual storage account name

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", formula1_demo_sas_token)
