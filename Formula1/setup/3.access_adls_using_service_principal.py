# Databricks notebook source
# MAGIC %md
# MAGIC # Access Aure Data Lake using Service Principal

# COMMAND ----------

client_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-account-client-id')
tenant_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-client-secret')


# COMMAND ----------

storage_account_name = "formula001adls"  # Replace with your actual storage account name

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula001adls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula001adls.dfs.core.windows.net/002 circuits.csv"))
