# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Using Service Principal

# COMMAND ----------

client_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-account-client-id')
tenant_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-client-secret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula001adls.dfs.core.windows.net/",
  mount_point = "/mnt/formuladl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formuladl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formuladl/demo/002 circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formuladl/demo')
