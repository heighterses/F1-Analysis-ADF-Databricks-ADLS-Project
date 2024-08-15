# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Containers for Project

# COMMAND ----------

def mount_adls(storage_acc_name, container_name):
    client_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-account-client-id')
    tenant_id =dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-service-principal-client-secret')

    # configuration code

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
      
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")
    
    # Databricks  File System Utility

    dbutils.fs.mount(
    source = f"abfss://demo@{storage_acc_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_acc_name}/{container_name}",
    extra_configs = configs)

    # Display the Mounted Files

    display(dbutils.fs.mounts())



# COMMAND ----------

dbutils.fs.refreshMounts()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Demo Container

# COMMAND ----------

mount_adls('formula001adls', 'demo')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Raw Container

# COMMAND ----------

mount_adls('formula001adls', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Presentation Container

# COMMAND ----------

mount_adls('formula001adls', 'presentation' )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Processed Container

# COMMAND ----------

mount_adls('formula001adls', 'processed')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount('/mnt/formula001adls/raw')

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula001adls/raw
