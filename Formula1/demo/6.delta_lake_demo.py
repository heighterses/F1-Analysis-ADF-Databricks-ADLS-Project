# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Lake Demo
# MAGIC - Create Manage Table(Write Data)
# MAGIC - Create External Table(Write Data)
# MAGIC - Read Data from Delta Lake(Table)
# MAGIC - Read Data from Delta Lake(File)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION 'abfss://demo@formula001adls.dfs.core.windows.net/'

# COMMAND ----------


result_df = spark.read.option("inferSchema", True) \
.json("abfss://raw@formula001adls.dfs.core.windows.net/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Create Manage Table(Write Data)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create External Table(Write Data)
# MAGIC

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("abfss://demo@formula001adls.dfs.core.windows.net/results_external")


# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://demo@formula001adls.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data from Delta Lake(Table)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data from Delta Lake(File)

# COMMAND ----------

results_external_df = spark.read.format("delta").load("abfss://demo@formula001adls.dfs.core.windows.net/results_external")
# display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Partion in Delta Table

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_managed_partitionBy_rconstructor_id")


# COMMAND ----------

# MAGIC %md
# MAGIC - ### Update Delta Table
# MAGIC - ### Delete Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Section

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formula001adls.dfs.core.windows.net/results_managed")
deltaTable.update("position <= 10", {"position":"21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE SECTION

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formula001adls.dfs.core.windows.net/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT USING MERGE

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("temp_day1_df")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))



# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("temp_day2_df")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge
# MAGIC (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING temp_day1_df upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING temp_day2_df upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Day3 (Using Pyspark API)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. History & Versioning
# MAGIC ##### 2. Time Travel
# MAGIC ##### 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-08-09T22:47:26.000+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC #### By Using Pyspark

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-08-09T22:47:26.000+00:00').load(f"{demo_folder_path}/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### If we Want to Delete the Data Immediately

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-08-09T21:59:24.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge
# MAGIC order by driverid asc

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 9 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC #### For Parquet File

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save(f"{demo_folder_path}/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@formula001adls.dfs.core.windows.net/drivers_convert_to_delta_new`
