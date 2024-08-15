# Databricks notebook source
# MAGIC %md
# MAGIC # Injest Drivers Json File

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# dbutils.widgets.text("p_data_source", "")
# v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the Naming Nested Schema  

# COMMAND ----------

name_schema = StructType(fields=[ StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define The Whole Schema

# COMMAND ----------

drivers_schema = StructType(fields=[ StructField("driverId", IntegerType(), False),
                                  StructField("driverRef", StringType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("code", StringType(), True),
                                  StructField("name", name_schema),
                                  StructField("dob", DateType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True)  


])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                                        .withColumnRenamed("driverRef", "driver_ref")\
                                                            .withColumn("ingestion_date", current_timestamp())\
                                                                .withColumn("name", concat(col("name.forename"), lit(""), col("name.surname"))) \
                                                                    .withColumn("file_date", lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# # Drop the table if it exists
# spark.sql("DROP TABLE IF EXISTS f1_processed.driver")


# # Remove the existing location
# dbutils.fs.rm("abfss://processed@formula001adls.dfs.core.windows.net/driver", recurse=True)

# Write the DataFrame to the table again
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.driver")

# drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.driver")

# COMMAND ----------

# overwrite_partition(drivers_final_df, 'f1_processed', 'drivers', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.driver
