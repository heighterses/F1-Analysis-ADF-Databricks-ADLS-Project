# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv File in this Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV File using Spark API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType


# COMMAND ----------

races_schema = StructType( fields=[ 
                                   
StructField("raceId", IntegerType(), False),
StructField("year", IntegerType(), True),
StructField("round", IntegerType(), True),
StructField("circuitId", IntegerType(),  True),
StructField("name", StringType(), True),
StructField("date", DateType(), True),
StructField("time", StringType(), True),
StructField("url", StringType(), True),



])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1 - Add ingestion date and race_timestamp columns to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                      .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2 - Select the Required Columns & renamed as required

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

races_final_df = races_selected_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3  - Write data to  the Data Lake as Delta
# MAGIC

# COMMAND ----------

# # Drop the table if it exists
# spark.sql("DROP TABLE IF EXISTS f1_processed.races")

# # Remove the existing location
# dbutils.fs.rm("abfss://processed@formula001adls.dfs.core.windows.net/races", recurse=True)

# Write the DataFrame to the table again
races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# overwrite_partition(races_final_df, 'f1_processed', 'races', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM f1_processed.races
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
