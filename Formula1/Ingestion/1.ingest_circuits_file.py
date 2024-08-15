# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv File in this Notebook

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

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV File using Spark API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1 - Select the Required Columns

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")  \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 - Add Ingestion Date to the Dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4  - Write data to  the Data Lake as Delta

# COMMAND ----------

# # Drop the table if it exists
# spark.sql("DROP TABLE IF EXISTS f1_processed.circuits")

# # Remove the existing location
# dbutils.fs.rm("abfss://processed@formula001adls.dfs.core.windows.net/circuits", recurse=True)

# Write the DataFrame to the table again
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# overwrite_partition(circuits_final_df, 'f1_processed', 'circuits', 'race_id')

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.circuits
# WHERE name = 'Albert Park Grand Prix Circuit'

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM f1_processed.circuits
# MAGIC
