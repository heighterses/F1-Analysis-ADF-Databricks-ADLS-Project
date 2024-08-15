# Databricks notebook source
# MAGIC %md
# MAGIC # Injest Constructor File in JSON Format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")
display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 Write output to parquet file

# COMMAND ----------

# # Drop the table if it exists
# spark.sql("DROP TABLE IF EXISTS f1_processed.constructor")

# # Remove the existing location
# dbutils.fs.rm("abfss://processed@formula001adls.dfs.core.windows.net/constructor", recurse=True)


# # Write the DataFrame to the table again
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")


# constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

# overwrite_partition(constructor_final_df, 'f1_processed', 'constructor', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructor
