# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/driver")  \
    .withColumnRenamed("name","driver_name") \
        .withColumnRenamed("nationality", "driver_nationality") \
            .withColumnRenamed("number", "driver_number")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor") \
    .withColumnRenamed("name","constructor_name")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
        .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the Race and Circuits Dataframe..

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Results to all of the Data Frames

# COMMAND ----------

race_results_df = results_df \
    .join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "constructor_name", "grid", "fastest_lap","race_time", "points", "position", "result_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transorm Data to Presentation Folder in Azure Data Lake Storage

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Method to Transfer Data to Presentation Folder

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id desc
