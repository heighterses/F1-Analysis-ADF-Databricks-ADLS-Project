# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructor.name AS team_name,
                     driver.driver_id,
                     driver.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.driver ON (results.driver_id = driver.driver_id)
                JOIN f1_processed.constructor ON (results.constructor_id = constructor.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM f1_presentation.calculated_race_results

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
# USING parquet
# AS
# SELECT 
# ra.race_year,
# c.name AS team_name, 
# d.name AS driver_name, 
# r.position, 
# r.points,
# 11 - r.position AS calculated_points

# FROM results as r
# JOIN f1_processed.driver as d ON (r.driver_id = d.driver_id)
# JOIN f1_processed.constructor as c ON (r.constructor_id = c.constructor_id)
# JOIN f1_processed.races as ra ON (r.race_id = ra.race_id)
# WHERE r.position <= 10;


# COMMAND ----------

# %sql
# USE f1_presentation;

# SELECT * from calculated_race_results
