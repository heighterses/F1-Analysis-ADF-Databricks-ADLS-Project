-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """ </head><body><div style="text-align: center;"><h1>Report on Dominant F1 Drivers</h1></div></body></html>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT 
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results

GROUP BY driver_name
HAVING COUNT(1) >=50
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers WHERE driver_rank <= 10)

GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers WHERE driver_rank <= 10)

GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers WHERE driver_rank <= 10)

GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;
