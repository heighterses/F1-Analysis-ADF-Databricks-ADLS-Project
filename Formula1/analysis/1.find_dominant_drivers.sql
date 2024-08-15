-- Databricks notebook source
SELECT 
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >=50
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT 
driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY driver_name
HAVING total_races >=50
ORDER BY average_points DESC;
