-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """ </head><body><div style="text-align: center;"><h1>Report on Dominant F1 Teams</h1></div></body></html>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT 
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_driver_points,
AVG(calculated_points) AS average_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) teams_rank
FROM f1_presentation.calculated_race_results

GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_team_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams WHERE teams_rank <= 5)

GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_team_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_teams WHERE teams_rank <= 5)

GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT 
race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_team_points,
AVG(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams WHERE teams_rank <= 5)

GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;
