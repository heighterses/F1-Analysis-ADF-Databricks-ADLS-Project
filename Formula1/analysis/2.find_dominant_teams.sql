-- Databricks notebook source
SELECT
team_name,
count(1) AS total_races,
sum(calculated_points) AS total_points,
avg(calculated_points) AS average_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2021
GROUP BY team_name
HAVING COUNT(1) >=100
ORDER BY average_points DESC

