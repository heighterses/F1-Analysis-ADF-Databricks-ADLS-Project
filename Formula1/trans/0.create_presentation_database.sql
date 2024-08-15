-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@formula001adls.dfs.core.windows.net/"

-- COMMAND ----------

DESCRIBE DATABASE f1_presentation
