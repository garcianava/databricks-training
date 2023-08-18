-- Databricks notebook source
-- drop main table if it already exists in the workspace

DROP TABLE IF EXISTS diamonds;

-- COMMAND ----------

-- create the main table in the workspace, use a CSV file from Databricks Datasets in the cluster

CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- how many rows in the dataset?

SELECT count(*) FROM diamonds;

-- COMMAND ----------

-- change the cell to Python, then create a Delta table from the CSV file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").mode("overwrite").save("/delta/diamonds")

-- COMMAND ----------

-- drop the main table and generate it again, but this time from the saved Delta table
-- it is much faster!

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

-- confirm the table is active again in the workspace

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results
-- MAGIC Specifically, the command:
-- MAGIC
-- MAGIC 1. Selects color and price columns, averages the price, and groups and orders by color.
-- MAGIC 1. Displays a table of the results.

-- COMMAND ----------

WITH average_prices_by_color (color, average_price) AS (
  SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color)
SELECT color, average_price
FROM average_prices_by_color
WHERE average_price > 4000

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------


