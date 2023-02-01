-- Databricks notebook source
-- MAGIC %python
-- MAGIC drivershtml = """<h1 style="color:Black;text-align:center;font-family:Arial">Report on Dominant Drivers"""
-- MAGIC displayHTML(drivershtml)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructorshtml = """<h1 style="color:Black;text-align:center;font-family:Arial">Report on Dominant Constructors"""
-- MAGIC displayHTML(constructorshtml)

-- COMMAND ----------

USE silver;

-- COMMAND ----------

DROP TABLE IF EXISTS raceResults;
CREATE TABLE raceResults
USING parquet
AS
SELECT
  YEAR(results.date) AS year,
  constructors.constructorName,
  CONCAT(drivers.firstName, ' ', drivers.surname) AS driverName,
  results.position,
  results.points,
  11 - results.position AS adjustedPoints
FROM
  results
  
  JOIN drivers ON
    drivers.driverRef = results.driverRef
  
  JOIN constructors ON
    constructors.constructorRef = results.constructorRef
  
  JOIN circuits ON
    circuits.circuitRef = results.circuitRef
WHERE
  results.position <= 10;

-- COMMAND ----------

SELECT
  year,
  driverName,
  COUNT(1) AS totalRaces,
  SUM(adjustedPoints) AS totalAdjustedPoints,
  AVG(adjustedPoints) AS avgAdjustedPoints
FROM
  raceResults
GROUP BY
  driverName,
  year
ORDER BY
  year,
  avgAdjustedPoints DESC,
  totalAdjustedPoints DESC;

-- COMMAND ----------

SELECT
  year,
  constructorName,
  COUNT(1) AS totalRaces,
  SUM(adjustedPoints) AS totalAdjustedPoints,
  AVG(adjustedPoints) AS avgAdjustedPoints
FROM
  raceResults
GROUP BY
  constructorName,
  year
ORDER BY
  year,
  avgAdjustedPoints DESC,
  totalAdjustedPoints DESC;

-- COMMAND ----------

SELECT
  raceResults.driverName,
  COUNT(1) AS totalRaces,
  SUM(raceResults.adjustedPoints) AS totalAdjustedPoints,
  AVG(raceResults.adjustedPoints) AS avgAdjustedPoints,
  RANK() OVER (ORDER BY AVG(raceResults.adjustedPoints) DESC) AS rank
FROM
  raceResults
GROUP BY
  raceResults.driverName
HAVING
  COUNT(1) >= 8
ORDER BY
  avgAdjustedPoints DESC

-- COMMAND ----------

WITH drivers_rank AS (
  SELECT
    raceResults.driverName,
    COUNT(1) AS totalRaces,
    SUM(raceResults.adjustedPoints) AS totalAdjustedPoints,
    AVG(raceResults.adjustedPoints) AS avgAdjustedPoints,
    RANK() OVER (ORDER BY AVG(raceResults.adjustedPoints) DESC) AS rank
  FROM
    raceResults
  GROUP BY
    raceResults.driverName
  HAVING
    COUNT(1) >= 50
)
SELECT
  raceResults.year,
  raceResults.driverName,
  COUNT(1) AS totalRaces,
  SUM(raceResults.adjustedPoints) AS totalAdjustedPoints,
  AVG(raceResults.adjustedPoints) AS avgAdjustedPoints
FROM
  raceResults
  
  JOIN drivers_rank ON
    raceResults.driverName = drivers_rank.driverName
WHERE
  drivers_rank.rank <= 15
GROUP BY
  raceResults.driverName,
  raceResults.year
ORDER BY
  raceResults.year,
  avgAdjustedPoints DESC,
  totalAdjustedPoints DESC;

-- COMMAND ----------

WITH constructors_rank AS (
  SELECT
    raceResults.constructorName,
    COUNT(1) AS totalRaces,
    SUM(raceResults.adjustedPoints) AS totalAdjustedPoints,
    AVG(raceResults.adjustedPoints) AS avgAdjustedPoints,
    RANK() OVER (ORDER BY AVG(raceResults.adjustedPoints) DESC) AS rank
  FROM
    raceResults
  GROUP BY
    raceResults.constructorName
  HAVING
    COUNT(1) >= 100
)
SELECT
  raceResults.year,
  raceResults.constructorName,
  COUNT(1) AS totalRaces,
  SUM(raceResults.adjustedPoints) AS totalAdjustedPoints,
  AVG(raceResults.adjustedPoints) AS avgAdjustedPoints
FROM
  raceResults
  
  JOIN constructors_rank ON
    raceResults.constructorName = constructors_rank.constructorName
WHERE
  constructors_rank.rank <= 5
GROUP BY
  raceResults.constructorName,
  raceResults.year
ORDER BY
  raceResults.year,
  avgAdjustedPoints DESC,
  totalAdjustedPoints DESC;
