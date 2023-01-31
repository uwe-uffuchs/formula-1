# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanse & Transform Silver Data
# MAGIC ### Creating commonly used objects

# COMMAND ----------

import pytz
import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Specify mount locations
silverFolder = '/mnt/sadlformula1/silver/'
goldFolder = '/mnt/sadlformula1/gold/'

# Get list of subfolders in mount locations
silverSubFolders = dbutils.fs.ls(silverFolder)
newestFolder = ''

# Get ingestion time as local time
ingestionDate = from_utc_timestamp(current_timestamp(), 'Pacific/Auckland')
utc_now = pytz.utc.localize(datetime.datetime.utcnow())
auckNow = utc_now.astimezone(pytz.timezone('Pacific/Auckland'))

# Find the subfolder that was last added (i.e., the one with the greatest modification time)
modificationTime = 0
for folder in silverSubFolders:
    if modificationTime < folder.modificationTime:
        newestFolder = folder.name

# COMMAND ----------

# Get required datasets
driversdf = spark.read.parquet(silverFolder + newestFolder + '/drivers')
constructorsdf = spark.read.parquet(silverFolder + newestFolder + '/constructors')
circuitsdf = spark.read.parquet(silverFolder + newestFolder + '/circuits')
#seasonsdf = spark.read.parquet(silverFolder + newestFolder + '/seasons')
#scheduledf = spark.read.parquet(silverFolder + newestFolder + '/schedule')
#qualifyingdf = spark.read.parquet(silverFolder + newestFolder + '/qualifying')
#statusdf = spark.read.parquet(silverFolder + newestFolder + '/status')
resultsdf = spark.read.parquet(silverFolder + newestFolder + '/results')
pitstopsdf = spark.read.parquet(silverFolder + newestFolder + '/pitstops')
#lapsdf = spark.read.parquet(silverFolder + newestFolder + '/laps')
#constructorStandingsdf = spark.read.parquet(silverFolder + newestFolder + '/constructorStandings')
#driverStandingsdf = spark.read.parquet(silverFolder + newestFolder + '/driverStandings')

# COMMAND ----------

# Get number of pitstops per race per driver
pitstopsdf = pitstopsdf.groupBy('circuitRef', 'date', 'driverRef').agg(max('stopNo').alias('numberOfPitstops'))

# Get fastest lap per race
fastestlapdf = resultsdf.groupBy('circuitRef', 'date', 'fastestLapTime').agg(max('fastestLapTime').alias('fastestLap'))

# COMMAND ----------

# Join required datasets
gpresultsdf = resultsdf \
.join(driversdf, driversdf.driverRef == resultsdf.driverRef, 'inner') \
.join(constructorsdf, constructorsdf.constructorRef == resultsdf.constructorRef, 'inner') \
.join(circuitsdf, circuitsdf.circuitRef == resultsdf.circuitRef, 'inner') \
.join(pitstopsdf, (pitstopsdf.circuitRef == resultsdf.circuitRef) & (pitstopsdf.date == resultsdf.date) & (pitstopsdf.driverRef == resultsdf.driverRef)) \
.select(resultsdf.raceName \
        , circuitsdf.location \
        , circuitsdf.circuitName \
        , resultsdf.date \
        , resultsdf.position \
        , driversdf.nationality \
        , driversdf.firstName \
        , driversdf.surname \
        , driversdf.permanentNumber \
        , constructorsdf.constructorName \
        , resultsdf.startGrid \
        , pitstopsdf.numberOfPitstops \
        , resultsdf.fastestLapTime \
        , resultsdf.raceTime \
        , resultsdf.status \
        , resultsdf.points)

#display(gpresultsdf)
# Write this file to Gold
gpresultsdf.write.mode('overwrite').parquet(goldFolder + newestFolder + '/raceResults')

# COMMAND ----------

driverstandingsdf = gpresultsdf \
.withColumn('year', year(col('date')).cast(IntegerType())) \
.drop('date') \
.groupBy('year', 'firstName', 'surname', 'nationality', 'constructorName') \
.agg(sum('points').alias('totalPoints'),
    count(when(col('position') == 1, True)).alias('wins'))

driverRank = Window.partitionBy('year').orderBy(desc('totalPoints'), desc('wins'))

driverstandingsdf = driverstandingsdf.withColumn('position', rank().over(driverRank))

driverstandingsdf.write.mode('overwrite').parquet(goldFolder + newestFolder + '/driverStandings')

# COMMAND ----------

constructorstandingsdf = gpresultsdf \
.withColumn('year', year(col('date')).cast(IntegerType())) \
.drop('date') \
.groupBy('year', 'constructorName') \
.agg(sum('points').alias('totalPoints'),
    count(when(col('position') == 1, True)).alias('wins'))

constructorRank = Window.partitionBy('year').orderBy(desc('totalPoints'), desc('wins'))

constructorstandingsdf = constructorstandingsdf.withColumn('position', rank().over(constructorRank))

constructorstandingsdf.write.mode('overwrite').parquet(goldFolder + newestFolder + '/constructorStandings')
