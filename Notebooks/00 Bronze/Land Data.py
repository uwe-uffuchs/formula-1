# Databricks notebook source
# Import Libraries
import requests
import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, lit
from pyspark.sql.types import StringType, IntegerType
from typing import Dict

# COMMAND ----------

# Set up Notebook inputs & global variables
dbutils.widgets.text('endPoints', 'seasons,circuits,drivers,constructors,status')
# Full Datasets (i.e, single page): seasons,circuits,drivers,constructors,status
# Order of the import: 1. Full Datasets; 2. schedule, results, qualifying; 3. laps, driverStandings, constructorStandings
dbutils.widgets.text('fileType', 'json')
dbutils.widgets.text('limit', '1000')
dbutils.widgets.text('offset', '0')
dbutils.widgets.text('destination', 'bronze')
dbutils.widgets.text('runId', '1')

endPoints = dbutils.widgets.get('endPoints')
fileType = dbutils.widgets.get('fileType')
limit = int(dbutils.widgets.get('limit'))
offset = int(dbutils.widgets.get('offset'))
destination = dbutils.widgets.get('destination')
runId = dbutils.widgets.get('runId')

# Since this is used in a SQL statement - let's protect against SQL injection
destination = destination.replace(';', '').replace('\'', '')

bronzeFolder = '/mnt/sadlformula1/' + destination + '/'

# COMMAND ----------

def renameDfColumns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    return df.select(*[col(colName).alias(columns.get(colName, colName)) for colName in df.columns])

# COMMAND ----------

def updateColumnNames(df: DataFrame, index: int) -> DataFrame:
    dfTemp = df
    allColumns = dfTemp.columns
    newColumns = dict((column, f'{column}*{index}') for column in allColumns)
    dfTemp = dfTemp.transform(lambda df_x: renameDfColumns(df_x, newColumns))
    return dfTemp

# COMMAND ----------

def flattenJson(df_arg: DataFrame, runId: IntegerType, index: int = 1) -> DataFrame:
    try:
        # Update all column to have index = 1
        df = updateColumnNames(df_arg, index) if index == 1 else df_arg

        # Get all field names
        fields = df.schema.fields

        # For all the columns
        for field in fields:
            dataType = str(field.dataType)
            columnName = field.name

            first10Char = dataType[0:10]

            # If it is an array
            if first10Char == 'ArrayType(':
                # Explode array
                dfTemp = df.withColumn(columnName, explode_outer(col(columnName)))
                return flattenJson(dfTemp, runId, index + 1)

            # If it is a json object
            elif first10Char == 'StructType':
                currentColumn = columnName
                appendString = currentColumn

                # Get datatype of the current column
                dataTypeString = str(df.schema[currentColumn].dataType)

                # Change the column name if the current column name already exists for type string
                dfTemp = df.withColumnRenamed(columnName, columnName + '#1') \
                    if columnName in dataTypeString else df
                currentColumn = currentColumn + '#1' if columnName in dataTypeString else currentColumn

                # Expand the struct column values
                dfBeforeExpand = dfTemp.select(f'{currentColumn}.*')
                newColumns = dfBeforeExpand.columns

                # Find next level value for the column
                beginIndex = appendString.rfind('*')
                endIndex = len(appendString)
                level = appendString[beginIndex + 1: endIndex]
                nextLevel = int(level) + 1

                # Update the column names in the new level
                customColumns = dict((field, f'{appendString}->{field}*{nextLevel}') for field in newColumns)
                dfTemp2 = dfTemp.select('*', f'{currentColumn}.*').drop(currentColumn)
                dfTemp3 = dfTemp2.transform(lambda df_x: renameDfColumns(df_x, customColumns))
                return flattenJson(dfTemp3, runId, index + 1)

        df = df.withColumnRenamed('MRData*1->total*2', 'total') \
        .withColumn('runId', lit(runId)) \
        .drop('MRData*1->total*2', 'MRData*1->limit*2', 'MRData*1->offset*2', 'MRData*1->series*2', 'MRData*1->url*2', 'MRData*1->xmlns*2')
        return df
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'Failure: Unable to flatten Json response. Exception: {error}')

# COMMAND ----------

def getData(endPoint: StringType, fileType: StringType, limit: IntegerType, offset: IntegerType) -> StringType:
    try:
        response = requests.request('GET', f'http://ergast.com/api/f1/{endPoint}.{fileType}?limit={limit}&offest={offset}')
        if response != None and response.status_code == 200:
            return response.text
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'Failure: Get data from \'http://ergast.com/api/f1/{endPoint}.{fileType}?limit={limit}&offest={offset}\' failed. Exception: {error}')

# COMMAND ----------

def readData(jsonText: StringType, runId: IntegerType) -> DataFrame:
    try:
        df = spark.read.json(sc.parallelize([jsonText]))
        df = flattenJson(df, runId)
        return df
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'Failure: Converting Json text to dataframe. Exception: {error}')

# COMMAND ----------

def createDbIfNotExists():
    try:
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {destination} LOCATION \'{bronzeFolder}\';')
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'Failure: Creating bronze database (SQL executed: \'CREATE DATABASE IF NOT EXISTS {destination} LOCATION \'{bronzeFolder}\';)\'. Exception: {error}')

# COMMAND ----------

def importDataset(df: DataFrame, dataSet: StringType):
    try:
        df.write.mode('append').option('mergeSchema', 'true').format('delta').saveAsTable(destination + '.' + dataSet)
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'Failure: Importing / creating delta table ({dataSet}) in {destination} database. Exception: {error}')

# COMMAND ----------

def landData (endPoints: StringType):
    try:
        res = None
        offset = 0
        offset = offset
        errorMessage = f'Failure: Landing data'
        runId = createDbIfNotExists()
        for endPoint in endPoints.split(','):
            errorMessage = errorMessage + f'. Importing data ({endPoint})'
            if (endPoint == 'seasons') or (endPoint == 'circuits') or (endPoint == 'drivers') or (endPoint == 'constructors') or (endPoint == 'status'):
                errorMessage = errorMessage + '. Retrieving JSON text from the API'
                jsonText = getData(endPoint, fileType, limit, offset)
                errorMessage = errorMessage + '. Reading and converting JSON text to DataFrame'
                df = readData(jsonText, runId)
                errorMessage = errorMessage + f'. Importing dataset into deltaTable ({destination}.{endPoint}) "{endPoint} Response: {jsonText}"'
                importDataset(df, endPoint)
            elif (endPoint == 'schedule') or (endPoint == 'results') or (endPoint == 'qualifying'):
                seasonsArray = spark.read.table(f'{destination}.seasons').select(col('MRData*1->SeasonTable*2->Seasons*3->season*4').alias('season')).distinct().collect()
                for row in seasonsArray:
                    pageNo = 1
                    errorMessage = errorMessage + f'. Retrieving JSON text from the API (season = {row.season}; page = {pageNo})'
                    if (endPoint == 'schedule'):
                        apiEndPoint = row.season
                    else:
                        apiEndPoint = row.season + '/' + endPoint
                    jsonText = getData(apiEndPoint, fileType, limit, offset)
                    errorMessage = errorMessage + '. Reading and converting JSON text to DataFrame'
                    df = readData(jsonText, runId)
                    errorMessage = errorMessage + f'. Importing dataset into deltaTable ({destination}.{endPoint}) "{apiEndPoint} Response: {jsonText}"'
                    importDataset(df, endPoint)
                    totalRecords = df.collect()[0]['total']
                    while int(totalRecords) > offset:
                        offset = offset + limit
                        errorMessage = errorMessage + f'. Retrieving JSON text from the API (season = {row.season}; page = {pageNo})'
                        jsonText = getData(row.season + '/' + endPoint, fileType, limit, offset)
                        errorMessage = errorMessage + '. Reading and converting JSON text to DataFrame'
                        df = readData(jsonText, runId)
                        errorMessage = errorMessage + f'. Importing dataset into deltaTable ({destination}.{endPoint}) "{apiEndPoint} Response: {jsonText}"'
                        importDataset(df, endPoint)
            elif (endPoint == 'constructorStandings') or (endPoint == 'driverStandings') or (endPoint == 'laps'):
                scheduleArray = spark.read.table(f'{destination}.schedule').select(col('MRData*1->RaceTable*2->Races*3->season*4').alias('season'), col('MRData*1->RaceTable*2->Races*3->round*4').alias('round')).distinct().collect()
                for row in scheduleArray:
                    pageNo = 1
                    errorMessage = errorMessage + f'. Retrieving JSON text from the API (season = {row.season}; round = {row.round}; page = {pageNo})'
                    jsonText = getData(row.season + '/' + row.round + '/' + endPoint, fileType, limit, offset)
                    errorMessage = errorMessage + '. Reading and converting JSON text to DataFrame'
                    df = readData(jsonText, runId)
                    errorMessage = errorMessage + f'. Importing dataset into deltaTable ({destination}.{endPoint}) "{row.season} + \'/\' + {row.round} + \'/\' + {endPoint} Response: {jsonText}"'
                    importDataset(df, endPoint)
                    totalRecords = df.collect()[0]['total']
                    while int(totalRecords) > offset:
                        offset = offset + limit
                        errorMessage = errorMessage + f'. Retrieving JSON text from the API (season = {row.season}; round = {row.round}; page = {pageNo})'
                        jsonText = getData(row.season + '/' + row.round + '/' + endPoint, fileType, limit, offset)
                        errorMessage = errorMessage + '. Reading and converting JSON text to DataFrame'
                        df = readData(jsonText, runId)
                        errorMessage = errorMessage + f'. Importing dataset into deltaTable ({destination}.{endPoint}) "{row.season} + \'/\' + {row.round} + \'/\' + {endPoint} Response: {jsonText}"'
                        importDataset(df, endPoint)
    except Exception as e:
        error = str(e)
        dbutils.notebook.exit(f'{errorMessage}. Exception: {error}')

# COMMAND ----------

landData(endPoints)

# COMMAND ----------

dbutils.notebook.exit("Success")
