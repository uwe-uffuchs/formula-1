# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Bronze Data
# MAGIC ### Creating commonly used objects

# COMMAND ----------

from typing import Final, Dict, Tuple
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.window import Window
import pytz
import datetime

# Specify mount locations
bronzeFolder = '/mnt/sadlformula1/bronze/'
silverFolder = '/mnt/sadlformula1/silver/'

# Get list of subfolders in mount locations
bronzeSubFolders = dbutils.fs.ls(bronzeFolder)
newestFolder = ''

# Get ingestion time as local time
ingestionDate = from_utc_timestamp(current_timestamp(), 'Pacific/Auckland')
utc_now = pytz.utc.localize(datetime.datetime.utcnow())
auckNow = utc_now.astimezone(pytz.timezone('Pacific/Auckland'))

# Find the subfolder that was last added (i.e., the one with the greatest modification time)
modificationTime = 0
for folder in bronzeSubFolders:
    if modificationTime < folder.modificationTime:
        modificationTime = folder.modificationTime
        newestFolder = folder.name
        
# Get list of files to ingest
foldersToIngest = bronzeFolder + newestFolder
        
# Functions to flatten JSON files

def renameDfColumns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    return df.select(*[col(colName).alias(columns.get(colName, colName)) for colName in df.columns])

def updateColumnNames(df: DataFrame, index: int) -> DataFrame:
    dfTemp = df
    allColumns = dfTemp.columns
    newColumns = dict((column, f'{column}*{index}') for column in allColumns)
    dfTemp = dfTemp.transform(lambda df_x: renameDfColumns(df_x, newColumns))
    
    return dfTemp

def flattenJson(df_arg: DataFrame, index: int = 1) -> DataFrame:
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
            return flattenJson(dfTemp, index + 1)
        
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
            return flattenJson(dfTemp3, index + 1)
        
    return df

def appendToFile(df: DataFrame, df_new: DataFrame) -> DataFrame:
    df = df.append(df_new)
    return df

def readFiles(folder: StringType) -> DataFrame:
    df = spark.read.json(folder)
    return df

def renameAddSchema(df: DataFrame, dataset: StringType) -> DataFrame:
    df = df.withColumn('ingestionDate', ingestionDate) \
    .drop('MRData*1->limit*2', 'MRData*1->offset*2', 'MRData*1->series*2', 'MRData*1->total*2', 'MRData*1->url*2', 'MRData*1->xmlns*2')

    if dataset == 'circuits/':
        df = df.withColumnRenamed('MRData*1->CircuitTable*2->Circuits*3->circuitId*4', 'circuitRef') \
        .withColumnRenamed('MRData*1->CircuitTable*2->Circuits*3->circuitName*4', 'circuitName') \
        .withColumnRenamed('MRData*1->CircuitTable*2->Circuits*3->url*4', 'aboutUrl') \
        .withColumnRenamed('MRData*1->CircuitTable*2->Circuits*3->Location*4->country*5', 'country') \
        .withColumnRenamed('MRData*1->CircuitTable*2->Circuits*3->Location*4->locality*5', 'location') \
        .withColumn('latitude', col('MRData*1->CircuitTable*2->Circuits*3->Location*4->lat*5').cast(DoubleType())) \
        .withColumn('longitude', col('MRData*1->CircuitTable*2->Circuits*3->Location*4->long*5').cast(DoubleType())).drop('MRData*1->CircuitTable*2->Circuits*3->Location*4->long*5', 'MRData*1->CircuitTable*2->Circuits*3->Location*4->lat*5')
        return df

    if dataset == 'schedule/':
        df = df.withColumnRenamed('MRData*1->RaceTable*2->Races*3->raceName*4', 'raceName') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->url*4', 'aboutUrl') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
        .withColumn('year', col('MRData*1->RaceTable*2->Races*3->season*4').cast(IntegerType())) \
        .withColumn('round', col('MRData*1->RaceTable*2->Races*3->round*4').cast(IntegerType())) \
        .withColumn('MRData*1->RaceTable*2->Races*3->time*4', regexp_replace('MRData*1->RaceTable*2->Races*3->time*4', 'Z', '')) \
        .withColumn('MRData*1->RaceTable*2->Races*3->FirstPractice*4->time*5', regexp_replace('MRData*1->RaceTable*2->Races*3->FirstPractice*4->time*5', 'Z', '')) \
        .withColumn('MRData*1->RaceTable*2->Races*3->SecondPractice*4->time*5', regexp_replace('MRData*1->RaceTable*2->Races*3->SecondPractice*4->time*5', 'Z', '')) \
        .withColumn('MRData*1->RaceTable*2->Races*3->ThirdPractice*4->time*5', regexp_replace('MRData*1->RaceTable*2->Races*3->ThirdPractice*4->time*5', 'Z', '')) \
        .withColumn('MRData*1->RaceTable*2->Races*3->Qualifying*4->time*5', regexp_replace('MRData*1->RaceTable*2->Races*3->Qualifying*4->time*5', 'Z', '')) \
        .withColumn('MRData*1->RaceTable*2->Races*3->Sprint*4->time*5', regexp_replace('MRData*1->RaceTable*2->Races*3->Sprint*4->time*5', 'Z', '')) \
        .withColumn('dateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->date*4'), lit(' '), col('MRData*1->RaceTable*2->Races*3->time*4')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('FP1DateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->FirstPractice*4->date*5'), lit(' '), col('MRData*1->RaceTable*2->Races*3->FirstPractice*4->time*5')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('FP2DateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->SecondPractice*4->date*5'), lit(' '), col('MRData*1->RaceTable*2->Races*3->SecondPractice*4->time*5')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('FP3DateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->ThirdPractice*4->date*5'), lit(' '), col('MRData*1->RaceTable*2->Races*3->ThirdPractice*4->time*5')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('qualifyingDateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->Qualifying*4->date*5'), lit(' '), col('MRData*1->RaceTable*2->Races*3->Qualifying*4->time*5')), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('sprintDateTime', to_timestamp(concat(col('MRData*1->RaceTable*2->Races*3->Sprint*4->date*5'), lit(' '), col('MRData*1->RaceTable*2->Races*3->Sprint*4->time*5')), 'yyyy-MM-dd HH:mm:ss')) \
        .drop('MRData*1->RaceTable*2->season*3', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->time*4', 'MRData*1->RaceTable*2->Races*3->Sprint*4->date*5', 'MRData*1->RaceTable*2->Races*3->Sprint*4->time*5', 'MRData*1->RaceTable*2->Races*3->Qualifying*4->date*5', 'MRData*1->RaceTable*2->Races*3->Qualifying*4->time*5', 'MRData*1->RaceTable*2->Races*3->ThirdPractice*4->date*5', 'MRData*1->RaceTable*2->Races*3->ThirdPractice*4->time*5', 'MRData*1->RaceTable*2->Races*3->SecondPractice*4->date*5', 'MRData*1->RaceTable*2->Races*3->SecondPractice*4->time*5', 'MRData*1->RaceTable*2->Races*3->FirstPractice*4->date*5', 'MRData*1->RaceTable*2->Races*3->FirstPractice*4->time*5', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->round*4')
        return df

    if dataset == 'constructors/':
        df = df.withColumnRenamed('MRData*1->ConstructorTable*2->Constructors*3->constructorId*4', 'constructorRef') \
        .withColumnRenamed('MRData*1->ConstructorTable*2->Constructors*3->name*4', 'constructorName') \
        .withColumnRenamed('MRData*1->ConstructorTable*2->Constructors*3->url*4', 'aboutUrl') \
        .withColumnRenamed('MRData*1->ConstructorTable*2->Constructors*3->nationality*4', 'nationality')
        return df

    if dataset == 'constructorStandings/':
        df = df.withColumn('points', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->points*5').cast(DoubleType())) \
        .withColumn('position', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->position*5').cast(IntegerType())) \
        .withColumn('wins', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->wins*5').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->constructorId*6', 'constructorRef') \
        .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->positionText*5', 'positionText') \
        .drop('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->wins*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->position*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->points*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->name*6', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->round*4', 'MRData*1->StandingsTable*2->StandingsLists*3->season*4')
        return df

    if dataset == 'drivers/':
        df = df.withColumn('dateOfBirth', col('MRData*1->DriverTable*2->Drivers*3->dateOfBirth*4').cast(DateType())) \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->driverId*4', 'driverRef') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->familyName*4', 'surname') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->givenName*4', 'firstName') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->nationality*4', 'nationality') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->code*4', 'code') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->permanentNumber*4', 'permanentNumber') \
        .withColumnRenamed('MRData*1->DriverTable*2->Drivers*3->url*4', 'url') \
        .drop('MRData*1->DriverTable*2->Drivers*3->dateOfBirth*4')
        return df

    if dataset == 'driverStandings/':
        df = df.withColumn('round', col('MRData*1->StandingsTable*2->StandingsLists*3->round*4').cast(IntegerType())) \
        .withColumn('season', col('MRData*1->StandingsTable*2->StandingsLists*3->season*4').cast(IntegerType())) \
        .withColumn('points', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->points*5').cast(DoubleType())) \
        .withColumn('position', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->position*5').cast(IntegerType())) \
        .withColumn('wins', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->wins*5').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->constructorId*6', 'constructorRef') \
        .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->driverId*6', 'driverRef') \
        .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->positionText*5', 'positionText') \
        .drop('MRData*1->StandingsTable*2->StandingsLists*3->round*4', 'MRData*1->StandingsTable*2->StandingsLists*3->season*4', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->points*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->position*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->wins*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->name*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->dateOfBirth*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->familyName*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->givenName*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->code*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->permanentNumber*6')
        return df

    if dataset == 'laps/':
        df = df.withColumn('lapNumber', col('MRData*1->RaceTable*2->Races*3->Laps*4->number*5').cast(IntegerType())) \
        .withColumn('position', col('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->position*6').cast(IntegerType())) \
        .withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->driverId*6', 'driverRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->time*6', 'lapTime') \
        .drop('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->position*6', 'MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->Laps*4->number*5', 'MRData*1->RaceTable*2->Races*3->raceName*4', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->round*3', 'MRData*1->RaceTable*2->season*3', 'MRData*1->RaceTable*2->Races*3->time*4')
        return df

    if dataset == 'pitstops/':
        df = df.withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
        .withColumn('lapNumber', col('MRData*1->RaceTable*2->Races*3->PitStops*4->lap*5').cast(IntegerType())) \
        .withColumn('stopNo', col('MRData*1->RaceTable*2->Races*3->PitStops*4->stop*5').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->driverId*5', 'driverRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->duration*5', 'pitstopDuration') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->time*5', 'pitstopTime') \
        .drop('MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->PitStops*4->lap*5', 'MRData*1->RaceTable*2->Races*3->PitStops*4->stop*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->round*3', 'MRData*1->RaceTable*2->season*3', 'MRData*1->RaceTable*2->Races*3->raceName*4', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->time*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5')
        return df

    if dataset == 'qualifying/':
        df = df.withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
        .withColumn('position', col('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->position*5').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Constructor*5->constructorId*6', 'constructorRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->driverId*6', 'driverRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Q1*5', 'q1') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Q2*5', 'q2') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Q3*5', 'q3') \
        .drop('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->position*5', 'MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Constructor*5->name*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Constructor*5->nationality*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Constructor*5->url*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->dateOfBirth*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->familyName*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->givenName*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->nationality*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->url*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->number*5', 'MRData*1->RaceTable*2->Races*3->raceName*4', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->time*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->code*6', 'MRData*1->RaceTable*2->Races*3->QualifyingResults*4->Driver*5->permanentNumber*6')
        return df

    if dataset == 'results/':
        df = df.withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
        .withColumn('startGrid', col('MRData*1->RaceTable*2->Races*3->Results*4->grid*5').cast(IntegerType())) \
        .withColumn('lapsCompleted', col('MRData*1->RaceTable*2->Races*3->Results*4->laps*5').cast(IntegerType())) \
        .withColumn('number', col('MRData*1->RaceTable*2->Races*3->Results*4->number*5').cast(IntegerType())) \
        .withColumn('points', col('MRData*1->RaceTable*2->Races*3->Results*4->points*5').cast(DoubleType())) \
        .withColumn('position', col('MRData*1->RaceTable*2->Races*3->Results*4->position*5').cast(IntegerType())) \
        .withColumn('millis', col('MRData*1->RaceTable*2->Races*3->Results*4->Time*5->millis*6').cast(IntegerType())) \
        .withColumn('fastestLapAverageSpeed', col('MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->AverageSpeed*6->speed*7').cast(DoubleType())) \
        .withColumn('fastestLapNumber', col('MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->lap*6').cast(IntegerType())) \
        .withColumn('fastestLapRank', col('MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->rank*6').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->raceName*4', 'raceName') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->Constructor*5->constructorId*6', 'constructorRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->driverId*6', 'driverRef') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->positionText*5', 'positionText') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->status*5', 'status') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->AverageSpeed*6->units*7', 'fastestLapAverageSpeedUnits') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->Time*6->time*7', 'fastestLapTime') \
        .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Results*4->Time*5->time*6', 'raceTime') \
        .drop('MRData*1->RaceTable*2->Races*3->Results*4->grid*5', 'MRData*1->RaceTable*2->Races*3->Results*4->laps*5', 'MRData*1->RaceTable*2->Races*3->Results*4->number*5', 'MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->Results*4->points*5', 'MRData*1->RaceTable*2->Races*3->Results*4->position*5', 'MRData*1->RaceTable*2->Races*3->Results*4->Time*5->millis*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5', 'MRData*1->RaceTable*2->Races*3->Results*4->Constructor*5->name*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Constructor*5->nationality*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Constructor*5->url*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->dateOfBirth*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->familyName*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->givenName*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->nationality*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->url*6', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->time*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->code*6', 'MRData*1->RaceTable*2->Races*3->Results*4->Driver*5->permanentNumber*6', 'MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->lap*6', 'MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->rank*6', 'MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->AverageSpeed*6->speed*7', 'MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->AverageSpeed*6->units*7', 'MRData*1->RaceTable*2->Races*3->Results*4->FastestLap*5->Time*6->time*7')
        return df

    if dataset == 'seasons/':
        df = df.withColumn('season', col('MRData*1->SeasonTable*2->Seasons*3->season*4').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->SeasonTable*2->Seasons*3->url*4', 'aboutUrl') \
        .drop('MRData*1->SeasonTable*2->Seasons*3->season*4')
        return df

    if dataset == 'status/':
        df = df.withColumn('count', col('MRData*1->StatusTable*2->Status*3->count*4').cast(IntegerType())) \
        .withColumn('statusId', col('MRData*1->StatusTable*2->Status*3->statusId*4').cast(IntegerType())) \
        .withColumnRenamed('MRData*1->StatusTable*2->Status*3->status*4', 'status') \
        .drop('MRData*1->StatusTable*2->Status*3->statusId*4', 'MRData*1->StatusTable*2->Status*3->count*4')
        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest and Transform data

# COMMAND ----------

# Loop through files in the latest bronze import
for folder in dbutils.fs.ls(foldersToIngest):
    folderName = folder.name
    folderPath = folder.path
    
    # Read the files in the folder
    df = readFiles(folderPath)
    
    # Flatten the JSON file
    df = flattenJson(df)
    #display(df.where(col('MRData*1->RaceTable*2->Races*3->date*4') == '2020-12-13'))
    
    # Re-name columns and add schema
    #print(folderName)
    df = renameAddSchema(df, folderName)

    # Write file back to blob
    df.write.mode('overwrite').parquet(silverFolder + newestFolder + '/' + folderName.replace('/', ''))
