# Databricks notebook source
try:
    # Import required libraries
    from delta.tables import DeltaTable
    from typing import Final, Dict, Tuple
    from pyspark.sql.session import SparkSession
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import *
    from pyspark.sql.types import StructType, ArrayType, DoubleType, IntegerType, DateType, TimestampType
    from pyspark.sql.window import Window
    import pytz
    import datetime
except:
    dbutils.notebook.exit("Failure: Cmd1 - Import required libraries failed")

# COMMAND ----------

try:
    # Get and set mount locations
    dbutils.widgets.text('source', 'bronze')
    dbutils.widgets.text('destination', 'silver')
    source = dbutils.widgets.get('source')
    destination = dbutils.widgets.get('destination')

    bronzeFolder = '/mnt/sadlformula1/' + source + '/'
    silverFolder = '/mnt/sadlformula1/' + destination + '/'
except:
    dbutils.notebook.exit("Failure: Cmd2 - Get and set mount locations failed")

# COMMAND ----------

try:
    # Get a comma separated list of subfolders for full load
    dbutils.widgets.text('sourceFolderList', 'constructorStandings/,driverStandings/,laps/,pitstops/,qualifying/,results/')

    sourceFolderList = dbutils.widgets.get('sourceFolderList')
except:
    dbutils.notebook.exit("Failure: Cmd3 - Get a comma separated list of subfolders for full load failed")

# COMMAND ----------

try:
    # Get list of subfolders in mount locations
    bronzeSubFolders = dbutils.fs.ls(bronzeFolder)
except:
    dbutils.notebook.exit("Failure: Cmd4 - Get list of subfolders in mount locations failed")

# COMMAND ----------

try:
    # Find the subfolder that was last added (i.e., the one with the greatest modification time)
    modificationTime = 0
    newestFolder = ''
    for folder in bronzeSubFolders:
        if modificationTime < folder.modificationTime:
            modificationTime = folder.modificationTime
            newestFolder = folder.name
except:
    dbutils.notebook.exit("Failure: Cmd5 - Find the subfolder that was last added (i.e., the one with the greatest modification time) failed")

# COMMAND ----------

try:
    # Get list of folders to ingest
    foldersToIngest = bronzeFolder + newestFolder
except:
    dbutils.notebook.exit("Failure: Cmd6 - Get list of folders to ingest failed")

# COMMAND ----------

try:
    # Functions to re-name/alias column in dataframe
    def renameDfColumns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
        return df.select(*[col(colName).alias(columns.get(colName, colName)) for colName in df.columns])

    def updateColumnNames(df: DataFrame, index: int) -> DataFrame:
        dfTemp = df
        allColumns = dfTemp.columns
        newColumns = dict((column, f'{column}*{index}') for column in allColumns)
        dfTemp = dfTemp.transform(lambda df_x: renameDfColumns(df_x, newColumns))

        return dfTemp
except:
    dbutils.notebook.exit("Failure: Cmd7 - Creating functions to re-name/alias column in dataframe failed")

# COMMAND ----------

try:
    # Function to flatten json files
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
except:
    dbutils.notebook.exit("Failure: Cmd8 - Creating function to flatten json files failed")

# COMMAND ----------

try:
    # Function to read json files from folder
    def readFiles(folder: StringType) -> DataFrame:
        df = spark.read.json(folder)
        return df
except:
    dbutils.notebook.exit("Failure: Cmd9 - Creating function to read json files from folder failed")

# COMMAND ----------

try:
    # Function to normalize column names and apply the required schema
    def normalizeSchema(df: DataFrame, dataset: StringType) -> DataFrame:
        df = df.withColumn('ingestionDate', from_utc_timestamp(current_timestamp(), 'Pacific/Auckland')) \
        .withColumn('sourceFolder', lit(newestFolder)) \
        .drop('MRData*1->limit*2', 'MRData*1->offset*2', 'MRData*1->series*2', 'MRData*1->total*2', 'MRData*1->url*2', 'MRData*1->xmlns*2')
        
        if dataset == 'driverStandings/':
            df = df.withColumn('round', col('MRData*1->StandingsTable*2->StandingsLists*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->StandingsTable*2->StandingsLists*3->season*4').cast(IntegerType())) \
            .withColumn('points', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->points*5').cast(DoubleType())) \
            .withColumn('position', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->position*5').cast(IntegerType())) \
            .withColumn('wins', col('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->wins*5').cast(IntegerType())) \
            .withColumn('round', col('MRData*1->StandingsTable*2->StandingsLists*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->StandingsTable*2->StandingsLists*3->season*4').cast(IntegerType())) \
            .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->constructorId*6', 'constructorRef') \
            .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->driverId*6', 'driverRef') \
            .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->positionText*5', 'positionText') \
            .drop('MRData*1->StandingsTable*2->StandingsLists*3->round*4', 'MRData*1->StandingsTable*2->StandingsLists*3->season*4', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->points*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->position*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->wins*5', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->name*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Constructors*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->dateOfBirth*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->familyName*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->givenName*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->code*6', 'MRData*1->StandingsTable*2->StandingsLists*3->DriverStandings*4->Driver*5->permanentNumber*6')
            return df
        
        if dataset == 'constructorStandings/':
            df = df.withColumn('points', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->points*5').cast(DoubleType())) \
            .withColumn('position', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->position*5').cast(IntegerType())) \
            .withColumn('wins', col('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->wins*5').cast(IntegerType())) \
            .withColumn('round', col('MRData*1->StandingsTable*2->StandingsLists*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->StandingsTable*2->StandingsLists*3->season*4').cast(IntegerType())) \
            .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->constructorId*6', 'constructorRef') \
            .withColumnRenamed('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->positionText*5', 'positionText') \
            .drop('MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->wins*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->position*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->points*5', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->name*6', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->nationality*6', 'MRData*1->StandingsTable*2->StandingsLists*3->ConstructorStandings*4->Constructor*5->url*6', 'MRData*1->StandingsTable*2->StandingsLists*3->round*4', 'MRData*1->StandingsTable*2->StandingsLists*3->season*4')
            return df
        
        if dataset == 'laps/':
            df = df.withColumn('lapNumber', col('MRData*1->RaceTable*2->Races*3->Laps*4->number*5').cast(IntegerType())) \
            .withColumn('position', col('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->position*6').cast(IntegerType())) \
            .withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
            .withColumn('round', col('MRData*1->RaceTable*2->Races*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->RaceTable*2->Races*3->season*4').cast(IntegerType())) \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->driverId*6', 'driverRef') \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->time*6', 'lapTime') \
            .drop('MRData*1->RaceTable*2->Races*3->Laps*4->Timings*5->position*6', 'MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->Laps*4->number*5', 'MRData*1->RaceTable*2->Races*3->raceName*4', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->round*3', 'MRData*1->RaceTable*2->season*3', 'MRData*1->RaceTable*2->Races*3->time*4')
            return df
        
        if dataset == 'pitstops/':
            df = df.withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
            .withColumn('lapNumber', col('MRData*1->RaceTable*2->Races*3->PitStops*4->lap*5').cast(IntegerType())) \
            .withColumn('stopNo', col('MRData*1->RaceTable*2->Races*3->PitStops*4->stop*5').cast(IntegerType())) \
            .withColumn('round', col('MRData*1->RaceTable*2->Races*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->RaceTable*2->Races*3->season*4').cast(IntegerType())) \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->Circuit*4->circuitId*5', 'circuitRef') \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->driverId*5', 'driverRef') \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->duration*5', 'pitstopDuration') \
            .withColumnRenamed('MRData*1->RaceTable*2->Races*3->PitStops*4->time*5', 'pitstopTime') \
            .drop('MRData*1->RaceTable*2->Races*3->date*4', 'MRData*1->RaceTable*2->Races*3->PitStops*4->lap*5', 'MRData*1->RaceTable*2->Races*3->PitStops*4->stop*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->circuitName*5', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->country*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->lat*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->long*6', 'MRData*1->RaceTable*2->Races*3->Circuit*4->Location*5->locality*6', 'MRData*1->RaceTable*2->round*3', 'MRData*1->RaceTable*2->season*3', 'MRData*1->RaceTable*2->Races*3->raceName*4', 'MRData*1->RaceTable*2->Races*3->round*4', 'MRData*1->RaceTable*2->Races*3->season*4', 'MRData*1->RaceTable*2->Races*3->time*4', 'MRData*1->RaceTable*2->Races*3->url*4', 'MRData*1->RaceTable*2->Races*3->Circuit*4->url*5')
            return df

        if dataset == 'qualifying/':
            df = df.withColumn('date', col('MRData*1->RaceTable*2->Races*3->date*4').cast(DateType())) \
            .withColumn('position', col('MRData*1->RaceTable*2->Races*3->QualifyingResults*4->position*5').cast(IntegerType())) \
            .withColumn('round', col('MRData*1->RaceTable*2->Races*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->RaceTable*2->Races*3->season*4').cast(IntegerType())) \
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
            .withColumn('round', col('MRData*1->RaceTable*2->Races*3->round*4').cast(IntegerType())) \
            .withColumn('season', col('MRData*1->RaceTable*2->Races*3->season*4').cast(IntegerType())) \
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
except:
    dbutils.notebook.exit("Failure: Cmd10 - Creating function to normalize column names and apply the required schema failed")

# COMMAND ----------

# Reload dataframe to the database
def reloadDataIntoDatabase(df: DataFrame, dataset: StringType):
    dataset = dataset.replace('/', '')
        
    loadData(df, dataset)

def dropPartition(dataset: StringType, partitionKey1: StringType, partitionKey2: StringType):
    partitionList = df.select(f'{partitionKey1}', f'{partitionKey2}').distinct().collect()

    for partition in partitionList:
        spark.sql(f'ALTER TABLE {destination}.{dataset} DROP IF EXISTS PARTITION ({partitionKey1} = {partition.round}, {partitionKey2} = {partition.season})')

def loadData(df: DataFrame, dataset: StringType):
    destinationdf = DeltaTable.forPath(spark, f'{silverFolder}/{dataset}')
    destinationdf.merge(df, 'destinationdf.season = df.season and destinationdf.round = df.round') \
    .whenMatched()
    df.write.mode('append').partitionBy(f'{partitionKey1}', f'{partitionKey2}').format('delta').saveAsTable(destination + '.' + dataset)

# COMMAND ----------

try:
    # Create silver Database
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {destination} LOCATION \'{silverFolder}\';')
except:
    dbutils.notebook.exit("Failure: Cmd12 - Creating silver Database failed")

# COMMAND ----------

# Get schedule
#scheduledf = spark.sql(f'SELECT * FROM {destination}.schedule WHERE dateTime BETWEEN DATEADD(month, -6, GETDATE()) AND GETDATE()')

# COMMAND ----------

# Loop through files in the latest bronze import
for folder in dbutils.fs.ls(foldersToIngest):
    folderName = folder.name
    folderPath = folder.path

    if folderName in sourceFolderList:
        # Read the files in the folder
        df = readFiles(folderPath)

        # Flatten the JSON file
        df = flattenJson(df)

        # Re-name columns and add schema
        df = normalizeSchema(df, folderName)
        #df = df.join(scheduledf, (df.round == scheduledf.round) & (df.season == scheduledf.year), 'inner').select(df['*'])

        # Write file back to blob
        reloadDataIntoDatabase(df, folderName)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM silver.constructorStandings LIMIT 10; --date
# MAGIC --SELECT * FROM silver.driverStandings LIMIT 10;
# MAGIC --SELECT * FROM silver.laps LIMIT 10;
# MAGIC --SELECT * FROM silver.pitstops LIMIT 10;
# MAGIC --SELECT * FROM silver.qualifying LIMIT 10;
# MAGIC --SELECT * FROM silver.results LIMIT 10;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver.constructorStandings;
# MAGIC DROP TABLE IF EXISTS silver.driverStandings;
# MAGIC DROP TABLE IF EXISTS silver.laps;
# MAGIC DROP TABLE IF EXISTS silver.pitstops;
# MAGIC DROP TABLE IF EXISTS silver.qualifying;
# MAGIC DROP TABLE IF EXISTS silver.results;
