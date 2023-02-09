# Databricks notebook source
try:
    # Import required libraries
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
    dbutils.widgets.text('sourceFolderList', 'circuits/,schedule/,constructors/,drivers/,seasons/,status/')

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
except:
    dbutils.notebook.exit("Failure: Cmd10 - Creating function to normalize column names and apply the required schema failed")

# COMMAND ----------

try:
    # Create silver Database
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {destination} LOCATION \'{silverFolder}\';')
except:
    dbutils.notebook.exit("Failure: Cmd11 - Creating silver Database failed")

# COMMAND ----------

try:
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

            # Write file back to blob
            df.write.mode('overwrite').format('delta').saveAsTable(destination + '.' + folderName.replace('/', ''))
except:
    dbutils.notebook.exit("Failure: Cmd12 - Looping through files in the latest bronze files to import failed")

# COMMAND ----------

dbutils.notebook.exit("Success")
