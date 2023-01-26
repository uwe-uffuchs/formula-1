# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuits Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Json File

# COMMAND ----------

bronzeFolder = '/mnt/sadlformula1/bronze/'
circuitsFolder = bronzeFolder
bronzeSubFolders = dbutils.fs.ls(bronzeFolder)
newestFolder = ''
modificationTime = 0
for folder in bronzeSubFolders:
    if modificationTime < folder.modificationTime:
        newestFolder = folder.name
        
circuitsFolder = circuitsFolder + newestFolder + 'circuits/'
circuitsfile = dbutils.fs.ls(circuitsFolder)[0].path

df = spark.read.json(circuitsfile).select('MRData.CircuitTable.Circuits')
#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Flatten Circuits File

# COMMAND ----------

from pyspark.sql.functions import explode, col

level1Explodeddf = df.select(explode('Circuits'))
level2Explodeddf = level1Explodeddf.select('col.*')

import pyspark.sql.functions as F

flat_cols = [c[0] for c in level2Explodeddf.dtypes if c[1][:6] != 'struct']
nested_cols = [c[0] for c in level2Explodeddf.dtypes if c[1][:6] == 'struct']

circuitsDF = level2Explodeddf.select(flat_cols +
                           [F.col(nc+'.'+c).alias(nc+'_'+c)
                            for nc in nested_cols
                            for c in level2Explodeddf.select(nc+'.*').columns])

display(circuitsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Re-name Columns

# COMMAND ----------

from pyspark.sql.types import DoubleType
circuitsDF = circuitsDF.withColumnRenamed('circuitId', 'circuitRef')
circuitsDF = circuitsDF.withColumnRenamed('Location_country', 'country')
circuitsDF = circuitsDF.withColumnRenamed('Location_locality', 'locality')
circuitsDF = circuitsDF.withColumn('latitude', col('Location_lat').cast(DoubleType())).drop('Location_lat')
circuitsDF = circuitsDF.withColumn('longitude', col('Location_long').cast(DoubleType())).drop('Location_long')
#Location_country  Location_locality Location_long
circuitsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Id Column

# COMMAND ----------

from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
w = Window().orderBy(lit('A'))
circuitsDF = circuitsDF.withColumn("circuitId", row_number().over(w))
display(circuitsDF)
