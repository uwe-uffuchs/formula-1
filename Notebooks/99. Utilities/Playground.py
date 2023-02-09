# Databricks notebook source
# MAGIC %md
# MAGIC # Playground
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC This is a playground Notebook to allow DE users to see some common commands and code snippets
# MAGIC 
# MAGIC ## Commands on show:
# MAGIC 
# MAGIC | Item        | Description |
# MAGIC | ----------- | ----------- |
# MAGIC | %*language* | This allows you to easily switch between the language being used / run. E.g. %sql, %r, %python, %scala, %md (Markdown language) |
# MAGIC | %fs | File system command allowing you to easily see what files are mounted |
# MAGIC | %sh | Shell command allowing you to easily see all the shells that are active |
# MAGIC | dbutils.fs.ls | This is the same as %fs above but would be used when you need to programmically inteact with the file system |
# MAGIC | dbutils.fs.ls | This is the same as %fs above but would be used when you need to programmically inteact with the file system |

# COMMAND ----------

welcomeMessage = "Hello World, from Databricks \"Playground\" Notebook."
language = "This is run using python"

dbutils.widgets.text("welcomeMessage", welcomeMessage)
dbutils.widgets.text("language", language)

# COMMAND ----------

print(welcomeMessage + ' ' + language + '.')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CONCAT("${welcomeMessage}", REPLACE("${language}", 'python', 'sql')) AS Message;
# MAGIC --SELECT ${welcomeMessage} + ${language};

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith('/'):
        print(files.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS silver CASCADE;
