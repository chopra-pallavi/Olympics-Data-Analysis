# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "0ab32cc9-9d72-443c-a454-f932dd7e1265",
"fs.azure.account.oauth2.client.secret": '7Wp8Q~6w~6pE.027A-HH3JuO5R2JZWYrA9cQ8bCy',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/c3d53147-5c2f-4fb4-8347-d47f37cce9ff/oauth2/token"}



# COMMAND ----------


dbutils.fs.mount(
source = "abfss://olympics-data@olympicsdata2.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/olympicsdata",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olympicsdata"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympicsdata/raw-data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympicsdata/raw-data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympicsdata/raw-data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympicsdata/raw-data/Medalscsv")
Teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/olympicsdata/raw-data/Teams.csv")

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympicsdata/transformed-data/athletes")
Coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympicsdata/transformed-data/Coaches")
Medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympicsdata/transformed-data/Medals")
Teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympicsdata/transformed-data/Teams")
EntriesGender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/olympicsdata/transformed-data/Entriedgender")
