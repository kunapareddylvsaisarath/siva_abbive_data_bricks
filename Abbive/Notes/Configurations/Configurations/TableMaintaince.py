# Databricks notebook source
import json
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled',False)

# COMMAND ----------

df = spark.sql("SHOW TABLES FROM silverzone")
tableList = df.collect()
print(tableList)

# COMMAND ----------

for table in tableList:
    print("Current Time =", datetime.now())
    tableName = table.database+'.'+table.tableName
    print(tableName)
    dlt = DeltaTable.forName(spark,tableName)
    dlt.optimize().executeCompaction()
    dlt.vacuum(0)
    # spark.sql("ALTER TABLE {} SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')".format(tableName))