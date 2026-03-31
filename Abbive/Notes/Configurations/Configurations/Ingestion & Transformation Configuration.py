# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

SourceFilePath = '/mnt/raw/Ingestion & Transformation Configuration'
TargetFilePath = '/mnt/silver'

# COMMAND ----------

# DBTITLE 1,Data Load Type
DataType_df =spark.read.format("csv").option("header",True).load(f"{SourceFilePath}/DataLoadType.csv")\
                    .withColumn("CreatedBy",expr("current_user"))\
                    .withColumn("CreatedDt",current_timestamp())\
                    .withColumn("UpdatedBy",expr("current_user"))\
                    .withColumn("UpdatedDt",current_timestamp())\
                             
DataType_df.display()
DataType_df.write.format('delta').option("mergeSchema","true").mode('overwrite').save(f"{TargetFilePath}/DataLoadType")

# COMMAND ----------

# DBTITLE 1,Source Type
SourceType_df =spark.read.format("csv").option("header",True).load(f"{SourceFilePath}//SourceType.csv")\
                    .withColumn("CreatedBy",expr("current_user"))\
                    .withColumn("CreatedDt",current_timestamp())\
                    .withColumn("UpdatedBy",expr("current_user"))\
                    .withColumn("UpdatedDt",current_timestamp())\
                             
SourceType_df.display()
SourceType_df.write.format('delta').option("mergeSchema","true").mode('overwrite').save(f"{TargetFilePath}/SourceType")

# COMMAND ----------

# DBTITLE 1,Conf Table
Config_df =spark.read.format("csv").option("header",True).load(f"{SourceFilePath}/IngestionConfiguration.csv")\
                    .withColumn("CreatedBy",expr("current_user"))\
                    .withColumn("CreatedDt",current_timestamp())\
                    .withColumn("UpdatedBy",expr("current_user"))\
                    .withColumn("UpdatedDt",current_timestamp())\
                             
Config_df.display()

Config_df.write.format('delta').option("mergeSchema","true").mode('overwrite').save(f"{TargetFilePath}//IngestionTransformationConfig")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silverzone.DataLoadType;
# MAGIC CREATE TABLE IF NOT EXISTS silverzone.DataLoadType USING DELTA LOCATION '/mnt/silver/DataLoadType/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silverzone.SourceType;
# MAGIC CREATE TABLE IF NOT EXISTS silverzone.SourceType USING DELTA LOCATION '/mnt/silver/SourceType/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silverzone.IngestionTransformationConfig;
# MAGIC CREATE TABLE IF NOT EXISTS silverzone.IngestionTransformationConfig USING DELTA LOCATION '/mnt/silver/IngestionTransformationConfig/';