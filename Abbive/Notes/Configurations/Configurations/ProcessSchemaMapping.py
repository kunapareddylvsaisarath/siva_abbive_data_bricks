# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql import Window

# COMMAND ----------

SourceSchemaPath='/mnt/raw/SchemaMapping/SchemaMappingNew.csv'
targetSchemaPath='/mnt/silver/DatabaseSchema'

# COMMAND ----------

Schema= StructType([
    # StructField('SchemaID', IntegerType()),
    StructField('DeviceType',StringType()),
    StructField('TableName', StringType()),
    StructField('LogType', StringType()),
    StructField('ContractName', StringType()),
    StructField('SourceName',StringType(),True),
    StructField('TargetName', StringType()),
    StructField('DataType', StringType()),
    StructField('ColumnType', StringType()),
    StructField('updateEnabled', StringType()),
    # StructField('CreatedDate', TimestampType()),
    # StructField('CreatedBy', StringType()),
    # StructField('UpdatedDate', TimestampType()),
    # StructField('UpdatedBy', StringType()),
    StructField('isActive', StringType())
])

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
print(user_id)

# COMMAND ----------

SchemaMapping=(spark.read.format('csv').option("header","True")
               .option("delimiter",",").schema(Schema)
                .load(SourceSchemaPath))
                
SchemaMapping=(SchemaMapping.withColumn("DeviceType",split(col("DeviceType"),","))
               .withColumn("LogType",split(col("LogType"),","))
               .withColumn("SourceName",split(col("SourceName"),","))
               .withColumn("CreatedDate",current_timestamp())
               .withColumn("UpdatedDate",current_timestamp())
               .withColumn("CreatedBy", lit(user_id))
               .withColumn("updatedBy", lit(user_id))
               .withColumn("updatedBy", lit(user_id))
               .withColumn("SchemaID",row_number().over(Window.orderBy("TableName")))
             )
SchemaMapping.display()

# COMMAND ----------

SchemaMapping.write.format('delta').option("mergeSchema","true").mode('overwrite').save(targetSchemaPath)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if Exists SilverZone.DatabaseSchema;
# MAGIC create table SilverZone.DatabaseSchema using delta location '/mnt/silver/DatabaseSchema'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverzone.databaseschema 

# COMMAND ----------

dbutils.notebook.exit("done")

# COMMAND ----------

