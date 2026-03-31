# Databricks notebook source
dbutils.widgets.text('sourceFilePath','/mnt/silver/FactIngestionLog')
sourceFilePath = dbutils.widgets.get('sourceFilePath')
checkPointLocation = "/_checkpoints/"

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts

# COMMAND ----------


subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
QueueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")



# COMMAND ----------

# MAGIC %md
# MAGIC # Read Source File and Autoloader settings

# COMMAND ----------

df_Source = (spark.readStream.option("cloudFiles.maxFilesPerTrigger",10) 
                              .option("cloudFiles.maxBytesPerTrigger",'10g')
                              .option("skipChangeCommits", "true")
                              .format("delta").load(sourceFilePath))

# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):  
    batchEnd(q,batchId)
    microBatchOutputDF = microBatchOutputDF.drop('LogUUID')       
    (microBatchOutputDF.write.format("jdbc")
                            .option("url", jdbcUrl)
                            .option("dbtable", "CONF.Ingestion_Log")
                            .option("user", username)
                            .option("password", password)
                            .mode("append")
                            .save())
    spark.sql("clear cache")
  
    

# COMMAND ----------


q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Ingestion_Log_SQLDB_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", sourceFilePath+checkPointLocation)
                  .outputMode("update")
                  .start()
                
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()
