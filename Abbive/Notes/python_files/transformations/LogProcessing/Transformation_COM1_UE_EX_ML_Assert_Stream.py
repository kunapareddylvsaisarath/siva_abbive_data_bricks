# Databricks notebook source
SourceTypeID=dbutils.widgets.text("SourceTypeID","8")
SourceTypeID=dbutils.widgets.get("SourceTypeID")

MLConfigID=dbutils.widgets.text("MLConfigID","26")
MLConfigID=dbutils.widgets.get("MLConfigID")

UEConfigID=dbutils.widgets.text("UEConfigID","29")
UEConfigID=dbutils.widgets.get("UEConfigID")

EXConfigID=dbutils.widgets.text("EXConfigID","28")
EXConfigID=dbutils.widgets.get("EXConfigID")

AssertConfigID=dbutils.widgets.text("AssertConfigID","30")
AssertConfigID=dbutils.widgets.get("AssertConfigID")

CreatedBy=dbutils.widgets.text("CreatedBy","ADB_COM1MLUEEXAssertLogs")
CreatedBy=dbutils.widgets.get("CreatedBy")

Job_id=dbutils.widgets.text("Job_id","-1")
Job_id=dbutils.widgets.get("Job_id")

run_id=dbutils.widgets.text("run_id","-1")
run_id=dbutils.widgets.get("run_id")

print(Job_id)
print(run_id)

dbutils.widgets.text('EmailNotificationID','3')
EmailNotificationID = dbutils.widgets.get('EmailNotificationID')

dbutils.widgets.text('Env','Dev')
Env = dbutils.widgets.get('Env')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initializing the functions:

# COMMAND ----------

import traceback

pipelinename = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipelinename = pipelinename.rsplit('/', 1)[-1]
display(pipelinename)

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts $job_id=$Job_id $run_id=$run_id $parent_run_id=-1

# COMMAND ----------

# MAGIC %run
# MAGIC /Configurations/EmailNotificationConfiguration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declaring the source and destination paths:

# COMMAND ----------

# Source
sourceFilePath = '/mnt/raw/DeviceLogs/COM1/*/*/*/{Measurement,Exception,Assert}/*.{csv,txt}'
src_filesProcessed = '/mnt/silver/LogSourceFilesProcessed/'

# Destination
destinationFilePath = '/mnt/silver/LogSourceFilesProcessed/'
checkPointLocation = destinationFilePath + "_checkpoints/"

# COMMAND ----------

subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
queueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")
queueName = 'com1mlueexfilefileprocess-queue'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Schema:

# COMMAND ----------

# Schema:
Schema = StructType([
    StructField("ContractNbr", StringType(), False),
    StructField("StartDateDt", StringType(), False),
    StructField("StartTimeTm", StringType(), False)
   ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Streaming data:

# COMMAND ----------

# Autoloader ML,UE,EX and Assert:
df_MLUEEXAssertLogsRaw = (spark.readStream.format("cloudFiles")
                          .option("cloudFiles.subscriptionId", subscriptionId)
                          .option("cloudFiles.connectionString", queueConnectionString)
                          .option("cloudFiles.queueName", queueName)
                          .option("cloudFiles.resourceGroup", resourceGroup)
                          .option("cloudFiles.tenantId", tenantId)
                          .option("cloudFiles.clientId", clientId)
                          .option("cloudFiles.clientSecret", clientSecret)
                          .option("cloudFiles.format", "csv")
                          .option("cloudFiles.allowOverwrites","true") 
                          .option("cloudFiles.maxFilesPerTrigger",5000)
                          .option("cloudFiles.includeExistingFiles",'false')
                          .option("cloudFiles.backfillInterval",'15 day')
                          .option("cloudFiles.useNotifications",'true')
                          .option("rescuedDataColumn", "_rescued_data") 
                          .schema(Schema)
                          .load(sourceFilePath)
                          .select('ContractNbr','StartDateDt','StartTimeTm',
                                  col("_metadata.file_path").alias('SourceFilePath'),
                                  col("_metadata.file_name").alias('SourceFileName'),
                                  col("_metadata.file_modification_time").alias('RawFileModificationTime'),
                                  col("_metadata.file_size").alias('SourceFileSize'))
                          .withColumn('SourceFileName',regexp_replace(col('SourceFileName'),'%20',' '))
                          .withColumn('SourceFilePath', regexp_replace(regexp_replace('SourceFilePath','dbfs:/mnt/',''),'/mnt/',''))
                          .withColumn('StartDateDt',to_date(col('StartDateDt'),'M/d/yyyy'))
                          .withColumn('LogStartDate',to_timestamp(concat(col('StartDateDt'),lit(' '),col('StartTimeTm'))))                        
                         )       

# COMMAND ----------

DIMEquipmentMaster=(spark.read.format('delta').load("/mnt/silver/DIMEquipmentMaster/")
                     .select("SerialNumberNbr",col("ShipStartDt").cast("Timestamp"),col("ShipEndDt").cast("Timestamp"),"Mfg_BatchNbr").withColumn("Rnk",row_number().over(Window.partitionBy(upper(("Mfg_BatchNbr"))).orderBy(col("ShipStartDt").desc())))).where("rnk==1 and Mfg_BatchNbr is not null").select(expr("upper(Mfg_BatchNbr) as sbc_sn"),expr("upper(SerialNumberNbr) as ExternalSerialNbr")
                   )
                     
logSourceFileProcessedDF = (spark.read.format('delta')
                             .option('header', True)
                             .load(src_filesProcessed)
                             .filter("LogFileStatus = 'InProgress' and configid in ({0},{1},{2})".format(MLConfigID,EXConfigID,AssertConfigID))
                             .select('SourceFilePath','FileNameUUID')
                            )                     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation:

# COMMAND ----------

def upsertToDelta(df_Source, batchId):
    try:       
        batchEnd(q,batchId)
        print("Running for BatchID: {0}".format(batchId))
        df_Source.persist()

        # Deriving ConfigId and SourceTypeId from the Notebook parameters:
        df_Source = (df_Source
                     .withColumn("SourceTypeId",lit(SourceTypeID))
                     .withColumn("ConfigId",when(split(col('SourceFilePath'), '/').getItem(6) == 'Measurement',lit(MLConfigID))
                                 .when(split(col('SourceFilePath'), '/').getItem(6) == 'UserException',lit(UEConfigID))
                                 .when(split(col('SourceFilePath'), '/').getItem(6) == 'Exception',lit(EXConfigID))
                                 .when(split(col('SourceFilePath'), '/').getItem(6) == 'Assert',lit(AssertConfigID))
                                 .otherwise(lit(None)))
                    .withColumn("FileNameDeviceTypeCd",split(col('SourceFilePath'),'/').getItem(2))
                    .withColumn("InternalSerialNbr",regexp_replace(split(col('SourceFileName'), '_').getItem(1),'.txt',''))
                    .withColumn("FileNameMessageTypeCd",when(split(col('SourceFilePath'), '/').getItem(6) == 'Measurement','ML')
                                .when(split(col('SourceFilePath'), '/').getItem(6) == 'UserException','UE')
                                .when(split(col('SourceFilePath'), '/').getItem(6) == 'Exception','EX')
                                .when(split(col('SourceFilePath'), '/').getItem(6) == 'Assert','Assert')
                                .otherwise(None))   
                    .withColumn("InternalSerialNbr",upper(col('InternalSerialNbr')))                  
                    .withColumn("DeviceType",col('FileNameDeviceTypeCd'))
                    .withColumn("LogType",col('FileNameMessageTypeCd'))
                    .withColumn("DeviceId",col('InternalSerialNbr'))
                    .withColumn('RunID',lit(batchId))
                    # .withColumn("InternalSerialNbr",col('ExternalSerialNbr'))
                    )
                    
                              
        loadAuditTables_Ingestion_Log(df_Source,destinationFilePath,CreatedBy,'InProgress','')
        
        # Assert Data Transformation:
        AssertLogDF = (df_Source.where(expr("ContractNbr like 'Assert occurred on%' and ConfigID==30"))
                      .withColumn("LogStartDate",to_timestamp(regexp_replace(col("ContractNbr"),"Assert occurred on ",""),"M/d/yyyy h:m:ss a"))
                      .drop('StartDateDt','StartTimeTm','ContractNbr'))

        # ML,UE,EX Data Transformation:
        MLUEEXLogDF = (df_Source.filter("ConfigID!=30 and ConfigID is not null and LogStartDate is not null")
                       .drop('StartDateDt','StartTimeTm','ContractNbr'))    
               
        # Merge allLogs df:
        df_MLUEEXAssertLogs = MLUEEXLogDF.unionByName(AssertLogDF)
        
        # Deriving Meta data for ML,UE,EX, Assert logs:    
        df_MLUEEXAssertLog_final = (df_MLUEEXAssertLogs.groupBy("SourceFilePath","SourceFileName","SourceFileSize","ConfigId","SourceTypeId",
                                                                "FileNameDeviceTypeCd","InternalSerialNbr","FileNameMessageTypeCd","RawFileModificationTime","RunID")
                                    .agg(min(col('LogStartDate')).alias('LogStartDate'), max(col('LogStartDate')).alias('LogEndDate'),
                                         count('SourceFilePath').alias('RecdCnt'))
                                    .withColumn("FileNameDtTmstmp", when(col('FileNameMessageTypeCd')=='ML',
                                                when(length(split(col('SourceFileName'), '_').getItem(1)) > 19, datetime.utcnow().strftime("%Y%m%d%H%M%S"))
                                                .otherwise(substring(concat(split(col('SourceFileName'), '_').getItem(2),
                                                                                 split(col('SourceFileName'), '_').getItem(3),
                                                                                 split(col('SourceFileName'), '_').getItem(4)),0,14)))
                                                .when(col('SourceFileName').like('%Assert%'),substring(regexp_replace(concat(split(col('SourceFileName'), '_').getItem(2),
                                                                                                                   split(col('SourceFileName'), '_').getItem(3),
                                                                                                                   split(col('SourceFileName'), '_').getItem(4)),'.txt',''),0,14))
                                                .otherwise(when(length(split(col('SourceFileName'), '_').getItem(3)) > 1,datetime.utcnow().strftime("%Y%m%d%H%M%S"))
                                                           .otherwise(substring(concat(split(col('SourceFileName'), '_').getItem(4),
                                                                                            split(col('SourceFileName'), '_').getItem(5),
                                                                                            split(col('SourceFileName'), '_').getItem(6)),0,14)
                                                                                            
                                                                                            )))
                                  .withColumn("FileNameApplicatorPortCd",lit(None).cast('String'))
                                  .withColumn("FileNameCycleNbr", when(col('SourceFileName').like('%UE%'), 
                                                                       regexp_replace(split(col('SourceFileName'), '_').getItem(3), '.csv', ''))
                                              .when(col('SourceFileName').like('%EX%'), 
                                                    regexp_replace(regexp_replace(regexp_replace(split(col('SourceFileName'), '_').getItem(3), '.csv', ''),'.zip',''),'.txt',''))
                                              .otherwise(lit(None))))
        

        df_MLUEEXAssertLog_final=df_MLUEEXAssertLog_final.join(DIMEquipmentMaster,df_MLUEEXAssertLog_final.InternalSerialNbr==DIMEquipmentMaster.sbc_sn,'left')
        # df_MLUEEXAssertLog_final.display()

        log_df = df_MLUEEXAssertLog_final.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(destinationFilePath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(job_id)))
        
        Microbatch_df = df_MLUEEXAssertLog_final

        # raise Exception("No Exception: Manual Failure")

        loadlogProcessesDeltaTable(df_MLUEEXAssertLog_final,destinationFilePath,CreatedBy,'InProgress','')

        df_MLUEEXAssertLog_final=df_MLUEEXAssertLog_final.withColumn('SourceFilePath',expr("regexp_replace(SourceFilePath,'raw/','')")) .join(logSourceFileProcessedDF,['SourceFilePath'],'inner')
        # print("df_MLUEEXAssertLog_final:"+(str(df_MLUEEXAssertLog_final.count())))

        loadlogProcessesDeltaTable(df_MLUEEXAssertLog_final,destinationFilePath,CreatedBy,'Succeeded','')
        loadAuditTables_Ingestion_Log(df_Source,destinationFilePath,CreatedBy,'Succeeded','')
        df_Source.unpersist()
        spark.sql("clear cache")
    except Exception as e:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(e)
        loadlogProcessesDeltaTable(df_MLUEEXAssertLog_final,destinationFilePath,CreatedBy,'Failed',ErrorMessage)
        loadAuditTables_Ingestion_Log(df_Source,destinationFilePath,CreatedBy,'Failed',ErrorMessage)
        
        logIntoStreamLogTable(log_df,CreatedBy,"Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Data to delta Table:

# COMMAND ----------

# Reading the file and applying transformations:
# (df_MLUEEXAssertLogsRaw
#  .writeStream
#  .trigger(availableNow=True)
#  .outputMode("update")
#  .foreachBatch(upsertToDelta)
#  .option("checkpointLocation", checkPointLocation)
#  .start())

#Adding Microbarch of 10 seconds for streaming capability. 
q=(df_MLUEEXAssertLogsRaw
 .writeStream
 .queryName("Transformation_COM1_UE_EX_ML_Assert_Stream")
 .trigger(processingTime='10 seconds')
 .outputMode("update")
 .foreachBatch(upsertToDelta)
 .option("checkpointLocation", checkPointLocation)
 .start())

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()

# COMMAND ----------


