# Databricks notebook source
UEConfigID=dbutils.widgets.text("UEConfigID","29")
UEConfigID=dbutils.widgets.get("UEConfigID")

dbutils.widgets.text('sourceTypeId','8')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

DeviceType=dbutils.widgets.text("DeviceType","COM1")
DeviceType=dbutils.widgets.get("DeviceType")

Job_id=dbutils.widgets.text("Job_id","-1")
Job_id=dbutils.widgets.get("Job_id")

run_id=dbutils.widgets.text("run_id","-1")
run_id=dbutils.widgets.get("run_id")

print("UEConfigID:"+UEConfigID)
print("DeviceType:"+DeviceType)
print("Job_id:"+str(Job_id))
print("run_id:"+str(run_id))

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

sourceFilePath = '/mnt/raw/DeviceLogs/COM1/*/*/*/{UserException}/*_UE_*.csv'
destinationFilePath='/mnt/silver/FACTUELogs_COM1'
srcFilesProcessed="/mnt/silver/LogSourceFilesProcessed/"
checkPointLocation = destinationFilePath+"/_checkpoints/"

DLTableName='silverzone.factuelogs_com1'
CreatedBy='ADB_COM1UELog'
queueName = 'com1uefilefileprocess-queue'

# COMMAND ----------

subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
queueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Schema:

# COMMAND ----------

FileSchema = StructType([
    StructField("ContractName", StringType(), False),
    StructField("ErrorDt", StringType(), False),
    StructField("ErrorTm", StringType(), False),
    StructField("Zcode", StringType(), False),
    StructField("ExceptionCode", StringType(), False),
    StructField("ExceptionCondition", StringType(), False),
    StructField("ResourceName", StringType(), False),
    StructField("arg1", StringType(), False),
    StructField("arg2", StringType(), False)
])

# COMMAND ----------

# DBTITLE 1,Read  Streaming
UERawFile = (spark.readStream.format("cloudFiles")
                              .option("cloudFiles.subscriptionId", subscriptionId)
                              .option("cloudFiles.connectionString", queueConnectionString)
                              .option("cloudFiles.queueName", queueName)
                              .option("cloudFiles.resourceGroup", resourceGroup)
                              .option("cloudFiles.tenantId", tenantId)
                              .option("cloudFiles.clientId", clientId)
                              .option("cloudFiles.clientSecret", clientSecret)
                              .option("cloudFiles.format", "csv")
                              .option("cloudFiles.includeExistingFiles",'false')
                              .option("cloudFiles.allowOverwrites","true")  
                              .option("cloudFiles.maxFilesPerTrigger",5000)
                              .option("cloudFiles.backfillInterval",'15 day')             
                              .option("cloudFiles.useNotifications",'true')
                              .schema(FileSchema) # provide a schema here for the files
                              .load(sourceFilePath)
                    )

UERawFile = UERawFile.select("*",col("_metadata.file_path").alias('SourceFilePath'),
                                          col("_metadata.file_name").alias('SourceFileName'),
                                          col("_metadata.file_modification_time").alias('RawFileModificationTime'),
                                          col("_metadata.file_size").alias('SourceFileSize')
                                     ) \
                              .withColumn('SourceFilePath', regexp_replace(regexp_replace('SourceFilePath','dbfs:/mnt/raw/',''),'/mnt/raw/',''))\
                              .withColumn('SourceFileName',regexp_replace(col('SourceFileName'),'%20',' '))


# COMMAND ----------

# DBTITLE 1,Read Dependent data from Silver Zone 
logSourceFileProcessedDF = (spark.read.format('delta')
                             .load(srcFilesProcessed)
                             .filter("LogFileStatus = 'InProgress' and configid = {0}".format(UEConfigID))
                             .select('SourceFilePath','FileNameUUID')
                            )
 
DIMEquipmentMaster=(spark.read.format('delta').load("/mnt/silver/DIMEquipmentMaster/")\
                    .select("SerialNumberNbr",col("ShipStartDt").cast("Timestamp"),col("ShipEndDt").cast("Timestamp"),"Mfg_BatchNbr")\
                    .withColumn("Rnk",row_number().over(Window.partitionBy(upper(("Mfg_BatchNbr"))).orderBy(col("ShipStartDt").desc(),col("ShipEndDt").desc()))))\
                    .where("rnk==1 and Mfg_BatchNbr is not null")\
                    .select(expr("upper(Mfg_BatchNbr) as sbc_sn"),expr("upper(SerialNumberNbr) as ExternalSerialNbr")
                ) 

# COMMAND ----------

# DBTITLE 1,Upsert the data to FactueLogs_Com1
def upsertToDelta(DFSource, batchId):
    try:
        batchEnd(q,batchId)
        print("Running for BatchID: {0}".format(batchId))

        DFSource=(DFSource.withColumn('ConfigId',lit(UEConfigID))
                .withColumn('SourceTypeID',lit(sourceTypeId))
                .withColumn("FileNameDeviceTypeCd",lit(DeviceType))
                .withColumn("FileNameDeviceSerialNbr",when(length(split(regexp_replace(col("SourceFileName"),"%20"," "), '_')
                                                     .getItem(2))==2,split(regexp_replace(col("SourceFileName"),"%20"," "), '_').getItem(1))
                                .otherwise(split(regexp_replace(col("SourceFileName"),"%20"," "), '_').getItem(2)) )
                .withColumn("FileNameMessageTypeCd",when(length(split(col('SourceFileName'), '_').getItem(2))==2,split(col('SourceFileName'), '_').getItem(2))
                                .otherwise(split(col('SourceFileName'), '_').getItem(3)))
                .withColumn("DeviceType",col('FileNameDeviceTypeCd'))
                .withColumn("LogType",col('FileNameMessageTypeCd'))
                .withColumn("FileNameDeviceSerialNbr",upper(col('FileNameDeviceSerialNbr')))
                .withColumn("DeviceId",col('FileNameDeviceSerialNbr'))
                .withColumn("InternalSerialNbr",col('FileNameDeviceSerialNbr'))
                .withColumn('RunID',lit(batchId))
                )


        filesMicroBatch = (DFSource.groupBy("SourceFilePath","SourceFileName","SourceFileSize","ConfigID","SourceTypeID","FileNameDeviceTypeCd","FileNameDeviceSerialNbr","InternalSerialNbr","FileNameMessageTypeCd","RawFileModificationTime","RunID")
                    .count()
                    .withColumnRenamed("count","RecdCnt")
                    .withColumn('SourceFilePath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
                    .withColumn("FileNameDtTmstmp",when(length(split(col('SourceFileName'), '_').getItem(3)) > 1,datetime.utcnow().strftime("%Y%m%d%H%M%S"))
                                .otherwise(substring(concat(split(col('SourceFileName'), '_').getItem(4),
                                                                                            split(col('SourceFileName'), '_').getItem(5),
                                                                                            split(col('SourceFileName'), '_').getItem(6)),0,14)))
                    .withColumn("FileNameApplicatorPortCd",lit(None))
                    .withColumn("FileNameCycleNbr",when(length(split(regexp_replace(col('SourceFileName'),'.csv',''), '_').getItem(3))==1,
                                    regexp_replace(split(col('SourceFileName'), '_').getItem(3),'.csv',''))
                              .when(length(split(col('SourceFileName'), '_').getItem(4))==1,
                                    split(col('SourceFileName'), '_').getItem(4))
                              .otherwise(lit('null')))
                    .withColumn("LogStartDate",lit(None))
                    .withColumn("LogEndDate",lit(None)))
        
        filesMicroBatch=filesMicroBatch.join(DIMEquipmentMaster,filesMicroBatch.InternalSerialNbr==DIMEquipmentMaster.sbc_sn,'left')
        
        loadlogProcessesDeltaTable(filesMicroBatch,srcFilesProcessed,CreatedBy,'InProgress','')
        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'InProgress','')

        
        log_df = filesMicroBatch.select(col('ConfigId').alias('ConfigID'), col('SourceTypeID').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(destinationFilePath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = filesMicroBatch

        # raise Exception("No Exception: Manual Failure")
        
        W_IdentifyDuplicates = (Window.partitionBy(col('ContractName'),col('ErrorDt'),col('ErrorTm'),col('Zcode'),col('ExceptionCode'),col('ExceptionCondition'),col('ResourceName'),col('arg1'),col('arg2'),col('InternalSerialNbr')).orderBy(col('ErrorDt').desc()))

        FactUELogs = DFSource.withColumn("DropDuplicates",row_number().over(W_IdentifyDuplicates))\
                           .filter(col("DropDuplicates") == 1)

        FactUELogs=FactUELogs.join(DIMEquipmentMaster,FactUELogs.InternalSerialNbr==DIMEquipmentMaster.sbc_sn,'left')\
                                .withColumn("ContractName",regexp_replace(col("ContractName"),"~",""))\
                                .withColumn("ErrorDt",to_date(col("ErrorDt"),"M/d/yyyy"))\
                                .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("ErrorDt",lit(" "),"ErrorTm")))\
                                .withColumnRenamed("ZCode","ZCodeMajorCd")\
                                .withColumnRenamed("arg1","ZCodeMinorCd")\
                                .withColumnRenamed("ExceptionCode","ExceptionDescription")\
                                .withColumn('UELogsUUID',expr('uuid()'))\
                                .withColumn('CreatedBy',lit('ADB_COM1UELog'))\
                                .withColumn('CreatedDt',current_timestamp())\
                                .withColumn('UpdatedBy',lit('ADB_COM1UELog'))\
                                .withColumn('UpdatedDt',current_timestamp())

        hash_columns = ['InternalSerialNbr','ZCodeMajorCd','ZCodeMinorCd','ErrorDt','ErrorTm','ExceptionDescription','ExceptionCondition','ContractName','ResourceName']
         
        FactUELogs = FactUELogs.withColumn("hash_id", sha2(concat_ws("||", *hash_columns), 256))

        DFSourceFileUUID = FactUELogs.join(logSourceFileProcessedDF,['SourceFilePath'],'inner')\
                                    .drop("arg2","RawFileModificationTime","SourceFileSize","ConfigId","SourceTypeID","FileNameDeviceSerialNbr","DeviceType","FileNameMessageTypeCd","LogType","DeviceId","RunID","sbc_sn","FileNameDeviceTypeCd")

        #DFSourceFileUUID.display()

        # #Merge Delta table
        FactUELogsTarget = DeltaTable.forPath(spark, destinationFilePath)  

        (FactUELogsTarget.alias("tgt")
                .merge(
                DFSourceFileUUID.alias("src"),
                   "tgt.hash_id = src.hash_id")
        .whenNotMatchedInsertAll()
        .execute())
       
        
        DFSourceWithLogDateTime=(DFSource.withColumn("LogDateTime",to_timestamp(concat(to_date(col('ErrorDt'),"M/d/yyyy"),lit(' '),col('ErrorTm')))))

        DFSourceAgg=(DFSourceWithLogDateTime.select("SourceFilePath","SourceFileName","LogDateTime","ConfigId").filter("LogDateTime is not null")).groupBy('SourceFilePath','SourceFileName','ConfigId').agg(min(col('LogDateTime')).cast('string').alias('LogStartDate'), max(col('LogDateTime')).cast('string').alias('LogEndDate'))

        DFSourceAgg=DFSourceAgg.join(logSourceFileProcessedDF,['SourceFilePath'],'inner')

        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'Succeeded','')
        loadlogProcessesDeltaTable(DFSourceAgg,destinationFilePath,CreatedBy,'Succeeded','')
        # DFSourceFileUUID.unpersist()
        #DFSource.unpersist()
        spark.sql("clear cache")
 
    except Exception as e:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(e).replace("'",'"')
        loadlogProcessesDeltaTable(filesMicroBatch,destinationFilePath,CreatedBy,'Failed',str(e).replace("'",'"'))
        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'Failed',str(e).replace("'",'"'))

        logIntoStreamLogTable(log_df,CreatedBy,"Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise

# COMMAND ----------

# DBTITLE 0,Run for Stream processing
#Adding Microbarch of 10 seconds for streaming capability. 
q=(UERawFile.writeStream
    .queryName("Transformation_COM1_UE_Stream")
     .trigger(processingTime='10 seconds')
      .outputMode("update")
      .foreachBatch(upsertToDelta)
     .option("checkpointLocation", checkPointLocation)
      .start()
      )

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()