# Databricks notebook source
dbutils.widgets.text("ExternalLocationName_raw", "/mntprod_raw")
ExternalLocationName_raw = dbutils.widgets.get("ExternalLocationName_raw")

dbutils.widgets.text("ExternalLocationName_silver", "/mntprod_silver")
ExternalLocationName_silver = dbutils.widgets.get("ExternalLocationName_silver")

dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")

dbutils.widgets.text('deltaTablePath_AllLogs','/FACTAllLogs')
deltaTablePath_AllLogs = dbutils.widgets.get('deltaTablePath_AllLogs')
deltaTablePath_AllLogs = ExternalLocationName_silver+deltaTablePath_AllLogs

dbutils.widgets.text('sourceFilePath','/DeviceLogs/{COM3,CT,RS}/*/*/*/*/')
sourceFilePath = dbutils.widgets.get('sourceFilePath')
sourceFilePath = ExternalLocationName_raw+sourceFilePath

dbutils.widgets.text('queueName','com3logprocess-queue')
queueName = dbutils.widgets.get('queueName')

dbutils.widgets.text('allLogs_ConfigId','20')
allLogs_ConfigId = dbutils.widgets.get('allLogs_ConfigId')

dbutils.widgets.text('sourceTypeId','3')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

dbutils.widgets.text('EmailNotificationID','3')
EmailNotificationID = dbutils.widgets.get('EmailNotificationID')

Job_id=dbutils.widgets.text("Job_id","-1")
Job_id=dbutils.widgets.get("Job_id")

run_id=dbutils.widgets.text("run_id","-1")
run_id=dbutils.widgets.get("run_id")

dbutils.widgets.text('Env','Dev')
Env = dbutils.widgets.get('Env')




# COMMAND ----------

import traceback

pipelinename = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipelinename = pipelinename.rsplit('/', 1)[-1]
display(pipelinename)

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts

# COMMAND ----------

# MAGIC %run ../../Configurations/EmailNotificationConfiguration

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------

checkPointLocation = "/_checkpoints/"

logTypes_Processed = ['UL','UE','SYS','ENGR'] 
contractNumberList_StartEnd = ["14020","14023"]
# contractNumberList_Processed = ["21544","21544:1","21544:2","21543","21543:1","21543:2","21519","84009","25003","25003:1","25003:2","25004","25004:1","25004:2","84005","84006","84005:1","84006:1",'21062','15024','15039','15008']

# COMMAND ----------


subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
QueueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")

vault_url = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-KeyVaultURL")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define File Schema

# COMMAND ----------

# Schema for thr log files from raw:
schema_All_Log_Files = StructType([
StructField("HdrFormatVersionCd", IntegerType(), False),
StructField("HdrDateGeneratedDt", DateType(), False),
StructField("HdrTimeGeneratedTmstmp", StringType(), False),
StructField("HdrLogtypeCd", StringType(), False),
StructField("HdrDestinationSubSystemCd", StringType(), False),
StructField("HdrSourceSubSystemCd", StringType(), False),
StructField("HdrCommandCD", StringType(), False),
StructField("HdrDataStoreID", StringType(), False),
StructField("HdrDataContractNbr", StringType(), False),
StructField("JSONField", StringType(), False)])

# Schema for thr log files from raw:
Schema_14020 = StructType([
    StructField("startts", StringType(), False),
    StructField("prodtype", StringType(), False),
    StructField("esn", StringType(), False),
    StructField("type", StringType(), False),
    StructField("version", StringType(), False),
    StructField("app", StringType(), False),
    StructField("priority", StringType(), False),
    StructField("timezone", StringType(), False)
])

# Schema for contract 14023:
Schema_14023 = StructType([
    StructField("endts", StringType(), False)
])



# COMMAND ----------

# Parsing contract 14020:
parsed_14020_cols = [col('HdrFormatVersionCd'),col('HdrDateGeneratedDt'),col('HdrTimeGeneratedTmstmp'),
                     col('HdrLogtypeCd'),col('HdrDestinationSubSystemCd'),col('HdrSourceSubSystemCd'),
                     col('HdrCommandCD'),col('HdrDataStoreID'),col('HdrDataContractNbr').alias('HdrDataContractNbr_StartParameter'),col('JSONField'),col('FileNameUUID'),
                     col('startts').alias('EventStartTmstmp'),col('prodtype').alias('CycleTypeCd'),col('ExternalSerialNbr'),col('InternalSerialNbr'),col('HdrAppHeadNbr'),
                     col('type').alias('Type'),col('version').alias('VersionNbr'),col('app').alias('ApplicatorPortCd'),
                     col('priority').alias('PriorityCd'),col('timezone').alias('TimeZoneDesc'),col('SourceFileName'),
                     col('SourceFilePath'),col('SourceFileSize')]

# parsing contract no. 14023:
parsed_14023_cols = [col('FileNameUUID'),col('HdrDataContractNbr').alias('HdrDataContractNbr_EndParameter'),col('endts').alias('EventEndTmstmp'),col('SourceFilePath'),col('HdrTimeGeneratedTmstmp')]

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Source File and Autoloader settings

# COMMAND ----------


# sourceFilePath=['/mnt/raw/DeviceLogs/COM3/CS4M2015301003/IOT/20230613/UL/COM3_D012020162003_UL_20230613172401_B_27.stx','/mnt/raw/DeviceLogs/COM3/CS4M2018201007/IOT/20230613/UL/COM3_D012020275013_UL_20230613185521_A_23.stx']
# df_Source = (spark.read.format("csv").option('quote','{')
#                               .option('unescapedQuoteHandling','STOP_AT_CLOSING_QUOTE')
#                               .option("rescuedDataColumn", "_rescued_data") # makes sure that you don't lose data
#                               .schema(schema_All_Log_Files) # provide a schema here for the files
#                               .load(sourceFilePath)
#                               .select("*",col("_metadata.file_path").alias('SourceFilePath'),
#                                           col("_metadata.file_name").alias('SourceFileName'),
#                                           col("_metadata.file_modification_time").alias('file_modification_time'),
#                                           col("_metadata.file_size").alias('SourceFileSize')
#                                      ) 
#                               .withColumn('JSONField',concat(lit('{'),col('JSONField')))
#                               .withColumn('SourceFilePath', regexp_replace('SourceFilePath','/mnt/raw/',''))
#                               .withColumn('SourceFileName', regexp_replace('SourceFileName','%20',''))
#                               .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
#                                                                           ,"yyyy-MM-dd HH:mm:ss.SSS"))
#                     )


# COMMAND ----------

df_Source = (spark.readStream.format("cloudFiles")
                              .option("cloudFiles.subscriptionId", subscriptionId)
                              .option("cloudFiles.connectionString", QueueConnectionString)
                              .option("cloudFiles.queueName", queueName)
                              .option("cloudFiles.resourceGroup", resourceGroup)
                              .option("cloudFiles.tenantId", tenantId)
                              .option("cloudFiles.clientId", clientId)
                              .option("cloudFiles.clientSecret", clientSecret)
                              .option("cloudFiles.format", "csv")
                              .option("cloudFiles.allowOverwrites","true")
                              .option("cloudFiles.includeExistingFiles","false")
                              .option("cloudFiles.backfillInterval",'15 day')             
                              .option("cloudFiles.useNotifications",'true')
                              .option("cloudFiles.maxFilesPerTrigger",5000) 
                              .option("cloudFiles.maxBytesPerTrigger",'10g')
                              .option('quote','{')
                              .option('unescapedQuoteHandling','STOP_AT_CLOSING_QUOTE')
                              .option("rescuedDataColumn", "_rescued_data") # makes sure that you don't lose data
                              .schema(schema_All_Log_Files) # provide a schema here for the files
                              .load(sourceFilePath)
                              .select("*",col("_metadata.file_path").alias('SourceFilePath'),
                                          col("_metadata.file_name").alias('SourceFileName'),
                                          col("_metadata.file_modification_time").alias('RawFileModificationTime'),
                                          col("_metadata.file_size").alias('SourceFileSize')
                                     ) 
                              .withColumn('JSONField',concat(lit('{'),col('JSONField')))
                              .withColumn('SourceFilePath', regexp_replace('SourceFilePath',ExternalLocationName_raw+'/',''))
                              .withColumn('SourceFileName', regexp_replace('SourceFileName','%20',''))
                              .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
                                                                          ,"yyyy-MM-dd HH:mm:ss.SSS"))
                    )

# COMMAND ----------

def processStartAndEndParameters(df_Source_StartEnd):
    try:
        
        df_raw_14020 = (df_Source_StartEnd.filter(col('HdrDataContractNbr') == '14020')
                                          .withColumn("JSONField", from_json("JSONField", Schema_14020))
                                          .select('*', col('JSONField.*')))        
        df_parsed_14020 = df_raw_14020.select(parsed_14020_cols)
                
            
        df_raw_14023 = (df_Source_StartEnd.filter(col('HdrDataContractNbr') == '14023')
                                            .withColumn("JSONField", from_json("JSONField", Schema_14023))
                                            .select('*', col('JSONField.*'))
                        )
        df_parsed_14023 = df_raw_14023.select(parsed_14023_cols)


        # Fix EventStartTmstmp: Fix missing or bad Event Start Tmstmps:
        df_14020_EveStrtmp = (df_parsed_14020
                                .withColumn("EventStartTmstmp",
                                        when((col('HdrFormatVersionCd') == 1) & (col('CycleTypeCd') == 'CT'),col('HdrTimeGeneratedTmstmp'))
                                        .otherwise(
                                            when(length(col('EventStartTmstmp')) == 19,to_timestamp(col('EventStartTmstmp'),"yyyy-MM-dd,HH:mm:ss"))
                                            .otherwise(
                                                when(length(col('EventStartTmstmp')) == 14,to_timestamp(col('EventStartTmstmp'),"yyyyMMddHHmmss"))
                                                .otherwise(
                                                    when(col("HdrTimeGeneratedTmstmp").isNull(),to_timestamp(lit('1901-01-01,00:00:00'),"yyyy-MM-dd,HH:mm:ss"))
                                                    .otherwise(col('HdrTimeGeneratedTmstmp'))
                                                )))))    

        # Fix missing or bad Event End Tmstmps for contract 14023:
        df_tfm_cn14023 = (df_parsed_14023.withColumn("EventEndTmstmp",
                                                        when(length(col('EventEndTmstmp')) == 19,to_timestamp(col('EventEndTmstmp'),"yyyy-MM-dd,HH:mm:ss"))
                                                        .otherwise(
                                                            when(length(col('EventEndTmstmp')) == 14,to_timestamp(col('EventEndTmstmp'),"yyyyMMddHHmmss"))
                                                            .otherwise(
                                                                when(col("HdrTimeGeneratedTmstmp").isNull(),to_timestamp(lit('1901-01-01,00:00:00'),"yyyy-MM-dd,HH:mm:ss"))
                                                                .otherwise(col('HdrTimeGeneratedTmstmp')))))
                                        .select('FileNameUUID','EventEndTmstmp','HdrDataContractNbr_EndParameter'))

        # Joining start and end parameters on the basis of filename:
        df_StardEndParameter = (df_14020_EveStrtmp
                                .join(df_tfm_cn14023,['FileNameUUID'],'left')
                                .withColumn('EventStartTmstmp',col("EventStartTmstmp"))                                
                                .withColumn('EventEndTmstmp',col("EventEndTmstmp"))
                                .withColumn('CreatedBy',lit("ADB_AllLogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_AllLogProcessing"))
                                .withColumn('UpdatedDt',lit(current_timestamp()))
                                .withColumn('StartEndParameterUUID',expr('uuid()'))
                                .drop('SourceFileName','ActualFileNameNm','EventSeqNbr'))
        
        w=(Window.partitionBy('FileNameUUID')
                 .orderBy(asc('EventStartTmstmp'),desc('EventEndTmstmp')))
        
        StardEndParameter_Final = (df_StardEndParameter.withColumn('rownum',row_number().over(w))
                                                       .filter("rownum = 1")
                                                       .drop('rownum'))
        
                                                                                                                                       

        StardEndParameter_Final=StardEndParameter_Final.withColumnRenamed("EventStartTmstmp","LogStartDate").withColumnRenamed("EventEndTmstmp","LogEndDate").select("LogStartDate","LogEndDate","FileNameUUID")
        return StardEndParameter_Final
         
    except Exception as exp:
        print(str(exp))
        # raise
                  


# COMMAND ----------

# MAGIC %md
# MAGIC # Process All Logs

# COMMAND ----------

def upsertAllLogFiles(df_logSource,configId, batchId):
    try:
        df_logSource_AllLogs = df_logSource.withColumn('ConfigId',lit(configId))
        
        w=Window.partitionBy('SourceFilePath','SourceFileSize').orderBy(desc('SourceFilePath'),desc('SourceFileSize'))
        df_UnProcessedLogs_CNT =(df_logSource_AllLogs.withColumn("row_Num",row_number().over(w))
                                .withColumn("RecdCnt",count('SourceFilePath').over(w))
                                .filter("row_num = 1").drop('row_num')
                                .withColumn('FileNameUUID',uuidUdf())
                                .withColumn("logstartdate",lit(None))
                                .withColumn("logEnddate",lit(None))
                                .select('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd'
                                        ,'FileNameDtTmstmp','FileNameApplicatorPortCd','FileNameCycleNbr','ConfigId','SourceFilePath'
                                        ,'SourceFileName','SourceFileSize', 'SourceTypeId','DeviceType','LogType','DeviceId'
                                        ,"RecdCnt",'FileNameUUID',"logstartdate","logEnddate","RawFileModificationTime","RunID"))
        
        
        # df_logSource_AllLogs=df_logSource_AllLogs.join(df_UnProcessedLogs_CNT.select("SourceFilePath","FileNameUUID"),'SourceFilePath','left')
    
        df_logSource_AllLogs.persist()
        df_UnProcessedLogs_CNT.persist()
        # print("df_logSource_AllLogs Count:"+str(df_logSource_AllLogs.count()))
        # print("df_UnProcessedLogs_CNT:"+str(df_UnProcessedLogs_CNT.count()))
        loadlogProcessesDeltaTable(df_UnProcessedLogs_CNT,deltaTablePath_AllLogs,'ADB_AllLogProcessing','InProgress','')
        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_AllLogProcessing','InProgress','')

        log_df = df_logSource_AllLogs.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                            .withColumn('Destination', lit(str(deltaTablePath_AllLogs))) \
                                            .withColumn('Run_ID', lit(str(batchId))) \
                                            .withColumn('Job_ID', lit(str(Job_id)))
            

        logSourceFileProcessedDF = (spark.read.format('delta')
                                .load(src_filesProcessed)
                                .filter("LogFileStatus = 'InProgress' and configid = {0}".format(configId))
                                
                                )
        logSourceFileProcessedDF.persist()
        # print("logSourceFileProcessedDF_Count:"+str(logSourceFileProcessedDF.count()))
        df_logSource_AllLogs=df_logSource_AllLogs.join(logSourceFileProcessedDF.select("SourceFilePath","FileNameUUID"),'SourceFilePath','inner')
        # print("matching_File Path Count:"+str(df_logSource_AllLogs.count()))
        
        Microbatch_df = df_logSource_AllLogs

        # raise Exception("No Exception: Manual Failure")

        df_logSource_AllLogs_Final  = (df_logSource_AllLogs.select('FileNameUUID','InternalSerialNbr','ExternalSerialNbr','FileNameDeviceTypeCd',
                                                                  'FileNameMessageTypeCd','HdrFormatVersionCd','HdrDateGeneratedDt','HdrTimeGeneratedTmstmp',
                                                                  'HdrLogtypeCd','HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCD',
                                                                  'HdrDataStoreID','HdrDataContractNbr','JSONField',"SourceFileName","SourceFilePath","SourceFileSize","RawFileModificationTime")
                                                            .withColumn('CreatedBy',lit("ABD_LogProcessingMaster"))
                                                            .withColumn('CreatedDt',lit(current_timestamp()))
                                                            .withColumn('UpdatedBy',lit("ABD_LogProcessingMaster"))
                                                            .withColumn('UpdatedDt',lit(current_timestamp()))
                                    )
        df_logSource_AllLogs_Final.write.format('delta').mode("append").save(deltaTablePath_AllLogs)

        # df_logSource_AllLogs=df_logSource_AllLogs.drop("FileNameUUID").join(logSourceFileProcessedDF.select("SourceFilePath","FileNameUUID"),'SourceFilePath','left')
        StartEndDf=processStartAndEndParameters(df_logSource_AllLogs)
        logSourceFileProcessedDF=logSourceFileProcessedDF.drop('LogStartDate','LogEndDate')
        logSourceFileProcessedDF=logSourceFileProcessedDF.join(StartEndDf,'FileNameUUID','left')
        # print("matching_File fileNameUUid Count:"+str(df_UnProcessedLogs_CNT.count()))

        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_AllLogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(logSourceFileProcessedDF,deltaTablePath_AllLogs,'ADB_AllLogProcessing','Succeeded','')
        
    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        loadlogProcessesDeltaTable(df_UnProcessedLogs_CNT,deltaTablePath_AllLogs,'ADB_AllLogProcessing','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_AllLogProcessing','Failed',str(exp))
        logIntoStreamLogTable(log_df,"ADB_AllLogProcessing","Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise
        

# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):      
    try:
        batchEnd(q,batchId)
        print("Running for BatchID: {0}".format(batchId))

        df_logSource = (microBatchOutputDF.filter('HdrDataContractNbr is not NULL')
                        .withColumn("SourceFileName_noext",regexp_replace(regexp_replace(
                                                                                split(col('SourceFileName'),'\.').getItem(0),'%20',''),' ',''))         
                        .withColumn("FileNameDeviceTypeCd",    upper(trim(split(col('SourceFileName_noext'), '_').getItem(0))))
                        .withColumn("FileNameDeviceSerialNbr", upper(trim(split(col('SourceFileName_noext'), '_').getItem(1))))
                        .withColumn("FileNameMessageTypeCd",   trim(split(col('SourceFileName_noext'), '_').getItem(2)))
                        .withColumn("FileNameDtTmstmp",        split(col('SourceFileName_noext'), '_').getItem(3))
                        .withColumn("FileNameApplicatorPortCd",split(col('SourceFileName_noext'), '_').getItem(4))
                        .withColumn("FileNameCycleNbr",        split(col('SourceFileName_noext'), '_').getItem(5))

                        .withColumn("DeviceType",col('FileNameDeviceTypeCd'))
                        .withColumn("LogType",col('FileNameMessageTypeCd'))
                        .withColumn("DeviceId",upper(trim(regexp_replace(regexp_replace(split(col("SourceFilePath"),'/').getItem(2),'%20',''),' ',''))))
                        .withColumn('SourceTypeId',lit(sourceTypeId))

                        .withColumn("HdrAppHeadNbr",when(col("HdrDataContractNbr").contains(':'),split(col("HdrDataContractNbr"),":").getItem(1)).otherwise(0))
                        .withColumn('InternalSerialNbr',upper(trim(regexp_replace(regexp_replace(split(col("SourceFilePath"),'/').getItem(2),'%20',''),' ',''))))
                        .withColumn('ExternalSerialNbr',col('FileNameDeviceSerialNbr'))
                        .withColumn('RunID',lit(batchId))

                        .drop('SourceFileName_noext')
                        .fillna({'FileNameApplicatorPortCd':''})
                        .filter(((col('LogType').isin(logTypes_Processed)) |
                                (col("HdrDataContractNbr").isin(contractNumberList_StartEnd))))                     
                )

        
        upsertAllLogFiles(df_logSource,allLogs_ConfigId, batchId)
        # print("---------------------------------------------------------")
        spark.sql("clear cache")
    except Exception as exp:
        print(str(exp))
        # raise    

    
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming job to process log data

# COMMAND ----------

q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Transformation_COM3_CT_AllLogs_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", deltaTablePath_AllLogs+checkPointLocation)
                  .outputMode("update")
                  .start()
                #   .awaitTermination()
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()

# COMMAND ----------

# q.stop()
