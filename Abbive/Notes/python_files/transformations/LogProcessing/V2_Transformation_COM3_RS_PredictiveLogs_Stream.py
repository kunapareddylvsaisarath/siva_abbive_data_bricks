# Databricks notebook source
# MAGIC %sql 
# MAGIC set spark.databricks.delta.schema.autoMerge.enabled = True

# COMMAND ----------

dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

#---------------dbutils.widgets.text('sourceFilePath','/mnt/silver/FACTAllLogs')
dbutils.widgets.text('sourceFilePath',ExternalLocation_silver+'/FACTAllLogs')
sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('PredictiveLogs_ConfigId','35')
PredictiveLogs_ConfigId = dbutils.widgets.get('PredictiveLogs_ConfigId')


dbutils.widgets.text('sourceTypeId','3')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

dbutils.widgets.text('startingVersion','355')
startingVersion = dbutils.widgets.get('startingVersion')
print("startingVersion:"+str(startingVersion))

dbutils.widgets.text('EmailNotificationID','3')
EmailNotificationID = dbutils.widgets.get('EmailNotificationID')

dbutils.widgets.text("job_id","-1")
JobId = dbutils.widgets.get("job_id")

dbutils.widgets.text("run_id","-1")
PipelineRunId = dbutils.widgets.get("run_id")

dbutils.widgets.text('Env','Dev')
Env = dbutils.widgets.get('Env')

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts

# COMMAND ----------

# MAGIC %run
# MAGIC /Configurations/EmailNotificationConfiguration

# COMMAND ----------

import traceback

pipelinename = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipelinename = pipelinename.rsplit('/', 1)[-1]
display(pipelinename)

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------

checkPointLocation = "/_checkpoints/"
#-------------startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
#-------------logFilesProcessedPath = '/mnt/silver/LogSourceFilesProcessed/'
#-------------PredictiveLogsPath =  '/mnt/silver/PredictiveLogs_H/'
startEndParameterPath = ExternalLocation_silver+'/DIMStartEndParameter'
logFilesProcessedPath = ExternalLocation_silver+'/LogSourceFilesProcessed/'
PredictiveLogsPath =  ExternalLocation_silver+'/PredictiveLogs_H/'

logTypes_Processed = ['SYS','ENGR'] 
contractNumberList_StartEnd = ["14020","14023"]

#-------------------df = spark.read.format('delta').load('/mnt/silver/DatabaseSchema')
df = spark.read.format('delta').load(ExternalLocation_silver+'/DatabaseSchema')


a = df.filter((df.TableName == "FactPredictiveHistory") & (df.ContractName.isNotNull()) & (df.isActive == "1")).select("ContractName").distinct()

a = a.collect()

lis = []
for i in a:
    lis.append(i["ContractName"])
contractNumberList_PredictiveLogs = []

for item in lis:
    split_values = item.split(",")
    contractNumberList_PredictiveLogs.extend(split_values)

print(contractNumberList_PredictiveLogs)

# COMMAND ----------


subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
QueueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")

vault_url = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-KeyVaultURL")

# COMMAND ----------

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

# MAGIC %md
# MAGIC # Expression Column Rename

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

df_Source = (spark.readStream.option("cloudFiles.maxFilesPerTrigger",10) 
                             .option("startingVersion", startingVersion)
                             .option("cloudFiles.maxBytesPerTrigger",'10g')
                             .option("skipChangeCommits", "true")
                             .format("delta").load(sourceFilePath))

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
                                .withColumn('CreatedBy',lit("ADB_PredictiveLogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_PredictiveLogProcessing"))
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
# MAGIC # Process Predictive logs for COM3 and RS

# COMMAND ----------

def Process_PredictiveLogs(DF_Source_PredictiveLogs,StartEndDf,PredictiveLogs_ConfigId,batchId):
    try:
        DF_Source_PredictiveLogs=DF_Source_PredictiveLogs.withColumn('ConfigId',lit(PredictiveLogs_ConfigId)).withColumn("LogStartDate",lit(None)).withColumn("LogEndDate",lit(None))
        DF_Source_PredictiveLogs.persist()


        df_PredictiveLogs_CNT = (DF_Source_PredictiveLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr',
                    'FileNameMessageTypeCd','FileNameDtTmstmp','FileNameApplicatorPortCd','FileNameCycleNbr','ConfigId',
                    'LogStartDate','LogEndDate','SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId','FileNameUUID',
                    'DeviceType','LogType','DeviceId',"RawFileModificationTime","RunID")
                     .count()
                     .withColumnRenamed("count","RecdCnt") 
                    #  .withColumn('FileNameUUID',uuidUdf())                           
                       )
        df_PredictiveLogs_CNT.persist()
        # DF_Source_PredictiveLogs = DF_Source_PredictiveLogs.join(df_PredictiveLogs_CNT.select('FileNameUUID','SourceFilePath'),'SourceFilePath','left') 

        loadlogProcessesDeltaTable(df_PredictiveLogs_CNT,PredictiveLogsPath,'ADB_PredictiveLogProcessing','InProgress','')
        
        DF_Source_PredictiveLogs = DF_Source_PredictiveLogs.filter(col("HdrDataContractNbr").isin(contractNumberList_PredictiveLogs))
        loadAuditTables_Ingestion_Log(DF_Source_PredictiveLogs,PredictiveLogsPath,'ADB_PredictiveLogProcessing','InProgress','')        

        # raise Exception("No Exception: Manual Failure")

        df_STG_PredictiveLogs=(DF_Source_PredictiveLogs.withColumn("CreatedBy",lit("ADB_PredictiveLogProcessing"))
                                            .withColumn("CreatedDt",current_timestamp())
                                            .withColumn("UpdatedBy",lit("ADB_PredictiveLogProcessing"))
                                            .withColumn("UpdatedDt",current_timestamp())
                                            # .withColumn("JSONField", from_json("JSONField", Schema_Sys))

                                            # .select('*', col('JSONField.*'))
                                            
                                            )

        df_STG_PredictiveLogs_Parsed = jsonParser(df_STG_PredictiveLogs,"JSONField",'FactPredictiveHistory')


        

        # w=Window.partitionBy('ExternalSerialNbr','HdrDataContractNbr').orderBy(desc('HdrTimeGeneratedTmstmp'),desc('SourceFileSize'))
        
        # df_STG_PredictiveLogs_Latest =(df_STG_PredictiveLogs_Parsed.dropDuplicates().withColumn("row_Num",row_number().over(w)).filter("row_num = 1").drop('row_num'))

     

        # return df_STG_PredictiveLogs_Latest
        
        df_STG_PredictiveLogs_Latest = df_STG_PredictiveLogs_Parsed.drop("FileNameMessageTypeCd","JSONField","SourceFilePath","SourceFileSize"
        ,"RawFileModificationTime"
        ,"FileNameDeviceSerialNbr"
        ,"FileNameDtTmstmp"
        ,"FileNameApplicatorPortCd"
        ,"FileNameCycleNbr"
        ,"DeviceType"
        ,"LogType"
        ,"DeviceId"
        ,"SourceTypeId"
        ,"RunID"
        ,"ConfigId"
        ,"LogStartDate"
        ,"LogEndDate"
        )

        df_STG_PredictiveLogs_Latest,InsertMergeDict,UpdateMergeDict=evolveSchema(df_STG_PredictiveLogs_Latest,'FactPredictiveHistory')


        column_list = df_STG_PredictiveLogs_Latest.columns
        column_list.sort()
        lis =  ['FileNameUUID','FileNameDeviceTypeCd','HdrFormatVersionCd','HdrLogTypeCd','HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCd', 'HdrDataStoreID','HdrAppHeadNbr','SourceFileName', 'CreatedBy','CreatedDt','UpdatedBy','UpdatedDt' ,'hash_id' ] 
        
        for i in lis:
            column_list.remove(i)

        column_list.sort()
        # print(column_list)
        df_STG_PredictiveLogs_Latest = df_STG_PredictiveLogs_Latest.withColumn("LastReplacedTime",
                                                        when(length(col('LastReplacedTime')) == 18,to_timestamp(col('LastReplacedTime'),"yyyyMMddHHmmss.SSS"))
                                                         .otherwise(col('LastReplacedTime')))
        
        df_STG_PredictiveLogs_Latest = df_STG_PredictiveLogs_Latest.withColumn("hash_id", sha2(concat_ws("||", *column_list), 256))

        
        df_STG_PredictiveLogs_Latest = df_STG_PredictiveLogs_Latest.dropDuplicates(subset=["hash_id"])



        DeltaTbl_Predictive = DeltaTable.forPath(spark, PredictiveLogsPath)  

        (DeltaTbl_Predictive.alias("tgt")
                .merge(df_STG_PredictiveLogs_Latest.alias("src"),
                        "tgt.hash_id = src.hash_id").whenNotMatchedInsert(values = InsertMergeDict).execute())

       
        
        df_PredictiveLogs_CNT=df_PredictiveLogs_CNT.drop('LogStartDate','LogEndDate')
        df_PredictiveLogs_CNT=df_PredictiveLogs_CNT.join(StartEndDf,'FileNameUUID','left')
        
        loadAuditTables_Ingestion_Log(DF_Source_PredictiveLogs,PredictiveLogsPath,'ADB_PredictiveLogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(df_PredictiveLogs_CNT,PredictiveLogsPath,'ADB_PredictiveLogProcessing','Succeeded','')
       
    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        print(ExceptionTraceback)
        loadlogProcessesDeltaTable(df_PredictiveLogs_CNT,PredictiveLogsPath,'ADB_PredictiveLogProcessing','Failed',str(exp))
        loadAuditTables_Ingestion_Log(DF_Source_PredictiveLogs,PredictiveLogsPath,'ADB_PredictiveLogProcessing','Failed',str(exp))
        
        # logIntoStreamLogTable(log_df,"ADB_PredictiveLogProcessing","Failed",Microbatch_df,ErrorMessage)
        # streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        raise ErrorMessage           

# COMMAND ----------

def processPredictiveLogs(microBatchOutputDF, batchId):
    df_logSource = (microBatchOutputDF.where("SourceFilePath is not Null")
                    # .withColumn('JSONField',concat(lit('{'),col('JSONField')))
                    .withColumn('SourceFilePath', regexp_replace('SourceFilePath','/mnt/raw/',''))
                    .withColumn('SourceFileName', regexp_replace('SourceFileName','%20',''))
                    
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
                     
                     .filter(((col('FileNameMessageTypeCd').isin(logTypes_Processed)) 
                            #   |(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
                             ))
                     .withColumn('RunID',lit(batchId))
                    #  .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
                    #                                             ,"yyyy-MM-dd HH:mm:ss.SSS"))

                     .withColumn("LogStartDate",lit(None))
                     .withColumn("LogEndDate",lit(None))
                     .drop('SourceFileName_noext')
                     .fillna({'FileNameApplicatorPortCd':''})
                     
               )
    StartEndLog = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
    StartEndDf=processStartAndEndParameters(StartEndLog)
    
    # df_PredictiveLogs = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_PredictiveLogs))
    Process_PredictiveLogs(df_logSource,StartEndDf,PredictiveLogs_ConfigId,batchId)
    spark.sql("clear cache")
    # print("-------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):  
    log_Stream = {
        "ConfigID" : PredictiveLogs_ConfigId,
        "SourceTypeID" : sourceTypeId,
        "Source" : "silverzone.factalllogs",
        "Destination" : "silverzone.factpredictivehistory",
        "Run_ID": str(batchId),
        "Job_ID": str(Job_id)
        }
    df_logstream = spark.createDataFrame([log_Stream])

    batchEnd(q,batchId)
    print("Running for BatchID: {0}".format(batchId))

    df_logSourceReprocessed = spark.read.table("silverzone.fact_streamlogs").filter((col("ConfigId") == f'{PredictiveLogs_ConfigId}') & (col("PipelineStatus") == 'Failed') & (col("ReprocessStatus") == '')) #trim status
    if len(df_logSourceReprocessed.head(1)) >= 1:
        try:
            print("Reprocess Predictive Logs :")
            setWorkFlowStatus_StreamLog(df_logSourceReprocessed,"InProgress")

            df_logSource = spark.read.table("silverzone.fact_streamlogs").filter((col("ConfigId") == f'{PredictiveLogs_ConfigId}') & (col("ReprocessStatus") == 'InProgress'))
            df_schema = spark.read.table("silverzone.factalllogs").limit(1)
            alllogs_schema = df_schema.schema

            df_flattenlogs = df_logSource.withColumn("flatten_batch",from_json(col("MicroBatchData"),alllogs_schema)).select(col("flatten_batch.*"))
            
            processPredictiveLogs(df_flattenlogs,batchId)
            setWorkFlowStatus_StreamLog(df_logSource,"Success")
        except Exception as exp:
            ExceptionTraceback = traceback.format_exc()
            ErrorMessage = ExceptionTraceback + str(exp)
            print(ExceptionTraceback)
            logIntoStreamLogTable(df_logSourceReprocessed,"ADB_SysLogProcessing","Failed",None,ErrorMessage)
            streamLogEmailNotification(EmailNotificationID,df_logSourceReprocessed, pipelinename, Env)
    
    try:
        print("Processing Predictive Logs")
        processPredictiveLogs(microBatchOutputDF,batchId)
    except Exception as exp:
        print(str(exp))
        logIntoStreamLogTable(df_logSourceReprocessed,"ADB_SysLogProcessing","Failed",None,exp)
        streamLogEmailNotification(EmailNotificationID,df_logSourceReprocessed, pipelinename, Env)       

# COMMAND ----------

 q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Transformation_COM3_CT_PredictiveLogs_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", PredictiveLogsPath+checkPointLocation)
                  .outputMode("update")
                  .start()
                #   .awaitTermination(600)
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()
