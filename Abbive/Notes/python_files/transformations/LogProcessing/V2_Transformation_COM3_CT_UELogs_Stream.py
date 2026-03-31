# Databricks notebook source
dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

#--------------dbutils.widgets.text('sourceFilePath','/mnt/silver/FACTAllLogs')
dbutils.widgets.text('sourceFilePath',ExternalLocation_silver+'/FACTAllLogs')

sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('ueLogs_ConfigId','18')
ueLogs_ConfigId = dbutils.widgets.get('ueLogs_ConfigId')

dbutils.widgets.text('sourceTypeId','3')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

dbutils.widgets.text('startingVersion','0')
startingVersion = dbutils.widgets.get('startingVersion')
print("startingVersion:"+str(startingVersion))

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

# MAGIC %run
# MAGIC ../../Configurations/EmailNotificationConfiguration

# COMMAND ----------

checkPointLocation = "/_checkpoints/"
ueLogsPath = ExternalLocation_silver+'/FACTUELogs/'
logTypes_Processed = ['UE'] 
contractNumberList_UE = ["21544","21544:1","21544:2","21543","21543:1","21543:2","21519","84009","21900"]
contractNumberList_StartEnd = ["14020","14023"]


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
# MAGIC #Initialize Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Define File Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
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



UESchema = StructType(
    [StructField('date', StringType(), True),
     StructField('time', StringType(), True),
 	 StructField('tcode', StringType(), True),
     StructField('adddata', StringType(), True),
     StructField('ZCodeMajor', StringType(), True),
     StructField('ZCodeMinor', StringType(), True),
     StructField('MajorCode', StringType(), True),
     StructField('MinorCode', StringType(), True),          
     
 	 StructField('ExceptioInfo', StructType([
        StructField('code', StringType(), True),
        StructField('exceptionType', StringType(), True),
        StructField('activeState', StringType(), True),
        StructField('actionType', StringType(), True),
        StructField('description', StringType(), True),
        StructField('data1', StringType(), True),
        StructField('data2', StringType(), True),
        StructField('string1', StringType(), True)
         ])),
        
 	 StructField('userRecoveryData', StructType([
         StructField('contractId', StringType(), True),
         StructField('recoveryData', StructType([
             StructField('active', StringType(), True),
             StructField('severity', StringType(), True),
             StructField('action', StringType(), True),
             StructField('data1', StringType(), True),
             StructField('data2', StringType(), True),
             StructField('string1', StringType(), True)
         ]))]))])
   

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


parsed_UE_cols = ['FileNameUUID','ExternalSerialNbr','InternalSerialNbr','HdrAppHeadNbr','HdrDateGeneratedDt', 'HdrFormatVersionCd', 'HdrTimeGeneratedTmstmp', 
                  'HdrLogtypeCd', 'HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCD', 'HdrDataStoreID', 'HdrDataContractNbr', 'SourceFileName',
                  'SourceFileSize','SourceFilePath','FileNameDeviceTypeCd',
                  when(col('MajorCode').isNull(),col('ZCodeMajor')).otherwise(col('MajorCode')).alias("ZCodeMajorCd"),
                when(col('MinorCode').isNull(),col('ZCodeMinor')).otherwise(col("MinorCode")).alias("ZCodeMinorCd"),
                  col('contractId').alias('RecoveryContractID'),
                  col('active').alias('RecoveryActiveStatusInd'),
                  col('severity').alias('RecoverySeverityCd'),
                  col('action').alias('RecoveryActionCd'),
                  col('data1').alias('RecoveryData1Txt'),
                  col('data2').alias('RecoveryData2Txt'),
                  col('string1').alias('RecoveryMessageTxt'),
                  col('date').alias('ErrorDt'),
                  col('time').alias('ErrorTm'),
                  col('tcode').alias('TCode'),
                  col('adddata').alias('ErrorDesc'),
                  col('ExceptioInfo.code').alias('RS_ExceptionInfoCode'),
                  col('ExceptioInfo.exceptionType').alias('RS_ExceptionInfoExceptiontype'),
                  col('ExceptioInfo.activeState').alias('RS_ExceptionInfoActiveState'),
                  col('ExceptioInfo.actionType').alias('RS_ExceptionInfoActiontype'),
                  col('ExceptioInfo.description').alias('RS_ExceptionInfoDescription'),
                  col('ExceptioInfo.data1').alias('RS_ExceptionInfoData1'),
                  col('ExceptioInfo.data2').alias('RS_ExceptionInfoData2'),
                  col('ExceptioInfo.string1').alias('RS_ExceptionInfoString1')]

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Source File and Autoloader settings

# COMMAND ----------

df_Source = (spark.readStream.option("cloudFiles.maxFilesPerTrigger",5000) 
                              .option("cloudFiles.maxBytesPerTrigger",'10g')
                                .option("startingVersion", startingVersion)
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
                                .withColumn('CreatedBy',lit("ADB_UELogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_UELogProcessing"))
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
# MAGIC # Process UE logs for COM3 and CT

# COMMAND ----------

def Process_UELogs(DF_Source_UELogs,StartEndDf,ueLogs_ConfigId,batchId):
    DF_Source_UELogs=DF_Source_UELogs.withColumn('ConfigId',lit(ueLogs_ConfigId)).withColumn("LogStartDate",lit(None)).withColumn("LogEndDate",lit(None))
    DF_Source_UELogs.persist()
    df_ueLogs_CNT = (DF_Source_UELogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp','FileNameApplicatorPortCd',
                                              'FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId','FileNameUUID',
                                              'DeviceType','LogType','DeviceId','ConfigId',"RawFileModificationTime","RunID")
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
    df_ueLogs_CNT.persist()
    loadlogProcessesDeltaTable(df_ueLogs_CNT,ueLogsPath,'ADB_UELogProcessing','InProgress','')

    DF_Source_UELogs = DF_Source_UELogs.filter(col("HdrDataContractNbr").isin(contractNumberList_UE))
    loadAuditTables_Ingestion_Log(DF_Source_UELogs,ueLogsPath,'ADB_UELogProcessing','InProgress','')    
   
    try:
        log_df = DF_Source_UELogs.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(ueLogsPath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = DF_Source_UELogs  

        # raise Exception("No Exception: Manual Failure")  
        
        df_UELogs = jsonParser(DF_Source_UELogs,'JSONField','FACTUELogs').drop('SourceFileName', 'SourceFilePath', 'SourceFileSize','RawFileModificationTime','FileNameMessageTypeCd','RunID', 'LogEndDate', 'JSONField', 'FileNameDeviceSerialNbr', 'FileNameApplicatorPortCd', 'DeviceId', 'FileNameCycleNbr', 'SourceTypeId', 'LogStartDate', 'LogType', 'FileNameDtTmstmp', 'DeviceType', 'ConfigId')
        # df_UELogs = (DF_Source_UELogs.withColumn("JSONField", from_json("JSONField", UESchema))
        #                                      .select('*',col('JSONField.*'))
        #                                      .select('*',col('userRecoveryData.*'))
        #                                      .select('*',col('recoveryData.*')))
                                             
        # df_UELogs_Parsed = df_UELogs.select(parsed_UE_cols)
        

        df_AllUELogs = (df_UELogs
                         .withColumn('CreatedBy', lit("ADB_UELogProcessing"))
                         .withColumn('CreatedDt',lit(current_timestamp()))
                         .withColumn('UpdatedBy', lit("ADB_UELogProcessing"))
                         .withColumn('UpdatedDt',lit(current_timestamp()))
                         .withColumn('UELogsUUID',expr('uuid()')))

        DedupSeq = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","HdrDataContractNbr","HdrAppHeadNbr","HdrTimeGeneratedTmstmp")
                        .orderBy(desc(col('ErrorTm'))))
        
        df_AllUELogs_Dedup  = df_AllUELogs.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')        

        df_UELogs_Final,InsertMergeDict,UpdateMergeDict=evolveSchema(df_AllUELogs_Dedup,'FACTUELogs')
        
####################################
        # Inserting new records to target:
        DeltaTbl_UE_Logs = DeltaTable.forPath(spark, ueLogsPath)  
#####################################################
        (DeltaTbl_UE_Logs.alias("tgt")
        .merge(df_UELogs_Final.alias("src"),
                "tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.HdrTimeGeneratedTmstmp = src.HdrTimeGeneratedTmstmp and tgt.HdrDataContractNbr = src.HdrDataContractNbr AND tgt.HdrAppHeadNbr = src.HdrAppHeadNbr"
                ).whenNotMatchedInsert(values = InsertMergeDict) 
        .execute()
        )
#####################################################
        # (DeltaTbl_UE_Logs.alias("tgt")
        #         .merge(df_AllUELogs_Dedup.alias("src"),
        #             ("tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.HdrTimeGeneratedTmstmp = src.HdrTimeGeneratedTmstmp and tgt.HdrDataContractNbr = src.HdrDataContractNbr AND tgt.HdrAppHeadNbr = src.HdrAppHeadNbr"))
        #   .whenNotMatchedInsert(values =
        #   {
        #     "tgt.UELogsUUID": "src.UELogsUUID",
        #     "tgt.FileNameUUID": "src.FileNameUUID",
        #     "tgt.HdrFormatVersionCd": "src.HdrFormatVersionCd",
        #     "tgt.HdrDateGeneratedDt": "src.HdrDateGeneratedDt",
        #     "tgt.HdrTimeGeneratedTmstmp": "src.HdrTimeGeneratedTmstmp",
        #     "tgt.HdrLogTypeCd": "src.HdrLogTypeCd",
        #     "tgt.HdrDestinationSubSystemCd": "src.HdrDestinationSubSystemCd",
        #     "tgt.HdrSourceSubSystemCd": "src.HdrSourceSubSystemCd",
        #     "tgt.HdrCommandCd": "src.HdrCommandCd",
        #     "tgt.HdrDataStoreID": "src.HdrDataStoreID",
        #     "tgt.HdrDataContractNbr": "src.HdrDataContractNbr",
        #     "tgt.FileNameDeviceTypeCd": "src.FileNameDeviceTypeCd",
        #     "tgt.ZCodeMajorCd": "src.ZCodeMajorCd",
        #     "tgt.ZCodeMinorCd": "src.ZCodeMinorCd",
        #     "tgt.RecoveryContractID": "src.RecoveryContractID",
        #     "tgt.RecoveryActiveStatusInd": "src.RecoveryActiveStatusInd",
        #     "tgt.RecoverySeverityCd": "src.RecoverySeverityCd",
        #     "tgt.RecoveryActionCd": "src.RecoveryActionCd",
        #     "tgt.RecoveryData1Txt": "src.RecoveryData1Txt",
        #     "tgt.RecoveryData2Txt": "src.RecoveryData2Txt",
        #     "tgt.RecoveryMessageTxt": "src.RecoveryMessageTxt",
        #     "tgt.ErrorDt": "src.ErrorDt",
        #     "tgt.ErrorTm": "src.ErrorTm",
        #     "tgt.TCode": "src.TCode",

        #     "tgt.RS_ExceptionInfoCode": "src.RS_ExceptionInfoCode",
        #     "tgt.RS_ExceptionInfoExceptiontype": "src.RS_ExceptionInfoExceptiontype",
        #     "tgt.RS_ExceptionInfoActiveState": "src.RS_ExceptionInfoActiveState",
        #     "tgt.RS_ExceptionInfoActiontype": "src.RS_ExceptionInfoActiontype",
        #     "tgt.RS_ExceptionInfoDescription": "src.RS_ExceptionInfoDescription",
        #     "tgt.RS_ExceptionInfoData1": "src.RS_ExceptionInfoData1",
        #     "tgt.RS_ExceptionInfoData2": "src.RS_ExceptionInfoData2",
        #     "tgt.RS_ExceptionInfoString1": "src.RS_ExceptionInfoString1",

        #     "tgt.HdrAppHeadNbr": "src.HdrAppHeadNbr",
        #     "tgt.InternalSerialNbr": "src.InternalSerialNbr",
        #     "tgt.ExternalSerialNbr": "src.ExternalSerialNbr",
        #     "tgt.CreatedBy": "src.CreatedBy",
        #     "tgt.CreatedDt": "src.CreatedDt",
        #     "tgt.UpdatedBy": "src.UpdatedBy",
        #     "tgt.UpdatedDt": "src.UpdatedDt"
        #   }
        # ).execute())

        df_ueLogs_CNT=df_ueLogs_CNT.drop('LogStartDate','LogEndDate')
        df_ueLogs_CNT=df_ueLogs_CNT.join(StartEndDf,'FileNameUUID','left')

        loadAuditTables_Ingestion_Log(DF_Source_UELogs,ueLogsPath,'ADB_UELogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(df_ueLogs_CNT,ueLogsPath,'ADB_UELogProcessing','Succeeded','')
        

    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        loadAuditTables_Ingestion_Log(DF_Source_UELogs,ueLogsPath,'ADB_UELogProcessing','Failed',str(exp))
        loadlogProcessesDeltaTable(df_ueLogs_CNT,ueLogsPath,'ADB_UELogProcessing','Failed',str(exp))
        
        logIntoStreamLogTable(log_df,"ADB_UELogProcessing","Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise            


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

def upsertToDelta(microBatchOutputDF, batchId):     
    batchEnd(q,batchId)
    print("Running for BatchID: {0}".format(batchId))
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
                     .withColumn('RunID',lit(batchId))
                     .filter(((col('LogType').isin(logTypes_Processed)) 
                            #   |(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
                             ))
                     
                    #  .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
                    #                                             ,"yyyy-MM-dd HH:mm:ss.SSS"))

                     .withColumn("LogStartDate",lit(None))
                     .withColumn("LogEndDate",lit(None))
                     .drop('SourceFileName_noext')
                     .fillna({'FileNameApplicatorPortCd':''})
                     
               )

    
    try:

        StartEndLog = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
        StartEndDf=processStartAndEndParameters(StartEndLog)

       
        Process_UELogs(df_logSource,StartEndDf,ueLogs_ConfigId,batchId)
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
                  .queryName("V2_Transformation_COM3_CT_UELogs_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", ueLogsPath+checkPointLocation)
                  .outputMode("update")
                  .start()
                #   .awaitTermination()
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()
