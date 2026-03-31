# Databricks notebook source
# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = true

# COMMAND ----------

dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

#-------------------------dbutils.widgets.text('sourceFilePath','/mnt/silver/FACTAllLogs')
dbutils.widgets.text('sourceFilePath',ExternalLocation_silver+'/FACTAllLogs')
sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('sysLogs_ConfigId','19')
sysLogs_ConfigId = dbutils.widgets.get('sysLogs_ConfigId')


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

print(sourceFilePath)

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

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------

checkPointLocation = "/_checkpoints/"
#-----------startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
#------------logFilesProcessedPath = '/mnt/silver/LogSourceFilesProcessed/'
#------------sysLogsPath = '/mnt/silver/FACTSysLogs/'
startEndParameterPath = ExternalLocation_silver+'/DIMStartEndParameter'
logFilesProcessedPath = ExternalLocation_silver+'/LogSourceFilesProcessed/'
sysLogsPath = ExternalLocation_silver+'/FACTSysLogs/'


logTypes_Processed = ['SYS','ENGR'] 
contractNumberList_StartEnd = ["14020","14023"]
# contractNumberList_SysLogs = ['21062','15024','15039','15008','21019', '15048' ,'15006' ,'15016','22270:2','22270:1','22270','15011']
#-------------df = spark.read.format('delta').load('/mnt/silver/DatabaseSchema')
df = spark.read.format('delta').load(ExternalLocation_silver+'/DatabaseSchema')
a = df.filter((df.TableName == "FACTSysLogs") & (df.ContractName.isNotNull()) & (df.isActive == "1")).select("ContractName").distinct()

a = a.collect()
# print(a)
lis = []
for i in a:
    lis.append(i["ContractName"])
contractNumberList_SysLogs = []

for item in lis:
    split_values = item.split(",")
    contractNumberList_SysLogs.extend(split_values)




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
              .option("skipChangeCommits", "true")
                              .option("cloudFiles.maxBytesPerTrigger",'10g').format("delta").load(sourceFilePath))

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
                                .withColumn('CreatedBy',lit("ADB_SysLogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_SysLogProcessing"))
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
# MAGIC # Process Sys logs for COM3 and CT

# COMMAND ----------

def Process_SYSLogs(DF_Source_SysLogs,StartEndDf,sysLogs_ConfigId,batchId):
    try:
        DF_Source_SysLogs=DF_Source_SysLogs.withColumn('ConfigId',lit(sysLogs_ConfigId)).withColumn("LogStartDate",lit(None)).withColumn("LogEndDate",lit(None))
        DF_Source_SysLogs.persist()


        
        df_sysLogs_CNT = (DF_Source_SysLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr',
                    'FileNameMessageTypeCd','FileNameDtTmstmp','FileNameApplicatorPortCd','FileNameCycleNbr','ConfigId',
                    'LogStartDate','LogEndDate','SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId','FileNameUUID',
                    'DeviceType','LogType','DeviceId',"RawFileModificationTime","RunID")
                     .count()
                     .withColumnRenamed("count","RecdCnt") 
                    #  .withColumn('FileNameUUID',uuidUdf())                           
                       )
        df_sysLogs_CNT.persist()
        # DF_Source_SysLogs = DF_Source_SysLogs.join(df_sysLogs_CNT.select('FileNameUUID','SourceFilePath'),'SourceFilePath','left') 

        loadlogProcessesDeltaTable(df_sysLogs_CNT,sysLogsPath,'ADB_SysLogProcessing','InProgress','')


        DF_Source_SysLogs = DF_Source_SysLogs.filter(col("HdrDataContractNbr").isin(contractNumberList_SysLogs))
        loadAuditTables_Ingestion_Log(DF_Source_SysLogs,sysLogsPath,'ADB_SysLogProcessing','InProgress','')        
        
        log_df = DF_Source_SysLogs.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(sysLogsPath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = DF_Source_SysLogs

        # raise Exception("No Exception: Manual Failure")

        df_STG_SysLogs=(DF_Source_SysLogs.withColumn("CreatedBy",lit("ADB_SysLogProcessing"))
                                            .withColumn("CreatedDt",current_timestamp())
                                            .withColumn("UpdatedBy",lit("ADB_SysLogProcessing"))
                                            .withColumn("UpdatedDt",current_timestamp())
                                            .withColumn("JSONField", when((DF_Source_SysLogs.HdrDataContractNbr == "15011") | (DF_Source_SysLogs.HdrDataContractNbr == "15016")  , regexp_replace('JSONField', '\\\\', '\\\\\\\\')).otherwise(DF_Source_SysLogs.JSONField))
                                            # .withColumn("JSONField", from_json("JSONField", Schema_Sys))

                                            # .select('*', col('JSONField.*'))
                                            
                                            )
        
        df_STG_SysLogs_Parsed = jsonParser(df_STG_SysLogs,"JSONField",'FACTSysLogs')


        

        w=Window.partitionBy('ExternalSerialNbr','HdrDataContractNbr').orderBy(desc('HdrTimeGeneratedTmstmp'),desc('SourceFileSize'))
        
        df_STG_SysLogs_Latest =(df_STG_SysLogs_Parsed.dropDuplicates().withColumn("row_Num",row_number().over(w)).filter("row_num = 1").drop('row_num'))

        df_STG_SysLogs_Latest = (df_STG_SysLogs_Latest
                                 .filter(((col('HdrDataContractNbr') == '15008') & (col('SIMCardID').isNotNull()) )
                                          | 
                                         (col('HdrDataContractNbr') != '15008')))

        df_STG_SysLogs_Latest_87516 = (df_STG_SysLogs_Latest.filter(col('HdrDataContractNbr') == '87516'))
        df_STG_SysLogs_Latest = (df_STG_SysLogs_Latest.filter(col('HdrDataContractNbr') !='87516'))
        
        df_STG_SysLogs_Latest_87516 = df_STG_SysLogs_Latest_87516.withColumn("RS_PowerOnTimeStamp",df_STG_SysLogs_Latest_87516.RS_PowerOnTimeStamp.cast(StringType()))

        df_STG_SysLogs_Latest_87516 = df_STG_SysLogs_Latest_87516.withColumn("RS_PowerOnTimeStamp",
                                                        when(length(col('RS_PowerOnTimeStamp')) == 18,to_timestamp(col('RS_PowerOnTimeStamp'),"yyyyMMddHHmmss.SSS"))
                                                         .otherwise(None))
       


        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.union(df_STG_SysLogs_Latest_87516)

###################################################Changes as per AAIOT-3116######################################################
       #Adding RSRP and RSRQ Columns
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.withColumn(
                                                        "RSRP",
                                                        when(
                                                            col("HdrDataContractNbr") == "15024",
                                                            regexp_extract(col("crespid"), r"RSRP:([-\d\.]+)", 1)
                                                        ).otherwise(None).cast(StringType())
                                                    ).withColumn(
                                                        "RSRQ",
                                                        when(
                                                            col("HdrDataContractNbr") == "15024",
                                                            regexp_extract(col("crespid"), r"RSRQ:([-\d\.]+)", 1)
                                                        ).otherwise(None).cast(StringType())
                                                    )
############################################################END################################################################
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.drop("LogStartDate")
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.drop("LogEndDate")
 
        # return df_STG_SysLogs_Latest
        
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.drop("FileNameMessageTypeCd","JSONField","SourceFilePath","SourceFileSize"
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
        )
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.withColumn("HdrFormatVersionCd",df_STG_SysLogs_Latest.HdrFormatVersionCd.cast(StringType()))
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.withColumn("HdrAppHeadNbr",df_STG_SysLogs_Latest.HdrAppHeadNbr.cast(IntegerType()))
        
        df_STG_SysLogs_Latest = df_STG_SysLogs_Latest.withColumn("RS_PowerOnTimeStamp",df_STG_SysLogs_Latest.RS_PowerOnTimeStamp.cast(TimestampType()))

        df_STG_SysLogs_Latest,InsertMergeDict,UpdateMergeDict=evolveSchema(df_STG_SysLogs_Latest,'FACTSysLogs')


        DeltaTbl_SYS = DeltaTable.forPath(spark, sysLogsPath)  

        (DeltaTbl_SYS.alias("tgt")
                .merge(df_STG_SysLogs_Latest.alias("src"),
                        "tgt.ExternalSerialNbr = src.ExternalSerialNbr AND tgt.InternalSerialNbr = src.InternalSerialNbr AND tgt.HdrDataContractNbr = src.HdrDataContractNbr")
                .whenMatchedUpdate(
                    condition = "src.HdrTimeGeneratedTmstmp > tgt.HdrTimeGeneratedTmstmp",
                  set = UpdateMergeDict).whenNotMatchedInsert(values = InsertMergeDict) 
        .execute()
        )

        # print("done")
        
        df_sysLogs_CNT=df_sysLogs_CNT.drop('LogStartDate','LogEndDate')
        df_sysLogs_CNT=df_sysLogs_CNT.join(StartEndDf,'FileNameUUID','left')
        
        loadAuditTables_Ingestion_Log(DF_Source_SysLogs,sysLogsPath,'ADB_SysLogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(df_sysLogs_CNT,sysLogsPath,'ADB_SysLogProcessing','Succeeded','')
       
    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        loadlogProcessesDeltaTable(df_sysLogs_CNT,sysLogsPath,'ADB_SysLogProcessing','Failed',str(exp))
        loadAuditTables_Ingestion_Log(DF_Source_SysLogs,sysLogsPath,'ADB_SysLogProcessing','Failed',str(exp))

        logIntoStreamLogTable(log_df,"ADB_SysLogProcessing","Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise            



# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

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
    
    # df_SysLogs = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_SysLogs))
    Process_SYSLogs(df_logSource,StartEndDf,sysLogs_ConfigId,batchId)
    spark.sql("clear cache")
    # print("-------------------------------------------")
    

# COMMAND ----------

q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Transformation_COM3_CT_SysLogs_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", sysLogsPath+checkPointLocation)
                  .outputMode("update")
                  .start()
                #   .awaitTermination(600)
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()
