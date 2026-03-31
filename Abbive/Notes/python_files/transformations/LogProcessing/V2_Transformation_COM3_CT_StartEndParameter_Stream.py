# Databricks notebook source
dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

#-----------------dbutils.widgets.text('sourceFilePath','/mnt/silver/FACTAllLogs')
dbutils.widgets.text('sourceFilePath',ExternalLocation_silver+'/FACTAllLogs')
sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('startEndParmater_ConfigId','21')
startEndParmater_ConfigId = dbutils.widgets.get('startEndParmater_ConfigId')

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

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------


# Source path:
checkPointLocation = "/_checkpoints/"
#------------------startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
startEndParameterPath = ExternalLocation_silver+'/DIMStartEndParameter'

logTypes_Processed = ['UL','UE','SYS','ENGR'] 
contractNumberList_StartEnd = ["14020","14023"]

CreatedBy='ADB_StartEndLogProcessing'


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

df_Source = (spark.readStream.option("cloudFiles.maxFilesPerTrigger",5000) 
                              .option("cloudFiles.maxBytesPerTrigger",'10g')
                            .option("startingVersion", startingVersion)
                            .option("skipChangeCommits", "true")
                              .format("delta").load(sourceFilePath)).where("SourceFilePath is not Null")

# COMMAND ----------

# MAGIC %md
# MAGIC # Process Start and End Parameters 14020 and 14023

# COMMAND ----------

def processStartAndEndParameters(DF_Source_SEP,startEndParmater_ConfigId,batchId):
    try:
        df_Source_StartEnd = DF_Source_SEP.withColumn('ConfigId',lit(startEndParmater_ConfigId))
        df_Source_StartEnd.persist()
        df_Source_StartEnd_CNT = (df_Source_StartEnd.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                                    'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','ConfigId',
                                                    'SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId','FileNameUUID',"RawFileModificationTime","RunID")
                                            .count()
                                            .withColumnRenamed("count","RecdCnt")
                                            
                                )
        df_Source_StartEnd_CNT.persist()
        # print("logging into logProcess")
        # print("df_Source_StartEnd_CNT:"+str(df_Source_StartEnd_CNT.count()))
        loadlogProcessesDeltaTable(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_StartEndLogProcessing','InProgress','')
        # print("logging into Ingestion")
        loadAuditTables_Ingestion_Log(df_Source_StartEnd,startEndParameterPath,'ADB_StartEndLogProcessing','InProgress','')

        log_df = df_Source_StartEnd.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(startEndParameterPath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))

        Microbatch_df = df_Source_StartEnd.limit(1)

        # raise Exception("No Exception: Manual Failure")

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
                                .withColumn('CreatedBy',lit("ADB_StartEndLogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_StartEndLogProcessing"))
                                .withColumn('UpdatedDt',lit(current_timestamp()))
                                .withColumn('StartEndParameterUUID',expr('uuid()'))
                                .drop('SourceFileName','ActualFileNameNm','EventSeqNbr'))
        
        w=(Window.partitionBy('FileNameUUID')
                 .orderBy(asc('EventStartTmstmp'),desc('EventEndTmstmp')))
        
        StardEndParameter_Final = (df_StardEndParameter.withColumn('rownum',row_number().over(w))
                                                       .filter("rownum = 1")
                                                       .drop('rownum'))
        
        # Inserting new records to target:
        # DeltaTbl_SEP_Logs = DeltaTable.forPath(spark, startEndParameterPath)
        # print("writting")
        StardEndParameter_Final.select("StartEndParameterUUID","FileNameUUID","HdrFormatVersionCd","HdrDateGeneratedDt","HdrTimeGeneratedTmstmp","HdrLogTypeCd","HdrDestinationSubSystemCd","HdrSourceSubSystemCd","HdrCommandCd","HdrDataStoreID","HdrDataContractNbr_StartParameter","HdrDataContractNbr_EndParameter","EventStartTmstmp","CycleTypeCd","Type","VersionNbr","ApplicatorPortCd","PriorityCd","TimeZoneDesc","EventEndTmstmp","HdrAppHeadNbr","InternalSerialNbr","ExternalSerialNbr","CreatedBy","CreatedDt","UpdatedBy","UpdatedDt").write.format('delta').mode("append").save(startEndParameterPath)
                                                                                                                                                        


        StardEndParameter_Final=StardEndParameter_Final.withColumnRenamed("EventStartTmstmp","LogStartDate").withColumnRenamed("EventEndTmstmp","LogEndDate").select("LogStartDate","LogEndDate","FileNameUUID")
        # print("logging into Ingestion")
        loadAuditTables_Ingestion_Log(df_Source_StartEnd,startEndParameterPath,'ADB_StartEndLogProcessing','Succeeded','')

        df_Source_StartEnd_CNT=df_Source_StartEnd_CNT.drop('LogStartDate','LogEndDate')
        df_Source_StartEnd_CNT=df_Source_StartEnd_CNT.join(StardEndParameter_Final,'FileNameUUID','left')
        # print("df_Source_StartEnd_CNT:"+str(df_Source_StartEnd_CNT.count()))
        # print("logging into LogProcess")
        loadlogProcessesDeltaTable(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_StartEndLogProcessing','Succeeded','')  
         
    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        loadAuditTables_Ingestion_Log(df_Source_StartEnd,startEndParameterPath,'ADB_StartEndLogProcessing','Failed',str(exp))
        loadlogProcessesDeltaTable(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_StartEndLogProcessing','Failed',str(exp))

        logIntoStreamLogTable(log_df,CreatedBy,"Failed",Microbatch_df,ErrorMessage)
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
                        .withColumn("LogStartDate",lit(None))
                        .withColumn("LogEndDate",lit(None))
                        .drop('SourceFileName_noext')
                        .fillna({'FileNameApplicatorPortCd':''})
                        .filter(((col('LogType').isin(logTypes_Processed)) |
                                (col("HdrDataContractNbr").isin(contractNumberList_StartEnd))))                     
                )

       
       
        processStartAndEndParameters(df_logSource,startEndParmater_ConfigId,batchId)
        spark.sql("clear cache")
    except Exception as exp:
        print("Exception in processStartAndEndParameters:"+str(exp))
        # raise    
  
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming job to process log data

# COMMAND ----------

q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Transformation_COM3_CT_StartEndParameter_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", startEndParameterPath+checkPointLocation)
                  .outputMode("update")
                  .start()
                #   .awaitTermination()
)

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------


q.awaitTermination()
