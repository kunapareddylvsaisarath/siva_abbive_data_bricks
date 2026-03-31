# Databricks notebook source
ConfigId = dbutils.widgets.text("ConfigId","27")
ConfigId = dbutils.widgets.get('ConfigId')


SourceTypeId = dbutils.widgets.text("SourceTypeId","7")
SourceTypeId = dbutils.widgets.get('SourceTypeId')

#ingestion_related
IngestionConfigId = dbutils.widgets.text("IngestionConfigId","23")
IngestionConfigId = dbutils.widgets.get('IngestionConfigId')
SourceContainerPath = dbutils.widgets.text("SourceContainerPath","lastcontactreport")
SourceContainerPath = dbutils.widgets.get('SourceContainerPath')
SourceFolderPath = dbutils.widgets.text("SourceFolderPath","//")
SourceFolderPath = dbutils.widgets.get('SourceFolderPath')
DestinationContainerPath = dbutils.widgets.text("DestinationContainerPath","raw")
DestinationContainerPath = dbutils.widgets.get('DestinationContainerPath')
DestinationFolderPath = dbutils.widgets.text("DestinationFolderPath","/COM1LastConnected/")
DestinationFolderPath = dbutils.widgets.get('DestinationFolderPath')

CreatedBy = dbutils.widgets.text("CreatedBy","ADB_COM1LastContact")
CreatedBy = dbutils.widgets.get('CreatedBy')
DeviceTypeCd = dbutils.widgets.text("DeviceTypeCd","COM1")
DeviceTypeCd = dbutils.widgets.get('DeviceTypeCd')
MessageTypeCd = dbutils.widgets.text("MessageTypeCd","LastContact")
MessageTypeCd = dbutils.widgets.get('MessageTypeCd')
Job_id=dbutils.widgets.text("Job_id","-1")
Job_id=dbutils.widgets.get("Job_id")
run_id=dbutils.widgets.text("run_id","-1")
run_id=dbutils.widgets.get("run_id")

# COMMAND ----------

dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initializing The Functions:

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts $job_id=$Job_id $run_id=$run_id $parent_run_id=-1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy the file Storage account folder lastcontactreport into Raw container folder COM1LastConnected 

# COMMAND ----------

sourcefile = getLastFile("/mnt/lastcontactreport")
#--------destination = f'/mnt/raw/{DestinationFolderPath}'
destination = f'{ExternalLocation_raw}/{DestinationFolderPath}'
sf = sourcefile.split('/')
sf= sf[3]
try:
    dbutils.fs.cp(sourcefile, destination)
    processedFileList =([{'ConfigId':IngestionConfigId,'SourceTypeId':SourceTypeId,'SourceContainerPath':SourceContainerPath,'SourceFolderPath':SourceFolderPath,'SourceFileName':sf,
                     'DestinationContainerPath':DestinationContainerPath,'DestinationFolderPath':DestinationFolderPath,'DestinationFileName':sf,'PipelineStatus':"Succeeded",'PipelineRunId':run_id, 'JobId':job_id
                      }]) 
    logIntoIngestionLogTable(processedFileList,'ADB_COM1LastContact') 
except Exception as e:
    processedFileList =([{'ConfigId':IngestionConfigId,'SourceTypeId':SourceTypeId,'SourceContainerPath':SourceContainerPath,'SourceFolderPath':SourceFolderPath,'SourceFileName':sf,
                     'DestinationContainerPath':DestinationContainerPath,'DestinationFolderPath':DestinationFolderPath,'DestinationFileName':sf,'PipelineStatus':"Failed",'PipelineRunId':run_id, 'JobId':job_id
                      }]) 
    logIntoIngestionLogTable(processedFileList,'ADB_COM1LastContact') 
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## Declaring the source and destination paths:

# COMMAND ----------

# #Source Path:
#-----------------src_folder = getLastFile('/mnt/raw/COM1LastConnected/')
src_folder = getLastFile(f'{ExternalLocation_raw}/COM1LastConnected/')

# print(src_folder)
#-----------------equipMstrPath = '/mnt/silver/DIMEquipmentMaster'
#-----------------src_filesProcessed = '/mnt/silver/LogSourceFilesProcessed'
equipMstrPath = f'{ExternalLocation_silver}/DIMEquipmentMaster'
src_filesProcessed = f'{ExternalLocation_silver}/LogSourceFilesProcessed'

# Destination
#------------------destinationFilePath = '/mnt/silver/FACTLastContact_COM1/'
destinationFilePath = f'{ExternalLocation_silver}/FACTLastContact_COM1/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining Schema:

# COMMAND ----------

Schema = StructType([
    StructField("LastContactInfoTxt", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data From RawZone:

# COMMAND ----------

df_LastConnectedUnparsed = (spark.read
                            .format('csv')
                            .options(header='True', delimiter='|')
                            .schema(Schema)
                            .load(src_folder)
                            .select("*",col("_metadata.file_path").alias('SourceFilePath'),
                                    col("_metadata.file_name").alias('SourceFileName'),
                                    col("_metadata.file_modification_time").alias('LoadDateTmstmp'),
                                    col("_metadata.file_size").alias('SourceFileSize'))
                            .withColumn('ConfigId', lit(ConfigId).cast('int'))
                            .withColumn('SourceTypeId', lit(SourceTypeId).cast('int'))
                            .withColumn('SourceFilePath', regexp_replace('SourceFilePath','dbfs:/mnt/raw/',''))
                            .withColumn("DeviceType",lit(DeviceTypeCd))
                            .withColumn("LogType",lit(MessageTypeCd))
                            .withColumn("DeviceId",lit(None).cast('string')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Dependent Tables From SilverZone:

# COMMAND ----------

# Reading Equipment Data from Silver zone table
df_equip = (spark.read.format('delta')
            .option('header',True)
            .load(equipMstrPath)
            .filter(~col('Mfg_BatchNbr').isNull())
            .filter((col('SerialNumberNbr').like('UPM%')) & (col('ShipEndDt')=='2111-11-11') & (col('EquipmentTransitStatusNm')=='SHIPPED'))
            .select('SerialNumberNbr', upper('Mfg_BatchNbr').alias('Mfg_BatchNbr'), 'ShipStartDt', 'ShipEndDt'))

# Read logsourcefileprocessed from silver zone:
logsourcefileprocessed_df = (spark.read.format('delta')
                             .option('header', True)
                             .load(src_filesProcessed)
                             .filter("LogFileStatus = 'InProgress'")
                             .select('SourceFilePath','FileNameUUID'))                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation:

# COMMAND ----------

try:
    # Logging data into IngestionLog Table:
    loadAuditTables_Ingestion_Log(df_LastConnectedUnparsed,destinationFilePath,CreatedBy,'InProgress','')
        
    # Deriving Meta data from SourceFileName
    df_Source_metadata = (df_LastConnectedUnparsed.groupBy("SourceFilePath","SourceFileName","SourceFileSize","ConfigId","SourceTypeId").count()
                          .withColumnRenamed("count",'RecdCnt')
                          .withColumn("FileNameDeviceTypeCd",lit(DeviceTypeCd))
                          .withColumn("ExternalSerialNbr",lit(None))
                          .withColumn("InternalSerialNbr",lit(None))
                          .withColumn("FileNameMessageTypeCd",lit(MessageTypeCd))
                        .withColumn('FileNameDtTmstmp',
                                    date_format(to_timestamp(substring(col('SourceFileName'),0,17),'MM-dd-yy.HH-mm-ss'),'yyyyMMddHHmmss'))
                          .withColumn("FileNameApplicatorPortCd",lit(None))
                          .withColumn("FileNameCycleNbr",lit(None))
                          .withColumn('LogStartDate',lit(None))
                          .withColumn('LogEndDate',lit(None)))
        
    # Logging data into logSourcefileprocessed table:
    loadlogProcessesDeltaTable(df_Source_metadata,destinationFilePath,CreatedBy,'InProgress','')

    # Parsing Data into Columns:        
    df_LastConnectedUnparsed = (df_LastConnectedUnparsed
                 .withColumn('HdrDateGeneratedDate',to_date(substring(col('SourceFileName'),1,8),"MM-dd-yy"))
                 .withColumn('HdrTimeGeneratedTmstmp',to_timestamp(substring(col('SourceFileName'),1,17),'MM-dd-yy.HH-mm-ss'))
                 .withColumn('InternalSerialNbr',
                             when(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('SBC:',LastContactInfoTxt)+4,1)") == '?',None)
                             .when(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('SBC:',LastContactInfoTxt)+4,7)") == 'UNKNOWN',None)
                              .otherwise(upper(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('SBC:',LastContactInfoTxt)+4,19)"))))
                 .withColumn('ExternalSerialNbr',
                             regexp_replace(
                                when(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('UPM: ',LastContactInfoTxt)+5,1)")=='?',None)
                                .when(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('UPM: ',LastContactInfoTxt)+5,7)") == 'UNKNOWN','UNKNOWN')
                                .otherwise(upper(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('UPM: ',LastContactInfoTxt)+5,19)"))),' ',''))
                 .withColumn('SIMCardID',when(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('SIM:',LastContactInfoTxt)+4,1)") == '?',None)
                             .otherwise(expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('SIM:',LastContactInfoTxt)+4,20)")))
                 .withColumn('LastContactDtTmstmp',
                            to_timestamp(
                                expr("SUBSTRING(LastContactInfoTxt,CHARINDEX('LastContact:',LastContactInfoTxt)+12,28)"),
                                     'E MMM dd HH:mm:ss Z yyyy'))
                 .filter(col('InternalSerialNbr').isNotNull())
                 .drop_duplicates())      
    
    # Lookup Missing UPM
    W_LatestFile = (Window.partitionBy(col('InternalSerialNbr')).orderBy(col('ShipStartDt').desc()))
    df_LastConnected = (df_LastConnectedUnparsed.alias('ls')
                        .join(df_equip.alias('eq'),col('ls.InternalSerialNbr') == col('eq.Mfg_BatchNbr'),'left')
                        .withColumn('ExternalSerialNbr',when(col('ExternalSerialNbr').like('UPM%'),col('ExternalSerialNbr'))
                                    .otherwise(col('SerialNumberNbr')))
                        .drop('SerialNumberNbr','Mfg_BatchNbr').drop_duplicates()
                        .withColumn('CreatedBy',lit(CreatedBy))
                        .withColumn('CreatedDt',current_timestamp())
                        .withColumn('UpdatedBy',lit(CreatedBy))
                        .withColumn('UpdatedDt',current_timestamp())
                        .withColumn('rownum',row_number().over(W_LatestFile)).filter(col('rownum')==1).drop('rownum')
                        .drop_duplicates(['InternalSerialNbr','ExternalSerialNbr'])
                        .withColumn('LastContactUUID',expr('uuid()')))
    
    # Joining with logfileprocessed table to get FilenameUUID
    df_LastConnected_UUID = df_LastConnected.join(logsourcefileprocessed_df,"SourceFilePath",'inner').drop_duplicates()

    
    # Read Delta table
    Dl_LastConnected = DeltaTable.forPath(spark, destinationFilePath)
        
    # Merge Delta table
    (Dl_LastConnected.alias("tgt")
     .merge(
         df_LastConnected_UUID.alias("src"),
         "tgt.InternalSerialNbr = src.InternalSerialNbr")
     .whenNotMatchedInsert(values ={
         "tgt.FileNameUUID" : "src.FileNameUUID",
         "tgt.LastContactUUID" : "src.LastContactUUID",
         "tgt.SourceFilePath" : "src.SourceFilePath",
         "tgt.SourceFileName" : "src.SourceFileName",
         "tgt.HdrDateGeneratedDate" : "src.HdrDateGeneratedDate",
         "tgt.HdrTimeGeneratedTmstmp" : "src.HdrTimeGeneratedTmstmp",
         "tgt.LastContactDtTmstmp" : "src.LastContactDtTmstmp",
         "tgt.InternalSerialNbr" : "src.InternalSerialNbr",
         "tgt.SIMCardID" : "src.SIMCardID",
         "tgt.ExternalSerialNbr" : "src.ExternalSerialNbr",
         "tgt.CreatedBy" : "src.CreatedBy",
         "tgt.CreatedDt" : "src.CreatedDt",
         "tgt.UpdatedBy" : "src.UpdatedBy",
         "tgt.UpdatedDt" : "src.UpdatedDt"
     })
     .whenMatchedUpdate(set ={
         "tgt.FileNameUUID" : "src.FileNameUUID",
         "tgt.LastContactUUID" : "src.LastContactUUID",
         "tgt.SourceFilePath" : "src.SourceFilePath",
         "tgt.SourceFileName" : "src.SourceFileName",
         "tgt.HdrDateGeneratedDate" : "src.HdrDateGeneratedDate",
         "tgt.HdrTimeGeneratedTmstmp" : "src.HdrTimeGeneratedTmstmp",
         "tgt.LastContactDtTmstmp" : "src.LastContactDtTmstmp",
         "tgt.InternalSerialNbr" : "src.InternalSerialNbr",
         "tgt.SIMCardID" : "src.SIMCardID",
         "tgt.ExternalSerialNbr" : "src.ExternalSerialNbr",
         "tgt.CreatedBy" : "src.CreatedBy",
         "tgt.CreatedDt" : "src.CreatedDt",
         "tgt.UpdatedBy" : "src.UpdatedBy",
         "tgt.UpdatedDt" : "src.UpdatedDt"
     })
     .execute())
    
    df_LastConnected_LogDate = df_LastConnected_UUID.withColumn('LogStartDate',to_timestamp(concat(col('HdrDateGeneratedDate'),lit(' '), 
                                                                                                   col('HdrTimeGeneratedTmstmp'))))
    
    df_LastContact_LogDate = (df_LastConnected_LogDate.groupBy('SourceFilePath','SourceFileName','FileNameUUID','ConfigId')
                                    .agg(min(col('LogStartDate')).alias('LogStartDate'),
                                         max(col('LogStartDate')).alias('LogEndDate')))
         
    loadlogProcessesDeltaTable(df_LastContact_LogDate,destinationFilePath,CreatedBy,'Succeeded','')
    loadAuditTables_Ingestion_Log(df_LastConnectedUnparsed,destinationFilePath,CreatedBy,'Succeeded','')
except Exception as e:
    loadlogProcessesDeltaTable(df_LastContact_LogDate,destinationFilePath,CreatedBy,'Failed',str(e))
    loadAuditTables_Ingestion_Log(df_LastConnectedUnparsed,destinationFilePath,CreatedBy,'Failed',str(e))
    raise