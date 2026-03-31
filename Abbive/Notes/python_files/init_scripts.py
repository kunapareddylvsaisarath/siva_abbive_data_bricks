# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions",200)
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
# spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
#--------spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

import json
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from time import sleep
import requests
import json

# from Crypto.PublicKey import RSA
# from Crypto.Cipher import PKCS1_v1_5
# import base64 as b64

# from cryptography.hazmat.primitives.serialization import pkcs12
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption

# COMMAND ----------

# #import the packages
# from email.mime.text import MIMEText
# from email.mime.application import MIMEApplication
# from email.mime.multipart import MIMEMultipart
# from smtplib import SMTP
# import smtplib
# import sys
# import pandas as pd
# import os
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail
# from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

defaultValue = "0"
dbutils.widgets.text('job_id',defaultValue)
job_id = dbutils.widgets.get('job_id')
dbutils.widgets.text('run_id',defaultValue)
run_id = dbutils.widgets.get('run_id')
dbutils.widgets.text('parent_run_id',defaultValue)
parent_run_id = dbutils.widgets.get('parent_run_id')

print(job_id)
print(run_id)    
print(parent_run_id)

#----------------src_filesProcessed = '/mnt/silver/LogSourceFilesProcessed/'
#----------------dst_IngestionLog = '/mnt/silver/FactIngestionLog/'
#----------------dst_TransformationLog = '/mnt/silver/FactTransformationLog/'

src_filesProcessed = ExternalLocation_silver+'/LogSourceFilesProcessed/'
dst_IngestionLog = ExternalLocation_silver+'/FactIngestionLog/'
dst_TransformationLog = ExternalLocation_silver+'/FactTransformationLog/'


# COMMAND ----------

SQLDBConnection = dbutils.secrets.get(scope="ABV_AKV_ADB_SCOPE", key="SQLDBConnection")

jdbcHostname = SQLDBConnection.split(';')[0].replace('Server=tcp:','').split(',')[0]
jdbcDatabase = 'CS-DataMonitoring'
jdbcPort = SQLDBConnection.split(';')[0].replace('Server=tcp:','').split(',')[1]
username = SQLDBConnection.split(';')[3].split('=')[1]
password = SQLDBConnection.split(';')[4].split('=')[1]
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};queryTimeout=30".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
"user" : username,
"password" : password,
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print(jdbcUrl)

# COMMAND ----------

import uuid
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
uuidUdf = udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# DBTITLE 1,Moxie Credentials
Client_id = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-ClientID')
Client_secret = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-Secret')
Username = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-UserName')
Password = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-Password')
InstanceURL = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-InstanceURL')
Domain   = dbutils.secrets.get('ABV_AKV_ADB_SCOPE','Moxie-Domain')

API_RETRY_LIMIT = 5

# COMMAND ----------

def getEnvironmentName():
    current_workspace_name = spark.conf.get("spark.databricks.workspaceUrl")
    workspace_mapping = {
        'adb-6730937708229757.17.azuredatabricks.net': 'DEV',
        'adb-6228830814044614.14.azuredatabricks.net': 'QA',
        'adb-4720418283187325.5.azuredatabricks.net': 'PROD',
    }
    environment_name = workspace_mapping.get(current_workspace_name)
    print(environment_name)
    return environment_name

# COMMAND ----------

# def SendNotification(sender_email,recipient_emails,cc_emails,query,subject,HTMLContent,sortByColumn,sortByAscending):

#     to_emailsList = recipient_emails.split(',')
#     cc_emailsList = cc_emails.split(',')
#     sortByColumnList = sortByColumn.split(',')
#     sortByAscendingList = sortByAscending.split(',')
#     sortByAscendingList_Bool = [eval(x) for x in sortByAscendingList]

#     spark_df = spark.sql(query)
#     pd_df = spark_df.toPandas().sort_values(by=sortByColumnList, ascending=sortByAscendingList_Bool)
#     HTMLContent = HTMLContent.replace('<basic_query>',pd_df.to_html(index=False))
    
#     for email in to_emailsList:
#         if email in cc_emailsList:
#             cc_emailsList.remove(email)
    
#     message = Mail(
#         from_email=sender_email,
#         to_emails=to_emailsList,
#         subject=subject,
#         html_content = HTMLContent
#     )

#     for ccemail in cc_emailsList:
#         message.add_cc(cc_email=ccemail)

#     sg = SendGridAPIClient(dbutils.secrets.get("ABV_AKV_ADB_SCOPE","SendGridAPIClient"))
#     response = sg.send(message)
#     print(response.status_code)
#     print(response.body)
#     print(response.headers)
    
    


# COMMAND ----------

def getLastFile(FolderPath):
    list = dbutils.fs.ls(FolderPath)
    df = spark.createDataFrame(list,['FileInfo'])
    lastModifiedDate = df.agg({"modificationTime": "max"}).collect()[0][0]
    LastFile = df.filter(df.modificationTime == lastModifiedDate).collect()[0][0]
    return LastFile.replace('dbfs:','')

# COMMAND ----------


def getpartition(DF_Source):
    ConfigIDs=DF_Source.select("ConfigID").distinct().collect()
    par=''
    ConfigIDstr=''
    for rowValue in ConfigIDs:
        par=par+' or tgt.ConfigID = "'+str(rowValue[0])+'"'
        ConfigIDstr=ConfigIDstr+str(rowValue[0])+','
    parString = ' and (' +par[4:] +')'
    return parString,ConfigIDstr[:-1]

# COMMAND ----------

# def loadAuditTables_DIM_InProgress(DF_Source,DEST_Folder,ConfigId,SourceTypeId,CreatedBy):
#     max_retries = 3
#     num_retries = 0
#     while True:
#         try:
#             partitionString='tgt.ConfigId="'+str(ConfigId)+'"'
#             DEST_Folder = DEST_Folder.replace('/mnt/silver/','')
#             DF_Source_Count = DF_Source.groupBy('SourceFilePath','SourceFileName','SourceFileSize').count()
#             DF_Ingestion_Log =  (DF_Source_Count
#                                         .withColumn('SourceFolderPath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
#                                         .withColumn('DestinationFolderPath',lit(DEST_Folder))
#                                         .withColumn('PipelineStatus',lit('InProgress'))
#                                         .withColumn('ConfigId',lit(ConfigId).cast("int"))
#                                         .withColumn('SourceTypeId',lit(SourceTypeId).cast("int"))  
#                                         .withColumn('SourceContainerPath',lit('raw'))
#                                         .withColumn('DestinationContainerPath',lit('silver'))
#                                         .withColumn('PipelineRunId',lit(job_id))
#                                         .withColumn('CreatedBy',lit(CreatedBy))
#                                         .withColumn('UpdatedBy',lit(CreatedBy))
#                                         .withColumn('CreatedDate',current_timestamp())
#                                         .withColumn('UpdatedDate',current_timestamp())
#                                         .drop('count','SourceFilePath')
#                                 )

#             # (DF_Ingestion_Log.write
#             #                 .format("jdbc")
#             #                 .option("url", jdbcUrl)
#             #                 .option("dbtable", "CONF.Ingestion_Log")
#             #                 .option("user", username)
#             #                 .option("password", password)
#             #                 .mode("append")
#             #                 .save())            
#             (DF_Ingestion_Log.write
#                             .format("delta")
#                             .mode("append")
#                             .option("mergeSchema", "true")
#                             .option("header", True)                            
#                             .save(dst_IngestionLog))

#             filesProcessed_DL = DeltaTable.forPath(spark,src_filesProcessed)
#             filesMicroBatch = (DF_Source_Count.groupBy("SourceFilePath","SourceFileName","SourceFileSize")
#                                 .count()
#                                 .withColumnRenamed("count","RecdCnt")
#                                 .withColumn('ConfigId',lit(ConfigId))
#                                 .withColumn('CreatedBy',lit(CreatedBy))
#                                 .withColumn('CreatedDt',current_timestamp())
#                                 .withColumn('UpdatedBy',lit(CreatedBy))
#                                 .withColumn('UpdatedDt',current_timestamp())
#                                 .withColumn('FileNameUUID',uuidUdf())
#                             )
            
#             (filesProcessed_DL.alias("tgt")
#                             .merge(filesMicroBatch.alias("src"),'(src.SourceFilePath = tgt.SourceFilePath) AND (src.ConfigId = tgt.ConfigId) AND (tgt.LogFileStatus = "Succeeded") AND ({0})'.format(partitionString))
#                             .whenNotMatchedInsert(values ={
#                                 "tgt.FileNameUUID":   "src.FileNameUUID",
#                                 "tgt.SourceFilePath": "src.SourceFilePath",
#                                 "tgt.SourceFileName": "src.SourceFileName",
#                                 "tgt.SourceFileSize": "src.SourceFileSize",
#                                 "tgt.ConfigId": "src.ConfigId",                         
#                                 "tgt.SourceFileRecordCt":"src.RecdCnt",
#                                 "tgt.LogFileStatus":lit('InProgress'),
#                                 "tgt.CreatedBy":"src.CreatedBy",
#                                 "tgt.CreatedDt":"src.CreatedDt",
#                                 "tgt.UpdatedBy":"src.UpdatedBy",
#                                 "tgt.UpdatedDt":"src.UpdatedDt"
#                                 })
#                             .execute())   
#             return                             
#         except Exception as e:
#             if num_retries > max_retries:
#                 raise e
#             else:
#                 print("Retrying error", e)
#                 num_retries += 1
#                 sleep(30)                

# COMMAND ----------

# def loadAuditTables_DIM_Complete(DF_Source,DEST_Folder,ConfigId,SourceTypeId,CreatedBy,Status,ErrorMessage):
#     max_retries = 3
#     num_retries = 0
#     while True:
#         try:
#             partitionString='tgt.ConfigId="'+str(ConfigId)+'"'
#             DEST_Folder = DEST_Folder.replace('/mnt/silver/','')
#             DF_Source_Count = DF_Source.groupBy('SourceFilePath','SourceFileName').count()
#             DF_Ingestion_Log =  (DF_Source_Count
#                                         .withColumn('SourceFolderPath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
#                                         .withColumn('DestinationFolderPath',lit(DEST_Folder))
#                                         .withColumn('PipelineStatus',lit(Status))
#                                         .withColumn('ErrorMessage',lit(ErrorMessage))      
#                                         .withColumn('ConfigId',lit(ConfigId))
#                                         .withColumn('SourceTypeId',lit(SourceTypeId))  
#                                         .withColumn('SourceContainerPath',lit('raw'))
#                                         .withColumn('DestinationContainerPath',lit('silver'))
#                                         .withColumn('PipelineRunId',lit(job_id))
#                                         .withColumn('CreatedBy',lit(CreatedBy))
#                                         .withColumn('UpdatedBy',lit(CreatedBy))
#                                         .withColumn('CreatedDate',current_timestamp())
#                                         .withColumn('UpdatedDate',current_timestamp())
#                                         .drop('count','SourceFilePath')                         
#                                 )
            
#             (DF_Ingestion_Log.write
#                             .format("delta")
#                             .mode("append")
#                             .option("mergeSchema", "true")
#                             .option("header", True)                            
#                             .save(dst_IngestionLog))
                        
#             # (DF_Ingestion_Log.write
#             #                 .format("jdbc")
#             #                 .option("url", jdbcUrl)
#             #                 .option("dbtable", "[CONF].[Ingestion_Log]")
#             #                 .option("user", username)
#             #                 .option("password", password)
#             #                 .mode("append")
#             #                 .save())

#             DL_Source = DF_Source_Count.withColumn('ConfigId',lit(ConfigId))        
#             filesProcessed_DL = DeltaTable.forPath(spark,src_filesProcessed)
#             (filesProcessed_DL.alias("tgt")
#                             .merge(DL_Source.alias("src"),
#                                     '(src.SourceFilePath = tgt.SourceFilePath) AND (src.ConfigId = tgt.ConfigId) AND ({0})'.format(partitionString))
#                             .whenMatchedUpdate(set ={
#                                 "tgt.LogFileStatus":lit(Status),
#                                 "tgt.UpdatedBy":lit(CreatedBy),
#                                 "tgt.UpdatedDt":current_timestamp()
#                                 })
#                             .execute())  
#             return                            
#         except Exception as e:
#             if num_retries > max_retries:
#                 raise e
#             else:
#                 print("Retrying error", e)
#                 num_retries += 1
#                 sleep(30)                    

# COMMAND ----------

# def loadAuditTables_Logs_InProgress(DF_Source,DEST_Folder,ConfigId,SourceTypeId,CreatedBy):
#     max_retries = 3
#     num_retries = 0
#     while True:
#         try:
#             partitionString='tgt.ConfigId="'+str(ConfigId)+'"'
#             DEST_Folder = DEST_Folder.replace('/mnt/silver/','')
#             DF_Source_Count = DF_Source.groupBy('SourceFilePath','SourceFileName').count()
#             DF_Ingestion_Log =  (DF_Source_Count
#                                         .withColumn('SourceFolderPath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
#                                         .withColumn('DestinationFolderPath',lit(DEST_Folder))
#                                         .withColumn('PipelineStatus',lit('InProgress'))
#                                         .withColumn('ConfigId',lit(ConfigId))
#                                         .withColumn('SourceTypeId',lit(SourceTypeId))  
#                                         .withColumn('SourceContainerPath',lit('raw'))
#                                         .withColumn('DestinationContainerPath',lit('silver'))
#                                         .withColumn('PipelineRunId',lit(job_id))
#                                         .withColumn('CreatedBy',lit(CreatedBy))
#                                         .withColumn('UpdatedBy',lit(CreatedBy))
#                                         .withColumn('CreatedDate',current_timestamp())
#                                         .withColumn('UpdatedDate',current_timestamp())
#                                         .drop('count','SourceFilePath')
#                                 )

#             (DF_Ingestion_Log.write
#                 .format("delta")
#                 .mode("append")
#                 .option("mergeSchema", "true")
#                 .option("header", True)                            
#                 .save(dst_IngestionLog))

#             # (DF_Ingestion_Log.write
#             #                 .format("jdbc")
#             #                 .option("url", jdbcUrl)
#             #                 .option("dbtable", "CONF.Ingestion_Log")
#             #                 .option("user", username)
#             #                 .option("password", password)
#             #                 .mode("append")
#             #                 .save())

            
#             filesProcessed_DL = DeltaTable.forPath(spark,src_filesProcessed)
#             filesMicroBatch = (DF_Source.groupBy("SourceFilePath","SourceFileName","SourceFileSize")
#                                 .count()
#                                 .withColumnRenamed("count","RecdCnt")
#                                 .withColumn('ConfigId',lit(ConfigId))                       
#                                 .withColumn('CreatedBy',lit('ADB_AllLogFiles'))
#                                 .withColumn('CreatedDt',current_timestamp())
#                                 .withColumn('UpdatedBy',lit('ADB_AllLogFiles'))
#                                 .withColumn('UpdatedDt',current_timestamp())
#                                 .withColumn('FileNameUUID',uuidUdf())
#                                 .withColumn("FileNameDeviceTypeCd",split(col('SourceFileName'), '_')
#                                                                     .getItem(0))
#                                 .withColumn("FileNameDeviceSerialNbr",split(col('SourceFileName'), '_')
#                                                                     .getItem(1))
#                                 .withColumn("FileNameMessageTypeCd",split(col('SourceFileName'), '_')
#                                                                     .getItem(2))
#                                 .withColumn("FileNameDtTmstmp",split(col('SourceFileName'), '_')
#                                                                     .getItem(3))
#                                 .withColumn("FileNameApplicatorPortCd",split(col('SourceFileName'), '_')
#                                                                     .getItem(4))
#                                 .withColumn("FileNameCycleNbr",split(col('SourceFileName'), '_')
#                                                                     .getItem(5))
#                             )
            
#             (filesProcessed_DL.alias("tgt")
#                             .merge(filesMicroBatch.alias("src"),
#         '(src.SourceFileName = tgt.SourceFileName and tgt.LogFileStatus = "Succeeded" and src.ConfigId = tgt.ConfigId {0}) or (src.SourceFilePath = tgt.SourceFilePath and src.ConfigId = tgt.ConfigId and tgt.LogFileStatus = "InProgress" AND {0})'.format(partitionString))
#                             .whenNotMatchedInsert(values ={
#                                 "tgt.FileNameUUID":   "src.FileNameUUID",
#                                 "tgt.SourceFilePath": "src.SourceFilePath",
#                                 "tgt.SourceFileName": "src.SourceFileName",
#                                 "tgt.SourceFileSize": "src.SourceFileSize",
#                                 "tgt.SourceFileRecordCt":"src.RecdCnt",
#                                 "tgt.ConfigId": "src.ConfigId",                         
#                                 "tgt.FileNameDeviceTypeCd":"src.FileNameDeviceTypeCd",
#                                 "tgt.FileNameDeviceSerialNbr":"src.FileNameDeviceSerialNbr",
#                                 "tgt.FileNameMessageTypeCd":"src.FileNameMessageTypeCd",
#                                 "tgt.FileNameDtTmstmp":"src.FileNameDtTmstmp",
#                                 "tgt.FileNameApplicatorPortCd":"src.FileNameApplicatorPortCd",
#                                 "tgt.FileNameCycleNbr":"src.FileNameCycleNbr",
#                                 "tgt.IsLogFileProcessedInd":lit('N'),
#                                 "tgt.LogFileStatus":lit('InProgress'),
#                                 "tgt.CreatedBy":"src.CreatedBy",
#                                 "tgt.CreatedDt":"src.CreatedDt",
#                                 "tgt.UpdatedBy":"src.UpdatedBy",
#                                 "tgt.UpdatedDt":"src.UpdatedDt"
#                                 })
#                             .execute())

#             (filesProcessed_DL.alias("tgt")
#                             .merge(filesMicroBatch.alias("src"),
#                                     'src.SourceFilePath = tgt.SourceFilePath and src.ConfigId = tgt.ConfigId  AND ({0})'.format(partitionString))
#                             .whenNotMatchedInsert(values ={
#                                 "tgt.FileNameUUID":   "src.FileNameUUID",
#                                 "tgt.SourceFilePath": "src.SourceFilePath",
#                                 "tgt.SourceFileName": "src.SourceFileName",
#                                 "tgt.SourceFileRecordCt":"src.RecdCnt",
#                                 "tgt.ConfigId": "src.ConfigId",                         
#                                 "tgt.FileNameDeviceTypeCd":"src.FileNameDeviceTypeCd",
#                                 "tgt.FileNameDeviceSerialNbr":"src.FileNameDeviceSerialNbr",
#                                 "tgt.FileNameMessageTypeCd":"src.FileNameMessageTypeCd",
#                                 "tgt.FileNameDtTmstmp":"src.FileNameDtTmstmp",
#                                 "tgt.FileNameApplicatorPortCd":"src.FileNameApplicatorPortCd",
#                                 "tgt.FileNameCycleNbr":"src.FileNameCycleNbr",                         
#                                 "tgt.IsLogFileProcessedInd":lit('N'),
#                                 "tgt.LogFileStatus":lit('Duplicate'),
#                                 "tgt.CreatedBy":"src.CreatedBy",
#                                 "tgt.CreatedDt":"src.CreatedDt",
#                                 "tgt.UpdatedBy":"src.UpdatedBy",
#                                 "tgt.UpdatedDt":"src.UpdatedDt"
#                                 })
#                                 .execute())
#             return                                
#         except Exception as e:
#             if num_retries > max_retries:
#                 raise e
#             else:
#                 print("Retrying error", e)
#                 num_retries += 1                                
#                 sleep(30)

# COMMAND ----------

# def upsertProcessedFiles_Logs_Complete(microBatchOutputDF_FileUUID):
#     max_retries = 3
#     num_retries = 0
#     while True:
#         try:
#             partitionString='tgt.ConfigId="'+str(ConfigId)+'"'
#             DEST_Folder = DEST_Folder.replace('/mnt/silver/','')
#             DF_Source_Count = DF_Source.groupBy('SourceFilePath','SourceFileName','SourceFileSize').count()
#             DF_Ingestion_Log =  (DF_Source_Count
#                                         .withColumn('SourceFolderPath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
#                                         .withColumn('DestinationFolderPath',lit(DEST_Folder))
#                                         .withColumn('PipelineStatus',lit(Status))
#                                         .withColumn('ErrorMessage',lit(ErrorMessage))      
#                                         .withColumn('ConfigId',lit(ConfigId))
#                                         .withColumn('SourceTypeId',lit(SourceTypeId))  
#                                         .withColumn('SourceContainerPath',lit('raw'))
#                                         .withColumn('DestinationContainerPath',lit('silver'))
#                                         .withColumn('PipelineRunId',lit(job_id))
#                                         .withColumn('CreatedBy',lit(CreatedBy))
#                                         .withColumn('UpdatedBy',lit(CreatedBy))
#                                         .withColumn('CreatedDate',current_timestamp())
#                                         .withColumn('UpdatedDate',current_timestamp())
#                                         .drop('count','SourceFilePath')                         
#                                 )
            
#             (DF_Ingestion_Log.write
#                 .format("delta")
#                 .mode("append")
#                 .option("mergeSchema", "true")
#                 .option("header", True)                            
#                 .save(dst_IngestionLog))
                        
#             # (DF_Ingestion_Log.write
#             #                 .format("jdbc")
#             #                 .option("url", jdbcUrl)
#             #                 .option("dbtable", "[CONF].[Ingestion_Log]")
#             #                 .option("user", username)
#             #                 .option("password", password)
#             #                 .mode("append")
#             #                 .save())

#             DL_Source = DF_Source_Count.withColumn('ConfigId',lit(ConfigId))        
#             filesProcessed_DL = DeltaTable.forPath(spark,src_filesProcessed)
#             (filesProcessed_DL.alias("tgt")
#                             .merge(DL_Source.alias("src"),
#                                     '(src.SourceFilePath = tgt.SourceFilePath) AND (src.ConfigId = tgt.ConfigId) AND ({0})'.format(partitionString))
#                             .whenMatchedUpdate(set ={
#                                 "tgt.LogFileStatus":lit(Status),
#                                 "tgt.UpdatedBy":lit(CreatedBy),
#                                 "tgt.UpdatedDt":current_timestamp()
#                                 })
#                             .execute())   
#             return                             
#         except Exception as e:
#             if num_retries > max_retries:
#                 raise e
#             else:
#                 print("Retrying error", e)
#                 num_retries += 1
#                 sleep(30)                                           

# COMMAND ----------

def logIntoIngestionLogTable(processedFileList,auditColumn):
    max_retries = 3
    num_retries = 0
    while True:
        try:
            df_ProcessedFiles = spark.createDataFrame(processedFileList)
            df_ProcessedFiles = (df_ProcessedFiles
                                 .withColumn('ConfigId',col("ConfigId").cast("Integer"))
                                 .withColumn('SourceTypeId',col("SourceTypeId").cast("Integer")) 
                                 .withColumn("LogUUID",expr('uuid()'))                                                                 
                                 .withColumn('CreatedBy',lit(auditColumn))
                                 .withColumn('UpdatedBy',lit(auditColumn))
                                 .withColumn('CreatedDate',current_timestamp())
                                 .withColumn('UpdatedDate',current_timestamp())
                                    )
        #     display(df_ProcessedFiles)
            (df_ProcessedFiles.coalesce(1).write.format("delta").mode("append").save(dst_IngestionLog))
                        
            # (df_ProcessedFiles.write
            # .format("jdbc")
            # .option("url", jdbcUrl)
            # .option("dbtable", "[CONF].[Ingestion_Log]")
            # .option("user", username)
            # .option("password", password)
            # .mode("append")
            # .save()
            # )
            return            
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(30)

# COMMAND ----------

import shutil

def copyFiles(sourceFilePath,destinationPath):
    os.makedirs(os.path.dirname(destinationPath), exist_ok=True)
    with open(sourceFilePath, 'rb') as fsrc, open(destinationPath, 'wb') as fdst:
        shutil.copyfileobj(fsrc,fdst)

# COMMAND ----------

def cleanUpStagingFolders(fileList):
    for file in fileList:
        os.remove(file)
        
        if os.path.isdir(stagingFilePath):
            if not os.listdir(dir_name):
                os.rmdir(stagingFilePath)
    

# COMMAND ----------

# while using loadAuditTables_filesProcessed_Logs Function DF_Source must contain SourceFilePath,SourceFileName,SourceFileSize,LogStartDate,LogEndDate,RecdCnt,ConfigId                         ,FileNameDeviceTypeCd,FileNameDeviceSerialNbr,FileNameMessageTypeCd,FileNameDtTmstmp,FileNameApplicatorPortCd,FileNameCycleNbr when Status is InProgress,DF_Source must contaian SourceFilePath,ConfigId if Status is Failed otherewise SourceFilePath,ConfigId,LogStartDate,LogEndDate
def loadlogProcessesDeltaTable(DF_Source,DEST_Folder,CreatedBy,Status,ErrorMessage):
    max_retries = 3
    num_retries = 0
    while True:
        try:
        #     print("getting partition ")
            partitionString,ConfigIDstr=getpartition(DF_Source)
        #     print(partitionString,ConfigIDstr)
            
            columnList=[x.lower() for x in DF_Source.columns]
            if 'filenameuuid' in columnList:
                filesMicroBatch = (DF_Source .withColumn('IsLogFileProcessedInd',lit('N'))
                                    .withColumn('LogFileStatus',lit(Status))
                                    .withColumnRenamed("RecdCnt","SourceFileRecordCt")
                                    .withColumn('SourceFilePath',expr("regexp_replace(SourceFilePath,'raw/','')")) 
                                    # .withColumn('RunID',lit(run_id))
                                    .withColumn('CreatedBy',lit(CreatedBy))
                                    .withColumn('CreatedDt',current_timestamp())
                                    .withColumn('UpdatedBy',lit(CreatedBy))
                                    .withColumn('UpdatedDt',current_timestamp())
                                    #    .withColumn('FileNameUUID',uuidUdf())
                                    )
    
            else:
                filesMicroBatch = (DF_Source .withColumn('IsLogFileProcessedInd',lit('N'))
                                .withColumn('LogFileStatus',lit(Status))
                                .withColumnRenamed("RecdCnt","SourceFileRecordCt")
                                .withColumn('SourceFilePath',expr("regexp_replace(SourceFilePath,'raw/','')")) 
                                # .withColumn('RunID',lit(run_id))
                                .withColumn('CreatedBy',lit(CreatedBy))
                                .withColumn('CreatedDt',current_timestamp())
                                .withColumn('UpdatedBy',lit(CreatedBy))
                                .withColumn('UpdatedDt',current_timestamp())
                                .withColumn('FileNameUUID',uuidUdf())
                                )
                
            if 'runid' not in columnList:
                filesMicroBatch = (filesMicroBatch.withColumn('RunID',lit(run_id)).withColumn('RawFileModificationTime',lit(None)))
        #     print(filesMicroBatch.dtypes)
            filesProcessed_DL = DeltaTable.forPath(spark,src_filesProcessed)
            
        #     print('update silverzone.logsourcefilesprocessed set LogFileStatus="Failed" where LogFileStatus="InProgress" and configid in ({0}) {1}'.format(ConfigIDstr,partitionString.replace("tgt.",'')))
            if len(ConfigIDstr)>0:
                if (Status.lower() == 'inprogress'):
                    spark.sql('update silverzone.logsourcefilesprocessed set LogFileStatus="Failed" where LogFileStatus="InProgress"  {0}'.format(partitionString.replace("tgt.",'')))
                    (filesMicroBatch.select("FileNameUUID","SourceFilePath","SourceFileName","SourceFileSize","LogStartDate","LogEndDate","SourceFileRecordCt",expr("cast(ConfigId as Integer)")
                                        ,"FileNameDeviceTypeCd","ExternalSerialNbr","InternalSerialNbr","FileNameMessageTypeCd","FileNameDtTmstmp","FileNameApplicatorPortCd",expr("cast(FileNameCycleNbr as Integer)")
                                        ,"CreatedBy","CreatedDt","UpdatedBy","UpdatedDt","IsLogFileProcessedInd","LogFileStatus",expr("cast(RunID as Integer)"),"RawFileModificationTime")
                                .write.format('delta').mode('append').save(src_filesProcessed))


                elif Status.lower() == 'failed':
                    (filesProcessed_DL.alias("tgt")
                            .merge(filesMicroBatch.alias("src"),
                                    '(src.SourceFilePath = tgt.SourceFilePath) AND (src.ConfigId = tgt.ConfigId) and (tgt.LogFileStatus= "InProgress") {0}'.format(partitionString))
                            .whenMatchedUpdate(set ={
                                "tgt.LogFileStatus":lit(Status),
                                "tgt.ErrorMessage":lit(ErrorMessage),
                                "tgt.UpdatedBy":lit(CreatedBy),
                                "tgt.UpdatedDt":current_timestamp()
                                })
                            .execute())
                else:
                    (filesProcessed_DL.alias("tgt")
                                .merge(filesMicroBatch.alias("src"),
                                        '(src.SourceFilePath = tgt.SourceFilePath) and (src.ConfigId = tgt.ConfigId) and (src.FileNameUUID=tgt.FileNameUUID) and (tgt.LogFileStatus= "InProgress") {0}'.format(partitionString))
                                .whenMatchedUpdate(set ={
                                    "tgt.LogStartDate":"src.LogStartDate",
                                    "tgt.LogEndDate":"src.LogEndDate",
                                    "tgt.IsLogFileProcessedInd":lit('Y'),
                                    "tgt.LogFileStatus":lit(Status),
                                    "tgt.UpdatedBy":lit(CreatedBy),
                                    "tgt.UpdatedDt":current_timestamp()
                                    })
                                .execute())      
            return                                  
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(30)                

# COMMAND ----------

# while using loadAuditTables_Ingestion_Log DF_Source Should Contain 'SourceFilePath','SourceFileName','ConfigId','SourceFileSize','SourceTypeId' Columns abd pass appropraite Status and ErrorMessage.can be used for State managment the information into CONF.Ingestion_Log table
def loadAuditTables_Ingestion_Log(DF_Source,DEST_Folder,CreatedBy,Status,ErrorMessage=None):
    max_retries = 3
    num_retries = 0
    while True:
        try:
            #-----------DEST_Folder = DEST_Folder.replace('/mnt/silver','')
            DEST_Folder = DEST_Folder.replace( ExternalLocation_silver,'')
            DF_Source_Count = DF_Source.groupBy('SourceFilePath','SourceFileName','ConfigId','SourceFileSize','SourceTypeId','DeviceId','LogType','DeviceType').count()
            DF_Ingestion_Log =  (DF_Source_Count.withColumn('SourceFolderPath',concat(lit('/'),expr("regexp_replace(regexp_replace(regexp_replace(SourceFilePath,'%20',' '), SourceFileName, ''),'raw/','')")))
                                        .withColumn('DestinationFolderPath',lit(DEST_Folder))
                                        .withColumn('PipelineStatus',lit(Status))
                                        .withColumn('ErrorMessage',lit(ErrorMessage)) 
                                        .withColumn('SourceContainerPath',lit('raw'))
                                        .withColumn('DestinationContainerPath',lit('silver'))
                                        .withColumn('PipelineRunId',lit(run_id))
                                        .withColumn('JobId',lit(job_id))
                                        .withColumn("LogUUID",expr('uuid()'))                                        
                                        .withColumn('CreatedBy',lit(CreatedBy))
                                        .withColumn('UpdatedBy',lit(CreatedBy))
                                        .withColumn('ConfigId',col("ConfigId").cast("Integer"))
                                        .withColumn('SourceTypeId',col("SourceTypeId").cast("Integer"))
                                        .withColumn("Createddate", current_timestamp())
                                        .withColumn("Updateddate", current_timestamp())
                                        .drop('count','SourceFilePath') # add SourceFileSize column
                                )

            # (DF_Ingestion_Log.write.format("jdbc")
            #                 .option("url", jdbcUrl)
            #                 .option("dbtable", "CONF.Ingestion_Log")
            #                 .option("user", username)
            #                 .option("password", password)
            #                 .mode("append")
            #                 .save())
            DF_Ingestion_Log.coalesce(1).write.format('delta').mode('append').save(dst_TransformationLog)
            return                            
        except Exception as e:
            print("Failed to append"+str(e))
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(30)

# COMMAND ----------

#-------------SchemaDatabasePath='/mnt/silver/DatabaseSchema'
SchemaDatabasePath=ExternalLocation_silver+'/DatabaseSchema'
def jsonParser(df,jsonFiledName,tableName,contractNumber=None):
    if contractNumber is None:
        SchemaMapping=(spark.read.format("delta").load(SchemaDatabasePath)
                       .where("TableName='{0}' and isActive=1 and lower(ColumnType)='jsonfield'".format(tableName)).select("ContractName","SourceName","TargetName","DataType"))
    else:
        SchemaMapping=(spark.read.format("delta").load(SchemaDatabasePath).where("TableName='{0}' and ContractName='{1}' and isActive=1 and lower(ColumnType)='jsonfield'".format(tableName,contractNumber)).select("ContractName","SourceName","TargetName","DataType","updateEnabled"))
    
    # check for the count 
    # SchemaMapping.display()
    SchemaMappingRow=SchemaMapping.collect()
    jsonParser=''
    jsonParserdict=dict()
    for RowList in SchemaMappingRow:
        jsonParser=jsonParser+')).withColumn("'+RowList["TargetName"]+'",coalesce('
        for SourceColName in RowList["SourceName"]:
            jsonParser = jsonParser + 'get_json_object(col("' + jsonFiledName + '"), "$.' + SourceColName + '").cast("' + ("String" if RowList["DataType"] =='TimeStamp' else RowList["DataType"]) + '"),'
    finalJsonParser='df'+jsonParser[2:]+'))'
    # print("-----------------------------------------------------------------")
    # print(finalJsonParser)
    return eval(finalJsonParser)



# COMMAND ----------

def evolveSchema(sourcedf,tableName):

    SourceList=sourcedf.dtypes
    targetdf=spark.sql("select * from  silverzone.{0}".format(tableName))
    targetList=targetdf.dtypes
    # print(SourceList)
    # print(targetList)
    
    # make Sure that column Name and datatype Matchs exactly
    columnsNotInSourceList=list(set(targetList)-set(SourceList))
    columnsNotInTargetList=list(set(SourceList)-set(targetList))

    # print(columnsNotInSourceList)
    # print(columnsNotInTargetList)
    #Add the Column that are not present in Source DF
    if len(columnsNotInSourceList)>0:
        withColumnstr=''
        for columnName,datatype in columnsNotInSourceList:
            withColumnstr=withColumnstr+'.withColumn("'+columnName+'",lit(None).cast("'+datatype+'"))'            
        sourcedf=eval('sourcedf'+withColumnstr)

    # generate the Dynamic merge dictionary
    SchemaMapping=(spark.read.format("delta").load(SchemaDatabasePath).where("TableName='{0}'  and isActive=1 ".format(tableName)).select("ContractName","SourceName","TargetName","DataType","updateEnabled"))
    SchemaMappingRow=SchemaMapping.collect()
    insertDict=dict()
    UpsertDict=dict()
    tragetColumn=list()
    for RowList in SchemaMappingRow:
        tragetColumn.append(RowList["TargetName"])
        insertDict['tgt.'+RowList["TargetName"]] = 'src.'+RowList["TargetName"]
        if RowList["updateEnabled"]=='1':
            UpsertDict['tgt.'+RowList["TargetName"]] = 'src.'+RowList["TargetName"]

    if  not (len(insertDict.keys())==len(sourcedf.columns)):
        print("Total distinct Column from databaseSchema not matching with Source dataFrame")
        print("Extra Column in Source df:"+ str(set(sourcedf.columns)-set(tragetColumn)))
        print("Column not present in Source df:"+ str(set(tragetColumn)-set(sourcedf.columns)))
        raise Exception("Total distinct Column from databaseSchema not matching with SourcedataFrame")
    else:
        if len(columnsNotInTargetList)>0:
            print("Adding New Column to the Table "+ tableName)
            newColumns=''
            for columnName,datatype in columnsNotInTargetList:
                newColumns=newColumns+columnName+' '+datatype+','
            AletrCMD='ALTER TABLE silverzone.'+tableName+' ADD columns ('+newColumns[:-1]+')'
            print(AletrCMD)
            spark.sql(AletrCMD)

    
    # print("-----------------------------------------------------------------")
    # print(UpsertDict)
    return sourcedf,insertDict,UpsertDict
    

# COMMAND ----------

import time
def graceStop(query,status_flag):
    while status_flag == 1:
        status_flag = spark.sql(f"Select StreamFlag from silverzone.streamFlag ").collect()[0][0]
        #print(f"Status of a Stream Flag:{status_flag}")
        #print(f"Status of a stream query:{query.status}")
        if status_flag == 1 :
            sleep(60)
    if (status_flag == 0):
        while query.isActive:
            if (not query.status.get('isTriggerActive') or not query.status.get('isDataAvailable')) and query.status.get('message') != 'Initializing sources' :
                print("Job Terminated")
                query.stop()
            # elif query.status.get('isDataAvailable') and query.status.get('isTriggerActive'):
            #     return 0
            sleep(60)
            

# COMMAND ----------

def batchEnd(query,batchId):
    #print(f"Batch Id:{batchId}")
    status_flag = spark.sql(f"Select StreamFlag from silverzone.streamFlag ").collect()[0][0]
    if status_flag == 0:
        print("Stopping the Stream")
        query.stop()

# COMMAND ----------

# DBTITLE 1,Insert fact_promotion_log
#adding the function for logging in promotion log table. 
#-----------------dst_PromotionLog = '/mnt/silver/FACTPromotionLog'
dst_PromotionLog = ExternalLocation_silver+'/FACTPromotionLog'
def logIntoPromotionLogTable(DF_Source,CreatedBy,Status,ErrorMessage=None):
    max_retries = 3
    num_retries = 0
    while True:
        try:
            if 'ErrorMessage' in DF_Source.columns and 'PipelineStatus' in DF_Source.columns:
                DF_Ingestion_Log =  (DF_Source.withColumn('ConfigId',col("ConfigId").cast("Int"))
                                                    .withColumn('SourceTypeId',col("SourceTypeId").cast("Int"))
                                                    .withColumn('Run_ID',coalesce(col("Run_ID"),lit(run_id)))
                                                    .withColumn('Job_ID',coalesce(col("Run_ID"),lit(job_id)))
                                                    .withColumn("RowUUID",expr('uuid()'))                                        
                                                    .withColumn('CreatedBy',lit(CreatedBy))
                                                    .withColumn('UpdatedBy',lit(CreatedBy))
                                                    .withColumn("Createddate", current_timestamp())
                                                    .withColumn("Updateddate", current_timestamp())
                                            )
            else:
                DF_Ingestion_Log =  (DF_Source.withColumn('PipelineStatus',lit(Status))
                                            .withColumn('ConfigId',col("ConfigId").cast("Int"))
                                            .withColumn('SourceTypeId',col("SourceTypeId").cast("Int"))
                                            .withColumn('ErrorMessage',lit(ErrorMessage)) 
                                            .withColumn('Run_ID',coalesce(col("Run_ID"),lit(run_id)))
                                            .withColumn('Job_ID',coalesce(col("Run_ID"),lit(job_id)))
                                            .withColumn("RowUUID",expr('uuid()'))                                        
                                            .withColumn('CreatedBy',lit(CreatedBy))
                                            .withColumn('UpdatedBy',lit(CreatedBy))
                                            .withColumn("Createddate", current_timestamp())
                                            .withColumn("Updateddate", current_timestamp())
                                    )

            DF_Ingestion_Log.coalesce(1).write.format('delta').mode('append').save(dst_PromotionLog)
            return                            
        except Exception as e:
            print("Failed to append"+str(e))
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(30)

# COMMAND ----------

# DBTITLE 1,generate_ScannedId
def generate_ScannedId(df_source):
    max_retries = 3
    num_retries = 0

    while True:
        try:
            columns = ["CoolsculptingId", "ScannedId"]
            condition = "tgt.CoolsculptingId = src.CoolsculptingId"

            delta_table = DeltaTable.forPath(spark, 'dbfs:/mnt/silver/DIMConsumer') 
            df = delta_table.toDF() 
            max_value = df.agg(max(col("ScannedId")).alias("max_ScannedId")).collect()[0]["max_ScannedId"]
            window_spec = Window.orderBy(monotonically_increasing_id())

            df_source = df_source.withColumn("ScannedId", row_number().over(window_spec) + lit(max_value))\
                    .withColumn("CreatedDate",current_timestamp())\
                .withColumn("UpdatedDate",current_timestamp())\
                .withColumn("CreatedBy", lit("ADB_ULLogProcessing"))\
                .withColumn("UpdatedBy", lit("ADB_ULLogProcessing"))

            (delta_table.alias("tgt").merge(df_source.alias("src"), condition) \
                .whenNotMatchedInsertAll() \
                .execute()
            )
            return
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(30) 

# COMMAND ----------

# DBTITLE 1,Generate Moxie  Access token
def generate_moxie_token(Client_id,Client_secret,Username,Password,Domain):   
    token_endpoint = f'https://{Domain}/services/oauth2/token' 
    API_RETRY_LIMIT = 5
    payload = {
        'grant_type': 'password',
        'client_id': Client_id,
        'client_secret': Client_secret,
        'username': Username,
        'password': Password
    }

    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
      response = requests.post(token_endpoint, data=payload)
      if(response.status_code == 200): 
          response_data = response.json()
          return response_data
      elif(500 <= response.status_code <= 599 or response.status_code == 429):
        print(f'url:{token_endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')
        print("Waiting for 90 seconds...")
        sleep(30)
        retry_count += 1
      else:
        raise Exception(f'url:{token_endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')
    raise Exception(f'url:{token_endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')

# COMMAND ----------

# DBTITLE 1,Moxie Case Comments
def createMoxieCaseComments(data):
    access_token = generate_moxie_token(Client_id,Client_secret,Username,Password,Domain)['access_token']

    headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
    
    endpoint = f"https://{InstanceURL}/services/data/v58.0/sobjects/CaseComment"

    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
        response = requests.post(endpoint, headers=headers, json=data)
        if response.status_code == 201:
            response_data = response.json()
            return response_data
        else:
            print("Waiting for 30 seconds...")
            sleep(10)
            retry_count += 1
    raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')

# COMMAND ----------

# DBTITLE 1,Moxie Case Flag
def updateMoxieCaseFlag(MoxieCaseID):
    access_token = generate_moxie_token(Client_id,Client_secret,Username,Password,Domain)['access_token']

    headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

    endpoint = f"https://{InstanceURL}/services/data/v58.0/sobjects/Case/{MoxieCaseID}"
    current_timestamp = datetime.now().isoformat()
    print(current_timestamp)
    data = {"FLAGS__Enable_Case_Flags__c":"true",
            "FLAGS__ViewedFlag__c":current_timestamp}

    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
        response = requests.patch(endpoint, headers=headers, json=data)
        if response.status_code == 204:
            return True
        else:
            print("Waiting for 30 seconds...")
            sleep(10)
            retry_count += 1
    raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')

# COMMAND ----------

# DBTITLE 1,Update fact_invoiceexception
def upsertInvoiceException(df_InvoiceException):
    num_retries = 0
    max_retries = 5
    while True:
        try:
            df_InvoiceException = df_InvoiceException.withColumn("UpdatedBy",lit("ADB_MoxieCaseComment"))\
                                                    .withColumn("UpdatedDate",current_timestamp())

            dl_FACTInvoiceException = DeltaTable.forName(spark, 'Promotion.FACT_InvoiceException')
            (dl_FACTInvoiceException.alias("tgt")
                    .merge(df_InvoiceException.alias("src"),
                        ("tgt.InvoiceExceptionUUID = src.InvoiceExceptionUUID"))
                    .whenMatchedUpdate(set ={
                        "tgt.MoxieCaseCommentId" : "src.MoxieCaseCommentId",
                        "tgt.UpdatedBy": "src.UpdatedBy",
                        "tgt.UpdatedDate": "src.UpdatedDate"})
            .execute())
            return 
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(10)

# COMMAND ----------

# DBTITLE 1,Rules Engine Invoice Exception
def sendExceptionStatusToMoxie(df_InvoiceException):
    schema = StructType([
      StructField("CommentBody", StringType(), True),
      StructField("InvoiceExceptionUUID", StringType(), True),
      StructField("MoxieCaseCommentId", StringType(), True),
      StructField("ParentId", StringType(), True)
    ])
    try:
      cols_tocheck = ['MoxieCaseID','Comments','InvoiceExceptionUUID']
      
      #Check columns are there in Dataframe
      if all(col in df_InvoiceException.columns for col in cols_tocheck):
        caselist = [{"ParentId": row["MoxieCaseID"], "CommentBody": row["Comments"], "InvoiceExceptionUUID": row["InvoiceExceptionUUID"]}
                    for row in df_InvoiceException.select(cols_tocheck).collect()]
        
        #Update Moxie case flags and create comments
        for index,data in enumerate(caselist):
          respone = updateMoxieCaseFlag(data["ParentId"])
          if respone:
            payload = {"ParentId":data["ParentId"],
                      "CommentBody":data["CommentBody"]}
            respone = createMoxieCaseComments(payload)
            responseId = respone['id']
            caselist[index]["MoxieCaseCommentId"] = responseId

        #Merge into fact_invoiceexception
        df_InvoiceExceptionComments = spark.createDataFrame(caselist,schema)
        # df_InvoiceExceptionComments.show()
        upsertInvoiceException(df_InvoiceExceptionComments)

      else:
        print("Columns are not found")    
    except Exception as e:
      print("Failed")
      print(e)

# COMMAND ----------

# DBTITLE 1,Stream Log
#adding the function for logging in promotion log table. 
def logIntoStreamLogTable(DF_Source,CreatedBy,Status,microBatchOutputDF=None,ErrorMessage=None):
    max_retries = 5
    num_retries = 0
    #-----------dst_AuditLog = '/mnt/silver/FACTStreamLogs'
    dst_AuditLog = ExternalLocation_silver+'/FACTStreamLogs'
    while True:
        try:
            if microBatchOutputDF == None:
                DF_IngestionLog = DF_Source
            else:
                #Convert MicrobatchDf fields into JSON
                microBatchOutputDF_Json = microBatchOutputDF.select(to_json(struct(microBatchOutputDF.columns)).alias("MicroBatchData"))

                DF_IngestionLog = microBatchOutputDF_Json.crossJoin(broadcast(DF_Source))

            DF_Stream_Log = (DF_IngestionLog.withColumn('PipelineStatus',lit(Status))
                                .withColumn('ErrorMessage',lit(ErrorMessage))
                                .withColumn('ConfigId',col("ConfigId").cast("Int"))
                                .withColumn('SourceTypeId',col("SourceTypeId").cast("Int"))
                                .withColumn("RowUUID",expr('uuid()'))                                        
                                .withColumn('CreatedBy',lit(CreatedBy))
                                .withColumn('UpdatedBy',lit(CreatedBy))
                                .withColumn("Createddate", current_timestamp())
                                .withColumn("Updateddate", current_timestamp()))
                          
            DF_Stream_Log.coalesce(1).write.format('delta').mode('append').save(dst_AuditLog)
            return                             
        except Exception as e:
            print("Failed to append"+str(e))
            if num_retries > max_retries:
                print("Failed to add into Stream Log Table")
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(10)

# COMMAND ----------

# WorkFlow Status
def setWorkFlowStatus_StreamLog(df_source,Status,Updatedby):
    max_retries = 5
    num_retries = 0
    try:
        print(Status)
        print(Updatedby)
        df_logsOverride = (df_source
                            .withColumn('ReprocessStatus',lit(Status))
                            .withColumn('UpdatedBy',lit(Updatedby))
                            .withColumn('UpdatedDate',lit(current_timestamp()))
                            )
        
        #Merge based on RowUUID and ConfigId
        dl_FactStreamLogs = DeltaTable.forName(spark, 'f"{CatalogName}".silverzone.fact_streamlogs')
        (dl_FactStreamLogs.alias("tgt")
                .merge(df_logsOverride.alias("src"),
                    ("tgt.RowUUID = src.RowUUID AND tgt.ConfigId = src.ConfigId "))
                .whenMatchedUpdate(set ={
                    "tgt.ReprocessStatus": "src.ReprocessStatus",
                    "tgt.UpdatedBy": "src.UpdatedBy",
                    "tgt.UpdatedDate": "src.UpdatedDate",                                
                    })
        .execute())
    except Exception as e:
        print(traceback.format_exc())
        if num_retries > max_retries:
            print("Failed to update data into Stream Log Table")
        else:
            print("Retrying error", e)
            num_retries += 1
            sleep(random.randint(15,30))

