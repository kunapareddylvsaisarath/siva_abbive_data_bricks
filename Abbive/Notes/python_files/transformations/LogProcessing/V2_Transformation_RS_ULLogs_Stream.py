# Databricks notebook source
dbutils.widgets.text("CatalogName", "cd_prod")
CatalogName = dbutils.widgets.get("CatalogName")
 
dbutils.widgets.text('ExternalLocation_raw',"/mntprod_raw")
ExternalLocation_raw = dbutils.widgets.get('ExternalLocation_raw')
 
dbutils.widgets.text('ExternalLocation_silver',"/mntprod_silver")
ExternalLocation_silver = dbutils.widgets.get('ExternalLocation_silver')

# COMMAND ----------

#--------------------dbutils.widgets.text('sourceFilePath','/mnt/silver/FACTAllLogs')
dbutils.widgets.text('sourceFilePath',ExternalLocation_silver+'/FACTAllLogs')
sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('ulLogs_ConfigId','33')
ulLogs_ConfigId = dbutils.widgets.get('ulLogs_ConfigId')

dbutils.widgets.text('sourceTypeId','10')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

dbutils.widgets.text('allLogs_ConfigId','20')
allLogs_ConfigId = dbutils.widgets.get('allLogs_ConfigId')

dbutils.widgets.text('startingVersion','0')
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

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import base64 as b64

from cryptography.hazmat.primitives.serialization import pkcs12
from azure.identity import *
from azure.keyvault.secrets import SecretClient
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption

import traceback

pipelinename = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipelinename = pipelinename.rsplit('/', 1)[-1]
display(pipelinename)

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
# MAGIC # Define FilePath variables and widgets

# COMMAND ----------

# Source path:
checkPointLocation = "/_checkpoints/"
#--------------dest_ULLogs = '/mnt/silver/FACTULLogs_RS/'
#--------------dest_CarttridgeLogPath='/mnt/silver/FactULCartridgeLogs_RS/'
#--------------equip_mstr_path = '/mnt/silver/DIMEquipmentMaster'
#--------------startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
dest_ULLogs = ExternalLocation_silver+'/FACTULLogs_RS/'
dest_CarttridgeLogPath=ExternalLocation_silver+'/FactULCartridgeLogs_RS/'
equip_mstr_path = ExternalLocation_silver+'/DIMEquipmentMaster'
startEndParameterPath = ExternalLocation_silver+'/DIMStartEndParameter'

certificate_name = "ZeltiqAbbVie20200716"
cycleIDSequence = 1

logTypes_Processed = ['UL'] 
contractNumberList_StartEnd = ["14020","14023"]
contractNumberList_UL_RS=["87900:1","87901:1","87902:1","87903:1"]


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
                                .withColumn('CreatedBy',lit("ADB_ULLogProcessing"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_ULLogProcessing"))
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
# MAGIC # Process UL log Cycle Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to Decrypt RSA Key

# COMMAND ----------

#Get the RSA Private Key from Azure KV 
def get_rsa_key(vault_url,certificate_name):
    tenant_id = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
    client_id = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
    client_secret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")

    # credential = DefaultAzureCredential()
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    client = SecretClient(vault_url=vault_url, credential=credential)
    certificate_secret = client.get_secret(certificate_name)
    certificate_data = certificate_secret.value.encode("ascii")
    certificate_bytes = b64.b64decode(certificate_data)
    pkcs12_data = pkcs12.load_key_and_certificates(data=certificate_bytes,password=None)
    private_key = pkcs12_data[0]
    private_key_pem = (private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=NoEncryption()))
    return private_key_pem

# define the function to decrypt a value using the private key
def rsa_decrypt(value, private_key_pem):
    try:
        rsa_key = RSA.import_key(private_key_pem)
        cipher = PKCS1_v1_5.new(rsa_key)
        value_decoded = b64.b64decode(value)
        decrypted_data = cipher.decrypt(value_decoded,None)
        decrypted_string = decrypted_data.decode()
        return decrypted_string
    
    except ValueError:
        decrypted_string = 'Value Error Invalid Length'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to Populate CycleId

# COMMAND ----------

def populateCycleID(DFSource,DLTablePath,cycleIDSequence):
    MaxCycleID= spark.read.load(DLTablePath).select(max("CycleID")).head()[0]
    if(MaxCycleID is None or MaxCycleID < cycleIDSequence):
        MaxCycleID=cycleIDSequence

    DFSourceWithCycleID = (DFSource
                           .withColumn("MaxCycleID",lit(MaxCycleID))
                        .withColumn("RowNum",row_number().over(Window.partitionBy("MaxCycleID").orderBy(col("CreatedDt"))))
                        .withColumn("CycleID",lit(col("MaxCycleID")+col("RowNum")))
                           )
    return DFSourceWithCycleID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process ul logs for RS 87900:1 , 87901:1 , 87902:1 and 87903:1

# COMMAND ----------

def ProcessULLogs87900_87901(df_Source_ULLogs,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId):
    try:

      df_Source_ULLogs=df_Source_ULLogs.withColumn('ConfigId',lit(ulLogs_ConfigId))
      df_Source_ULLogs.persist()
      df_ulLogs_CNT = (df_Source_ULLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                        'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath',
                                        'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId','FileNameUUID','ConfigId',"RawFileModificationTime","RunID")
                      .count()
                      .withColumnRenamed("count","RecdCnt")                            
                        )

      df_ulLogs_CNT.persist()

      loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')
      df_Source_ULLogs = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin(contractNumberList_UL_RS)))

      # DedupSeq = (Window.partitionBy("SourceFilePath").orderBy(desc(col('RawFileModificationTime'))))
      # df_Source_ULLogs  =  df_Source_ULLogs.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')


      loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')

      Streamlog_df = df_Source_ULLogs.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                          .withColumn('Destination', lit(str(dest_ULLogs))) \
                                          .withColumn('Run_ID', lit(str(batchId))) \
                                          .withColumn('Job_ID', lit(str(JobId)))
          
      Microbatch_df = df_Source_ULLogs

      # raise Exception("No Exception: Manual Failure")

      df_EquipmentMasterCS = (spark.read.format('delta').load(equip_mstr_path).filter(col('EquipmentTransitStatusNm')=='SHIPPED')
                        .dropDuplicates()
                        .select("SerialNumberNbr","ShipStartDt","ShipEndDt","SoldToID","ShipToID","MaterialCd","EquipmentTransitStatusNm")
                        )

      #Fetch MLT File Path from log processed
      MLTWindow = (Window.partitionBy("SourceFileName").orderBy(desc(col('UpdatedDt'))))
      df_MLTFiles =(spark.read.format("delta")
                                        .load(src_filesProcessed)
                                        .filter((col('ConfigId') == allLogs_ConfigId) & (col('FileNameMessageTypeCd') =='ML') & (col('FileNameDeviceTypeCd')=='RS'))
                                        .withColumn('rownum',row_number().over(MLTWindow))
                                        .filter('rownum = 1').drop('rownum')                                       
                                        .withColumn("SourceFileName", regexp_replace("SourceFileName", ".stx", ""))
                                        .select(col('SourceFileName').alias('MLTSourceFileName'),
                                                col('SourceFilePath').alias('MsrmntLogTreatmentFileURLTxt'))
                            )


      df_start_end_raw = StartEndDf.withColumnRenamed("LogStartDate","EventStartTmstmp").withColumnRenamed("LogEndDate","EventEndTmstmp")

      df_Source_ULLogs=(df_Source_ULLogs.withColumnRenamed("HdrTimeGeneratedTmstmp","HdrStartTimeGeneratedTmstmp")
                  .withColumnRenamed("HdrDateGeneratedDt","HdrStartDateGeneratedDt") 
                  .withColumnRenamed('HdrLogTypeCd','HdrLogtypeCd')
                  .withColumnRenamed('HdrCommandCd','HdrCommandCD')
                  .withColumn("HdrAppHeadNbr",col("HdrAppHeadNbr").cast("int"))
                  )  

      df_raw_ULLogs_87900 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('87900:1')) )
      df_raw_ULLogs_87900=jsonParser(df_raw_ULLogs_87900,'JSONField','FactULLogs_RS','87900:1').drop("JSONField","History")
        # df_raw_ULLogs_87900.display()
    
      df_raw_ULLogs_87901 = (df_Source_ULLogs.select("FileNameUUID","JSONField","SourceFilePath").filter(col("HdrDataContractNbr").isin('87901:1')))
      df_raw_ULLogs_87901=jsonParser(df_raw_ULLogs_87901,'JSONField','FactULLogs_RS','87901:1').drop("JSONField")
        # df_raw_ULLogs_87901.display()

      df_raw_ULLogs_87902 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('87902:1')).withColumn("EventStartTmstmp",col("HdrStartTimeGeneratedTmstmp")))
                            #    .withColumn('RowNbr1',row_number().over((Window.partitionBy("SourceFilePath","SourceFileName").orderBy(col('HdrStartTimeGeneratedTmstmp'))))))
      df_raw_ULLogs_87902=jsonParser(df_raw_ULLogs_87902,'JSONField','FactULCartridgeLogs_RS','87902:1').drop("JSONField")


      df_raw_ULLogs_87903 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('87903:1')).withColumn("EventEndTmstmp",col("HdrStartTimeGeneratedTmstmp"))
                            #    .withColumn('RowNbr1',row_number().over((Window.partitionBy("SourceFilePath","SourceFileName").orderBy(col('HdrStartTimeGeneratedTmstmp')))))
                            .select("FileNameUUID","JSONField","SourceFilePath","EventEndTmstmp"))
      df_raw_ULLogs_87903=jsonParser(df_raw_ULLogs_87903,'JSONField','FactULCartridgeLogs_RS','87903:1').drop("JSONField")

       
      # Join Cycle Start and End data
      df_ULLog = ((df_raw_ULLogs_87900).alias('src').join(df_raw_ULLogs_87901.alias('tgt'),['SourceFilePath','FileNameUUID'],'left').withColumn("ExternalSerialNbr",upper(col("ExternalSerialNbr"))).withColumn("InternalSerialNbr",upper(col("InternalSerialNbr"))))
      # df_ULLog.display()

      df_ULCartridgeLogs = ((df_raw_ULLogs_87902).alias('src').join(df_raw_ULLogs_87903.alias('tgt'),['SourceFilePath','FileNameUUID','CartridgeSerialNbr'],'left').drop('SourceFileName', 'HdrStartDateGeneratedDt', 'FileNameApplicatorPortCd', 'RawFileModificationTime', 'FileNameDeviceTypeCd', 'ExternalSerialNbr', 'RunID', 'LogEndDate', 'SourceFileSize', 'DeviceId', 'FileNameMessageTypeCd', 'SourceTypeId', 'HdrDataContractNbr', 'FileNameDtTmstmp', 'FileNameCycleNbr', 'LogType', 'InternalSerialNbr', 'FileNameDeviceSerialNbr', 'DeviceType', 'ConfigId', 'LogStartDate').withColumn("PrimeTime",to_timestamp(col("PrimeTime"),"yyyyMMddHHmmss"))
                              .withColumn("CartridgeSerialNbr",upper(col("CartridgeSerialNbr")))
                              .withColumn('CreatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('UpdatedDt',current_timestamp()))
        
      # df_ULCartridgeLogs.display()
      # print(df_ULCartridgeLogs.schema)
      # return ''
      CardRowNumWindow = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","HdrStartDateGeneratedDt").orderBy(desc(col('DEVICE.ShipEndDt')),desc(col('DEVICE.ShipStartDt'))))

      df_ULLog_EQMstr = (df_ULLog.alias("CYC").join(df_EquipmentMasterCS.alias("DEVICE"),
                                    (upper(col('CYC.ExternalSerialNbr')) == upper(col('DEVICE.SerialNumberNbr')))
                                        & (col('CYC.HdrStartDateGeneratedDt')>=(col('DEVICE.ShipStartDt')))
                                        & (col('CYC.HdrStartDateGeneratedDt')<col('DEVICE.ShipEndDt')) ,how ='left_outer')
                                .withColumn('equipRowNum',row_number().over(CardRowNumWindow))
                                .filter('equipRowNum = 1')
                                .drop('equipRowNum')
                                .select('CYC.*',
                                        col("DEVICE.SerialNumberNbr").alias("DEVICE_SerialNumberNbr"),
                                        col('DEVICE.SoldToID'),
                                        col('DEVICE.ShipToID')
                                        )
                                    .withColumn('CoolDeviceShippedInd',when(col('DEVICE_SerialNumberNbr').isNull(),0).otherwise(1))
                                    .withColumn("SoldToAccountID",when(col('DEVICE.SoldToID').isNull(),'0000000000').otherwise(col('DEVICE.SoldToID')))
                                    .withColumn("ShipToAccountID",when(col('DEVICE.ShipToID').isNull(),'0000000000').otherwise(col('DEVICE.ShipToID')))
                                    .drop('DEVICE_SerialNumberNbr','SoldToID','ShipToID','HdrDataContractNbr',
                                            'HdrDataContractNbr','PartitionKey','CardSPSerialNb','DeviceSerialNbr','FileNameDtTmstmp',
                                            'FileNameCycleNbr','FileNameMessageTypeCd','SBCSerialNbr'
                                            )
                            )
      # df_ULLog_EQMstr.display()
      df_AllCycles_AllFlags = (df_ULLog_EQMstr.alias("dcp")
                            
                            .join(df_MLTFiles.alias("MLTFile"),col("MLTFile.MLTSourceFileName") == col("dcp.MsrmntLogTreatmentFileNm"),'left')
                            .join(df_start_end_raw.alias("StEnd"),'FileNameUUID','left')
                            .withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('CreatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('UpdatedDt',current_timestamp())
                        )
        
        
      # print("After EQMstJoin")
      # df_AllCycles_AllFlags.display()

      #AlleId Decryption Code
      private_key_pem = get_rsa_key(vault_url,certificate_name)
      rsa_decrypt_udf = udf(lambda x: rsa_decrypt(x, private_key_pem), StringType())

      df_AllCycles_Encrypted= (df_AllCycles_AllFlags.withColumn("BDIDEncryptdFlg",when(col('BDIDEncryptdFlg') == "true","True")
                                                .when(col('BDIDEncryptdFlg') == "false","False")
                                                .otherwise(col('BDIDEncryptdFlg'))))
      # print("After df_AllCycles_Encrypted")
      # df_AllCycles_Encrypted.display()

      df_AllCycles_Encrypted_E = (df_AllCycles_Encrypted.filter(col('BDIDEncryptdFlg')=='True').withColumn("CoolSculptingID",rsa_decrypt_udf(col('BrilliantDistinctionID'))))

      # print("After df_AllCycles_Encrypted_E")
      # df_AllCycles_Encrypted_E.display()

      df_AllCycles_Encrypted_N = (df_AllCycles_Encrypted.where(("BDIDEncryptdFlg != 'True' or BDIDEncryptdFlg is null")).withColumn("CoolSculptingID",when(col('BDIDEncryptdFlg') == "False",
                                                                                                        col('BrilliantDistinctionID'))
                                                                                    .when(col('BDIDEncryptdFlg').isNull() ,lit(None))
                                                                                    .otherwise(col('BrilliantDistinctionID'))))
      # print("After df_AllCycles_Encrypted_N")
      # df_AllCycles_Encrypted_N.display()

      df_AllCycles_Decrypted = df_AllCycles_Encrypted_E.unionByName(df_AllCycles_Encrypted_N, allowMissingColumns=True)
      # print("After df_AllCycles_Decrypted Union")
      # df_AllCycles_Decrypted.display()

      df_AllCycles = (df_AllCycles_Decrypted
                                .withColumn("AlleIDDecryptedFlg", when(col('BDIDEncryptdFlg') == "True","True")
                                    .when(col('BDIDEncryptdFlg') == "False",lit(None))
                                    .otherwise(lit(None))))
      # print("Befor Drop duplicate")
      # df_AllCycles.display()
                    
      DedupSeq = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","HdrAppHeadNbr","HdrStartTimeGeneratedTmstmp")
                               .orderBy(desc(col('EventEndTmstmp'))))
        
      df_AllCycles_Dedup  =  df_AllCycles.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')

      #populate Cycle ID
      df_AllCycles_CycleId=populateCycleID(df_AllCycles_Dedup,dest_ULLogs,cycleIDSequence).drop("FileNameDeviceSerialNbr","SourceFileSize","SourceTypeId","CycleErrorCd","ConfigId","DeviceId","DeviceType","LogEndDate","LogStartDate","LogType","MaxCycleID","MLTSourceFileName","RawFileModificationTime","RowNum","RunID")
      df_AllCycles_CycleId,InsertMergeDict,UpdateMergeDict=evolveSchema(df_AllCycles_CycleId,'FactULLogs_RS')


      df_ULCartridgeLogs=df_ULCartridgeLogs.join(df_AllCycles_CycleId.select('SourceFilePath','FileNameUUID','CycleID'),['SourceFilePath','FileNameUUID'],'left')
      df_ULCartridgeLogs,CatridgeInsertMergeDict,CatridgeUpdateMergeDict=evolveSchema(df_ULCartridgeLogs,'FactULCartridgeLogs_RS')
      df_ULCartridgeLogs=(df_ULCartridgeLogs.withColumn("rowNumDedup1", row_number().over(Window.partitionBy("CartridgeSerialNbr","CurTreatDoseCount")
                               .orderBy(desc(col('HdrStartTimeGeneratedTmstmp'))))).filter('rowNumDedup1=1').drop('rowNumDedup1'))
     

      Deltatbl_ULCartridge_Logs = DeltaTable.forPath(spark, dest_CarttridgeLogPath)
      # print("merging data")
      (Deltatbl_ULCartridge_Logs.alias("tgt")
                .merge(df_ULCartridgeLogs.alias("src"),
                       "tgt.CartridgeSerialNbr = src.CartridgeSerialNbr and tgt.CurTreatDoseCount = src.CurTreatDoseCount"
                      ).whenMatchedUpdate(set = CatridgeUpdateMergeDict).whenNotMatchedInsert(values = CatridgeInsertMergeDict) 
          .execute()
      )

      # df_AllCycles_CycleId.display()
      # print(InsertMergeDict)
      Deltatbl_UL_Logs = DeltaTable.forPath(spark, dest_ULLogs)
      # print("merging data")
      (Deltatbl_UL_Logs.alias("tgt")
                .merge(df_AllCycles_CycleId.alias("src"),
                       "tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.HdrStartTimeGeneratedTmstmp = src.HdrStartTimeGeneratedTmstmp"
                      ).whenNotMatchedInsert(values = InsertMergeDict) 
          .execute()
      )

      # print("merging data completed")
      df_ulLogs_CNT=df_ulLogs_CNT.drop('LogStartDate','LogEndDate')
      df_ulLogs_CNT=df_ulLogs_CNT.join(StartEndDf,'FileNameUUID','left')


      loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
      # print("Writting data to logsourceFileProcess")
      loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
           

    except Exception as exp:
      ExceptionTraceback = traceback.format_exc()
      ErrorMessage = ExceptionTraceback + str(exp)
      loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
      loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
        
      logIntoStreamLogTable(Streamlog_df,"ADB_ULLogProcessing","Failed",Microbatch_df,ErrorMessage)
      streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
      print(ExceptionTraceback)
      # raise            


# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):     
    # print("Running for BatchID:"+str(batchId))
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
                     .filter(((col('LogType').isin(logTypes_Processed)) ))
                     
    
                     .withColumn("LogStartDate",lit(None))
                     .withColumn("LogEndDate",lit(None))
                     .drop('SourceFileName_noext')
                     .fillna({'FileNameApplicatorPortCd':''})
                     
               )

    
    try:

        StartEndLog = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
        StartEndDf=processStartAndEndParameters(StartEndLog)

        df_LogsULRS = df_logSource.where("FileNameDeviceTypeCd='RS'")
        ProcessULLogs87900_87901(df_LogsULRS,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId)
    
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
                  .queryName("V2_Transformation_RS_ULLogs_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", dest_ULLogs+checkPointLocation)
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

# df_Source_batch = (spark.read.format("delta").load(sourceFilePath).where("SourceFileName='COM3_D012021319013_UL_20230303173532_A_04.stx' and updateddt='2023-08-18T06:48:54.172'"))
# df_Source_batch.persist()
# df_Source_batch.display()

# COMMAND ----------

# upsertToDelta(df_Source_batch, 1)

# COMMAND ----------


