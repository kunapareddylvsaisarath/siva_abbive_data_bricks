# Databricks notebook source
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

dbutils.widgets.text('ulLogs_ConfigId','17')
ulLogs_ConfigId = dbutils.widgets.get('ulLogs_ConfigId')

dbutils.widgets.text('sourceTypeId','3')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

dbutils.widgets.text('allLogs_ConfigId','20')
allLogs_ConfigId = dbutils.widgets.get('allLogs_ConfigId')

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
#--------checkPointLocation = "/_checkpoints/"
#--------dest_ULLogs = '/mnt/silver/FACTULLogs/'
#--------equip_mstr_path = '/mnt/silver/DIMEquipmentMaster'
#--------startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
dest_ULLogs = ExternalLocation_silver+'/FACTULLogs/'
equip_mstr_path = ExternalLocation_silver+'/DIMEquipmentMaster'
startEndParameterPath = ExternalLocation_silver+'/DIMStartEndParameter'

certificate_name = "ZeltiqAbbVie20200716"
cycleIDSequence = 3000000

logTypes_Processed = ['UL'] 
contractNumberList_StartEnd = ["14020","14023"]
contractNumberList_UL_COM3 = ["25003","25003:1","25003:2","25004","25004:1","25004:2"]
contractNumberList_UL_CT=["84005","84006","84005:1","84006:1"]



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

# # Defining Schema:
# # Schema for contract 25003
# Schema_25003 = StructType([
#     StructField("sbcVer", StringType(), False),
#     StructField("pibTecVer", StringType(), False),
#     StructField("pibVer", StringType(), False),
#     StructField("appConfig", StringType(), False),
#     StructField("blob0CRC", StringType(), False),
#     StructField("blob1CRC", StringType(), False),
#     StructField("pibTecFactoryCRC", StringType(), False),
#     StructField("pibFactoryCRC", StringType(), False),
#     StructField("curCnt", StringType(), False),
#     StructField("appSN", StringType(), False),
#     StructField("appSPSN", StringType(), False),
#     StructField("externalSN", StringType(), False),
#     StructField("sbcInternal", StringType(), False),
#     StructField("pibSPSN", StringType(), False),
#     StructField("cardPN", StringType(), False),
#     StructField("cardSPSN", StringType(), False),
#     StructField("appSPAppletVer", StringType(), False),
#     StructField("cardSPAppletVer", StringType(), False),
#     StructField("pibSPAppletVer", StringType(), False),
#     StructField("profileIndex", StringType(), False),
#     StructField("profileTreatmentTime", StringType(), False),
#     StructField("profileTreatmentTemperature", StringType(), False),
#     StructField("controlChannels", StringType(), False),
#     StructField("sameNextPat", StringType(), False),
#     StructField("patType", StringType(), False),
#     StructField("bodyPart", StringType(), False),
#     StructField("newPatient", StringType(), False),
#     StructField("history", StringType(), False),
#     StructField("bd", StringType(), False),
#     StructField("isBDNumberEncrypted", StringType(), False),
#     StructField("alleCertDate", StringType(), False),
#     StructField("MLTFile", StringType(), False),
#     StructField("start", StringType(), False)
# ])

# # Schema for contract 25004

# Schema_25004 = StructType([
#     StructField("status", StringType(), False),
#     StructField("zcode", StringType(), False),
#     StructField("completion", StringType(), False)
# ])


# # Schema for contract 84005

# Schema_84005 = StructType([
#     StructField("date", StringType(), False),
#     StructField("time", StringType(), False),
#     StructField("swvers", StringType(), False),
#     StructField("remtrtcnt", StringType(), False),
#     StructField("pattreat", StringType(), False),
#     StructField("patreturn", StringType(), False),
#     StructField("patgender", StringType(), False),
#     StructField("trtbody", StringType(), False),
#     StructField("app1sn", StringType(), False),
#     StructField("app2sn", StringType(), False),
#     StructField("prevctcnt", StringType(), False),
#     StructField("bdsn", StringType(), False),
#     StructField("scsn", StringType(), False),
#     StructField("chosenmin", StringType(), False),
#     StructField("trtproto", StringType(), False),
#     StructField("alleCertDate", StringType(), False)
# ])

# # Schema for contract 84006

# Schema_84006 = StructType([
#     StructField("date", StringType(), False),
#     StructField("time", StringType(), False),
#     StructField("status", StringType(), False),
#     StructField("tcode", StringType(), False),
#     StructField("maxcooltemp", StringType(), False),
#     StructField("maxapp1temp", StringType(), False),
#     StructField("maxapp2temp", StringType(), False),
#     StructField("tottrt", StringType(), False)
# ])    

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


# Cols_25003 = [col('HdrFormatVersionCd'),
#               col('HdrDateGeneratedDt').alias('HdrStartDateGeneratedDt'),
#               col('HdrTimeGeneratedTmstmp').alias('HdrStartTimeGeneratedTmstmp'),
#               col('HdrLogtypeCd'),
#               col('HdrDestinationSubSystemCd'),
#               col('HdrSourceSubSystemCd'),
#               col('HdrCommandCD'),
#               col('HdrDataStoreID'),
#               col('HdrDataContractNbr'),
#               col('HdrAppHeadNbr'),
#               col('JSONField'),
#               col('sbcVer').alias('SBCVersionNbr'),
#               col('pibTecVer').alias('PIBTecVerNbr'),
#               col('pibVer').alias('PibVersionNbr'),
#               col('appConfig').alias('ApplicatorConfigurantionNbr'),
#               col('blob0CRC').alias('Blob0CRCCd'),
#               col('blob1CRC').alias('Blob1CRCCd'),
#               col('pibTecFactoryCRC').alias('PIBTecFactoryCRC'),
#               col('pibFactoryCRC').alias('PibFactoryCRC'),
#               col('curCnt').alias('CurrentCycleNbr'),
#               col('appSN').alias('ApplicatorSerialNbr'),
#               col('appSPSN').alias('ApplicatorInternalSerialNbr'),
#               col('externalSN').alias('DeviceSerialNbr'),
#               col('sbcInternal').alias('SBCSerialNbr'),
#               col('pibSPSN').alias('PibInternalSerialNbr'),
#               col('cardPN').alias('CardPartNbr'),
#               trim(upper(col('cardSPSN'))).alias('CardSPSerialNbr'),
#               col('appSPAppletVer').alias('AppSPAppletVerNbr'),
#               col('cardSPAppletVer').alias('CardSPAppletVerNbr'),
#               col('pibSPAppletVer').alias('PIBSPAppletVerNbr'),
#               col('profileIndex').alias('ContourProfileNbr'),
#               col('profileTreatmentTime').alias('ContourProfileTreatmentTm'),
#               col('profileTreatmentTemperature').alias('ContourProfileTreatmentTemp'),
#               col('controlChannels').alias('controlChannelCd'),
#               col('sameNextPat').alias('PatientNextSameFlg'),
#               col('patType').alias('PatientGenderCd'),
#               col('bodyPart').alias('PatientBodyPartNm'),
#               col('newPatient').alias('PatientNewFlg'),
#               col('history').alias('History'),col('bd').alias('BrilliantDistinctionID'),
#               col('isBDNumberEncrypted').alias('BDIDEncryptdFlg'),
#               col('alleCertDate').alias('AlleCertDt'),
#               col('MLTFile').alias('MsrmntLogTreatmentFileNm'),
#               col('start').alias('StartFlg'),
#               col('ExternalSerialNbr'),
#               col('InternalSerialNbr'),
#               col("FileNameCycleNbr"),
#               col('SourceFileName'),
#               col('SourceFilePath'),
#               col('FileNameUUID'),
#               col('SourceFileSize'),
#               col('FileNameDeviceTypeCd')
#              ]
            
# Cols_25004 = [col('status').alias('CycleFinalStatusReasonDesc'),
#               col('zcode').alias('CycleErrorZCd'),
#               col('completion').alias('CycleFinalStatusDesc'),
#               col('SourceFilePath'),
#               col('FileNameUUID')]

# Cols_84005 = [col('HdrFormatVersionCd'),
#               col('HdrDateGeneratedDt').alias('HdrStartDateGeneratedDt'),
#               col('HdrTimeGeneratedTmstmp').alias('HdrStartTimeGeneratedTmstmp'),
#               col('HdrLogtypeCd'),
#               col('HdrDestinationSubSystemCd'),
#               col('HdrSourceSubSystemCd'),
#               col('HdrCommandCD'),
#               col('HdrDataStoreID'),
#               col('HdrDataContractNbr'),
#               col('HdrAppHeadNbr'),
#               col('JSONField'),
#               col('date').alias('CycleStartDt'),
#               col('time').alias('CycleStartTm'),
#               col('swvers').alias('SwVersionNbr'),
#               col('remtrtcnt').alias('CurrentCycleNbr'),
#               col('pattreat').alias('PatientNextSameFlg'),
#               col('patreturn').alias('PatientNewFlg'),
#               col('patgender').alias('PatientGenderCd'),
#               col('trtbody').alias('PatientBodyPartNm'),
#               col('app1sn').alias('Applicator1SerialNbr'),
#               col('app2sn').alias('Applicator2SerialNbr'),
#               col('prevctcnt').alias('PrevCTCnt'),
#               col('bdsn').alias('BrilliantDistinctionID'),
#               trim(upper(col('scsn'))).alias('CardSPSerialNbr'),
#               col('chosenmin').alias('ChsnTrtmntDurationTm'),
#               col('trtproto').alias('TrtmntPrtclCd'),
#               col('alleCertDate').alias('AlleCertDt'),
#               col('ExternalSerialNbr'),
#               col('InternalSerialNbr'),
#               col("FileNameCycleNbr"),
#               col('SourceFileName'),
#               col('SourceFilePath'),
#               col('FileNameUUID'),
#               col('SourceFileSize'),
#               col('FileNameDeviceTypeCd')
#             ]

# Cols_84006 = [col('JSONField'),
#               col('date').alias('CycleEndDt'),
#               col('time').alias('CycleEndTm'),
#               col('status').alias('CycleFinalStatusReasonDesc'),
#               col('tcode').alias('CycleErrorTCd'),
#               col('maxcooltemp').alias('MaxCoolTmp'),
#               col('maxapp1temp').alias('MaxApplicator1Tmp'),
#               col('maxapp2temp').alias('MaxApplicator2Tmp'),
#               col('tottrt').alias('TtlTrtmntLngthMinNbr'),
#               col('SourceFilePath'),
#               col('FileNameUUID')
#              ]



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

tenant_id = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
client_id = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
client_secret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")

# COMMAND ----------

#Get the RSA Private Key from Azure KV 
def get_rsa_key(vault_url,certificate_name):
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
# MAGIC ## Process ul logs for COM3 25003 and 25004

# COMMAND ----------

def ProcessULLogs25003_25004(df_Source_ULLogs,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId):
    df_Source_ULLogs=df_Source_ULLogs.withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_Source_ULLogs.persist()
    df_ulLogs_CNT = (df_Source_ULLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                       'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath',
                                       'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId','FileNameUUID','ConfigId',"RawFileModificationTime","RunID")
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
#     df_ueLogs_CNT.display()
    # df_ulLogs_CNT = df_ulLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_ulLogs_CNT.persist()

    # print("logging into logProcess")
    # print("df_ulLogs_CNT:"+str(df_ulLogs_CNT.count()))
    loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')
    df_Source_ULLogs = df_Source_ULLogs.filter(col("HdrDataContractNbr").isin(contractNumberList_UL_COM3))
    loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')

    try:
        log_df = df_Source_ULLogs.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(dest_ULLogs))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = df_Source_ULLogs

        # raise Exception("No Exception: Manual Failure")        
                
        # df_cycles = spark.read.format('delta').load(dest_ULLogs)
        #Change DeviceSerialNumber to correct format 
        df_EquipmentMasterCS = (spark.read.format('delta').load(equip_mstr_path).filter(col('EquipmentTransitStatusNm')=='SHIPPED')
                                .dropDuplicates()
                               .select("SerialNumberNbr","ShipStartDt","ShipEndDt","SoldToID","ShipToID","MaterialCd","EquipmentTransitStatusNm")
                              )

        #Fetch MLT File Path from log processed
        MLTWindow = (Window.partitionBy("SourceFileName").orderBy(desc(col('UpdatedDt'))))
        df_MLTFiles =(spark.read.format("delta")
                                       .load(src_filesProcessed)
                                       .filter((col('FileNameMessageTypeCd') =='MLT') & (col('ConfigId') == allLogs_ConfigId))
                                       .withColumn('rownum',row_number().over(MLTWindow))
                                       .filter('rownum = 1').drop('rownum')                                       
                                       .withColumn("SourceFileName", regexp_replace("SourceFileName", ".stx", ""))
                                       .select(col('SourceFileName').alias('MLTSourceFileName'),
                                              col('SourceFilePath').alias('MsrmntLogTreatmentFileURLTxt'))
                           )

        #Fetch Start and End dates to populate event start and end timestamps
        df_start_end_raw = StartEndDf.withColumnRenamed("LogStartDate","EventStartTmstmp").withColumnRenamed("LogEndDate","EventEndTmstmp")
        # (spark.read.format("delta")
                            # .load(startEndParameterPath)
                            # .select('FileNameUUID','EventStartTmstmp','EventEndTmstmp'))

        # lst=["25003","25003:1","25003:2","25004","25004:1","25004:2"]


        df_Source_ULLogs=(df_Source_ULLogs.withColumnRenamed("HdrTimeGeneratedTmstmp","HdrStartTimeGeneratedTmstmp")
                  .withColumnRenamed("HdrDateGeneratedDt","HdrStartDateGeneratedDt") 
                  .withColumnRenamed('HdrLogTypeCd','HdrLogtypeCd')
                  .withColumnRenamed('HdrCommandCd','HdrCommandCD')
                  .withColumn("HdrAppHeadNbr",col("HdrAppHeadNbr").cast("int"))
                  ) 
            
        # Parse contract 25003
        df_raw_ULLogs_25003 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('25003','25003:1','25003:2'))
                            #    .withColumn("JSONField", from_json("JSONField", Schema_25003))
                            #    .select('*', col('JSONField.*'))
                            #    .select(Cols_25003).drop('JSONField')
                              )
        df_raw_ULLogs_25003=jsonParser(df_raw_ULLogs_25003,'JSONField','FactULLogs','25003').drop("JSONField")
        
        # Parse contract 25004
        df_raw_ULLogs_25004 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('25004','25004:1','25004:2')).select("FileNameUUID","JSONField","SourceFilePath")
                            #    .withColumn("JSONField", from_json("JSONField", Schema_25004))
                            #    .select('*', col('JSONField.*'))
                            #    .select(Cols_25004).drop('JSONField')
                              )
        df_raw_ULLogs_25004=jsonParser(df_raw_ULLogs_25004,'JSONField','FactULLogs','25004').drop("JSONField")

        # Join Cycle Start and End data
        df_ULLog = (df_raw_ULLogs_25003.alias('src').join(df_raw_ULLogs_25004.alias('tgt'),['SourceFilePath','FileNameUUID'],'left')
                    .withColumn('CompletedCycleUsedCnt',when(col("CycleFinalStatusDesc")=="Completed",1).otherwise(0))
                    .withColumn("CurrentCycleNbr",col("CurrentCycleNbr").cast("int"))
                    # .withColumn('IsNewData',lit('Yes'))
                   )

        # CardSPSerialNbr=df_ULLog.select("CardSPSerialNbr").toPandas()['CardSPSerialNbr']
        # listCardSPSerialNbr=list(CardSPSerialNbr)
        
        #Get SmartCardShippedInd and CoolDeviceShippedInd use ExternalSerialNbr
        CardRowNumWindow = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","CardSPSerialNbr","CurrentCycleNbr","HdrStartTimeGeneratedTmstmp").orderBy(desc(col('DEVICE.ShipEndDt')),desc(col('DEVICE.ShipStartDt')),desc(col('CARD.ShipEndDt')),desc(col('CARD.ShipStartDt'))))

        df_ULLog_EQMstr = (df_ULLog.alias("CYC")
                              .join(df_EquipmentMasterCS.alias("CARD"),
                                    (col('CYC.CardSPSerialNbr') == col('CARD.SerialNumberNbr')) 
                                        & (col('CYC.HdrStartDateGeneratedDt')>=col('CARD.ShipStartDt'))
                                        & (col('CYC.HdrStartDateGeneratedDt')<col('CARD.ShipEndDt')) ,how ='left_outer')
                              .join(df_EquipmentMasterCS.alias("DEVICE"),
                                    (col('CYC.ExternalSerialNbr') == col('DEVICE.SerialNumberNbr')) 
                                        & (col('CYC.HdrStartDateGeneratedDt')>=(col('DEVICE.ShipStartDt')))
                                        & (col('CYC.HdrStartDateGeneratedDt')<col('DEVICE.ShipEndDt')) ,how ='left_outer')
                              .withColumn('equipRowNum',row_number().over(CardRowNumWindow))
                              .filter('equipRowNum = 1')
                              .drop('equipRowNum')
                              .select('CYC.*',
                                      col("CARD.SerialNumberNbr").alias("CARD_SerialNumberNbr"),
                                      col("DEVICE.SerialNumberNbr").alias("DEVICE_SerialNumberNbr"),
                                      col('DEVICE.SoldToID'),
                                      col('DEVICE.ShipToID'),
                                      col("CARD.MaterialCd").alias("EQUIPCardPartNbr"),
                                      col("CARD.ShipStartDt").alias("SmartCardShipDt")
                                     )
                                    .withColumn('SmartCardShippedInd',when(col('CARD_SerialNumberNbr').isNull(),0).otherwise(1))
                                    .withColumn('CoolDeviceShippedInd',when(col('DEVICE_SerialNumberNbr').isNull(),0).otherwise(1))
                                    .withColumn("SoldToAccountID",when(col('DEVICE.SoldToID').isNull(),'0000000000').otherwise(col('DEVICE.SoldToID')))
                                    .withColumn("ShipToAccountID",when(col('DEVICE.ShipToID').isNull(),'0000000000').otherwise(col('DEVICE.ShipToID')))
                                    .drop('CARD_SerialNumberNbr','DEVICE_SerialNumberNbr','SoldToID','ShipToID','HdrDataContractNbr',
                                          'HdrDataContractNbr','PartitionKey','CardSPSerialNb','DeviceSerialNbr','FileNameDtTmstmp',
                                            'FileNameCycleNbr','FileNameMessageTypeCd','SBCSerialNbr'
                                         )
                          )

        # Identify fraud cycles
        # columnList = ["CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","ExternalSerialNbr","InternalSerialNbr"]
        
        # df_cycles_Card = df_cycles.filter(col('CardSPSerialNbr').isin(listCardSPSerialNbr)).withColumn('IsNewData',lit('No')).select(columnList)
        # df_Cycles_IdentifyFraud = df_cycles_Card.unionByName(df_ULLog_EQMstr,allowMissingColumns=True)
        
        # UnqCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr")
        #                 .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt")
        #                 .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCyclUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","ExternalSerialNbr","InternalSerialNbr")
        #                    .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCyclDateUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","ExternalSerialNbr","InternalSerialNbr")
        #                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time","ExternalSerialNbr"))

        # df_Cycles_IdentifyFraudInd = (df_Cycles_IdentifyFraud.withColumn("UnqCyclSeq", row_number().over(UnqCycl_Spec))
        #                                           .withColumn("DupCyclDateSeq", row_number().over(DupCycl_Spec))
        #                                           .withColumn("DupCyclUPMSeq", row_number().over(DupCyclUPM_Spec))
        #                                           .withColumn("DupCyclDateUPMSeq", row_number().over(DupCyclDateUPM_Spec)))
        
        # df_Cycles_PopulateFraudInd = (df_Cycles_IdentifyFraudInd
        #                                  .withColumn("UnsoldFraudFlg",when((col('SmartCardShippedInd') + col('CoolDeviceShippedInd'))== 2,"N").otherwise("Y"))
        #                                  .withColumn("DuplicateCycleFlg",when(col('UnqCyclSeq')== 1,"N").otherwise("Y"))
        #                                  .withColumn("CycleUtilizationFraudFlg",when((col('UnqCyclSeq') + col('DupCyclDateUPMSeq'))== 2,"N").otherwise("Y"))
        #                                  .withColumn("CycleUsedCnt",when(col('DupCyclDateUPMSeq')== 1,1).otherwise(0))
        #                                  .withColumn("IsFraudCycleFlg",when((col("UnsoldFraudFlg")=="N") & (col("CycleUtilizationFraudFlg")=="N"),"N").otherwise("Y"))
        #                              )
        
        df_AllCycles_AllFlags = (df_ULLog_EQMstr.alias("dcp")
                                #  .filter('IsNewData = "Yes"')
                            .join(df_MLTFiles.alias("MLTFile"),col("MLTFile.MLTSourceFileName") == col("dcp.MsrmntLogTreatmentFileNm"),'left')
                            .join(df_start_end_raw.alias("StEnd"),'FileNameUUID','left')
                            .withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('CreatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('UpdatedDt',current_timestamp())
                       )

        #AlleId Decryption Code
        private_key_pem = get_rsa_key(vault_url,certificate_name)
        rsa_decrypt_udf = udf(lambda x: rsa_decrypt(x, private_key_pem), StringType())
        
        df_AllCycles_Encrypted= (df_AllCycles_AllFlags.withColumn("BDIDEncryptdFlg",when(col('BDIDEncryptdFlg') == "true","True")
                                              .when(col('BDIDEncryptdFlg') == "false","False")
                                              .otherwise(col('BDIDEncryptdFlg'))))
        df_AllCycles_Encrypted_E = (df_AllCycles_Encrypted.filter(col('BDIDEncryptdFlg')=='True').withColumn("CoolSculptingID",rsa_decrypt_udf(col('BrilliantDistinctionID'))))
        df_AllCycles_Encrypted_N = (df_AllCycles_Encrypted.filter(col('BDIDEncryptdFlg')!='True').withColumn("CoolSculptingID",when(col('BDIDEncryptdFlg') == "False",
                                                                                                        col('BrilliantDistinctionID'))
                                                                                 .when(col('BDIDEncryptdFlg').isNull() ,lit(None))
                                                                                 .otherwise(col('BrilliantDistinctionID'))))
        df_AllCycles_Decrypted = df_AllCycles_Encrypted_E.unionByName(df_AllCycles_Encrypted_N, allowMissingColumns=True)
        df_AllCycles = (df_AllCycles_Decrypted
                                .withColumn("AlleIDDecryptedFlg", when(col('BDIDEncryptdFlg') == "True","True")
                                 .when(col('BDIDEncryptdFlg') == "False",lit(None))
                                 .otherwise(lit(None))))

        DedupSeq = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","CardSPSerialNbr","HdrAppHeadNbr","CurrentCycleNbr","HdrStartTimeGeneratedTmstmp")
                               .orderBy(desc(col('EventEndTmstmp'))))
        
        df_AllCycles_Dedup  =  df_AllCycles.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')

        #populate Cycle ID
        df_AllCycles_CycleId=populateCycleID(df_AllCycles_Dedup,dest_ULLogs,cycleIDSequence)
        df_AllCycles_CycleId=df_AllCycles_CycleId.drop('RunID', 'LogEndDate', 'RowNum', 'FileNameDeviceSerialNbr', 'MaxCycleID', 'SourceFileSize', 'DeviceId', 'SourceTypeId', 'LogStartDate', 'RawFileModificationTime', 'LogType', 'MLTSourceFileName', 'ConfigId', 'DeviceType')
        #Generating ScannedID
        df_CoolSculptingID = df_AllCycles_CycleId.select("CoolSculptingID").filter(upper(df_AllCycles_CycleId.CoolSculptingID) != 'MISSING').filter(lower(df_AllCycles_CycleId.CoolSculptingID) != 'unknown').filter(df_AllCycles_CycleId.CoolSculptingID.isNotNull()).filter(df_AllCycles_CycleId.CoolSculptingID != '0000000000').dropDuplicates()
        generate_ScannedId(df_CoolSculptingID)

        df_AllCycles_CycleId,InsertMergeDict,UpdateMergeDict=evolveSchema(df_AllCycles_CycleId,'FactULLogs')
        #df_AllCycles_CycleId.display()
        
        # return ''
       
        Deltatbl_UL_Logs = DeltaTable.forPath(spark, dest_ULLogs)
        (Deltatbl_UL_Logs.alias("tgt")
                .merge(df_AllCycles_CycleId.alias("src"),
                       "tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.CardSPSerialNbr = src.CardSPSerialNbr and tgt.CurrentCycleNbr = src.CurrentCycleNbr and tgt.HdrStartTimeGeneratedTmstmp = src.HdrStartTimeGeneratedTmstmp"
                      )
                      .whenNotMatchedInsert(values = InsertMergeDict) 
          .execute()
        )


        df_ulLogs_CNT=df_ulLogs_CNT.drop('LogStartDate','LogEndDate')
        df_ulLogs_CNT=df_ulLogs_CNT.join(StartEndDf,'FileNameUUID','left')


        loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
           

    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        print(ExceptionTraceback)
        loadlogProcessesDeltaTable(df_ulLogs_CNT,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_Source_ULLogs,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
        
        # logIntoStreamLogTable(log_df,"ADB_ULLogProcessing","Failed",Microbatch_df,ErrorMessage)
        # streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        raise ErrorMessage            


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process ul logs for CT 84005 and 84006

# COMMAND ----------

def ProcessULLogs84005_84006(df_Source_ULLogsCT,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId):

    df_Source_ULLogsCT=df_Source_ULLogsCT.withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_Source_ULLogsCT.persist()
    df_ulLogs_CT_CNT = (df_Source_ULLogsCT.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                       'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath',
                                       'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId','FileNameUUID','ConfigId',"RawFileModificationTime","RunID")
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
#     df_ueLogs_CNT.display()
    # df_ulLogs_CNT = df_ulLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_ulLogs_CT_CNT.persist()

    # print("logging into logProcess")
    # print("df_ulLogs_CNT:"+str(df_ulLogs_CT_CNT.count()))
    loadlogProcessesDeltaTable(df_ulLogs_CT_CNT,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')
    df_Source_ULLogsCT = df_Source_ULLogsCT.filter(col("HdrDataContractNbr").isin(contractNumberList_UL_CT))
    loadAuditTables_Ingestion_Log(df_Source_ULLogsCT,dest_ULLogs,'ADB_ULLogProcessing','InProgress','')


    try:
        # log_df = df_Source_ULLogsCT.select(col('ConfigId').alias('ConfigID'), col('SourceTypeId').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
        #                                  .withColumn('Destination', lit(str(dest_ULLogs))) \
        #                                  .withColumn('Run_ID', lit(str(batchId))) \
        #                                  .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = df_Source_ULLogsCT

        df_cycles = spark.read.format('delta').load(dest_ULLogs)
        #Change DeviceSerialNumber to correct format 
        df_EquipmentMasterCS = (spark.read.format('delta').load(equip_mstr_path).filter(col('EquipmentTransitStatusNm')=='SHIPPED')
                                .dropDuplicates()
                               .select("SerialNumberNbr","ShipStartDt","ShipEndDt","SoldToID","ShipToID","MaterialCd","EquipmentTransitStatusNm")
                              )
        #Fetch MLT Data from log file processed
        MLTWindow = (Window.partitionBy("SourceFileName").orderBy(desc(col('UpdatedDt'))))
        df_MLTFiles =(spark.read.format("delta")
                                       .load(src_filesProcessed)
                                       .filter((col('FileNameMessageTypeCd') =='MLT') & (col('ConfigId') == allLogs_ConfigId))
                                       .withColumn('rownum',row_number().over(MLTWindow))
                                       .filter('rownum = 1').drop('rownum')
                                       .withColumn("SourceFileName", regexp_replace("SourceFileName", ".stx", ""))
                                       .select(col('SourceFileName').alias('MLTSourceFileName'),
                                               col('SourceFilePath').alias('MsrmntLogTreatmentFileURLTxt'))
                           )

        #Fetch Start and End Dates to populate Envet start and EndDate 
        df_start_end_raw = StartEndDf.withColumnRenamed("LogStartDate","EventStartTmstmp").withColumnRenamed("LogEndDate","EventEndTmstmp")
        #  (spark.read.format('delta').load(startEndParameterPath)
        #                     .select('FileNameUUID','EventStartTmstmp','EventEndTmstmp'))

        # lst=["84005","84006","84005:1","84006:1"]

        df_Source_ULLogsCT=(df_Source_ULLogsCT.withColumnRenamed("HdrTimeGeneratedTmstmp","HdrStartTimeGeneratedTmstmp")
                  .withColumnRenamed("HdrDateGeneratedDt","HdrStartDateGeneratedDt") 
                  .withColumnRenamed('HdrLogTypeCd','HdrLogtypeCd')
                  .withColumnRenamed('HdrCommandCd','HdrCommandCD')
                  .withColumn("HdrAppHeadNbr",col("HdrAppHeadNbr").cast("int"))
                  ) 

        #Parse Cycle Start Data
        df_raw_ULLogs_84005 = (df_Source_ULLogsCT.filter(col("HdrDataContractNbr").isin('84005','84005:1'))
                            #    .withColumn("JSONField", from_json("JSONField", Schema_84005))
                            #    .select('*', col('JSONField.*'))
                            #    .select(Cols_84005)
                            #    .drop('JSONField')
                              )

        df_raw_ULLogs_84005=jsonParser(df_raw_ULLogs_84005,'JSONField','FactULLogs','84005').drop("JSONField")
        #Parse Cycle End Data
        df_raw_ULLogs_84006 = (df_Source_ULLogsCT.filter(col('HdrDataContractNbr').isin('84006','84006:1')).select("FileNameUUID","JSONField","SourceFilePath")
                            #    .withColumn("JSONField", from_json("JSONField", Schema_84006))
                            #    .select('*', col('JSONField.*'))
                            #    .select(Cols_84006)
                            #    .withColumn('TtlTrtmntDurationTm',col('TtlTrtmntLngthMinNbr').cast(StringType()))
                              
                            #    .drop('JSONField')
                              )
        
        df_raw_ULLogs_84006=(jsonParser(df_raw_ULLogs_84006,'JSONField','FactULLogs','84006').drop("JSONField").withColumn("CycleEndDt",col('CycleEndDt').cast('date'))
                               .withColumn("CycleEndTm",to_timestamp(concat("CycleEndDt",lit(" "),"CycleEndTm"),"yyyy-MM-dd HH:mm:ss")))


        #Combine both Cycle start and cycle end data
        df_ULLog = (df_raw_ULLogs_84005.join(df_raw_ULLogs_84006,['SourceFilePath','FileNameUUID'],'left')
                    .withColumn('CompletedCycleUsedCnt',lit(0))
                    .withColumn("CurrentCycleNbr",col("CurrentCycleNbr").cast("int"))
                    .withColumn("InterruptStart",to_timestamp(concat("CycleStartDt",lit(" "),"CycleStartTm")
                                                                          ,"yyyy-MM-dd HH:mm:ss"))
                    # .withColumn('IsNewData',lit('Yes'))
                   )
        # CardSPSerialNbr=df_ULLog.select("CardSPSerialNbr").toPandas()['CardSPSerialNbr']
        # listCardSPSerialNbr=list(CardSPSerialNbr)
        
        #Get SmartCardShippedInd and CoolDeviceShippedInd use ExternalSerialNbr
        CardRowNumWindow = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","CardSPSerialNbr","CurrentCycleNbr","HdrStartTimeGeneratedTmstmp").orderBy(desc(col('DEVICE.ShipEndDt')),desc(col('DEVICE.ShipStartDt')),desc(col('CARD.ShipEndDt')),desc(col('CARD.ShipStartDt'))))
        df_ULLog_EQMstr = (df_ULLog.alias("CYC")
                              .join(df_EquipmentMasterCS.alias("CARD"),(col('CYC.CardSPSerialNbr') == col('CARD.SerialNumberNbr')) 
                                  & ( col('CYC.HdrStartDateGeneratedDt').between(col('CARD.ShipStartDt'),col('CARD.ShipEndDt'))) ,how ='left_outer')
                              .join(df_EquipmentMasterCS.alias("DEVICE"),(lpad(col('CYC.ExternalSerialNbr'),18,'0') == col('DEVICE.SerialNumberNbr')) 
                                  & ( col('CYC.HdrStartDateGeneratedDt').between(col('DEVICE.ShipStartDt'),col('DEVICE.ShipEndDt'))) ,how ='left_outer')
                              .withColumn('equipRowNum',row_number().over(CardRowNumWindow))
                              .filter('equipRowNum = 1')
                              .drop('equipRowNum')                                  
                              .select('CYC.*',
                                      col("CARD.SerialNumberNbr").alias("CARD_SerialNumberNbr"),
                                      col("DEVICE.SerialNumberNbr").alias("DEVICE_SerialNumberNbr"),
                                      col('DEVICE.SoldToID'),
                                      col('DEVICE.ShipToID'),
                                      col("CARD.MaterialCd").alias("EQUIPCardPartNbr"),
                                      col("CARD.ShipStartDt").alias("SmartCardShipDt")
                                     )
                                    .withColumn('SmartCardShippedInd',when(col('CARD_SerialNumberNbr').isNull(),0).otherwise(1))
                                    .withColumn('CoolDeviceShippedInd',when(col('DEVICE_SerialNumberNbr').isNull(),0).otherwise(1))
                                    .withColumn("SoldToAccountID",when(col('DEVICE.SoldToID').isNull(),'0000000000').otherwise(col('DEVICE.SoldToID')))
                                    .withColumn("ShipToAccountID",when(col('DEVICE.ShipToID').isNull(),'0000000000').otherwise(col('DEVICE.ShipToID')))
                                    .drop('CARD_SerialNumberNbr','DEVICE_SerialNumberNbr','SoldToID','ShipToID','HdrDataContractNbr',
                                          'HdrDataContractNbr','PartitionKey','CardSPSerialNb','DeviceSerialNbr','FileNameDtTmstmp',
                                            'FileNameCycleNbr','FileNameMessageTypeCd','SBCSerialNbr'
                                          
                                         )
                          )

        # # Identify Fraud checks
        # columnList = ["CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","ExternalSerialNbr","InternalSerialNbr"]
        # df_cycles_Card = df_cycles.filter(col('CardSPSerialNbr').isin(listCardSPSerialNbr)).withColumn('IsNewData',lit('No')).select(columnList)
        # df_Cycles_IdentifyFraud = df_cycles_Card.unionByName(df_ULLog_EQMstr,allowMissingColumns=True)
        
        # UnqCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr")
        #                 .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt")
        #                 .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCyclUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","ExternalSerialNbr","InternalSerialNbr")
        #                    .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        # DupCyclDateUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","ExternalSerialNbr","InternalSerialNbr")
        #                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time","ExternalSerialNbr"))

        # df_Cycles_IdentifyFraudInd = (df_Cycles_IdentifyFraud.withColumn("UnqCyclSeq", row_number().over(UnqCycl_Spec))
        #                                           .withColumn("DupCyclDateSeq", row_number().over(DupCycl_Spec))
        #                                           .withColumn("DupCyclUPMSeq", row_number().over(DupCyclUPM_Spec))
        #                                           .withColumn("DupCyclDateUPMSeq", row_number().over(DupCyclDateUPM_Spec)))
        
        # df_Cycles_PopulateFraudInd = (df_Cycles_IdentifyFraudInd.withColumn("UnsoldFraudFlg",when((col('SmartCardShippedInd') + col('CoolDeviceShippedInd'))== 2,"N").otherwise("Y"))
        #                                  .withColumn("DuplicateCycleFlg",when(col('UnqCyclSeq')== 1,"N").otherwise("Y"))
        #                                  .withColumn("CycleUtilizationFraudFlg",when((col('UnqCyclSeq') + col('DupCyclDateUPMSeq'))== 2,"N").otherwise("Y"))
        #                                  .withColumn("CycleUsedCnt",when(col('DupCyclDateUPMSeq')== 1,1).otherwise(0))
        #                                  .withColumn("IsFraudCycleFlg",when((col("UnsoldFraudFlg")=="N") & (col("CycleUtilizationFraudFlg")=="N"),"N").otherwise("Y"))
        #                              )
        
        df_AllCycles = (df_ULLog_EQMstr.alias("dcp")
                        # .filter('IsNewData = "Yes"')
                            .join(df_start_end_raw.alias("StEnd"),'FileNameUUID','left')
                            # .withColumnRenamed("CycleErrorTCd","CycleErrorCd")
                            .withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('CreatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_ULLogProcessing'))
                            .withColumn('UpdatedDt',current_timestamp())
                       )
        #AlleId Decryption Code
        private_key_pem = get_rsa_key(vault_url,certificate_name)
        rsa_decrypt_udf = udf(lambda x: rsa_decrypt(x, private_key_pem), StringType())
        
        df_AllCycles_NC = (df_AllCycles.filter((col('BrilliantDistinctionID')=="Unknown")|(col('BrilliantDistinctionID').isNull()))
                  .withColumn("CoolSculptingID",when(col('BrilliantDistinctionID') == "Unknown",lit(None))
                              .otherwise("Unknown")))
        df_AllCycles_EC = (df_AllCycles.filter((col('BrilliantDistinctionID')!="Unknown")&(col('BrilliantDistinctionID').isNotNull()))
                   .withColumn("CoolSculptingID",rsa_decrypt_udf(col('BrilliantDistinctionID'))))
        df_AllCycles = df_AllCycles_NC.unionByName(df_AllCycles_EC, allowMissingColumns=True)
        
        df_AllCycles = (df_AllCycles.withColumn("BDIDEncryptdFlg", when(col('CoolSculptingID').isNull() ,lit(None))
                                                .otherwise("True"))
                                    .withColumn("AlleIDDecryptedFlg", when(col('BDIDEncryptdFlg').isNull() ,lit(None))
                                                .otherwise("True")))
        
        DedupSeq = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","CardSPSerialNbr","HdrAppHeadNbr","CurrentCycleNbr","HdrStartTimeGeneratedTmstmp")
                        .orderBy(desc(col('EventEndTmstmp'))))
        
        df_AllCycles_Dedup  =  df_AllCycles.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')
        
        #populate Cycle Id
        df_AllCycles_CycleId=populateCycleID(df_AllCycles_Dedup,dest_ULLogs,cycleIDSequence)
        df_AllCycles_CycleId=df_AllCycles_CycleId.drop('RunID', 'LogEndDate', 'RowNum', 'FileNameDeviceSerialNbr', 'MaxCycleID', 'SourceFileSize', 'DeviceId', 'SourceTypeId', 'LogStartDate', 'RawFileModificationTime', 'LogType', 'ConfigId', 'DeviceType')
        #Generating ScannedID
        df_CoolSculptingID = df_AllCycles_CycleId.select("CoolSculptingID").filter(upper(df_AllCycles_CycleId.CoolSculptingID) != 'MISSING').filter(lower(df_AllCycles_CycleId.CoolSculptingID) != 'unknown').filter(df_AllCycles_CycleId.CoolSculptingID.isNotNull()).filter(df_AllCycles_CycleId.CoolSculptingID != '0000000000').dropDuplicates()

        generate_ScannedId(df_CoolSculptingID)
        df_AllCycles_CycleId,InsertMergeDict,UpdateMergeDict=evolveSchema(df_AllCycles_CycleId,'FactULLogs')
        #df_AllCycles_CycleId.display()
  




        
        #display(df_AllCycles)
        Deltatbl_UL_Logs = DeltaTable.forPath(spark, dest_ULLogs)
        (Deltatbl_UL_Logs.alias("tgt")
                .merge(df_AllCycles_CycleId.alias("src"),
                    "tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.CardSPSerialNbr = src.CardSPSerialNbr and tgt.CurrentCycleNbr = src.CurrentCycleNbr and tgt.HdrStartTimeGeneratedTmstmp = src.HdrStartTimeGeneratedTmstmp")
                    #   .whenMatchedUpdate(set = UpdateMergeDict)
                      .whenNotMatchedInsert(values = InsertMergeDict) 
          .execute()
        )
        
        df_ulLogs_CT_CNT=df_ulLogs_CT_CNT.drop('LogStartDate','LogEndDate')
        df_ulLogs_CT_CNT=df_ulLogs_CT_CNT.join(StartEndDf,'FileNameUUID','left')

        loadAuditTables_Ingestion_Log(df_Source_ULLogsCT,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
        loadlogProcessesDeltaTable(df_ulLogs_CT_CNT,dest_ULLogs,'ADB_ULLogProcessing','Succeeded','')
     
    except Exception as exp:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(exp)
        print(ExceptionTraceback)
        loadlogProcessesDeltaTable(df_ulLogs_CT_CNT,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_Source_ULLogsCT,dest_ULLogs,'ADB_ULLogProcessing','Failed',str(exp))
        
        # logIntoStreamLogTable(log_df,"ADB_ULLogProcessing","Failed",Microbatch_df,ErrorMessage)
        # streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        raise ErrorMessage             


# COMMAND ----------

def processULlogs(microBatchOutputDF,batchId):
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
                            #    | (col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
                               ))
                     
                    #  .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
                    #                                             ,"yyyy-MM-dd HH:mm:ss.SSS"))

                     .withColumn("LogStartDate",lit(None))
                     .withColumn("LogEndDate",lit(None))
                     .drop('SourceFileName_noext')
                     .fillna({'FileNameApplicatorPortCd':''})
                     
               )

    StartEndLog = df_logSource.filter(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
    StartEndDf=processStartAndEndParameters(StartEndLog)


    df_LogsULCOM3 = df_logSource.where("FileNameDeviceTypeCd='COM3'")
    df_LogsULCT = df_logSource.where("FileNameDeviceTypeCd='CT'")

    #print("........running for Com3 UL........!!")
    ProcessULLogs25003_25004(df_LogsULCOM3,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId)
    #print("........running for CT UL........!!")
    ProcessULLogs84005_84006(df_LogsULCT,StartEndDf,ulLogs_ConfigId,allLogs_ConfigId,batchId)
    
    spark.sql("clear cache")

# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):     
    log_Stream = {
        "ConfigID" : ulLogs_ConfigId,
        "SourceTypeID" : sourceTypeId,
        "Source" : "silverzone.factalllogs",
        "Destination" : "silverzone.factullogs",
        "Run_ID": str(batchId),
        "Job_ID": str(Job_id)
        }
    df_logstream = spark.createDataFrame([log_Stream])
    batchEnd(q,batchId)  
    print("Running for BatchID: {0}".format(batchId))

    df_logSourceReprocessed = spark.read.table("f"{CatalogName}".silverzone.fact_streamlogs").filter((col("ConfigId") == f'{ulLogs_ConfigId}') & (col("PipelineStatus") == 'Failed') & (col("ReprocessStatus") == '')) #trim status
    if len(df_logSourceReprocessed.head(1)) >= 1:
        try:
            print("Reprocess UL Logs :")
            setWorkFlowStatus_StreamLog(df_logSourceReprocessed,"InProgress")

            df_logSource = spark.read.table("f"{CatalogName}".silverzone.fact_streamlogs").filter((col("ConfigId") == f'{ulLogs_ConfigId}') & (col("ReprocessStatus") == 'InProgress'))
            df_schema = spark.read.table("f"{CatalogName}".silverzone.factalllogs").limit(1)
            alllogs_schema = df_schema.schema

            df_flattenlogs = df_logSource.withColumn("flattern_batch",from_json(col("MicroBatchData"),alllogs_schema)).select(col("flattern_batch.*"))
            
            processULlogs(df_flattenlogs,batchId)
            setWorkFlowStatus_StreamLog(df_logSource,"Success")
        except Exception as exp:
            ExceptionTraceback = traceback.format_exc()
            ErrorMessage = ExceptionTraceback + str(exp)
            print(ExceptionTraceback)
            logIntoStreamLogTable(df_logSourceReprocessed,"ADB_SysLogProcessing","Failed",None,ErrorMessage)
            streamLogEmailNotification(EmailNotificationID,df_logSourceReprocessed, pipelinename, Env)
    
    try:
        print("Process UL Logs:")
        processULlogs(microBatchOutputDF,batchId)
    except Exception as exp:
        print(str(exp))
        logIntoStreamLogTable(df_logSourceReprocessed,"ADB_SysLogProcessing","Failed",None,exp)
        streamLogEmailNotification(EmailNotificationID,df_logSourceReprocessed, pipelinename, Env)

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming job to process log data

# COMMAND ----------

q=(df_Source.writeStream
                  .format("delta")
                  .queryName("V2_Transformation_COM3_CT_ULLogs_Stream")
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

# df_Source_batch = (spark.read.format("delta").load(sourceFilePath).where(" (SourceFileName = 'COM3_D012021319013_UL_20230303173532_A_04.stx' or SourceFileName ='CT_Unkonwn_UL_20220407084704_A_33.stx') and (updateddt = '2023-08-21 06:50:46.690' or updateddt = '2023-08-18 12:29:30.948') "))
# df_Source_batch.persist()
# df_Source_batch.display()

# COMMAND ----------

# upsertToDelta(df_Source_batch, 1)
