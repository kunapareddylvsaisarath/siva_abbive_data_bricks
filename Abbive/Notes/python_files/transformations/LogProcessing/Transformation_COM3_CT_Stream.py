# Databricks notebook source
# MAGIC %run ../../Configurations/Init_Scripts

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize Functions

# COMMAND ----------

from datetime import datetime
import json
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.window import Window
from datetime import datetime

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import base64 as b64

from cryptography.hazmat.primitives.serialization import pkcs12
from azure.identity import *
from azure.keyvault.secrets import SecretClient
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption

# COMMAND ----------

# MAGIC %md
# MAGIC # Define FilePath variables and widgets

# COMMAND ----------

checkPointLocation = "/_checkpoints/"
schemaLocation = "/_schema/"

dbutils.widgets.text('deltaTablePath_AllLogs','/mnt/silver/FACTAllLogs')
deltaTablePath_AllLogs = dbutils.widgets.get('deltaTablePath_AllLogs')

dbutils.widgets.text('deltatablename_AllLogs','silverzone.FACTAllLogs')
deltatablename_AllLogs = dbutils.widgets.get('deltatablename_AllLogs')


dbutils.widgets.text('sourceFilePath','/mnt/raw/DeviceLogs/{COM3,CT}/*/*/*/*/')
sourceFilePath = dbutils.widgets.get('sourceFilePath')

dbutils.widgets.text('queueName','com3logprocess-queue')
queueName = dbutils.widgets.get('queueName')

dbutils.widgets.text('startEndParmater_ConfigId','21')
startEndParmater_ConfigId = dbutils.widgets.get('startEndParmater_ConfigId')

dbutils.widgets.text('allLogs_ConfigId','20')
allLogs_ConfigId = dbutils.widgets.get('allLogs_ConfigId')

dbutils.widgets.text('sysLogs_ConfigId','19')
sysLogs_ConfigId = dbutils.widgets.get('sysLogs_ConfigId')

dbutils.widgets.text('ueLogs_ConfigId','18')
ueLogs_ConfigId = dbutils.widgets.get('ueLogs_ConfigId')

dbutils.widgets.text('ulLogs_ConfigId','17')
ulLogs_ConfigId = dbutils.widgets.get('ulLogs_ConfigId')

dbutils.widgets.text('sourceTypeId','3')
sourceTypeId = dbutils.widgets.get('sourceTypeId')

subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
QueueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")

vault_url = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-KeyVaultURL")

logTypes_Processed = ['UL','UE','SYS','ENGR'] 
contractNumberList_UE = ["21544","21544:1","21544:2","21543","21543:1","21543:2","21519","84009"]
contractNumberList_StartEnd = ["14020","14023"]
contractNumberList_UL_COM3 = ["25003","25003:1","25003:2","25004","25004:1","25004:2"]
contractNumberList_UL_CT=["84005","84006","84005:1","84006:1"]
contractNumberList_SysLogs = ['21062','15024','15039','15008']
contractNumberList_Processed = ["21544","21544:1","21544:2","21543","21543:1","21543:2","21519","84009","25003","25003:1","25003:2","25004","25004:1","25004:2","84005","84006","84005:1","84006:1",'21062','15024','15039','15008']



# COMMAND ----------

# Source path:
startEndParameterPath = '/mnt/silver/DIMStartEndParameter'
logFilesProcessedPath = '/mnt/silver/LogSourceFilesProcessed/'
ueLogsPath = '/mnt/silver/FACTUELogs/'
sysLogsPath = '/mnt/silver/FACTSysLogs/'

dest_ULLogs = '/mnt/silver/FACTULLogs/'
equip_mstr_path = '/mnt/silver/DIMEquipmentMaster'

CycleMLTLogAssoc = '/mnt/silver/DIMCycleMLTLogAssociation/'
CycleSysLogAssoc = '/mnt/silver/DIMCycleSYSLogAssociation/'
CycleMLLogAssoc = '/mnt/silver/DIMCycleMLLogAssociation/'
CycleAssertLogAssoc = '/mnt/silver/DIMCycleAssertLogAssociation/'
CycleUELogAssoc = '/mnt/silver/DIMCycleUELogAssociation/'
CycleLogAssoc = '/mnt/silver/DIMCycleLogAssociation/'
ApplicatorVerCd = '/mnt/silver/DIMApplicatorVerCd/'


certificate_name = "ZeltiqAbbVie20200716"
cycleIDSequence = 3000000





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

UESchema = StructType(
    [StructField('date', StringType(), True),
     StructField('time', StringType(), True),
 	 StructField('tcode', StringType(), True),
     StructField('adddata', StringType(), True),
     StructField('ZCodeMajor', StringType(), True),
     StructField('ZCodeMinor', StringType(), True),
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
         
Schema_Sys = StructType(
    [StructField('value', StringType(), False),
     StructField('imei', StringType(), True),
     StructField('imsi', StringType(), True),
 	 StructField('net', StringType(), True),
     StructField('lac', StringType(), True),
     StructField('id', StringType(), True),
     StructField('cresp', StringType(), True),
     StructField('scresp',StringType(), True),
     StructField('ccode', StringType(), True),
	 StructField('opcode', StringType(), True),
     StructField('intSerialNum', StringType(), True),
     StructField('devId', StringType(), True),
 	 StructField('iotHub', StringType(), True),
     StructField('idScope', StringType(), True),
     StructField('endPoint', StringType(), True),
     StructField('tpm', StringType(), True),
     StructField('status', StringType(), True),
     StructField('statusString', StringType(), True),
 	 StructField('simId', StringType(), True),
     StructField('qual', IntegerType(), True),
     StructField('rssi', IntegerType(), True),
     StructField('ber', StringType(), True),
     StructField('prodId',IntegerType(), True),
     StructField('psn', StringType(),  True),
     StructField('initMS', StringType(), True),
     StructField('connectMS',IntegerType(), True),
     StructField('connectError', StringType(),  True),
     StructField('modemInit', StringType(), True),
     StructField('usemodem',IntegerType(), True),
     StructField('error', StringType(),  True),
     StructField('con',IntegerType(), True),
     StructField('discon', StringType(),  True)
    ])         

# Defining Schema:
# Schema for contract 25003
Schema_25003 = StructType([
    StructField("sbcVer", StringType(), False),
    StructField("pibTecVer", StringType(), False),
    StructField("pibVer", StringType(), False),
    StructField("appConfig", StringType(), False),
    StructField("blob0CRC", StringType(), False),
    StructField("blob1CRC", StringType(), False),
    StructField("pibTecFactoryCRC", StringType(), False),
    StructField("pibFactoryCRC", StringType(), False),
    StructField("curCnt", StringType(), False),
    StructField("appSN", StringType(), False),
    StructField("appSPSN", StringType(), False),
    StructField("externalSN", StringType(), False),
    StructField("sbcInternal", StringType(), False),
    StructField("pibSPSN", StringType(), False),
    StructField("cardPN", StringType(), False),
    StructField("cardSPSN", StringType(), False),
    StructField("appSPAppletVer", StringType(), False),
    StructField("cardSPAppletVer", StringType(), False),
    StructField("pibSPAppletVer", StringType(), False),
    StructField("profileIndex", StringType(), False),
    StructField("profileTreatmentTime", StringType(), False),
    StructField("profileTreatmentTemperature", StringType(), False),
    StructField("controlChannels", StringType(), False),
    StructField("sameNextPat", StringType(), False),
    StructField("patType", StringType(), False),
    StructField("bodyPart", StringType(), False),
    StructField("newPatient", StringType(), False),
    StructField("history", StringType(), False),
    StructField("bd", StringType(), False),
    StructField("isBDNumberEncrypted", StringType(), False),
    StructField("alleCertDate", StringType(), False),
    StructField("MLTFile", StringType(), False),
    StructField("start", StringType(), False)
])

# Schema for contract 25004

Schema_25004 = StructType([
    StructField("status", StringType(), False),
    StructField("zcode", StringType(), False),
    StructField("completion", StringType(), False)
])


# Schema for contract 84005

Schema_84005 = StructType([
    StructField("date", StringType(), False),
    StructField("time", StringType(), False),
    StructField("swvers", StringType(), False),
    StructField("remtrtcnt", StringType(), False),
    StructField("pattreat", StringType(), False),
    StructField("patreturn", StringType(), False),
    StructField("patgender", StringType(), False),
    StructField("trtbody", StringType(), False),
    StructField("app1sn", StringType(), False),
    StructField("app2sn", StringType(), False),
    StructField("prevctcnt", StringType(), False),
    StructField("bdsn", StringType(), False),
    StructField("scsn", StringType(), False),
    StructField("chosenmin", StringType(), False),
    StructField("trtproto", StringType(), False),
    StructField("alleCertDate", StringType(), False)
])

# Schema for contract 84006

Schema_84006 = StructType([
    StructField("date", StringType(), False),
    StructField("time", StringType(), False),
    StructField("status", StringType(), False),
    StructField("tcode", StringType(), False),
    StructField("maxcooltemp", StringType(), False),
    StructField("maxapp1temp", StringType(), False),
    StructField("maxapp2temp", StringType(), False),
    StructField("tottrt", StringType(), False)
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
                     col('SourceFilePath'),col('SourceFileSize'),col('file_modification_time')]

# parsing contract no. 14023:
parsed_14023_cols = [col('FileNameUUID'),col('HdrDataContractNbr').alias('HdrDataContractNbr_EndParameter'),col('endts').alias('EventEndTmstmp'),col('SourceFilePath'),col('HdrTimeGeneratedTmstmp')]

parsed_UE_cols = ['FileNameUUID','ExternalSerialNbr','InternalSerialNbr','HdrAppHeadNbr','HdrDateGeneratedDt', 'HdrFormatVersionCd', 'HdrTimeGeneratedTmstmp', 
                  'HdrLogtypeCd', 'HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCD', 'HdrDataStoreID', 'HdrDataContractNbr', 'SourceFileName',
                  'SourceFileSize','SourceFilePath','FileNameDeviceTypeCd',
                  col('ZCodeMajor').alias('ZCodeMajorCd'),
                  col('ZCodeMinor').alias('ZCodeMinorCd'),
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
                  col('adddata').alias('ErrorDesc')]

parsed_SYS_cols = ['FileNameUUID','ExternalSerialNbr','InternalSerialNbr','FileNameDeviceTypeCd','HdrDateGeneratedDt', 'HdrTimeGeneratedTmstmp',
                   'HdrDataContractNbr','HdrFormatVersionCd','HdrLogTypeCd','HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCd',
                   'HdrDataStoreID','HdrAppHeadNbr','SourceFileName',                    
                   'CreatedBy','CreatedDt','UpdatedBy','UpdatedDt','SourceFileSize',
                #21062-software version schema
                col('value').alias('LatestSoftwareVersionNbr'),
                #15024 Mobile data Processing Schema
                col('imei').alias('IntMobileEquipID'),
                col('imsi').alias('IntMobileSubscriberID'),
                col('net').alias('ProviderNetworkNm'),
                col('lac').alias('LocalizationAreaCd'),
                col('id').alias('CellID'),
                col('cresp').alias('CrespID'),
                col('scresp').alias('ScrespID'),
                col('ccode').alias('CountryNbrCd'),
                col('opcode').alias('OperatorCd'),
                #15039 TPM Key Processing
                col('intSerialNum').alias('CoolDeviceSerialNbr'),
                col('iotHub').alias('IoTHubNm'),
                col('endPoint').alias('EndPointNm'),
                col('tpm').alias('TPMKey'),
                #15008 Modem Processing
                col('status').alias('ModemStatusCd'),
                col('statusString').alias('ModemStatusDesc'),
                col('simId').alias('SIMCardID')
                ]

Cols_25003 = [col('HdrFormatVersionCd'),
              col('HdrDateGeneratedDt').alias('HdrStartDateGeneratedDt'),
              col('HdrTimeGeneratedTmstmp').alias('HdrStartTimeGeneratedTmstmp'),
              col('HdrLogtypeCd'),
              col('HdrDestinationSubSystemCd'),
              col('HdrSourceSubSystemCd'),
              col('HdrCommandCD'),
              col('HdrDataStoreID'),
              col('HdrDataContractNbr'),
              col('HdrAppHeadNbr'),
              col('JSONField'),
              col('sbcVer').alias('SBCVersionNbr'),
              col('pibTecVer').alias('PIBTecVerNbr'),
              col('pibVer').alias('PibVersionNbr'),
              col('appConfig').alias('ApplicatorConfigurantionNbr'),
              col('blob0CRC').alias('Blob0CRCCd'),
              col('blob1CRC').alias('Blob1CRCCd'),
              col('pibTecFactoryCRC').alias('PIBTecFactoryCRC'),
              col('pibFactoryCRC').alias('PibFactoryCRC'),
              col('curCnt').alias('CurrentCycleNbr'),
              col('appSN').alias('ApplicatorSerialNbr'),
              col('appSPSN').alias('ApplicatorInternalSerialNbr'),
              col('externalSN').alias('DeviceSerialNbr'),
              col('sbcInternal').alias('SBCSerialNbr'),
              col('pibSPSN').alias('PibInternalSerialNbr'),
              col('cardPN').alias('CardPartNbr'),
              trim(upper(col('cardSPSN'))).alias('CardSPSerialNbr'),
              col('appSPAppletVer').alias('AppSPAppletVerNbr'),
              col('cardSPAppletVer').alias('CardSPAppletVerNbr'),
              col('pibSPAppletVer').alias('PIBSPAppletVerNbr'),
              col('profileIndex').alias('ContourProfileNbr'),
              col('profileTreatmentTime').alias('ContourProfileTreatmentTm'),
              col('profileTreatmentTemperature').alias('ContourProfileTreatmentTemp'),
              col('controlChannels').alias('controlChannelCd'),
              col('sameNextPat').alias('PatientNextSameFlg'),
              col('patType').alias('PatientGenderCd'),
              col('bodyPart').alias('PatientBodyPartNm'),
              col('newPatient').alias('PatientNewFlg'),
              col('history').alias('History'),col('bd').alias('BrilliantDistinctionID'),
              col('isBDNumberEncrypted').alias('BDIDEncryptdFlg'),
              col('alleCertDate').alias('AlleCertDt'),
              col('MLTFile').alias('MsrmntLogTreatmentFileNm'),
              col('start').alias('StartFlg'),
              col('ExternalSerialNbr'),
              col('InternalSerialNbr'),
              col("FileNameCycleNbr"),
              col('SourceFileName'),
              col('SourceFilePath'),
              col('FileNameUUID'),
              col('file_modification_time'),
              col('SourceFileSize'),
              col('FileNameDeviceTypeCd')
             ]
            
Cols_25004 = [col('status').alias('CycleFinalStatusReasonDesc'),
              col('zcode').alias('CycleErrorZCd'),
              col('completion').alias('CycleFinalStatusDesc'),
              col('SourceFilePath'),
              col('FileNameUUID')]

Cols_84005 = [col('HdrFormatVersionCd'),
              col('HdrDateGeneratedDt').alias('HdrStartDateGeneratedDt'),
              col('HdrTimeGeneratedTmstmp').alias('HdrStartTimeGeneratedTmstmp'),
              col('HdrLogtypeCd'),
              col('HdrDestinationSubSystemCd'),
              col('HdrSourceSubSystemCd'),
              col('HdrCommandCD'),
              col('HdrDataStoreID'),
              col('HdrDataContractNbr'),
              col('HdrAppHeadNbr'),
              col('JSONField'),
              col('date').alias('CycleStartDt'),
              col('time').alias('CycleStartTm'),
              col('swvers').alias('SwVersionNbr'),
              col('remtrtcnt').alias('CurrentCycleNbr'),
              col('pattreat').alias('PatientNextSameFlg'),
              col('patreturn').alias('PatientNewFlg'),
              col('patgender').alias('PatientGenderCd'),
              col('trtbody').alias('PatientBodyPartNm'),
              col('app1sn').alias('Applicator1SerialNbr'),
              col('app2sn').alias('Applicator2SerialNbr'),
              col('prevctcnt').alias('PrevCTCnt'),
              col('bdsn').alias('BrilliantDistinctionID'),
              trim(upper(col('scsn'))).alias('CardSPSerialNbr'),
              col('chosenmin').alias('ChsnTrtmntDurationTm'),
              col('trtproto').alias('TrtmntPrtclCd'),
              col('alleCertDate').alias('AlleCertDt'),
              col('ExternalSerialNbr'),
              col('InternalSerialNbr'),
              col("FileNameCycleNbr"),
              col('SourceFileName'),
              col('SourceFilePath'),
              col('FileNameUUID'),
              col('file_modification_time'),col('SourceFileSize'),
              col('FileNameDeviceTypeCd')
            ]

Cols_84006 = [col('JSONField'),
              col('date').alias('CycleEndDt'),
              col('time').alias('CycleEndTm'),
              col('status').alias('CycleFinalStatusReasonDesc'),
              col('tcode').alias('CycleErrorTCd'),
              col('maxcooltemp').alias('MaxCoolTmp'),
              col('maxapp1temp').alias('MaxApplicator1Tmp'),
              col('maxapp2temp').alias('MaxApplicator2Tmp'),
              col('tottrt').alias('TtlTrtmntLngthMinNbr'),
              col('SourceFilePath'),
              col('FileNameUUID')
             ]



# COMMAND ----------

# MAGIC %md
# MAGIC # Read Source File and Autoloader settings

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
                              .option("cloudFiles.backfillInterval",'1 day')             
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
                                          col("_metadata.file_modification_time").alias('file_modification_time'),
                                          col("_metadata.file_size").alias('SourceFileSize')
                                     ) 
                              .withColumn('JSONField',concat(lit('{'),col('JSONField')))
                              .withColumn('SourceFilePath', regexp_replace('SourceFilePath','/mnt/raw/',''))
                              .withColumn('SourceFileName', regexp_replace('SourceFileName','%20',''))
                              .withColumn("HdrTimeGeneratedTmstmp",to_timestamp(concat("HdrDateGeneratedDt",lit(" "),"HdrTimeGeneratedTmstmp")
                                                                          ,"yyyy-MM-dd HH:mm:ss.SSS"))
                    )

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
# MAGIC ## Process ul logs for COM3 25003 and 25004

# COMMAND ----------

def ProcessULLogs25003_25004(df_Source_ULLogs,ulLogs_ConfigId,allLogs_ConfigId):
    df_ulLogs_CNT = (df_Source_ULLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                       'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath',
                                       'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId')
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
#     df_ueLogs_CNT.display()
    df_ulLogs_CNT_IngestionLog = df_ulLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_ulLogs_CNT_LogsProcessed = df_ulLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(allLogs_ConfigId))

    try:
        loadAuditTables_Ingestion_Log(df_ulLogs_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','InProgress','')
        
        
        df_cycles = spark.read.format('delta').load(dest_ULLogs)
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
        df_start_end_raw = (spark.read.format("delta")
                            .load(startEndParameterPath)
                            .select('FileNameUUID','EventStartTmstmp','EventEndTmstmp'))

        lst=["25003","25003:1","25003:2","25004","25004:1","25004:2"]
            
        # Parse contract 25003
        df_raw_ULLogs_25003 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('25003','25003:1','25003:2'))
                               .withColumn("JSONField", from_json("JSONField", Schema_25003))
                               .select('*', col('JSONField.*'))
                               .select(Cols_25003).drop('JSONField')
                              )
        
        # Parse contract 25004
        df_raw_ULLogs_25004 = (df_Source_ULLogs.filter(col("HdrDataContractNbr").isin('25004','25004:1','25004:2'))
                               .withColumn("JSONField", from_json("JSONField", Schema_25004))
                               .select('*', col('JSONField.*'))
                               .select(Cols_25004).drop('JSONField')
                              )

        # Join Cycle Start and End data
        df_ULLog = (df_raw_ULLogs_25003.alias('src').join(df_raw_ULLogs_25004.alias('tgt'),['SourceFilePath','FileNameUUID'],'left')
                    .withColumn('CompletedCycleUsedCnt',when(col("CycleFinalStatusDesc")=="Completed",1).otherwise(0))
                    .withColumn("CurrentCycleNbr",col("CurrentCycleNbr").cast("int"))
                    .withColumn('IsNewData',lit('Yes'))
                   )

        CardSPSerialNbr=df_ULLog.select("CardSPSerialNbr").toPandas()['CardSPSerialNbr']
        listCardSPSerialNbr=list(CardSPSerialNbr)
        
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
                                            'FileNameCycleNbr','FileNameMessageTypeCd','FileNameApplicatorPortCd','SBCSerialNbr'
                                         )
                          )

        # Identify fraud cycles
        columnList = ["CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","ExternalSerialNbr","InternalSerialNbr"]
        
        df_cycles_Card = df_cycles.filter(col('CardSPSerialNbr').isin(listCardSPSerialNbr)).withColumn('IsNewData',lit('No')).select(columnList)
        df_Cycles_IdentifyFraud = df_cycles_Card.unionByName(df_ULLog_EQMstr,allowMissingColumns=True)
        
        UnqCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr")
                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt")
                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCyclUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","ExternalSerialNbr","InternalSerialNbr")
                           .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCyclDateUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","ExternalSerialNbr","InternalSerialNbr")
                               .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time","ExternalSerialNbr"))

        df_Cycles_IdentifyFraudInd = (df_Cycles_IdentifyFraud.withColumn("UnqCyclSeq", row_number().over(UnqCycl_Spec))
                                                  .withColumn("DupCyclDateSeq", row_number().over(DupCycl_Spec))
                                                  .withColumn("DupCyclUPMSeq", row_number().over(DupCyclUPM_Spec))
                                                  .withColumn("DupCyclDateUPMSeq", row_number().over(DupCyclDateUPM_Spec)))
        
        df_Cycles_PopulateFraudInd = (df_Cycles_IdentifyFraudInd
                                         .withColumn("UnsoldFraudFlg",when((col('SmartCardShippedInd') + col('CoolDeviceShippedInd'))== 2,"N").otherwise("Y"))
                                         .withColumn("DuplicateCycleFlg",when(col('UnqCyclSeq')== 1,"N").otherwise("Y"))
                                         .withColumn("CycleUtilizationFraudFlg",when((col('UnqCyclSeq') + col('DupCyclDateUPMSeq'))== 2,"N").otherwise("Y"))
                                         .withColumn("CycleUsedCnt",when(col('DupCyclDateUPMSeq')== 1,1).otherwise(0))
                                         .withColumn("IsFraudCycleFlg",when((col("UnsoldFraudFlg")=="N") & (col("CycleUtilizationFraudFlg")=="N"),"N").otherwise("Y"))
                                     )
        
        df_AllCycles_AllFlags = (df_Cycles_PopulateFraudInd.alias("dcp").filter('IsNewData = "Yes"')
                            .join(df_MLTFiles.alias("MLTFile"),col("MLTFile.MLTSourceFileName") == col("dcp.MsrmntLogTreatmentFileNm"),'left')
                            .join(df_start_end_raw.alias("StEnd"),'FileNameUUID','left')
                            .withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('CreatedBy',lit('ADB_LogProcessingMaster'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_LogProcessingMaster'))
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

        Deltatbl_UL_Logs = DeltaTable.forPath(spark, dest_ULLogs)
        (Deltatbl_UL_Logs.alias("tgt")
                .merge(df_AllCycles_CycleId.alias("src"),
                       "tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.CardSPSerialNbr = src.CardSPSerialNbr and tgt.CurrentCycleNbr = src.CurrentCycleNbr and tgt.HdrStartTimeGeneratedTmstmp = src.HdrStartTimeGeneratedTmstmp"
                      )
                      .whenNotMatchedInsert(values =
                      {
                        "tgt.CycleID" : "src.CycleID", 
                        "tgt.HdrFormatVersionCd" : "src.HdrFormatVersionCd",
                        "tgt.HdrStartDateGeneratedDt" : "src.HdrStartDateGeneratedDt",
                        "tgt.HdrStartTimeGeneratedTmstmp" : "src.HdrStartTimeGeneratedTmstmp",
                        "tgt.HdrLogtypeCd" : "src.HdrLogtypeCd",
                        "tgt.HdrAppHeadNbr" : "src.HdrAppHeadNbr",
                        "tgt.HdrDestinationSubSystemCd" : "src.HdrDestinationSubSystemCd",
                        "tgt.HdrSourceSubSystemCd" : "src.HdrSourceSubSystemCd",
                        "tgt.HdrCommandCD" : "src.HdrCommandCD",
                        "tgt.HdrDataStoreID" : "src.HdrDataStoreID",
                        "tgt.EventStartTmstmp" : "src.EventStartTmstmp",
                        "tgt.EventEndTmstmp" : "src.EventEndTmstmp",
                        "tgt.SBCVersionNbr" : "src.SBCVersionNbr",
                        "tgt.PibVersionNbr" : "src.PibVersionNbr",
                        "tgt.PibFactoryCRC" : "src.PibFactoryCRC",
                        "tgt.ApplicatorSerialNbr" : "src.ApplicatorSerialNbr",
                        "tgt.PibInternalSerialNbr" : "src.PibInternalSerialNbr",
                        "tgt.CardSPSerialNbr" : "src.CardSPSerialNbr",
                        "tgt.ContourProfileNbr" : "src.ContourProfileNbr",
                        "tgt.ContourProfileTreatmentTm" : "src.ContourProfileTreatmentTm",
                        "tgt.ContourProfileTreatmentTemp" : "src.ContourProfileTreatmentTemp",
                        "tgt.History" : "src.History",
                        "tgt.StartFlg" : "src.StartFlg",
                        "tgt.CycleErrorZCd" : "src.CycleErrorZCd",
                        "tgt.PIBTecVerNbr" : "src.PIBTecVerNbr",
                        "tgt.ApplicatorConfigurantionNbr" : "src.ApplicatorConfigurantionNbr",
                        "tgt.Blob0CRCCd" : "src.Blob0CRCCd",
                        "tgt.Blob1CRCCd" : "src.Blob1CRCCd",
                        "tgt.PIBTecFactoryCRC" : "src.PIBTecFactoryCRC",
                        "tgt.CurrentCycleNbr" : "src.CurrentCycleNbr",
                        "tgt.ApplicatorInternalSerialNbr" : "src.ApplicatorInternalSerialNbr",
                        "tgt.CardPartNbr" : "src.CardPartNbr",
                        "tgt.AppSPAppletVerNbr" : "src.AppSPAppletVerNbr",
                        "tgt.CardSPAppletVerNbr" : "src.CardSPAppletVerNbr",
                        "tgt.PIBSPAppletVerNbr" : "src.PIBSPAppletVerNbr",
                        "tgt.controlChannelCd" : "src.controlChannelCd",
                        "tgt.PatientNextSameFlg" : "src.PatientNextSameFlg",
                        "tgt.PatientGenderCd" : "src.PatientGenderCd",
                        "tgt.PatientBodyPartNm" : "src.PatientBodyPartNm",
                        "tgt.PatientNewFlg" : "src.PatientNewFlg",
                        "tgt.BrilliantDistinctionID" : "src.BrilliantDistinctionID",
                        "tgt.CoolSculptingID" : "src.CoolSculptingID",#Added
                        "tgt.BDIDEncryptdFlg" : "src.BDIDEncryptdFlg",
                        "tgt.AlleIDDecryptedFlg" : "src.AlleIDDecryptedFlg",#Added
                        "tgt.AlleCertDt" : "src.AlleCertDt",
                        "tgt.MsrmntLogTreatmentFileNm" : "src.MsrmntLogTreatmentFileNm",
                        "tgt.SourceFileName" : "src.SourceFileName",
                        "tgt.FileNameDeviceTypeCd" : "src.FileNameDeviceTypeCd",
                        "tgt.ExternalSerialNbr" : "src.ExternalSerialNbr",
                        "tgt.FileNameUUID" : "src.FileNameUUID",
                        "tgt.InternalSerialNbr" : "src.InternalSerialNbr",
                        "tgt.EQUIPCardPartNbr" : "src.EQUIPCardPartNbr",
                        "tgt.SmartCardShippedInd" : "src.SmartCardShippedInd",
                        "tgt.CoolDeviceShippedInd" : "src.CoolDeviceShippedInd",
                        "tgt.SoldToAccountID" : "src.SoldToAccountID",
                        "tgt.ShipToAccountID" : "src.ShipToAccountID",
                        "tgt.UnqCyclSeq" : "src.UnqCyclSeq",
                        "tgt.DupCyclDateSeq" : "src.DupCyclDateSeq",
                        "tgt.DupCyclUPMSeq" : "src.DupCyclUPMSeq",
                        "tgt.DupCyclDateUPMSeq" : "src.DupCyclDateUPMSeq",
                        "tgt.UnsoldFraudFlg" : "src.UnsoldFraudFlg",
                        "tgt.DuplicateCycleFlg" : "src.DuplicateCycleFlg",
                        "tgt.CycleUtilizationFraudFlg" : "src.CycleUtilizationFraudFlg",
                        "tgt.CycleUsedCnt" : "src.CycleUsedCnt",
                        "tgt.CycleFinalStatusReasonDesc" : "src.CycleFinalStatusReasonDesc",
                        "tgt.CycleFinalStatusDesc" : "src.CycleFinalStatusDesc",
                        "tgt.MsrmntLogTreatmentFileURLTxt" : "src.MsrmntLogTreatmentFileURLTxt",
                        "tgt.CompletedCycleUsedCnt" : "src.CompletedCycleUsedCnt",
                        "tgt.SmartCardShipDt" : "src.SmartCardShipDt",
                        "tgt.IsFraudCycleFlg" : "src.IsFraudCycleFlg",
                        "tgt.SourceFilePath" : "src.SourceFilePath",
                        "tgt.ULLogsUUID" : "src.ULLogsUUID",
                        "tgt.CreatedBy" : "src.CreatedBy",
                        "tgt.CreatedDt" : "src.CreatedDt",
                        "tgt.UpdatedBy" : "src.UpdatedBy",
                        "tgt.UpdatedDt" : "src.UpdatedDt"
                      }
                    ) 
          .execute()
        )
        
        loadlogProcessesDeltaTable(df_ulLogs_CNT_LogsProcessed,dest_ULLogs,'ADB_LogProcessingMaster','Succeeded','')
        loadAuditTables_Ingestion_Log(df_ulLogs_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','Succeeded','')        
    except Exception as exp:
        loadlogProcessesDeltaTable(df_ulLogs_CNT_LogsProcessed,dest_ULLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_ulLogs_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        raise            


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process ul logs for CT 84005 and 84006

# COMMAND ----------

def ProcessULLogs84005_84006(df_Source_ULLogsCT,ulLogs_ConfigId,allLogs_ConfigId):
    df_ulLogs_CT_CNT = (df_Source_ULLogsCT.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath',
                                'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId')
                .count()
                .withColumnRenamed("count","RecdCnt")                            
                )
    #     df_ueLogs_CNT.display()
    df_ulLogs_CT_CNT_IngestionLog = df_ulLogs_CT_CNT.drop('ConfigId').withColumn('ConfigId',lit(ulLogs_ConfigId))
    df_ulLogs_CT_CNT_LogsProcessed = df_ulLogs_CT_CNT.drop('ConfigId').withColumn('ConfigId',lit(allLogs_ConfigId))
    try:

        loadAuditTables_Ingestion_Log(df_ulLogs_CT_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','InProgress','')
        
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
        df_start_end_raw = (spark.read.format('delta').load(startEndParameterPath)
                            .select('FileNameUUID','EventStartTmstmp','EventEndTmstmp'))

        lst=["84005","84006","84005:1","84006:1"]

        #Parse Cycle Start Data
        df_raw_ULLogs_84005 = (df_Source_ULLogsCT.filter(col("HdrDataContractNbr").isin('84005','84005:1'))
                               .withColumn("JSONField", from_json("JSONField", Schema_84005))
                               .select('*', col('JSONField.*'))
                               .select(Cols_84005)
                               .drop('JSONField')
                              )
        #Parse Cycle End Data
        df_raw_ULLogs_84006 = (df_Source_ULLogsCT.filter(col('HdrDataContractNbr').isin('84006','84006:1'))
                               .withColumn("JSONField", from_json("JSONField", Schema_84006))
                               .select('*', col('JSONField.*'))
                               .select(Cols_84006)
                               .withColumn('TtlTrtmntDurationTm',col('TtlTrtmntLngthMinNbr').cast(StringType()))
                               .withColumn("CycleEndDt",col('CycleEndDt').cast('date'))
                               .withColumn("CycleEndTm",to_timestamp(concat("CycleEndDt",lit(" "),"CycleEndTm")
                                                                          ,"yyyy-MM-dd HH:mm:ss"))
                               .drop('JSONField')
                              )
        #Combine both Cycle start and cycle end data
        df_ULLog = (df_raw_ULLogs_84005.join(df_raw_ULLogs_84006,['SourceFilePath','FileNameUUID'],'left')
                    .withColumn('CompletedCycleUsedCnt',lit(0))
                    .withColumn("CurrentCycleNbr",col("CurrentCycleNbr").cast("int"))
                    .withColumn("InterruptStart",to_timestamp(concat("CycleStartDt",lit(" "),"CycleStartTm")
                                                                          ,"yyyy-MM-dd HH:mm:ss"))
                    .withColumn('IsNewData',lit('Yes'))
                   )
        CardSPSerialNbr=df_ULLog.select("CardSPSerialNbr").toPandas()['CardSPSerialNbr']
        listCardSPSerialNbr=list(CardSPSerialNbr)
        
        #Get SmartCardShippedInd and CoolDeviceShippedInd use ExternalSerialNbr
        CardRowNumWindow = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","CardSPSerialNbr","CurrentCycleNbr","HdrStartTimeGeneratedTmstmp").orderBy(desc(col('DEVICE.ShipEndDt')),desc(col('DEVICE.ShipStartDt')),desc(col('CARD.ShipEndDt')),desc(col('CARD.ShipStartDt'))))
        df_ULLog_EQMstr = (broadcast(df_ULLog).alias("CYC")
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
                                            'FileNameCycleNbr','FileNameMessageTypeCd','FileNameApplicatorPortCd','SBCSerialNbr'
                                          
                                         )
                          )

        # Identify Fraud checks
        columnList = ["CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","ExternalSerialNbr","InternalSerialNbr"]
        df_cycles_Card = df_cycles.filter(col('CardSPSerialNbr').isin(listCardSPSerialNbr)).withColumn('IsNewData',lit('No')).select(columnList)
        df_Cycles_IdentifyFraud = df_cycles_Card.unionByName(df_ULLog_EQMstr,allowMissingColumns=True)
        
        UnqCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr")
                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCycl_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt")
                        .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCyclUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","ExternalSerialNbr","InternalSerialNbr")
                           .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time"))
        DupCyclDateUPM_Spec = (Window.partitionBy("CardSPSerialNbr","CurrentCycleNbr","HdrAppHeadNbr","HdrStartDateGeneratedDt","ExternalSerialNbr","InternalSerialNbr")
                               .orderBy("HdrStartDateGeneratedDt","HdrStartTimeGeneratedTmstmp","file_modification_time","ExternalSerialNbr"))

        df_Cycles_IdentifyFraudInd = (df_Cycles_IdentifyFraud.withColumn("UnqCyclSeq", row_number().over(UnqCycl_Spec))
                                                  .withColumn("DupCyclDateSeq", row_number().over(DupCycl_Spec))
                                                  .withColumn("DupCyclUPMSeq", row_number().over(DupCyclUPM_Spec))
                                                  .withColumn("DupCyclDateUPMSeq", row_number().over(DupCyclDateUPM_Spec)))
        
        df_Cycles_PopulateFraudInd = (df_Cycles_IdentifyFraudInd.withColumn("UnsoldFraudFlg",when((col('SmartCardShippedInd') + col('CoolDeviceShippedInd'))== 2,"N").otherwise("Y"))
                                         .withColumn("DuplicateCycleFlg",when(col('UnqCyclSeq')== 1,"N").otherwise("Y"))
                                         .withColumn("CycleUtilizationFraudFlg",when((col('UnqCyclSeq') + col('DupCyclDateUPMSeq'))== 2,"N").otherwise("Y"))
                                         .withColumn("CycleUsedCnt",when(col('DupCyclDateUPMSeq')== 1,1).otherwise(0))
                                         .withColumn("IsFraudCycleFlg",when((col("UnsoldFraudFlg")=="N") & (col("CycleUtilizationFraudFlg")=="N"),"N").otherwise("Y"))
                                     )
        
        df_AllCycles = (df_Cycles_PopulateFraudInd.alias("dcp").filter('IsNewData = "Yes"')
                            .join(df_start_end_raw.alias("StEnd"),'FileNameUUID','left')
                            .withColumnRenamed("CycleErrorTCd","CycleErrorCd")
                            .withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('CreatedBy',lit('ADB_LogProcessingMaster'))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit('ADB_LogProcessingMaster'))
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
        
        #display(df_AllCycles)
        Deltatbl_UL_Logs = DeltaTable.forPath(spark, dest_ULLogs)
        (Deltatbl_UL_Logs.alias("tgt")
                .merge(df_AllCycles_CycleId.alias("src"),
                    "tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.CardSPSerialNbr = src.CardSPSerialNbr and tgt.CurrentCycleNbr = src.CurrentCycleNbr and tgt.HdrStartTimeGeneratedTmstmp = src.HdrStartTimeGeneratedTmstmp")
                      .whenNotMatchedInsert(values =
                      {
                        "tgt.CycleID" : "src.CycleID", #added
                        "tgt.HdrFormatVersionCd" : "src.HdrFormatVersionCd",
                        "tgt.HdrStartDateGeneratedDt" : "src.HdrStartDateGeneratedDt",
                        "tgt.HdrStartTimeGeneratedTmstmp" : "src.HdrStartTimeGeneratedTmstmp",
                        "tgt.HdrLogtypeCd" : "src.HdrLogtypeCd",
                        "tgt.HdrAppHeadNbr" : "src.HdrAppHeadNbr",
                        "tgt.HdrDestinationSubSystemCd" : "src.HdrDestinationSubSystemCd",
                        "tgt.HdrSourceSubSystemCd" : "src.HdrSourceSubSystemCd",
                        "tgt.HdrCommandCD" : "src.HdrCommandCD",
                        "tgt.HdrDataStoreID" : "src.HdrDataStoreID",
                        "tgt.EventStartTmstmp" : "src.EventStartTmstmp",
                        "tgt.EventEndTmstmp" : "src.EventEndTmstmp",
                        "tgt.CardSPSerialNbr" : "src.CardSPSerialNbr",
                        "tgt.PrevCTCnt" : "src.PrevCTCnt",
                        "tgt.CurrentCycleNbr" : "src.CurrentCycleNbr",
                        "tgt.CardSPSerialNbr":"src.CardSPSerialNbr",
                        "tgt.PatientNextSameFlg" : "src.PatientNextSameFlg",
                        "tgt.PatientGenderCd" : "src.PatientGenderCd",
                        "tgt.PatientBodyPartNm" : "src.PatientBodyPartNm",
                        "tgt.PatientNewFlg" : "src.PatientNewFlg",
                        "tgt.BrilliantDistinctionID" : "src.BrilliantDistinctionID",
                        "tgt.CoolSculptingID" : "src.CoolSculptingID", #added
                        "tgt.BDIDEncryptdFlg" : "src.BDIDEncryptdFlg",#added
                        "tgt.AlleIDDecryptedFlg" : "src.AlleIDDecryptedFlg",#added
                        "tgt.AlleCertDt" : "src.AlleCertDt",
                        "tgt.SourceFileName" : "src.SourceFileName",
                        "tgt.FileNameDeviceTypeCd" : "src.FileNameDeviceTypeCd",
                        "tgt.ExternalSerialNbr" : "src.ExternalSerialNbr",
                        "tgt.FileNameUUID" : "src.FileNameUUID",
                        "tgt.InternalSerialNbr" : "src.InternalSerialNbr",
                        "tgt.CycleStartDt" : "src.CycleStartDt",
                        "tgt.CycleStartTm" : "src.CycleStartTm",
                        "tgt.InterruptStart" : "src.InterruptStart", #added
                        "tgt.SwVersionNbr" : "src.SwVersionNbr",
                        "tgt.Applicator1SerialNbr" : "src.Applicator1SerialNbr",
                        "tgt.Applicator2SerialNbr" : "src.Applicator2SerialNbr",
                        "tgt.ChsnTrtmntDurationTm" : "src.ChsnTrtmntDurationTm",
                        "tgt.TrtmntPrtclCd" : "src.TrtmntPrtclCd",
                        "tgt.EQUIPCardPartNbr" : "src.EQUIPCardPartNbr",
                        "tgt.SmartCardShippedInd" : "src.SmartCardShippedInd",
                        "tgt.CoolDeviceShippedInd" : "src.CoolDeviceShippedInd",
                        "tgt.SoldToAccountID" : "src.SoldToAccountID",
                        "tgt.ShipToAccountID" : "src.ShipToAccountID",
                        "tgt.UnqCyclSeq" : "src.UnqCyclSeq",
                        "tgt.DupCyclDateSeq" : "src.DupCyclDateSeq",
                        "tgt.DupCyclUPMSeq" : "src.DupCyclUPMSeq",
                        "tgt.DupCyclDateUPMSeq" : "src.DupCyclDateUPMSeq",
                        "tgt.UnsoldFraudFlg" : "src.UnsoldFraudFlg",
                        "tgt.DuplicateCycleFlg" : "src.DuplicateCycleFlg",
                        "tgt.CycleUtilizationFraudFlg" : "src.CycleUtilizationFraudFlg",
                        "tgt.CycleUsedCnt" : "src.CycleUsedCnt",
                        "tgt.CycleFinalStatusReasonDesc" : "src.CycleFinalStatusReasonDesc",
                        "tgt.CycleEndDt" : "src.CycleEndDt",
                        "tgt.CycleEndTm" : "src.CycleEndTm",
                        "tgt.CycleErrorCd" : "src.CycleErrorCd",
                        "tgt.MaxCoolTmp" : "src.MaxCoolTmp",
                        "tgt.MaxApplicator1Tmp" : "src.MaxApplicator1Tmp",
                        "tgt.MaxApplicator2Tmp" : "src.MaxApplicator2Tmp",
                        "tgt.TtlTrtmntDurationTm" : "src.TtlTrtmntDurationTm",
                        "tgt.CompletedCycleUsedCnt" : "src.CompletedCycleUsedCnt",
                        "tgt.SmartCardShipDt" : "src.SmartCardShipDt",
                        "tgt.IsFraudCycleFlg" : "src.IsFraudCycleFlg",
                        "tgt.SourceFilePath" : "src.SourceFilePath",
                        "tgt.ULLogsUUID" : "src.ULLogsUUID",
                        "tgt.CreatedBy" : "src.CreatedBy",
                        "tgt.CreatedDt" : "src.CreatedDt",
                        "tgt.UpdatedBy" : "src.UpdatedBy",
                        "tgt.UpdatedDt" : "src.UpdatedDt"
                      }
                    ) 
          .execute()
        )
        
        loadlogProcessesDeltaTable(df_ulLogs_CT_CNT_LogsProcessed,dest_ULLogs,'ADB_LogProcessingMaster','Succeeded','')
        loadAuditTables_Ingestion_Log(df_ulLogs_CT_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','Succeeded','')        
    except Exception as exp:
        loadlogProcessesDeltaTable(df_ulLogs_CT_CNT_LogsProcessed,dest_ULLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_ulLogs_CT_CNT_IngestionLog,dest_ULLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        raise            


# COMMAND ----------

# MAGIC %md
# MAGIC # Process Sys logs for COM3 and CT

# COMMAND ----------

def Process_SYSLogs(DF_Source_SysLogs,sysLogs_ConfigId,allLogs_ConfigId):
    try:

        df_sysLogs_CNT = (DF_Source_SysLogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr',
        'FileNameMessageTypeCd','FileNameDtTmstmp','FileNameApplicatorPortCd','FileNameCycleNbr',
        'LogStartDate','LogEndDate','SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId',
        'DeviceType','LogType','DeviceId')
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
        df_sysLogs_CNT_IngestionLog  = df_sysLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(sysLogs_ConfigId))
        df_sysLogs_CNT_LogsProcessed = df_sysLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(allLogs_ConfigId))

        loadAuditTables_Ingestion_Log(df_sysLogs_CNT_IngestionLog,sysLogsPath,'ADB_LogProcessingMaster','InProgress','')        
        

        df_STG_SysLogs=(DF_Source_SysLogs.withColumn("CreatedBy",lit("ADB_LogProcessingMaster"))
                                            .withColumn("CreatedDt",current_timestamp())
                                            .withColumn("UpdatedBy",lit("ADB_LogProcessingMaster"))
                                            .withColumn("UpdatedDt",current_timestamp())
                                            .withColumn("JSONField", from_json("JSONField", Schema_Sys))
                                            .select('*', col('JSONField.*')))
        
        df_STG_SysLogs_Parsed = df_STG_SysLogs.select(parsed_SYS_cols)

        w=Window.partitionBy('ExternalSerialNbr','HdrDataContractNbr').orderBy(desc('HdrTimeGeneratedTmstmp'),desc('SourceFileSize'))
        
        df_STG_SysLogs_Latest =(df_STG_SysLogs_Parsed.drop_duplicates()
                                .withColumn("row_Num",row_number().over(w))
                                .filter("row_num = 1").drop('row_num'))

        DeltaTbl_SYS = DeltaTable.forPath(spark, sysLogsPath)  
        (DeltaTbl_SYS.alias("tgt")
                .merge(df_STG_SysLogs_Latest.alias("src"),
                        "tgt.ExternalSerialNbr = src.ExternalSerialNbr AND tgt.InternalSerialNbr = src.InternalSerialNbr AND tgt.HdrDataContractNbr = src.HdrDataContractNbr")
                .whenMatchedUpdate(
                    condition = "src.HdrTimeGeneratedTmstmp > tgt.HdrTimeGeneratedTmstmp",
                    set ={
                    "tgt.FileNameUUID": "src.FileNameUUID",
                    "tgt.FileNameDeviceTypeCd": "src.FileNameDeviceTypeCd",
                    "tgt.InternalSerialNbr": "src.InternalSerialNbr",
                    "tgt.HdrDateGeneratedDt": "src.HdrDateGeneratedDt",
                    "tgt.HdrTimeGeneratedTmstmp": "src.HdrTimeGeneratedTmstmp",
                    "tgt.HdrDataContractNbr": "src.HdrDataContractNbr",

                    "tgt.HdrFormatVersionCd": "src.HdrFormatVersionCd",
                    "tgt.HdrLogTypeCd": "src.HdrLogTypeCd",
                    "tgt.HdrDestinationSubSystemCd": "src.HdrDestinationSubSystemCd",
                    "tgt.HdrSourceSubSystemCd": "src.HdrSourceSubSystemCd",
                    "tgt.HdrCommandCd": "src.HdrCommandCd",
                    "tgt.HdrDataStoreID": "src.HdrDataStoreID",
                    "tgt.HdrAppHeadNbr": "src.HdrAppHeadNbr",
                    "tgt.SourceFileName": "src.SourceFileName",                    

                    "tgt.LatestSoftwareVersionNbr": "src.LatestSoftwareVersionNbr",

                    "tgt.IntMobileEquipID": "src.IntMobileEquipID",
                    "tgt.IntMobileSubscriberID": "src.IntMobileSubscriberID",
                    "tgt.ProviderNetworkNm": "src.ProviderNetworkNm",
                    "tgt.LocalizationAreaCd": "src.LocalizationAreaCd",
                    "tgt.CellID": "src.CellID",
                    "tgt.CrespID": "src.CrespID",
                    "tgt.ScrespID": "src.ScrespID",
                    "tgt.CountryNbrCd": "src.CountryNbrCd",
                    "tgt.OperatorCd": "src.OperatorCd",        

                    "tgt.CoolDeviceSerialNbr": "src.CoolDeviceSerialNbr",
                    "tgt.IoTHubNm": "src.IoTHubNm",
                    "tgt.EndPointNm": "src.EndPointNm",
                    "tgt.TPMKey": "src.TPMKey",

                    "tgt.SIMCardID": "src.SIMCardID",
                    "tgt.ModemStatusCd": "src.ModemStatusCd",
                    "tgt.ModemStatusDesc": "src.ModemStatusDesc",

                    "tgt.UpdatedBy": "src.UpdatedBy",
                    "tgt.UpdatedDt": "src.UpdatedDt"}) 
                .whenNotMatchedInsert(values =
                {
                    "tgt.FileNameUUID": "src.FileNameUUID",
                    "tgt.FileNameDeviceTypeCd": "src.FileNameDeviceTypeCd",
                    "tgt.ExternalSerialNbr": "src.ExternalSerialNbr",        
                    "tgt.InternalSerialNbr": "src.InternalSerialNbr",
                    "tgt.HdrDateGeneratedDt": "src.HdrDateGeneratedDt",
                    "tgt.HdrTimeGeneratedTmstmp": "src.HdrTimeGeneratedTmstmp",
                    "tgt.HdrDataContractNbr": "src.HdrDataContractNbr",
                    "tgt.HdrFormatVersionCd": "src.HdrFormatVersionCd",
                    "tgt.HdrLogTypeCd": "src.HdrLogTypeCd",
                    "tgt.HdrDestinationSubSystemCd": "src.HdrDestinationSubSystemCd",
                    "tgt.HdrSourceSubSystemCd": "src.HdrSourceSubSystemCd",
                    "tgt.HdrCommandCd": "src.HdrCommandCd",
                    "tgt.HdrDataStoreID": "src.HdrDataStoreID",
                    "tgt.HdrAppHeadNbr": "src.HdrAppHeadNbr",
                    "tgt.SourceFileName": "src.SourceFileName",

                    "tgt.LatestSoftwareVersionNbr": "src.LatestSoftwareVersionNbr",

                    "tgt.IntMobileEquipID": "src.IntMobileEquipID",
                    "tgt.IntMobileSubscriberID": "src.IntMobileSubscriberID",
                    "tgt.ProviderNetworkNm": "src.ProviderNetworkNm",
                    "tgt.LocalizationAreaCd": "src.LocalizationAreaCd",
                    "tgt.CellID": "src.CellID",
                    "tgt.CrespID": "src.CrespID",
                    "tgt.ScrespID": "src.ScrespID",
                    "tgt.CountryNbrCd": "src.CountryNbrCd",
                    "tgt.OperatorCd": "src.OperatorCd",        

                    "tgt.CoolDeviceSerialNbr": "src.CoolDeviceSerialNbr",
                    "tgt.IoTHubNm": "src.IoTHubNm",
                    "tgt.EndPointNm": "src.EndPointNm",
                    "tgt.TPMKey": "src.TPMKey",

                    "tgt.SIMCardID": "src.SIMCardID",
                    "tgt.ModemStatusCd": "src.ModemStatusCd",
                    "tgt.ModemStatusDesc": "src.ModemStatusDesc",

                    "tgt.CreatedBy": "src.CreatedBy",
                    "tgt.CreatedDt": "src.CreatedDt",
                    "tgt.UpdatedBy": "src.UpdatedBy",
                    "tgt.UpdatedDt": "src.UpdatedDt"
                }) 
        .execute()
        )

        loadlogProcessesDeltaTable(df_sysLogs_CNT_LogsProcessed,sysLogsPath,'ADB_LogProcessingMaster','Succeeded','')
        loadAuditTables_Ingestion_Log(df_sysLogs_CNT_IngestionLog,sysLogsPath,'ADB_LogProcessingMaster','Succeeded','')
    except Exception as exp:
        loadlogProcessesDeltaTable(df_sysLogs_CNT_LogsProcessed,sysLogsPath,'ADB_LogProcessingMaster','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_sysLogs_CNT_IngestionLog,sysLogsPath,'ADB_LogProcessingMaster','Failed',str(exp))
        raise            



# COMMAND ----------

# MAGIC %md
# MAGIC # Process UE logs for COM3 and CT

# COMMAND ----------

def Process_UELogs(DF_Source_UELogs,ueLogs_ConfigId,allLogs_ConfigId):
    df_ueLogs_CNT = (DF_Source_UELogs.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp','FileNameApplicatorPortCd',
                                              'FileNameCycleNbr','LogStartDate','LogEndDate','SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId',
                                              'DeviceType','LogType','DeviceId')
                     .count()
                     .withColumnRenamed("count","RecdCnt")                            
                       )
#     df_ueLogs_CNT.display()
    df_ueLogs_CNT_IngestionLog = df_ueLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(ueLogs_ConfigId))
    df_ueLogs_CNT_LogsProcessed = df_ueLogs_CNT.drop('ConfigId').withColumn('ConfigId',lit(allLogs_ConfigId))
#     df_ueLogs_CNT_LogsProcessed.display()
    try:    
        loadAuditTables_Ingestion_Log(df_ueLogs_CNT_IngestionLog,ueLogsPath,'ADB_LogProcessingMaster','InProgress','')
        
        
        df_UELogs = (DF_Source_UELogs.withColumn("JSONField", from_json("JSONField", UESchema))
                                             .select('*',col('JSONField.*'))
                                             .select('*',col('userRecoveryData.*'))
                                             .select('*',col('recoveryData.*')))
        df_UELogs_Parsed = df_UELogs.select(parsed_UE_cols)
        

        df_AllUELogs = (df_UELogs_Parsed
                         .withColumn('CreatedBy', lit("ADB_LogProcessingMaster"))
                         .withColumn('CreatedDt',lit(current_timestamp()))
                         .withColumn('UpdatedBy', lit("ADB_LogProcessingMaster"))
                         .withColumn('UpdatedDt',lit(current_timestamp()))
                         .withColumn('UELogsUUID',expr('uuid()')))

        DedupSeq = (Window.partitionBy("ExternalSerialNbr","InternalSerialNbr","HdrDataContractNbr","HdrAppHeadNbr","HdrTimeGeneratedTmstmp")
                        .orderBy(desc(col('ErrorTm'))))
        
        df_AllUELogs_Dedup  = df_AllUELogs.withColumn("rowNumDedup", row_number().over(DedupSeq)).filter('rowNumDedup=1').drop('rowNumDedup')        


        # Inserting new records to target:
        DeltaTbl_UE_Logs = DeltaTable.forPath(spark, ueLogsPath)  
        (DeltaTbl_UE_Logs.alias("tgt")
                .merge(df_AllUELogs_Dedup.alias("src"),
                    ("tgt.InternalSerialNbr = src.InternalSerialNbr and tgt.ExternalSerialNbr = src.ExternalSerialNbr and tgt.HdrTimeGeneratedTmstmp = src.HdrTimeGeneratedTmstmp and tgt.HdrDataContractNbr = src.HdrDataContractNbr AND tgt.HdrAppHeadNbr = src.HdrAppHeadNbr"))
          .whenNotMatchedInsert(values =
          {
            "tgt.UELogsUUID": "src.UELogsUUID",
            "tgt.FileNameUUID": "src.FileNameUUID",
            "tgt.HdrFormatVersionCd": "src.HdrFormatVersionCd",
            "tgt.HdrDateGeneratedDt": "src.HdrDateGeneratedDt",
            "tgt.HdrTimeGeneratedTmstmp": "src.HdrTimeGeneratedTmstmp",
            "tgt.HdrLogTypeCd": "src.HdrLogTypeCd",
            "tgt.HdrDestinationSubSystemCd": "src.HdrDestinationSubSystemCd",
            "tgt.HdrSourceSubSystemCd": "src.HdrSourceSubSystemCd",
            "tgt.HdrCommandCd": "src.HdrCommandCd",
            "tgt.HdrDataStoreID": "src.HdrDataStoreID",
            "tgt.HdrDataContractNbr": "src.HdrDataContractNbr",
            "tgt.FileNameDeviceTypeCd": "src.FileNameDeviceTypeCd",
            "tgt.ZCodeMajorCd": "src.ZCodeMajorCd",
            "tgt.ZCodeMinorCd": "src.ZCodeMinorCd",
            "tgt.RecoveryContractID": "src.RecoveryContractID",
            "tgt.RecoveryActiveStatusInd": "src.RecoveryActiveStatusInd",
            "tgt.RecoverySeverityCd": "src.RecoverySeverityCd",
            "tgt.RecoveryActionCd": "src.RecoveryActionCd",
            "tgt.RecoveryData1Txt": "src.RecoveryData1Txt",
            "tgt.RecoveryData2Txt": "src.RecoveryData2Txt",
            "tgt.RecoveryMessageTxt": "src.RecoveryMessageTxt",
            "tgt.ErrorDt": "src.ErrorDt",
            "tgt.ErrorTm": "src.ErrorTm",
            "tgt.TCode": "src.TCode",
            "tgt.ErrorDesc": "src.ErrorDesc",
            "tgt.HdrAppHeadNbr": "src.HdrAppHeadNbr",
            "tgt.InternalSerialNbr": "src.InternalSerialNbr",
            "tgt.ExternalSerialNbr": "src.ExternalSerialNbr",
            "tgt.CreatedBy": "src.CreatedBy",
            "tgt.CreatedDt": "src.CreatedDt",
            "tgt.UpdatedBy": "src.UpdatedBy",
            "tgt.UpdatedDt": "src.UpdatedDt"
          }
        ).execute())
        
        loadAuditTables_Ingestion_Log(df_ueLogs_CNT_IngestionLog,ueLogsPath,'ADB_LogProcessingMaster','Succeeded','')        
        loadlogProcessesDeltaTable(df_ueLogs_CNT_LogsProcessed,ueLogsPath,'ADB_LogProcessingMaster','Succeeded','')

    except Exception as exp:
        loadAuditTables_Ingestion_Log(df_ueLogs_CNT_IngestionLog,ueLogsPath,'ADB_LogProcessingMaster','Failed',str(exp))
        loadlogProcessesDeltaTable(df_ueLogs_CNT_LogsProcessed,ueLogsPath,'ADB_LogProcessingMaster','Failed',str(exp))

        raise            


# COMMAND ----------

# MAGIC %md
# MAGIC # Process Start and End Parameters 14020 and 14023

# COMMAND ----------

def processStartAndEndParameters(DF_Source_SEP,startEndParmater_ConfigId,allLogs_ConfigId):

    df_Source_StartEnd = DF_Source_SEP.withColumn('ConfigId',lit(startEndParmater_ConfigId))
    df_Source_StartEnd_CNT = (df_Source_StartEnd.groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                                  'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','ConfigId',
                                                  'SourceFilePath','SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId')
                                         .count()
                                         .withColumnRenamed("count","RecdCnt")
                           )

    try:
        loadAuditTables_Ingestion_Log(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_LogProcessingMaster','InProgress','')

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
                                .withColumn('CreatedBy',lit("ADB_LogProcessingMaster"))
                                .withColumn('CreatedDt',lit(current_timestamp()))
                                .withColumn('UpdatedBy',lit("ADB_LogProcessingMaster"))
                                .withColumn('UpdatedDt',lit(current_timestamp()))
                                .withColumn('StartEndParameterUUID',expr('uuid()'))
                                .drop('SourceFileName','SourceFilePath','ActualFileNameNm','file_modification_time','EventSeqNbr'))
        
        w=(Window.partitionBy('FileNameUUID')
                 .orderBy(asc('EventStartTmstmp'),desc('EventEndTmstmp')))
        StardEndParameter_Final = (df_StardEndParameter.withColumn('rownum',row_number().over(w))
                                                       .filter("rownum = 1")
                                                       .drop('rownum'))
        
        # Inserting new records to target:
        DeltaTbl_SEP_Logs = DeltaTable.forPath(spark, startEndParameterPath)
        (DeltaTbl_SEP_Logs.alias("tgt")
                .merge(
                StardEndParameter_Final.alias("src"),
                    ("tgt.FileNameUUID = src.FileNameUUID"))
          .whenNotMatchedInsert(values =
          {
            "tgt.StartEndParameterUUID": "src.StartEndParameterUUID",
            "tgt.FileNameUUID": "src.FileNameUUID",
            "tgt.HdrFormatVersionCd": "src.HdrFormatVersionCd",
            "tgt.HdrDateGeneratedDt": "src.HdrDateGeneratedDt",
            "tgt.HdrTimeGeneratedTmstmp": "src.HdrTimeGeneratedTmstmp",
            "tgt.HdrLogTypeCd": "src.HdrLogTypeCd",
            "tgt.HdrDestinationSubSystemCd": "src.HdrDestinationSubSystemCd",
            "tgt.HdrSourceSubSystemCd": "src.HdrSourceSubSystemCd",
            "tgt.HdrCommandCd": "src.HdrCommandCd",
            "tgt.HdrDataStoreID": "src.HdrDataStoreID",
            "tgt.HdrDataContractNbr_StartParameter": "src.HdrDataContractNbr_StartParameter",
            "tgt.HdrDataContractNbr_EndParameter": "src.HdrDataContractNbr_EndParameter",
            "tgt.EventStartTmstmp": "src.EventStartTmstmp",
            "tgt.CycleTypeCd": "src.CycleTypeCd",
            "tgt.Type": "src.Type",
            "tgt.VersionNbr": "src.VersionNbr",
            "tgt.ApplicatorPortCd": "src.ApplicatorPortCd",
            "tgt.PriorityCd": "src.PriorityCd",
            "tgt.TimeZoneDesc": "src.TimeZoneDesc",
            "tgt.EventEndTmstmp": "src.EventEndTmstmp",
            "tgt.HdrAppHeadNbr": "src.HdrAppHeadNbr",
            "tgt.InternalSerialNbr": "src.InternalSerialNbr",
            "tgt.ExternalSerialNbr": "src.ExternalSerialNbr",
            "tgt.CreatedBy": "src.CreatedBy",
            "tgt.CreatedDt": "src.CreatedDt",
            "tgt.UpdatedBy": "src.UpdatedBy",
            "tgt.UpdatedDt": "src.UpdatedDt"
          })
          .whenMatchedUpdate(set ={
                    "tgt.EventStartTmstmp": "src.EventStartTmstmp",
                    "tgt.EventEndTmstmp": "src.EventEndTmstmp"})
          .execute())

        
        loadAuditTables_Ingestion_Log(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_LogProcessingMaster','Succeeded','')        
    except Exception as exp:
        loadAuditTables_Ingestion_Log(df_Source_StartEnd_CNT,startEndParameterPath,'ADB_LogProcessingMaster','Failed',str(exp))
        raise            


# COMMAND ----------

# MAGIC %md
# MAGIC # Process All Logs

# COMMAND ----------

def upsertAllLogFiles(df_logSource,configId):
    df_UnProcessedLogs = df_logSource.filter(~col("HdrDataContractNbr").isin(contractNumberList_Processed))
    df_UnProcessedLogs_CNT = ( df_UnProcessedLogs.withColumn('ConfigId',lit(allLogs_ConfigId))
                                     .groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                              'FileNameApplicatorPortCd','FileNameCycleNbr',
                                              'LogStartDate','LogEndDate','ConfigId','SourceFilePath','SourceFileName','SourceFileSize',
                                              'SourceTypeId','DeviceType','LogType','DeviceId')
                                     .count()
                                     .withColumnRenamed("count","RecdCnt"))
    
    try:
        df_logSource_AllLogs = df_logSource.withColumn('ConfigId',lit(configId))
        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','InProgress','')
        
        df_logSource_AllLogs_Final  = (df_logSource_AllLogs.select('FileNameUUID','InternalSerialNbr','ExternalSerialNbr','FileNameDeviceTypeCd',
                                                                  'FileNameMessageTypeCd','HdrFormatVersionCd','HdrDateGeneratedDt','HdrTimeGeneratedTmstmp',
                                                                  'HdrLogtypeCd','HdrDestinationSubSystemCd','HdrSourceSubSystemCd','HdrCommandCD',
                                                                  'HdrDataStoreID','HdrDataContractNbr','JSONField')
                                                            .withColumn('CreatedBy',lit("ABD_LogProcessingMaster"))
                                                            .withColumn('CreatedDt',lit(current_timestamp()))
                                                            .withColumn('UpdatedBy',lit("ABD_LogProcessingMaster"))
                                                            .withColumn('UpdatedDt',lit(current_timestamp()))
                                    )
        df_logSource_AllLogs_Final.write.format('delta').mode("append").save(deltaTablePath_AllLogs)

        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','Succeeded','')
        loadlogProcessesDeltaTable(df_UnProcessedLogs_CNT,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','Succeeded','')

    except Exception as exp:
        loadlogProcessesDeltaTable(df_UnProcessedLogs_CNT,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        loadAuditTables_Ingestion_Log(df_logSource_AllLogs,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        raise
        

# COMMAND ----------

def populateLogStartEndDate(allLogs_ConfigId):
    #Update LogStartDate And LogEndDate with Event Start and end timestamps
    (spark.sql('''MERGE INTO Silverzone.logsourcefilesprocessed AS lsp
                        USING Silverzone.dimstartendparameter AS sep
                            On lsp.FileNameUUID = sep.FileNameUUID
                            AND (lsp.LogStartDate IS NULL OR lsp.LogEndDate IS NULL)
                            AND lsp.ConfigId = {}
                        WHEN MATCHED THEN UPDATE SET lsp.LogStartDate = sep.EventStartTmstmp, lsp.LogEndDate = sep.EventEndTmstmp'''.format(allLogs_ConfigId)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Master function to call all process logs in order

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):

    print(batchId)
    startTime = datetime.now()
    print("Current Time =", startTime)        
    
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

                     .withColumn("LogStartDate",lit(None))
                     .withColumn("LogEndDate",lit(None))
                     .drop('SourceFileName_noext')
                     .fillna({'FileNameApplicatorPortCd':''})
                     .filter(((col('LogType').isin(logTypes_Processed)) |
                             (col("HdrDataContractNbr").isin(contractNumberList_StartEnd))))                     
               )

    df_logSource_CNT = ( df_logSource.withColumn('ConfigId',lit(allLogs_ConfigId))
                                     .groupBy('FileNameDeviceTypeCd','ExternalSerialNbr','InternalSerialNbr','FileNameMessageTypeCd','FileNameDtTmstmp',
                                     'FileNameApplicatorPortCd','FileNameCycleNbr','LogStartDate','LogEndDate','ConfigId','SourceFilePath',
                                     'SourceFileName','SourceFileSize','SourceTypeId','DeviceType','LogType','DeviceId')
                                     .count()
                                     .withColumnRenamed("count","RecdCnt")
                                     .withColumn('FileNameUUID',uuidUdf())
                       )
    loadlogProcessesDeltaTable(df_logSource_CNT,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','InProgress','')

    df_logSourceFileProcessed = (spark.read.load(logFilesProcessedPath)
                                        .filter(col('ConfigId') == allLogs_ConfigId)
                                        .withColumn('rownum',row_number().over(Window.partitionBy("SourceFilePath").orderBy(desc(col("CreatedDt")))))
                                        .filter('rownum = 1').drop('rownum')
                                        .select('FileNameUUID','SourceFilePath'))

    df_logSource_FileNameUUID = df_logSource.join(df_logSourceFileProcessed,'SourceFilePath','inner')
    
    df_logSource_FileNameUUID.persist()
    try:
        print(df_logSource_FileNameUUID.select('SourceFileName').drop_duplicates().count())
        df_LogsStartEnd = df_logSource_FileNameUUID.filter(col("HdrDataContractNbr").isin(contractNumberList_StartEnd))
        df_SysLogs = df_logSource_FileNameUUID.filter(col("HdrDataContractNbr").isin(contractNumberList_SysLogs))
        df_UELogs = df_logSource_FileNameUUID.filter(col("HdrDataContractNbr").isin(contractNumberList_UE))
        df_LogsULCOM3 = df_logSource_FileNameUUID.filter(col("HdrDataContractNbr").isin(contractNumberList_UL_COM3))
        df_LogsULCT = df_logSource_FileNameUUID.filter(col("HdrDataContractNbr").isin(contractNumberList_UL_CT))

        processStartAndEndParameters(df_LogsStartEnd,startEndParmater_ConfigId,allLogs_ConfigId)
        populateLogStartEndDate(allLogs_ConfigId)

        ProcessULLogs25003_25004(df_LogsULCOM3,ulLogs_ConfigId,allLogs_ConfigId)

        ProcessULLogs84005_84006(df_LogsULCT,ulLogs_ConfigId,allLogs_ConfigId)

        Process_SYSLogs(df_SysLogs,sysLogs_ConfigId,allLogs_ConfigId)

        Process_UELogs(df_UELogs,ueLogs_ConfigId,allLogs_ConfigId)

        upsertAllLogFiles(df_logSource_FileNameUUID,allLogs_ConfigId)


    except Exception as exp:
        loadlogProcessesDeltaTable(df_logSource_CNT,deltaTablePath_AllLogs,'ADB_LogProcessingMaster','Failed',str(exp))
        raise    
    
    df_logSource_FileNameUUID.unpersist()
    spark.sql("clear cache")
    print("Total Time Takem for batch=", datetime.now()-startTime)

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming job to process log data

# COMMAND ----------

(df_Source.writeStream
                  .format("delta")
                  .queryName("Transformation_COM3_CT_Stream")
                  .trigger(processingTime='10 seconds')
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", deltaTablePath_AllLogs+checkPointLocation)
                  .outputMode("update")
                  .start()
                  .awaitTermination()
)

# COMMAND ----------

# spark.sql("SELECT * FROM cloud_files_state('{0}') ".format('/mnt/silver/FACTAllLogs/_checkpoints')).display()


# COMMAND ----------

# # Observe metric
# observed_df = df.observe("metric", count(lit(1)).as("cnt"), count(col("error")).as("malformed"))
# observed_df.writeStream.format("...").start()

# # Define my listener.
# class MyListener(StreamingQueryListener):
#     def onQueryStarted(self, event):
#         print(f"'{event.name}' [{event.id}] got started!")
#     def onQueryProgress(self, event):
#         row = event.progress.observedMetrics.get("metric")
#         if row is not None:
#             if row.malformed / row.cnt > 0.5:
#                 print("ALERT! Ouch! there are too many malformed "
#                       f"records {row.malformed} out of {row.cnt}!")
#             else:
#                 print(f"{row.cnt} rows processed!")
#     def onQueryTerminated(self, event):
#         print(f"{event.id} got terminated!")

# # Add my listener.
# spark.streams.addListener(MyListener())
