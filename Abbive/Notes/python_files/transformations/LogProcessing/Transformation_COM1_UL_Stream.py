# Databricks notebook source
ULConfigID=dbutils.widgets.text("ULConfigID","25")
ULConfigID=int(dbutils.widgets.get("ULConfigID"))

SourceTypeID=dbutils.widgets.text("SourceTypeID","8")
SourceTypeID=dbutils.widgets.get("SourceTypeID")

MLConfigID=dbutils.widgets.text("MLConfigID","26")
MLConfigID=dbutils.widgets.get("MLConfigID")

UEConfigID=dbutils.widgets.text("UEConfigID","29")
UEConfigID=dbutils.widgets.get("UEConfigID")

EXConfigID=dbutils.widgets.text("EXConfigID","28")
EXConfigID=dbutils.widgets.get("EXConfigID")

DeviceType=dbutils.widgets.text("DeviceType","COM1")
DeviceType=dbutils.widgets.get("DeviceType")


cycleIDSequence=dbutils.widgets.text("cycleIDSequence","32000000")
cycleIDSequence=int(dbutils.widgets.get("cycleIDSequence"))

Job_id=dbutils.widgets.text("Job_id","-1")
Job_id=dbutils.widgets.get("Job_id")

run_id=dbutils.widgets.text("run_id","-1")
run_id=dbutils.widgets.get("run_id")

dbutils.widgets.text('EmailNotificationID','3')
EmailNotificationID = dbutils.widgets.get('EmailNotificationID')

dbutils.widgets.text('Env','Dev')
Env = dbutils.widgets.get('Env')

print("ULConfigID:"+str(ULConfigID))
print("SourceTypeID:"+SourceTypeID)
print("MLConfigID:"+MLConfigID)
print("UEConfigID:"+UEConfigID)
print("EXConfigID:"+EXConfigID)
print("DeviceType:"+DeviceType)
print("cycleIDSequence:"+str(cycleIDSequence))
print("Job_id:"+str(Job_id))
print("run_id:"+str(run_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initializing the functions:

# COMMAND ----------

import traceback

pipelinename = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipelinename = pipelinename.rsplit('/', 1)[-1]
display(pipelinename)

# COMMAND ----------

# MAGIC %run ../../Configurations/Init_Scripts $job_id=$Job_id $run_id=$run_id $parent_run_id=-1

# COMMAND ----------

# MAGIC %run
# MAGIC /Configurations/EmailNotificationConfiguration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declaring the source and destination paths:

# COMMAND ----------

sourceFilePath = '/mnt/raw/DeviceLogs/COM1/*/*/*/{Usage}/*_UL_*.csv'
destinationFilePath='/mnt/silver/FACTULLogs_COM1/'
srcFilesProcessed="/mnt/silver/LogSourceFilesProcessed/"
checkPointLocation = destinationFilePath+"/_checkpoints/"

DLTableName='silverzone.factullogs_com1'
CreatedBy='ADB_COM1ULLog'
queueName = 'com1ulfilefileprocess-queue'

# COMMAND ----------

subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
queueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Schema:

# COMMAND ----------

# Generic column name is given because, we have to get the data from diffrent contract and later we can remane accordingly.
FileSchema = StructType([
    StructField("ContractNbr", StringType(), False),
    StructField("EventDate", StringType(), False),
    StructField("EventTime", StringType(), False),
    StructField("Column4", StringType(), False),
    StructField("Column5", StringType(), False),
    StructField("Column6", StringType(), False),
    StructField("CycleCount", StringType(), False),
    StructField("Column8", StringType(), False),
    StructField("Column9", StringType(), False),
    StructField("Column10", StringType(), False),
    StructField("Column11", StringType(), False),
    StructField("Column12", StringType(), False),
    StructField("Column13", StringType(), False),
    StructField("Column14", StringType(), False),
    StructField("Column15", StringType(), False),
    StructField("EzCardNbr", StringType(), False),
    StructField("Column17", StringType(), False),
    StructField("Column18", StringType(), False),
    StructField("Column19", StringType(), False),
    StructField("Column20", StringType(), False),
    StructField("Column21", StringType(), False),
    StructField("Column22", StringType(), False),
    StructField("Column23", StringType(), False),
    StructField("Column24", StringType(), False),
    StructField("Column25", StringType(), False),
    StructField("Column26", StringType(), False),
    StructField("Column27", StringType(), False),
    StructField("Column28", StringType(), False),
    StructField("Column29", StringType(), False)
    
])


# COMMAND ----------

# DBTITLE 1,Read  Streaming
ULRawFile = (spark.readStream.format("cloudFiles")
                              .option("cloudFiles.subscriptionId", subscriptionId)
                              .option("cloudFiles.connectionString", queueConnectionString)
                              .option("cloudFiles.queueName", queueName)
                              .option("cloudFiles.resourceGroup", resourceGroup)
                              .option("cloudFiles.tenantId", tenantId)
                              .option("cloudFiles.clientId", clientId)
                              .option("cloudFiles.clientSecret", clientSecret)
                              .option("cloudFiles.format", "csv")
                              .option("cloudFiles.includeExistingFiles",'false')
                              .option("cloudFiles.allowOverwrites","true")  
                              .option("cloudFiles.maxFilesPerTrigger",5000)
                              .option("cloudFiles.backfillInterval",'15 day')             
                              .option("cloudFiles.useNotifications",'true')
                              .schema(FileSchema) # provide a schema here for the files
                              .load(sourceFilePath)
                              .select("*",col("_metadata.file_path").alias('SourceFilePath'),
                                          col("_metadata.file_name").alias('SourceFileName'),
                                          col("_metadata.file_modification_time").alias('RawFileModificationTime'),
                                          col("_metadata.file_size").alias('SourceFileSize')
                                     ) 
                              .withColumn('SourceFilePath', regexp_replace(regexp_replace('SourceFilePath','dbfs:/mnt/raw/',''),'/mnt/raw/',''))
                              .withColumn('SourceFileName',regexp_replace(col('SourceFileName'),'%20',' '))
                    )


# COMMAND ----------

# DBTITLE 1,Read Dependent data from Silver Zone 
 logSourceFileProcessedDF = (spark.read.format('delta')
                             .option('header', True)
                             .load(srcFilesProcessed)
                             .filter("LogFileStatus = 'InProgress' and configid = {0}".format(ULConfigID))
                             .select('SourceFilePath','FileNameUUID')
                            )
DIMEquipmentMaster=(spark.read.format('delta').load("/mnt/silver/DIMEquipmentMaster/")
                     .select("SerialNumberNbr",col("ShipStartDt").cast("Timestamp"),col("ShipEndDt").cast("Timestamp"),"SoldToID","ShipToID","MaterialCd","Mfg_BatchNbr","EquipmentTransitStatusNm")
                   )
DIMEquipmentMasterRnk=(DIMEquipmentMaster.withColumn("Rnk",row_number().over(Window.partitionBy(upper("Mfg_BatchNbr")).orderBy(col("ShipStartDt").desc())))).where("rnk==1 and Mfg_BatchNbr is not null").select(expr("upper(Mfg_BatchNbr) as sbc_sn"),expr("upper(SerialNumberNbr) as ExternalSerialNbr"))              
COM1SoftwareRevisionDIM=spark.read.format('delta').load("/mnt/silver/LkpSoftwareVersion_COM1/").select("ssWinCE","EngineeringRev")

# COMMAND ----------

# DBTITLE 1,Calculate CycleID
def populateCycleID(DFSource,DLTableName,cycleIDSequence):
    MaxCycleID=MaxCycleID = spark.read.table(DLTableName).select(max("CycleID")).head()[0]
    if MaxCycleID is None:
        MaxCycleID=cycleIDSequence
    if(MaxCycleID < cycleIDSequence):
        MaxCycleID=cycleIDSequence

    DFSourceWithCycleID = (DFSource.withColumn("MaxCycleID",lit(MaxCycleID))
                        .withColumn("RowNum",row_number().over(Window.partitionBy("MaxCycleID").orderBy(col("CreatedDt"))))
                        .withColumn("CycleID",lit(col("MaxCycleID")+col("RowNum")))
                           )
    return DFSourceWithCycleID

# COMMAND ----------

# DBTITLE 1,ULlogsfile_COM1 Transformation
def Com1_UL_processing(DFSource):
    ULRawFile=(DFSource.where("ContractNbr== '~MB' or ContractNbr== '~ME' or ContractNbr== '~MA' or ContractNbr== '~MM'")
               .withColumn("EventDateTime",to_timestamp(concat("EventDate",lit(" "),"EventTime"),"M/d/yyyy HH:mm:ss")))
    ULRawFileMA=(ULRawFile.filter("ContractNbr='~MA'")
                                .withColumnRenamed("SourceFileName","SourceFileNameMA")
                                .withColumnRenamed("EventDateTime","EventDateTimeMA")
                                .select("SourceFileNameMA","EventDateTimeMA"))
    
    # joining MA contract with all Contract to get First MA Eventdatetime which will be used for deriving precycle and post cycle
    ULRawFileSelected1=(ULRawFile.join(ULRawFileMA,((ULRawFile["SourceFileName"]==ULRawFileMA["SourceFileNameMA"]) &(ULRawFile["EventDateTime"]==ULRawFileMA["EventDateTimeMA"])),"left")
                                    .withColumn("CommonEventDateTimeMA", coalesce(last("EventDateTimeMA",True).over(Window.partitionBy("SourceFileName").orderBy("EventDateTime")),lit('1900-01-01')))
                                    .drop("SourceFileNameMA","EventDateTimeMA")
                                    .withColumn("RowNumber",row_number().over(Window.partitionBy('SourceFileName').orderBy("SourceFileName",lit("1")))) 
                                    # .withColumn("FileRowCountDesc",row_number().over(Window.partitionBy('SourceFileName','ContractNbr').orderBy("SourceFileName",col("EventDateTime").desc())))  
                      )
    
    # #  getting Starting record for  MA Contract and last record MM Contract
    MAPrecycleDF=(ULRawFileSelected1.filter("ContractNbr=='~MA'")
                                    .withColumn("FileRowCountAsc",row_number().over(Window.partitionBy("SourceFileName").orderBy("SourceFileName",col("EventDateTime")))) 
                                    .filter( "FileRowCountAsc==1")
                                    .select("SourceFileName",expr("EventDateTime as PrecycleFirstRecord"),"FileRowCountAsc"))  

    MMPostcycleDF=(ULRawFileSelected1.filter("ContractNbr=='~MM' ")
                                    .withColumn("FileRowCountdesc",row_number().over(Window.partitionBy("SourceFileName").orderBy("SourceFileName",col("EventDateTime").desc()))) 
                                    .filter( "FileRowCountdesc==1").select("SourceFileName",expr("EventDateTime as PostcycleLastRecord"),"FileRowCountdesc"))
    
    # seperating the diffrent contract records. and filling the Ezcardnbr where is is null.  marking the row belonging to Same cycle 
    MA_df=ULRawFileSelected1.filter("ContractNbr=='~MA'").select(expr("EzCardNbr as AppBlobCRC"),expr("Column17 as AppBlobVersion"),"SourceFileName","CommonEventDateTimeMA")
    MB_ME_df=(ULRawFileSelected1.filter("ContractNbr=='~ME' or ContractNbr== '~MB'")
                        .withColumn("CycleCount",coalesce("CycleCount","Column5"))\
                        .withColumn("FillEzCardNbr", last("EzCardNbr",True).over(Window.partitionBy("SourceFileName").orderBy("CycleCount","EventDateTime"))) #for filling the empty ezcards places
                        .withColumn("CycleRowNbr",row_number().over(Window.partitionBy("SourceFileName","ContractNbr","CycleCount","FillEzCardNbr").orderBy("SourceFileName",col("EventDateTime").desc(),col("RowNumber").desc())))
                        .withColumn("CycleRowNbrAsc",row_number().over(Window.partitionBy("SourceFileName","ContractNbr","CycleCount","FillEzCardNbr").orderBy("SourceFileName",col("EventDateTime"),col("RowNumber"))))
                        ) # For getting the cycles row number for know the start and end of cycle so it can be used for joining
     
    
    
    #   separating MM contarct from DF
    MM_df=ULRawFileSelected1.filter("ContractNbr=='~MM'").select("EventDateTime","SourceFileName","CommonEventDateTimeMA")
    
    #     selecting MB contract and aliasing 
    MB_df=MB_ME_df.filter("ContractNbr == '~MB'").select(
                                                        expr("substring(Column4, 0 ,2) as ssWinCE"),
                                                        expr("substring(Column4, 4, 2) as ssCIB"),
                                                        expr("substring(Column4, 7, 2) as ssAPP"),
                                                        expr("substring(Column4, 10, 2) as ssTEC"),
                                                        expr("substring(Column4, 16, 8) as ssAPPident"),
                                                        expr("substring(Column4, 13, 2) as ssAdapter"),
                                                        expr(" Column5 as ProfilePN"),
                                                        expr("Column6 as ProfileIndex"),
                                                        expr("case when Column8 == 'Same' then 'S'  when Column8 == 'Next' then 'N' else 'U' END  as SameNextPatient"),
                                                        expr("case when Column9 == 'NP' then 'N'  when Column9 == 'RP' then 'R' when Column9 == 'Unknown' then 'U' else Column9 END as ReturningPatient"),
                                                        expr("case when Column10 == 'Unknown' then 'U'  when Column10 is null then 'U'  else Column10 END  as PatientType"),
                                                        expr("case when Column11 == 'Unknown' then 'U'  when Column11 is null then 'U' when Column11 == 'UBA' then 'A1'  else Column11 END as TreatmentBodyPart"),
                                                        expr("case when Column12 == 'Unknown' then 'U' else  Column12 END as TreatmentType"),
                                                        expr("case when Column13 == 'Unknown' then 'U'  when Column13 is null then 'U'  else Column13 END  as ReturningCoolSculpting"),
                                                        expr("Column14 as APP_sn"),
                                                        expr("Column15 as CIB_sn"),
                                                        expr("Column17 as EzCardDistTerritoryCd"),
                                                        expr("case when Column18 != 'Unknown' then Column18 else null END as ProfileTime"),
                                                        expr("case when Column19 != 'Unknown' then Column19 else null END as ProfileTemp"),
                                                        expr("Column20 as AlledID"),
                                                        expr("case when upper(Column21) == 'UNKNOWN' then null else Column21 END as TreatmentPlanNumber"),
                                                        expr("case when substring(Column22, 1, 3) == 'UPM' then Column22 else null END as UpperModuleNumber"),
                                                        expr("Column23 as EncryptedAlleID"),
                                                        expr("case when lower(substring(Column24, 1, 1)) == 'v' then Column24  else null END as ApplicatorSerialNbr"),
                                                        expr("Column25 as Contour"),
                                                        expr("Column26 as Adapter_sn"),
                                                        "CommonEventDateTimeMA",
                                                        "SourceFileName",
                                                        "CycleCount",
                                                        "FillEzCardNbr",
                                                        expr("EventDateTime as StartDatetime"),
                                                        "CycleRowNbr","SourceFilePath")\
                                                        .withColumn("CoolSculptingID", when(expr("EncryptedAlleID is Null"),"U").when(expr("EncryptedAlleID == 'Unknown'"),"U").otherwise(col('EncryptedAlleID')))
    #     selecting ME contract and aliasing 
    ME_df_All=MB_ME_df.filter("ContractNbr == '~ME'").select("CycleCount","FillEzCardNbr",expr("EventDateTime as EndDatetime"),"CycleRowNbr","CycleRowNbrAsc",expr('column4 as Status'),expr('Column6 as ErrorMessage'),'SourceFileName')
    ME_df_Successful=ME_df_All.filter("Status=='Successful'").select("SourceFileName","CycleCount","FillEzCardNbr",expr("EndDatetime as EndDatetimeSuccessful"))
    ME_df_All=ME_df_All.join(ME_df_Successful,["SourceFileName","CycleCount","FillEzCardNbr"],'left').filter("EndDatetime<=coalesce(EndDatetimeSuccessful,'2111-01-01')")
    
    ME_df_min_max=ME_df_All.groupBy("SourceFileName","CycleCount","FillEzCardNbr").agg(min(col('CycleRowNbr')).alias('MiniMa'), max(col('CycleRowNbr')).alias('Maxima'))
    ME_df=(ME_df_All.join(ME_df_min_max,["SourceFileName","CycleCount","FillEzCardNbr"],'left').where("CycleRowNbr == MiniMa or CycleRowNbr=Maxima or Status='Successful'").drop("MiniMa","Maxima","EndDatetimeSuccessful")
    .withColumn("CycleRowNbr",row_number().over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy(col("CycleRowNbr"))))
     .withColumn("CycleRowNbrAsc",row_number().over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy(col("CycleRowNbr").desc()))))


    #     Joining the seperated MB and ME contract so that they make one row
    MB_BE_df_joined=MB_df.join(ME_df,["CycleCount","FillEzCardNbr","CycleRowNbr","SourceFileName"],'Left')
    MB_BE_df_joined=(MB_BE_df_joined.withColumn("EndDatetime", last("EndDatetime",True).over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy("CycleRowNbr")))
    .withColumn("Status", last("Status",True).over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy("CycleRowNbr")))
    .withColumn("ErrorMessage", last("ErrorMessage",True).over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy("CycleRowNbr"))))
    
    #   If there are multiple cycle due to intrrupt than we have to get the first record for MB and the last record of ME, so we select the first MB contract (oldest MB contract for that cycle)
    oldestRecordCycle=MB_BE_df_joined.where("CycleRowNbr==1").withColumnRenamed('Contour','Contourold')
    
    #   If there are multiple cycle due to intrrupt than we have to get the first record for MB and the last record of ME, so we select the Last ME Contarct record (Latest ME contract for that cycle)
    latestRecordCycle=(MB_BE_df_joined.withColumn("CycleRowNbrDesc",row_number().over(Window.partitionBy("SourceFileName","CycleCount","FillEzCardNbr").orderBy("SourceFileName",col("StartDatetime"))))
                                    .where("CycleRowNbrDesc==1 and CycleRowNbr !=1")
                   .withColumnRenamed("StartDatetime","previousStartDatetime")
                     .withColumnRenamed("EndDatetime","previousEndDatetime")
                     .withColumnRenamed("Status","InterruptStatus")
                     .withColumnRenamed("ErrorMessage","InterruptError")
                     .select("previousStartDatetime","previousEndDatetime","InterruptStatus","InterruptError","CycleCount","FillEzCardNbr","SourceFileName","CommonEventDateTimeMA",expr("Contour as Contourlatest")
                    ).drop("CycleRankNbr"))
    
    oldestLatestDFJoined=oldestRecordCycle.join(latestRecordCycle,["CycleCount","FillEzCardNbr","SourceFileName","CommonEventDateTimeMA"],'Left').withColumn("Contour",coalesce("Contourlatest","Contourold"))
        
    #     deriving the Interupt Column
    interruptedDF=(oldestLatestDFJoined.withColumn("InterruptStartDateTime", when(expr("previousEndDatetime is Not Null"),col("previousEndDatetime")).otherwise(lit(None)))
                    .withColumn("InterruptEndDateTime", when(expr("previousEndDatetime is Not Null"),col("StartDatetime")).otherwise(lit(None)))
                     .withColumn("StartDatetime", when(expr("previousStartDatetime is Null"),col("StartDatetime")).otherwise(col("previousStartDatetime"))))                   
                    
    
    #     deriving the precycle and post cycle.
    MA_df=MA_df.dropDuplicates()
    FinalDF=(interruptedDF.join(MA_df,["CommonEventDateTimeMA","SourceFileName"],'left').join(MAPrecycleDF,["SourceFileName"],'left').join(MMPostcycleDF,["SourceFileName"],'left')
             .withColumn("PreCyclelag",lag("EndDatetime").over(Window.partitionBy("SourceFileName").orderBy("SourceFileName",col("StartDatetime"))))
                          .withColumn("PostCyclelead",lead("StartDatetime").over(Window.partitionBy("SourceFileName").orderBy("SourceFileName",col("StartDatetime"))))
             .withColumn("precycle",when(coalesce("PreCyclelag","PrecycleFirstRecord","StartDatetime")<col("StartDatetime"),coalesce("PreCyclelag","PrecycleFirstRecord")).otherwise(col("StartDatetime")))
             .withColumn("PostCycle",when(coalesce("PostCyclelead","PostcycleLastRecord","EndDatetime")>coalesce(col("EndDatetime"),lit('1900-01-01')),coalesce("PostCyclelead","PostcycleLastRecord")).otherwise(col("EndDatetime")))
            ).drop("CommonEventDateTimeMA","CycleRowNbr","previousStartDatetime","previousEndDatetime",
                    "PrecycleFirstRecord","FileRowCountdesc","PreCyclelag","PostCyclelead","FileRowCountAsc","PostcycleLastRecord","cyclerownbrasc")
                                 
    #To get interupt Status,InterruptStartDateTime and InterruptError
    ME_df_Interupt=(ME_df.filter('(CycleRowNbrAsc=1 and CycleRowNbr !=1)').withColumnRenamed("status","statusNew").withColumnRenamed("ErrorMessage","ErrorMessageNew").withColumnRenamed("EnddateTime","EnddateTimeNew"))

    FinalDF=(FinalDF.alias("Final").join(ME_df_Interupt.alias("interupt"),["FillEzCardNbr","CycleCount","SourceFileName"],'left')
                    .withColumn("InterruptStartDateTime",coalesce(col("Final.InterruptStartDateTime"),col("interupt.EnddateTimeNew")))
                    .withColumn("InterruptStatus",coalesce(col("Final.InterruptStatus"),col("interupt.statusNew")))
                    .withColumn("InterruptError",coalesce(col("Final.InterruptError"),col("interupt.ErrorMessageNew")))
    )
    
    # print("FinalDF:"+str(FinalDF.count()))

    FinalDFWithNoException=FinalDF.where("(InterruptStatus <> 'Successful' or InterruptStatus is null)")
    # print("FinalDFWithNoException:"+str(FinalDFWithNoException.count()))
    
    # handaling ExceptionalCase
    FinalDFWithException=(FinalDF.where("InterruptStatus='Successful'")
                    .withColumn('Status',col('InterruptStatus'))
                    .withColumn('EndDatetime',col('InterruptStartDateTime'))
                    .withColumn('ErrorMessage',col('InterruptError'))
                    .withColumn('InterruptStatus',lit(None))
                    .withColumn('InterruptStartDateTime',lit(None))
                    .withColumn('InterruptError',lit(None)))
    # print("FinalDFWithException:"+str(FinalDFWithException.count()))

    FinalDF=(FinalDFWithNoException.unionByName(FinalDFWithException)
                                    .withColumn("EndDatetime",when(expr("EndDatetime<nvl(InterruptEndDateTime,'1900-01-01')"),lit(None)).otherwise(col('EndDatetime')))
                                    .withColumn("Status",when(expr("EndDatetime is null"),lit(None)).otherwise(col('Status')))
                                    .withColumn("ErrorMessage",when(expr("EndDatetime is null"),lit(None)).otherwise(col('ErrorMessage')))
                                    )
    # print("FinalDF:"+str(FinalDF.count()))
    

    #     deriving the UnsoldFraudFlg and SoftwareVerTxt.
    joinedWithDimequipment=(FinalDF.join(DIMEquipmentMaster.filter(col('EquipmentTransitStatusNm')=='SHIPPED'),((FinalDF.FillEzCardNbr == DIMEquipmentMaster.SerialNumberNbr) 
                                                             & (FinalDF.StartDatetime >= DIMEquipmentMaster.ShipStartDt) 
                                          & (FinalDF.EndDatetime < DIMEquipmentMaster.ShipEndDt)
                                         )
                                         ,"left")
                                      .join(COM1SoftwareRevisionDIM,["ssWinCE"],'Left')
                                      .withColumnRenamed("EngineeringRev","SoftwareVerTxt")
                                      .withColumn("UnsoldFraudFlg",when(expr("SerialNumberNbr is NULL"),'Y').otherwise("N")).drop("SerialNumberNbr")
                                      .withColumn("SoldToAccountID",when(col('SoldToID').isNull(),'0000000000').otherwise(col('SoldToID')))
                                      .withColumn("ShipToAccountID",when(col('ShipToID').isNull(),'0000000000').otherwise(col('ShipToID')))
                                      .withColumnRenamed("MaterialCd","EQUIPCardPartNbr")
                                    .drop("ShipStartDt","SoldToID","ShipToID")
                                    
                           )
    
    #     deriving the DuplicateCycleFlg and other Audit Column.
    FactULlogsDelta=(joinedWithDimequipment.withColumn('ULLogsUUID',expr('uuid()'))
                            .withColumn('SBC_sn',split(regexp_replace(col("SourceFileName"),"%20"," "),'_').getItem(1))
                            .withColumn('SBC_sn',trim(upper(col('SBC_sn'))))
                            .withColumn('CreatedBy',lit(CreatedBy))
                            .withColumn('CreatedDt',current_timestamp())
                            .withColumn('UpdatedBy',lit(CreatedBy))
                            .withColumn('UpdatedDt',current_timestamp())
                            .withColumnRenamed("FillEzCardNbr","EzCard")
                            .withColumn('EzCard',trim(upper(col('EzCard'))))
                            .withColumnRenamed("CycleCount","CyclesLeft")
                            .withColumnRenamed("StartDatetime","Begintime")
                            .withColumnRenamed("EndDatetime","EndTime"))
    
    FactULlogsDelta=FactULlogsDelta.join(DIMEquipmentMasterRnk,['sbc_sn'],'left').withColumn("ExternalSerialNbr",coalesce("UpperModuleNumber","ExternalSerialNbr"))
    
    FactULlogsDelta=FactULlogsDelta.withColumn("DuplicateCycleFlg",row_number().over(Window.partitionBy("CyclesLeft","EzCard").orderBy(col("Begintime").desc(),col("ShipEndDt").desc()))).filter("DuplicateCycleFlg==1")

    FactULlogsDelta=FactULlogsDelta.withColumn("DuplicateCycleFlg",when(col("DuplicateCycleFlg")==1,None).otherwise(col("DuplicateCycleFlg")))
    return FactULlogsDelta                    

# COMMAND ----------

# DBTITLE 1,UL and ML Association

def UL_ML_association(DFSource):
    MLFileNameUUIDNotAssoc=DFSource.filter("MLAssocFileNameUUID is Null").select("CyclesLeft","SBC_sn","BeginTime","EndTime",expr("nvl(EzCard,'') as EZcard"))
    MLFileProcessedDF=(spark.read.table("silverzone.logsourcefilesprocessed")
                                 .where("ConfigId = {0} and LogFileStatus='Succeeded'".format(MLConfigID))
                                  .select("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate")
                      )
    
    UL_MLAssocDF=(MLFileNameUUIDNotAssoc.join(MLFileProcessedDF,((MLFileNameUUIDNotAssoc.SBC_sn == MLFileProcessedDF.ExternalSerialNbr) & (MLFileNameUUIDNotAssoc.BeginTime > MLFileProcessedDF.LogStartDate) 
                                                                   & (MLFileNameUUIDNotAssoc.EndTime < MLFileProcessedDF.LogEndDate)),"inner")
                                          .withColumn("MLAssocFileNameUUID",col("FileNameUUID"))
                    .withColumn("Rank",row_number().over(Window.partitionBy("SBC_sn","Ezcard","CyclesLeft","BeginTime","EndTime").orderBy(col("LogStartDate").desc())))
                    .where("Rank==1")
                    .drop("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate","Rank") )
    UL_MLAssocDF.createOrReplaceTempView("UL_MLAssocDF")
    return UL_MLAssocDF

# COMMAND ----------

# DBTITLE 1,UL and EX Association
def UL_EX_association(DFSource):
    EXFileNameUUIDNotAssoc=DFSource.filter("EXAssocFileNameUUID is Null").select("CyclesLeft","SBC_sn","BeginTime","EndTime",expr("nvl(EzCard,'') as EZcard"))
    EXFileProcessedDF=(spark.read.table("silverzone.logsourcefilesprocessed")
                                 .where("ConfigId = {0} and LogFileStatus='Succeeded'".format(EXConfigID))
                                  .select("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate")
                      )
    
    UL_EXAssocDF=(EXFileNameUUIDNotAssoc.join(EXFileProcessedDF,((EXFileNameUUIDNotAssoc.SBC_sn == EXFileProcessedDF.ExternalSerialNbr) & (EXFileNameUUIDNotAssoc.BeginTime > EXFileProcessedDF.LogStartDate) 
                                                                   & (EXFileNameUUIDNotAssoc.EndTime < EXFileProcessedDF.LogEndDate)),"inner")
                                          .withColumn("EXAssocFileNameUUID",col("FileNameUUID"))
                    .withColumn("Rank",row_number().over(Window.partitionBy("SBC_sn","Ezcard","CyclesLeft","BeginTime","EndTime").orderBy(col("LogStartDate").desc())))
                    .where("Rank==1")
                    .drop("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate","Rank") )
    UL_EXAssocDF.createOrReplaceTempView("UL_EXAssocDF")
    return UL_EXAssocDF

# COMMAND ----------

# DBTITLE 1,UL and UE Association
def UL_UE_association(DFSource):
    UEFileNameUUIDNotAssoc=DFSource.filter("UEAssocFileNameUUID is Null").select("CyclesLeft","SBC_sn","BeginTime","EndTime",expr("nvl(EzCard,'') as EZcard"))
    UEFileProcessedDF=(spark.read.table("silverzone.logsourcefilesprocessed")
                                 .where("ConfigId = {0} and LogFileStatus='Succeeded'".format(UEConfigID))
                                  .select("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate")
                      )
    
    UL_UEAssocDF=(UEFileNameUUIDNotAssoc.join(UEFileProcessedDF,((UEFileNameUUIDNotAssoc.SBC_sn == UEFileProcessedDF.ExternalSerialNbr) & (UEFileNameUUIDNotAssoc.BeginTime > UEFileProcessedDF.LogStartDate) 
                                                                   & (UEFileNameUUIDNotAssoc.EndTime < UEFileProcessedDF.LogEndDate)),"inner")
                                          .withColumn("UEAssocFileNameUUID",col("FileNameUUID"))
                    .withColumn("Rank",row_number().over(Window.partitionBy("SBC_sn","Ezcard","CyclesLeft","BeginTime","EndTime").orderBy(col("LogStartDate").desc())))
                    .where("Rank==1")
                    .drop("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate","Rank") )
    return UL_UEAssocDF

# COMMAND ----------

# DBTITLE 1,UL and Assert Association
def UL_Assert_association(DFSource):
    AssertFileNameUUIDNotAssoc=DFSource.filter("UEAssocFileNameUUID is Null").select("CyclesLeft","SBC_sn","BeginTime","EndTime",expr("nvl(EzCard,'') as EZcard"))
    AssertFileProcessedDF=(spark.read.table("silverzone.logsourcefilesprocessed")
                                 .where("ConfigId = {0} and LogFileStatus='Succeeded'".format(AssertConfigID))
                                  .select("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate")
                      )
    
    UL_AssertAssocDF=(AssertFileNameUUIDNotAssoc.join(AssertFileProcessedDF,((AssertFileNameUUIDNotAssoc.SBC_sn == AssertFileProcessedDF.ExternalSerialNbr) & (AssertFileNameUUIDNotAssoc.BeginTime > AssertFileProcessedDF.LogStartDate) 
                                                                   & (AssertFileNameUUIDNotAssoc.EndTime < AssertFileProcessedDF.LogEndDate)),"inner")
                                                                    # & (coalesce(AssertFileNameUUIDNotAssoc.EndTime,lit('1900-01-01')) < coalesce(AssertFileProcessedDF.LogEndDate,lit('1900-01-02')))),"inner")
                                                                   
                                          .withColumn("AssertAssocFileNameUUID",col("FileNameUUID"))
                    .withColumn("Rank",row_number().over(Window.partitionBy("SBC_sn","Ezcard","CyclesLeft","BeginTime","EndTime").orderBy(col("LogStartDate").desc())))
                    .where("Rank==1")
                    .drop("FileNameUUID","ExternalSerialNbr","LogStartDate","LogEndDate","Rank") )
    return UL_AssertAssocDF

# COMMAND ----------

# DBTITLE 1,Upsert the data to FactulLogs_Com1
def upsertToDelta(DFSource, batchId):
    try:
        batchEnd(q,batchId)   
        print("Running for BatchID: {0}".format(batchId))

        DFSource.persist()
        DFSource=(DFSource.withColumn('ConfigId',lit(ULConfigID)).withColumn('SourceTypeID',lit(SourceTypeID))
                  .withColumn("FileNameDeviceTypeCd",lit(DeviceType))
                  .withColumn("FileNameDeviceSerialNbr",when(length(split(regexp_replace(col("SourceFileName"),"%20"," "), '_')
                                                                    .getItem(2))==2,split(regexp_replace(col("SourceFileName"),"%20"," "), '_').getItem(1))
                              .otherwise(split(regexp_replace(col("SourceFileName"),"%20"," "), '_').getItem(2)) )
                  .withColumn("FileNameMessageTypeCd",when(length(split(col('SourceFileName'), '_').getItem(2))==2,split(col('SourceFileName'), '_').getItem(2))
                              .otherwise(split(col('SourceFileName'), '_').getItem(3)))
                  .withColumn("DeviceType",col('FileNameDeviceTypeCd'))
                  .withColumn("LogType",col('FileNameMessageTypeCd'))
                  .withColumn("FileNameDeviceSerialNbr",upper(col('FileNameDeviceSerialNbr')))
                  .withColumn("DeviceId",col('FileNameDeviceSerialNbr'))
                  .withColumn("InternalSerialNbr",col('FileNameDeviceSerialNbr'))
                  .withColumn('RunID',lit(batchId))
                  )
        
        
        DFSourceWithEventDateTime=(DFSource.withColumn("EventDateTime",to_timestamp(concat("EventDate",lit(" "),"EventTime"),"M/d/yyyy HH:mm:ss")))
        

        filesMicroBatch = (DFSource.groupBy("SourceFilePath","SourceFileName","SourceFileSize","ConfigID","SourceTypeID","FileNameDeviceTypeCd","FileNameDeviceSerialNbr","InternalSerialNbr","FileNameMessageTypeCd","RawFileModificationTime","RunID")
                           .count()
                           .withColumnRenamed("count","RecdCnt")
                           .withColumn('SourceFilePath',expr("regexp_replace(regexp_replace(SourceFilePath, SourceFileName, ''),'raw/','')")) 
                           .withColumn("FileNameDtTmstmp",when(length(split(col('SourceFileName'), '_').getItem(3))<=2,
                                                               substring(regexp_replace(regexp_replace(concat(split(regexp_replace(col('SourceFileName'),'Unparsed_',''), '_')
                                                                                     .getItem(4),split(regexp_replace(col('SourceFileName'),'Unparsed_',''), '_')
                                                                                     .getItem(5),split(regexp_replace(col('SourceFileName'),'Unparsed_',''), '_')
                                                                                     .getItem(6)),'.csv',''),'.zip',''),0,14))
                                       .otherwise(substring(regexp_replace(regexp_replace(concat(split(col('SourceFileName'), '_')
                                                                        .getItem(3),split(col('SourceFileName'), '_')
                                                                        .getItem(4),split(col('SourceFileName'), '_')
                                                                        .getItem(5)),'.csv',''),'.zip',''),0,14)))
                           .withColumn("FileNameApplicatorPortCd",lit(None))
                           .withColumn("FileNameCycleNbr",when(length(split(regexp_replace(col('SourceFileName'),'.csv',''), '_').getItem(3))==1,
                                    regexp_replace(split(col('SourceFileName'), '_').getItem(3),'.csv',''))
                              .when(length(split(col('SourceFileName'), '_').getItem(4))==1,
                                    split(col('SourceFileName'), '_').getItem(4))
                              .otherwise(lit('null')))
                          .withColumn("LogStartDate",lit(None))
                          .withColumn("LogEndDate",lit(None)))
        filesMicroBatch=filesMicroBatch.join(DIMEquipmentMasterRnk,filesMicroBatch.InternalSerialNbr==DIMEquipmentMasterRnk.sbc_sn,'left')
        loadlogProcessesDeltaTable(filesMicroBatch,srcFilesProcessed,CreatedBy,'InProgress','')
        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'InProgress','')
        
        log_df = filesMicroBatch.select(col('ConfigId').alias('ConfigID'), col('SourceTypeID').alias('SourceTypeID'), col('SourceFilePath').alias('Source')) \
                                         .withColumn('Destination', lit(str(destinationFilePath))) \
                                         .withColumn('Run_ID', lit(str(batchId))) \
                                         .withColumn('Job_ID', lit(str(Job_id)))
        
        Microbatch_df = filesMicroBatch

        # raise Exception("No Exception: Manual Failure")

        FactULlogsTransformedDF=Com1_UL_processing(DFSource)
        FactULlogsTransformedDF=populateCycleID(FactULlogsTransformedDF,DLTableName,cycleIDSequence)
#         FactULlogsTransformedDF.display()
        DFSourceFileUUID = FactULlogsTransformedDF.join(logSourceFileProcessedDF,['SourceFilePath'],'inner')
        DFSourceFileUUID.persist()
        # DFSourceFileUUID.display()
        FactULLogsTarget = DeltaTable.forPath(spark, destinationFilePath)  #         Read Delta table
#         Merge Delta table
        (FactULLogsTarget.alias("tgt")
                .merge(
                DFSourceFileUUID.alias("src"),
                    "upper(nvl(tgt.SBC_sn,'')) = upper(nvl(src.SBC_sn,'')) and upper(nvl(tgt.EzCard,'')) = upper(nvl(src.EzCard,'')) and nvl(tgt.CyclesLeft,0) = nvl(src.CyclesLeft,0)  and tgt.Begintime = src.Begintime")
          .whenNotMatchedInsert(values =
          {
                "tgt.Adapter_sn" : "src.Adapter_sn",
                "tgt.AlledID" : "src.AlledID",
                "tgt.APP_sn" : "src.APP_sn",
                "tgt.AppBlobCRC" : "src.AppBlobCRC",
                "tgt.AppBlobVersion" : "src.AppBlobVersion",
                "tgt.Applicator_sn" : "src.ApplicatorSerialNbr",
                "tgt.BeginTime" : "src.Begintime",
                "tgt.CIB_sn" : "src.CIB_sn",
                "tgt.Contour" : "src.Contour",
                "tgt.CoolSculptingID" : "src.CoolSculptingID",
                "tgt.CreatedBy" : "src.CreatedBy",
                "tgt.CreatedDt" : "src.CreatedDt",
                "tgt.CyclesLeft" : "src.CyclesLeft",
                "tgt.DTC" : "src.EzCardDistTerritoryCd",
                "tgt.DuplicateCycleFlg" : "src.DuplicateCycleFlg",
                "tgt.EncryptedAlleID" : "src.EncryptedAlleID",
                "tgt.EndTime" : "src.EndTime",
                "tgt.ErrorMessage" : "src.ErrorMessage",
                "tgt.EZCard" : "src.EzCard",
                "tgt.InterruptEnd" : "src.InterruptEndDateTime",
                "tgt.InterruptError" : "src.InterruptError",
                "tgt.InterruptStart" : "src.InterruptStartDateTime",
                "tgt.InterruptStatus" : "src.InterruptStatus",
                "tgt.PatientType" : "src.PatientType",
                "tgt.PostCycle" : "src.PostCycle",
                "tgt.precycle" : "src.precycle",
                "tgt.ProfileIndex" : "src.ProfileIndex",
                "tgt.ProfilePN" : "src.ProfilePN",
                "tgt.ProfileTemp" : "src.ProfileTemp",
                "tgt.ProfileTime" : "src.ProfileTime",
                "tgt.ReturningCoolSculpting" : "src.ReturningCoolSculpting",
                "tgt.ReturningPatient" : "src.ReturningPatient",
                "tgt.SameNextPatient" : "src.SameNextPatient",
                "tgt.SBC_sn" : "src.SBC_sn",
                "tgt.SoftwareVerTxt" : "src.SoftwareVerTxt",
                "tgt.SourceFileName" : "src.SourceFileName",
                "tgt.SourceFilePath" : "src.SourceFilePath",
                "tgt.ssAdapter" : "src.ssAdapter",
                "tgt.ssAPP" : "src.ssAPP",
                "tgt.ssAPPident" : "src.ssAPPident",
                "tgt.ssCIB" : "src.ssCIB",
                "tgt.ssTEC" : "src.ssTEC",
                "tgt.ssWinCE" : "src.ssWinCE",
                "tgt.Status" : "src.Status",
                "tgt.TreatmentBodyPart" : "src.TreatmentBodyPart",
                "tgt.TreatmentPlanNumber" : "src.TreatmentPlanNumber",
                "tgt.TreatmentType" : "src.TreatmentType",
                "tgt.UnsoldFraudFlg" : "src.UnsoldFraudFlg",
                "tgt.SoldToAccountID" : "src.SoldToAccountID",
                "tgt.ShipToAccountID" : "src.ShipToAccountID",
                "tgt.EQUIPCardPartNbr" : "src.EQUIPCardPartNbr",
                "tgt.UpdatedBy" : "src.UpdatedBy",
                "tgt.UpdatedDt" : "src.UpdatedDt",
                "tgt.ExternalSerialNbr" : "src.ExternalSerialNbr",
                "tgt.CycleID" : "src.CycleID",
                "tgt.ULLogsUUID" : "src.ULLogsUUID",
                "tgt.FileNameUUID" : "src.FileNameUUID",
                "tgt.CoolSculptingID2" : "src.CoolSculptingID"
          })
          .execute())
          
        
        DFSourceAgg=(DFSourceWithEventDateTime.select("SourceFilePath","SourceFileName","EventDateTime","ConfigId").filter("EventDateTime is not null")).groupBy('SourceFilePath','SourceFileName','ConfigId').agg(min(col('EventDateTime')).cast('string').alias('LogStartDate'), max(col('EventDateTime')).cast('string').alias('LogEndDate'))

        DFSourceAgg=DFSourceAgg.join(logSourceFileProcessedDF,['SourceFilePath'],'inner')
        
        
        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'Succeeded','')
        loadlogProcessesDeltaTable(DFSourceAgg,destinationFilePath,CreatedBy,'Succeeded','')
        DFSourceFileUUID.unpersist()
        DFSource.unpersist()
        spark.sql("clear cache")
    except Exception as e:
        ExceptionTraceback = traceback.format_exc()
        ErrorMessage = ExceptionTraceback + str(e).replace("'",'"')
        loadlogProcessesDeltaTable(filesMicroBatch,destinationFilePath,CreatedBy,'Failed',str(e).replace("'",'"'))
        loadAuditTables_Ingestion_Log(DFSource,destinationFilePath,CreatedBy,'Failed',str(e).replace("'",'"'))
        
        logIntoStreamLogTable(log_df,CreatedBy,"Failed",Microbatch_df,ErrorMessage)
        streamLogEmailNotification(EmailNotificationID,Microbatch_df, pipelinename, Env)
        print(ExceptionTraceback)
        # raise
    # factuULLogsCom1DF=spark.read.table(DLTableName)
    # factuULLogsCom1DF.persist()
    # UL_MLAssocDF=UL_ML_association(factuULLogsCom1DF)
    # UL_EXAssocDF=UL_EX_association(factuULLogsCom1DF)
    # UL_UEAssocDF=UL_UE_association(factuULLogsCom1DF)
    # UL_AssertAssocDF=UL_Assert_association(factuULLogsCom1DF)
    
    # finalAssoc=(factuULLogsCom1DF.withColumnRenamed("MLAssocFileNameUUID","MLAssocFileNameUUIDUL")
    #             .withColumnRenamed("EXAssocFileNameUUID","EXAssocFileNameUUIDUL")
    #             .withColumnRenamed("UEAssocFileNameUUID","UEAssocFileNameUUIDUL")
    #             .withColumnRenamed("AssertAssocFileNameUUID","AssertAssocFileNameUUIDUL")
    #             .join(UL_MLAssocDF,["CyclesLeft","SBC_sn","BeginTime","EndTime","EzCard"],"left")
    #                                 .join(UL_EXAssocDF,["CyclesLeft","SBC_sn","BeginTime","EndTime","EzCard"],"left")
    #                                 .join(UL_UEAssocDF,["CyclesLeft","SBC_sn","BeginTime","EndTime","EzCard"],"left")
    #                                 .join(UL_AssertAssocDF,["CyclesLeft","SBC_sn","BeginTime","EndTime","EzCard"],"left")
    #                                 .where("EXAssocFileNameUUID is not null or MLAssocFileNameUUID is not null or UEAssocFileNameUUID is not null or AssertAssocFileNameUUID is not null")
    #             .withColumn("MLAssocFileNameUUID",expr("coalesce(MLAssocFileNameUUID,MLAssocFileNameUUIDUL)"))
    #             .withColumn("UEAssocFileNameUUID",expr("coalesce(UEAssocFileNameUUID,UEAssocFileNameUUIDUL)"))
    #             .withColumn("EXAssocFileNameUUID",expr("coalesce(EXAssocFileNameUUID,EXAssocFileNameUUIDUL)"))
    #              .withColumn("AssertAssocFileNameUUID",expr("coalesce(AssertAssocFileNameUUID,AssertAssocFileNameUUIDUL)")))
    # finalAssoc=finalAssoc.withColumn("rowNumber",row_number().over(Window.partitionBy("CyclesLeft","EzCard",'Begintime','SBC_sn').orderBy(col("Begintime").desc()))).where('rowNumber=1')

    # print(finalAssoc.count())

    # FactULLogsCOM1 = DeltaTable.forPath(spark, destinationFilePath)
    # (FactULLogsCOM1.alias("tgt")
    #     .merge(
    #     finalAssoc.alias("src"), "nvl(tgt.CyclesLeft,0) = nvl(src.CyclesLeft,0) and nvl(tgt.EzCard,'') = nvl(src.EzCard,'') and nvl(tgt.Begintime,'') = nvl(src.Begintime,'') and nvl(tgt.SBC_sn,'') = nvl(src.SBC_sn,'') ")
    # .whenNotMatchedInsert(values =
    # {
    #     "tgt.EXAssocFileNameUUID" : "src.EXAssocFileNameUUID",
    #     "tgt.MLAssocFileNameUUID" : "src.MLAssocFileNameUUID",
    #     "tgt.UEAssocFileNameUUID" : "src.UEAssocFileNameUUID",
    #     "tgt.AssertAssocFileNameUUID" : "src.AssertAssocFileNameUUID"
    # })
    # .whenMatchedUpdate(set =
    # {
    # "tgt.EXAssocFileNameUUID" : "src.EXAssocFileNameUUID",
    #     "tgt.MLAssocFileNameUUID" : "src.MLAssocFileNameUUID",
    #     "tgt.UEAssocFileNameUUID" : "src.UEAssocFileNameUUID",
    #     "tgt.AssertAssocFileNameUUID" : "src.AssertAssocFileNameUUID"
    # })    
    # .execute())

    




# COMMAND ----------

# DBTITLE 0,Run for Stream processing
# (ULRawFile.writeStream
#      .trigger(availableNow=True)
#       .outputMode("update")
#       .foreachBatch(upsertToDelta)
#      .option("checkpointLocation", checkPointLocation)
#       .start().awaitTermination()
#       )

#Adding Microbarch of 10 seconds for streaming capability. 
q=(ULRawFile.writeStream
    .queryName("Transformation_COM1_UL_Stream")
     .trigger(processingTime='10 seconds')
      .outputMode("update")
      .foreachBatch(upsertToDelta)
     .option("checkpointLocation", checkPointLocation)
      .start()
      )

# COMMAND ----------

stop_process = 1
stop_process =  graceStop(q,1)

# COMMAND ----------

q.awaitTermination()
