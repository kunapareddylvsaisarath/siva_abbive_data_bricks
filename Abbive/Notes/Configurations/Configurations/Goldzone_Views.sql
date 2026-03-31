-- Databricks notebook source
-- MAGIC %md
-- MAGIC #GoldZone Views Queries

-- COMMAND ----------

-- DBTITLE 1,goldzone.sourcefilespath_v
-- MAGIC %python
-- MAGIC ADLSGEN2URL = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ADLSGen2StorageURL")
-- MAGIC ADLSGEN2SASToken = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ADLSGEN2SASToken")
-- MAGIC
-- MAGIC STORAGEACCOUNTURL = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","StorageAccountURL")
-- MAGIC STORAGEACCOUNTSASTOKEN = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","StorageAccountSASToken")
-- MAGIC
-- MAGIC ARCHIEVESTORAGEACCOUNTURL = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ArchieveStorageAccountURL").replace("com1archive2010-2015/", "")
-- MAGIC ARCHIEVESTORAGEACCOUNTSASToken = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ArchieveStorageAccountSASToken")
-- MAGIC
-- MAGIC spark.sql(''' 
-- MAGIC CREATE OR REPLACE VIEW cd_dev.goldzone.sourcefilespath_v AS
-- MAGIC   Select  LF.FileNameUUID,UPPER(LF.ExternalSerialNbr) AS ExternalSerialNbr,UPPER(LF.InternalSerialNbr) AS InternalSerialNbr,
-- MAGIC           ConfigId,SourceFilePath,SourceFileName,
-- MAGIC           CASE WHEN SourceFilePath like 'DeviceLogs%' THEN CONCAT('{0}/raw/',LF.SourceFilePath,'{1}')
-- MAGIC                WHEN SourceFilePath like 'com1archive2010-2015%' THEN CONCAT('{2}',LF.SourceFilePath,'{3}')
-- MAGIC                ELSE CONCAT('{4}',LF.SourceFilePath,'{5}') 
-- MAGIC                 END AS SourceFileURL,          
-- MAGIC           SourceFileSize,
-- MAGIC           FileNameDeviceTypeCd,FileNameMessageTypeCd,
-- MAGIC           replace(FileNameDtTmstmp,'.csv','') AS FileNameDtTmstmp,
-- MAGIC           FileNameApplicatorPortCd,FileNameCycleNbr,
-- MAGIC           SourceFileRecordCt,IsLogFileProcessedInd,LogFileStatus,ErrorMessage,LF.CreatedBy,LF.CreatedDt,LF.UpdatedBy,LF.UpdatedDt,
-- MAGIC           CASE WHEN FileNameDeviceTypeCd = 'COM1' THEN LogStartDate 
-- MAGIC                ELSE ifnull(LogStartDate,  to_timestamp(substring(FileNameDtTmstmp,1,14), 'yyyyMMddHHmmss')) 
-- MAGIC                END AS LogStartDate,
-- MAGIC           CASE WHEN FileNameDeviceTypeCd = 'COM1' THEN LogEndDate 
-- MAGIC                ELSE ifnull(LogEndDate,  to_timestamp(substring(FileNameDtTmstmp,1,14), 'yyyyMMddHHmmss')) 
-- MAGIC                END AS LogEndDate,
-- MAGIC           DSE.TimeZoneDesc,               
-- MAGIC           row_number() OVER(PARTITION BY ifnull(LF.InternalSerialNbr,''),ifnull(LF.ExternalSerialNbr,''),ifnull(LF.FileNameDeviceTypeCd,''),ifnull(LF.FileNameMessageTypeCd,''),LF.FileNameDtTmstmp,ifnull(FileNameApplicatorPortCd,''),ifnull(LF.FileNameCycleNbr,'') 
-- MAGIC                             ORDER BY CASE WHEN FileNameDeviceTypeCd = 'COM1' THEN LogStartDate 
-- MAGIC                                           ELSE ifnull(LogStartDate,  to_timestamp(substring(FileNameDtTmstmp,1,14), 'yyyyMMddHHmmss')) 
-- MAGIC                                     END DESC,
-- MAGIC                                       LF.UpdatedDt DESC) AS UniqueSequence
-- MAGIC   FROM    cd_dev.silverzone.LogSourceFilesProcessed AS LF
-- MAGIC     LEFT JOIN cd_dev.silverzone.dimstartendparameter AS DSE
-- MAGIC             ON LF.FileNameUUID = DSE.FileNameUUID  
-- MAGIC '''.format(ADLSGEN2URL,ADLSGEN2SASToken,ARCHIEVESTORAGEACCOUNTURL,ARCHIEVESTORAGEACCOUNTSASToken,STORAGEACCOUNTURL,STORAGEACCOUNTSASTOKEN))
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. assertlogassociation_v

-- COMMAND ----------

CREATE  OR REPLACE VIEW cd_dev.goldzone.assertlogassociation_v (
  CycleID,
  InternalSerialNbr,
  ExternalSerialNbr,
  FileNameUUID,
  SourceFileName,
  FileNameDeviceTypeCd,
  HdrFormatVersionCd,
  HdrStartDateGeneratedDt,
  HdrStartTimeGeneratedTmstmp,
  HdrLogtypeCd,
  HdrAppHeadNbr,
  FileNameApplicatorPortCd,
  HdrDestinationSubSystemCd,
  HdrSourceSubSystemCd,
  HdrCommandCD,
  HdrDataStoreID,
  EventStartTmstmp,
  EventEndTmstmp,
  SBCVersionNbr,
  PibVersionNbr,
  PibFactoryCRC,
  ApplicatorSerialNbr,
  PibInternalSerialNbr,
  CardSPSerialNbr,
  ContourProfileNbr,
  ContourProfileTreatmentTm,
  ContourProfileTreatmentTemp,
  History,
  StartFlg,
  PrevCTCnt,
  CycleErrorZCd,
  PIBTecVerNbr,
  ApplicatorConfigurantionNbr,
  Blob0CRCCd,
  Blob1CRCCd,
  PIBTecFactoryCRC,
  CurrentCycleNbr,
  ApplicatorInternalSerialNbr,
  CardPartNbr,
  AppSPAppletVerNbr,
  CardSPAppletVerNbr,
  PIBSPAppletVerNbr,
  controlChannelCd,
  PatientNextSameFlg,
  PatientGenderCd,
  PatientBodyPartNm,
  PatientNewFlg,
  BrilliantDistinctionID,
  CoolSculptingID,
  BDIDEncryptdFlg,
  AlleIDDecryptedFlg,
  AlleCertDt,
  MsrmntLogTreatmentFileNm,
  CycleStartDt,
  CycleStartTm,
  SwVersionNbr,
  Applicator1SerialNbr,
  Applicator2SerialNbr,
  ChsnTrtmntDurationTm,
  TrtmntPrtclCd,
  EQUIPCardPartNbr,
  SmartCardShippedInd,
  CoolDeviceShippedInd,
  SoldToAccountID,
  ShipToAccountID,
  UnqCyclSeq,
  DupCyclDateSeq,
  DupCyclUPMSeq,
  DupCyclDateUPMSeq,
  UnsoldFraudFlg,
  DuplicateCycleFlg,
  CycleUtilizationFraudFlg,
  CycleUsedCnt,
  CycleFinalStatusReasonDesc,
  CycleFinalStatusDesc,
  CycleEndDt,
  CycleEndTm,
  InterruptStart,
  CycleErrorCd,
  MaxCoolTmp,
  MaxApplicator1Tmp,
  MaxApplicator2Tmp,
  TtlTrtmntDurationTm,
  MsrmntLogTreatmentFileURLTxt,
  CompletedCycleUsedCnt,
  SmartCardShipDt,
  IsFraudCycleFlg,
  SourceFilePath,
  ULLogsUUID,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt,
  RowNbr,
  PreASSERTFileNameUUID,
  PreASSERTSourceFileName,
  PreASSERTSourceFilePath,
  PostASSERTFileNameUUID,
  PostASSERTSourceFileName,
  PostASSERTSourceFilePath,
  ROWNUM)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1684038423')
AS WITH associationData AS (
  Select FileNameUUID,InternalSerialNbr,ExternalSerialNbr,FileNameDeviceTypeCd,FileNameMessageTypeCd,UpdatedDt,LogStartDate,FileNameApplicatorPortCd,SourceFileName,SourceFilePath,
      ifnull(lag(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'1900-01-01T00:00:00.000+0000') AS PreviousLogStartDate,
      ifnull(lead(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'9999-12-31T00:00:00.000+0000') AS NextLogStartDate                            
  FROM   (Select *
          FROM cd_dev.goldzone.sourcefilespath_v AS LFL
          WHERE LFL.FileNameDeviceTypeCd IN ('COM3','CT','COM2')
            AND LFL.UniqueSequence = 1
            AND UPPER(LFL.FileNameMessageTypeCd) LIKE '%ASSERT%') AS GFL
)
Select *
FROM
(Select  ul.*,
         PreASSERT.FileNameUUID AS PreASSERTFileNameUUID,
         PreASSERT.SourceFileName AS PreASSERTSourceFileName,
         PreASSERT.SourceFilePath AS PreASSERTSourceFilePath,
         PostASSERT.FileNameUUID AS PostASSERTFileNameUUID,
         PostASSERT.SourceFileName AS PostASSERTSourceFileName,
         PostASSERT.SourceFilePath AS PostASSERTSourceFilePath,
         row_number() OVER (PARTITION BY ul.ULLogsUUID ORDER BY PreASSERT.LogStartDate DESC,PostASSERT.LogStartDate ASC) AS ROWNUM
FROM    cd_dev.silverzone.factullogs AS ul
  LEFT JOIN associationData AS PreASSERT
         ON  upper(ul.ExternalSerialNbr) = upper(PreASSERT.ExternalSerialNbr)
         AND ul.HdrStartTimeGeneratedTmstmp >= PreASSERT.LogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PreASSERT.NextLogStartDate
  LEFT JOIN associationData AS PostASSERT
         ON upper(ul.ExternalSerialNbr) = upper(PostASSERT.ExternalSerialNbr)
         AND ul.HdrStartTimeGeneratedTmstmp >= PostASSERT.PreviousLogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PostASSERT.LogStartDate
) AS T        
Where T.rownum = 1
ORDER BY T.InternalSerialNbr desc



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. cyclesdata_v

-- COMMAND ----------

-- CREATE or replace VIEW cd_dev.goldzone.cyclesdata_v (
--   CycleID,
--   BeginTime,
--   EndTime,
--   SBC_sn,
--   InternalSerialNbr,
--   HdrAppHeadNbr,
--   Applicator_sn,
--   APP_sn,
--   ssAPPident,
--   EZCard,
--   CyclesLeft,
--   Status,
--   ssWinCE,
--   InterruptStart,
--   InterruptStatus,
--   InterruptError,
--   SameNextPatient,
--   ReturningPatient,
--   PatientType,
--   TreatmentBodyPart,
--   CoolSculptingID2,
--   CoolSculptingID,
--   ScannedID,
--   UpperModuleNumber,
--   ProductTypeCd,
--   Applicator2SerialNbr,
--   MaxCoolTmp,
--   MaxApplicator1Tmp,
--   MaxApplicator2Tmp,
--   TtlTrtmntDurationTm,
--   TrtmntPrtclCd,
--   ChsnTrtmntDurationTm,
--   AlleIDEncryptedFlg,
--   AlleIDDecryptedFlg,
--   ApplicatorFrndlyNm,
--   ApplicatorEngnrNm,
--   AlleCertDt,
--   FileNameUUID,
--   ErrorDeviceScreen,
--   ErrorDescription,
--   CreatedBy,
--   CreatedDt,
--   UpdatedBy,
--   UpdatedDt)
-- TBLPROPERTIES (
--   'transient_lastDdlTime' = '1718218381')
-- AS (
-- SELECT
-- CYC.CycleID, 
-- cast(date_format(cyc.HdrStartTimeGeneratedTmstmp,'yyyy-MM-dd HH:mm:ss') as TIMESTAMP) as BeginTime,
-- cast(date_format(cyc.EventEndTmstmp ,'yyyy-MM-dd HH:mm:ss') as TIMESTAMP) as  EndTime,
-- CYC.InternalSerialNbr AS SBC_sn,
-- CYC.InternalSerialNbr,
-- CYC.HdrAppHeadNbr,
-- ifnull(CYC.Applicator1SerialNbr,CYC.ApplicatorSerialNbr) as Applicator_sn,
-- CYC.ApplicatorInternalSerialNbr as APP_sn,
-- CYC.ApplicatorConfigurantionNbr as ssAPPident,
-- CYC.CardSPSerialNbr as EZCard,
-- CYC.CurrentCycleNbr as CyclesLeft,
-- CYC.CycleFinalStatusReasonDesc as Status,
-- ifnull(CYC.SwVersionNbr,SBCVersionNbr) as ssWinCE,
-- CYC.InterruptStart,
-- CYC.CycleFinalStatusReasonDesc as InterruptStatus,
-- ifnull(CYC.CycleErrorCd,CYC.CycleErrorZCd)  as InterruptError,
-- CYC.PatientNextSameFlg as SameNextPatient,
-- CYC.PatientNewFlg as ReturningPatient,
-- CYC.PatientGenderCd as PatientType,
-- CYC.PatientBodyPartNm as TreatmentBodyPart,
-- CYC.BrilliantDistinctionID as CoolSculptingID2,
-- CYC.CoolSculptingID, 
-- consumer.ScannedID,
-- CYC.ExternalSerialNbr as UpperModuleNumber,
-- CYC.FileNameDeviceTypeCd as ProductTypeCd,
-- CYC.Applicator2SerialNbr,
-- CYC.MaxCoolTmp,
-- CYC.MaxApplicator1Tmp,
-- CYC.MaxApplicator2Tmp,
-- CYC.TtlTrtmntDurationTm,
-- CYC.TrtmntPrtclCd,
-- CYC.ChsnTrtmntDurationTm,
-- CYC.BDIDEncryptdFlg AS AlleIDEncryptedFlg,
-- CYC.AlleIDDecryptedFlg, 
-- AppVer.ApplicatorFrndlyNm,
-- Appver.ApplicatorEngnrNm,
-- CYC.AlleCertDt,
-- CYC.FileNameUUID,
-- err.EventText as ErrorDeviceScreen,
-- err.ErrorDescription,
-- CYC.CreatedBy,
-- CYC.CreatedDt,
-- CYC.UpdatedBy,
-- CYC.UpdatedDt
-- FROM cd_dev.silverzone.FACTULLogs CYC
-- LEFT JOIN cd_dev.silverzone.DIMApplicatorVerCd AppVer
-- ON AppVer.SSAPPIdentID = Left(CYC.ApplicatorConfigurantionNbr,7)
-- left join cd_dev.silverzone.errordescription Err  
-- on iff(err.DeviceType= 'CT', err.ErrorCode , substr(err.ErrorCode,2)) = ifnull(CYC.CycleErrorCd,CYC.CycleErrorZCd)   and cyc.FileNameDeviceTypeCd = err.DeviceType
-- left join cd_dev.promotion.dim_consumer consumer
-- on consumer.CoolsculptingId = cyc.CoolSculptingID
-- Where CYC.ExternalSerialNbr <> 'Unkonwn'
-- and CYC.EventStartTmstmp is not Null
-- )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. cyclesdatacom1_v

-- COMMAND ----------

-- CREATE VIEW cd_dev.goldzone.cyclesdatacom1_v (
--   ULLogsUUID,
--   CycleID,
--   SBC_sn,
--   EZCard,
--   CyclesLeft,
--   BeginTime,
--   InterruptStart,
--   InterruptStatus,
--   InterruptError,
--   InterruptEnd,
--   EndTime,
--   Status,
--   ErrorMessage,
--   precycle,
--   PostCycle,
--   ssWinCE,
--   ssCIB,
--   ssAPP,
--   ssTEC,
--   ssAPPident,
--   ProfilePN,
--   ProfileIndex,
--   SameNextPatient,
--   ReturningPatient,
--   PatientType,
--   TreatmentBodyPart,
--   TreatmentType,
--   ReturningCoolSculpting,
--   APP_sn,
--   CIB_sn,
--   DTC,
--   ProfileTime,
--   ProfileTemp,
--   AlledID,
--   TreatmentPlanNumber,
--   UpperModuleNumber,
--   EncryptedAlleID,
--   Applicator_sn,
--   Contour,
--   Adapter_sn,
--   AppBlobCRC,
--   AppBlobVersion,
--   SoftwareVerTxt,
--   UnsoldFraudFlg,
--   SoldToAccountID,
--   ShipToAccountID,
--   EQUIPCardPartNbr,
--   CoolSculptingID,
--   CoolSculptingID2,
--   ssAdapter,
--   DuplicateCycleFlg,
--   FileNameUUID,
--   SourceFilePath,
--   SourceFileName,
--   ErrorDeviceScreen,
--   ErrorDescription,
--   CreatedBy,
--   CreatedDt,
--   UpdatedBy,
--   UpdatedDt)
-- TBLPROPERTIES (
--   'transient_lastDdlTime' = '1695961956')
-- AS SELECT 
--                                                                         CYC.ULLogsUUID,
--                                                                         CYC.CycleID,
--                                                                         CYC.SBC_sn,
--                                                                         CYC.EZCard,
--                                                                         CYC.CyclesLeft,
--                                                                         CYC.BeginTime,
--                                                                         CYC.InterruptStart,
--                                                                         CYC.InterruptStatus,
--                                                                         CYC.InterruptError,
--                                                                         CYC.InterruptEnd,
--                                                                         CYC.EndTime,
--                                                                         CYC.Status,
--                                                                         CYC.ErrorMessage,
--                                                                         CYC.precycle,
--                                                                         CYC.PostCycle,
--                                                                         CYC.ssWinCE,
--                                                                         CYC.ssCIB,
--                                                                         CYC.ssAPP,
--                                                                         CYC.ssTEC,
--                                                                         CYC.ssAPPident,
--                                                                         CYC.ProfilePN,
--                                                                         CYC.ProfileIndex,
--                                                                         CYC.SameNextPatient,
--                                                                         CYC.ReturningPatient,
--                                                                         CYC.PatientType,
--                                                                         CYC.TreatmentBodyPart,
--                                                                         CYC.TreatmentType,
--                                                                         CYC.ReturningCoolSculpting,
--                                                                         CYC.APP_sn,
--                                                                         CYC.CIB_sn,
--                                                                         CYC.DTC,
--                                                                         CYC.ProfileTime,
--                                                                         CYC.ProfileTemp,
--                                                                         CYC.AlledID,
--                                                                         CYC.TreatmentPlanNumber,
--                                                                         CYC.ExternalSerialNbr as UpperModuleNumber,
--                                                                         CYC.EncryptedAlleID,
--                                                                         CYC.Applicator_sn,
--                                                                         CYC.Contour,
--                                                                         case when Adapter_sn='Unknown' then 'U' else Adapter_sn end as Adapter_sn,
--                                                                         CYC.AppBlobCRC,
--                                                                         CYC.AppBlobVersion,
--                                                                         CYC.SoftwareVerTxt,
--                                                                         CYC.UnsoldFraudFlg,
--                                                                         CYC.SoldToAccountID,
--                                                                         CYC.ShipToAccountID,
--                                                                         CYC.EQUIPCardPartNbr,
--                                                                         CYC.CoolSculptingID,
--                                                                         CYC.CoolSculptingID2,
--                                                                         CYC.ssAdapter,
--                                                                         CYC.DuplicateCycleFlg,
--                                                                         CYC.FileNameUUID,
--                                                                         CYC.SourceFilePath,
--                                                                         CYC.SourceFileName,
--                                                                         err.EventText as ErrorDeviceScreen,
--                                                                         err.ErrorDescription,
--                                                                         CYC.CreatedBy,
--                                                                         CYC.CreatedDt,
--                                                                         CYC.UpdatedBy,
--                                                                         CYC.UpdatedDt
                                                                        
                                                                        
--  FROM cd_dev.silverzone.factullogs_com1 CYC
--  left join cd_dev.silverzone.errordescription Err  
-- on Err.ErrorCode = CYC.ErrorMessage and Err.DeviceType = 'COM1'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. device_v

-- COMMAND ----------

CREATE OR REPLACE VIEW  cd_dev.goldzone.device_v 
AS
WITH syslogs AS (
  Select
    ExternalSerialNbr,
    InternalSerialNbr,
    FileNameDeviceTypeCd,
    max(HdrTimeGeneratedTmstmp) AS HdrTimeGeneratedTmstmp,
    max(LatestSoftwareVersionNbr) AS LatestSoftwareVersionNbr,
    max(IntMobileEquipID) AS IntMobileEquipID,
    max(IntMobileSubscriberID) AS IntMobileSubscriberID,
    max(ProviderNetworkNm) AS ProviderNetworkNm,
    max(LocalizationAreaCd) AS LocalizationAreaCd,
    max(CellID) AS CellID,
    max(CrespID) AS CrespID,
    max(ScrespID) AS ScrespID,
    max(CountryNbrCd) AS CountryNbrCd,
    max(OperatorCd) AS OperatorCd,
    max(CoolDeviceSerialNbr) AS CoolDeviceSerialNbr,
    max(IoTHubNm) AS IoTHubNm,
    max(EndPointNm) AS EndPointNm,
    max(TPMKey) AS TPMKey,
    coalesce(max(SIMCardID), 'NoSysLogEntry') AS SIMCardID,
    max(ModemStatusCd) AS ModemStatusCd,
    max(ModemStatusDesc) AS ModemStatusDesc,
    max(
      case
        when SIMCardID is not null then HdrTimeGeneratedTmstmp
      end
    ) AS SIMCardLastReportedDtTmstmp,
    max(HdrTimeGeneratedTmstmp) AS LastContactDtTmstmp,
    case
      when to_date(max(HdrTimeGeneratedTmstmp)) >= Date_add(now(), -30) then 'Connected'
      else 'Disconnected'
    end As ConnectionState,
    max(RSRP) AS RSRP, --Changes as Per AAIOT-3116 
    max(RSRQ) AS RSRQ  --Changes as Per AAIOT-3116 
  from
    (
      select
        *,
        row_number() over (
            partition by InternalSerialNbr, HdrDataContractNbr
            order by updateddt desc
          ) as rk
      from
        cd_dev.silverzone.FACTSysLogs
    )
  where
    rk = 1
  Group By
    ExternalSerialNbr,
    InternalSerialNbr,
    FileNameDeviceTypeCd
),
com1 as (
  Select
    ExternalSerialNbr,
    InternalSerialNbr,
    'COM1' AS FileNameDeviceTypeCd,
    HdrTimeGeneratedTmstmp,
    NULL AS LatestSoftwareVersionNbr,
    NULL AS IntMobileEquipID,
    NULL AS IntMobileSubscriberID,
    NULL AS ProviderNetworkNm,
    NULL AS LocalizationAreaCd,
    NULL AS CellID,
    NULL AS CrespID,
    NULL AS ScrespID,
    NULL AS CountryNbrCd,
    NULL AS OperatorCd,
    NULL AS CoolDeviceSerialNbr,
    NULL AS IoTHubNm,
    NULL AS EndPointNm,
    NULL AS TPMKey,
    SIMCardID,
    NULL AS ModemStatusCd,
    NULL AS ModemStatusDesc,
    case
      when SIMCardID is not null then LastContactDtTmstmp
    end AS SIMCardLastReportedDtTmstmp,
    LastContactDtTmstmp,
    case
      when to_date(LastContactDtTmstmp) >= Date_add(now(), -30) then 'Connected'
      else 'Disconnected'
    end as ConnectionState,
    NULL AS RSRP, --Changes as Per AAIOT-3116 
    NULL AS RSRQ  --Changes as Per AAIOT-3116 
  FROM
    cd_dev.silverzone.factlastcontact_com1 AS C1
),
device as (
  Select
    *
  from
    syslogs
  Union All
  Select
    *
  from
    com1
),
DeviceTwin AS (
  Select
    *
  FROM
    (
      Select
        *,
        row_number() OVER (
            partition by Ifnull(TWIN.ExternalSerialNbr, DeviceID)
            ORDER BY TWIN.LastActivityTime DESC
          ) AS ROWNUM
      From
        cd_dev.silverzone.FACTDeviceTwin AS TWIN
    ) AS DT
  Where
    DT.ROWNUM = 1
),
device_v as (
  SELECT
    ifnull(TWIN.ExternalSerialNbr, sys.ExternalSerialNbr) AS ExternalSerialNbr,
    ifnull(
      TWIN.DeviceID, ifnull(TWIN.InternalSerialNbr, sys.InternalSerialNbr)
    ) AS InternalSerialNbr,
    sys.HdrTimeGeneratedTmstmp,
    DtTmstmp As DeviceTwinDtTmstmp,
    DeviceTwinDt,
    ifnull(TWIN.Properties_Reported_ProductType, sys.FileNameDeviceTypeCd) as SysDeviceTypeCd,
    sys.LatestSoftwareVersionNbr,
    sys.IntMobileEquipID,
    sys.IntMobileSubscriberID,
    sys.ProviderNetworkNm,
    sys.LocalizationAreaCd,
    sys.CellID,
    sys.CrespID,
    sys.ScrespID,
    sys.CountryNbrCd,
    sys.OperatorCd,
    sys.CoolDeviceSerialNbr,
    sys.IoTHubNm,
    sys.EndPointNm,
    sys.TPMKey,
    coalesce(sys.SIMCardID, 'NoSysLogEntry') AS SIMCardID,
    sys.ModemStatusCd,
    sys.ModemStatusDesc,
    sys.SIMCardLastReportedDtTmstmp,
    Etag,
    StatusFlag,
    StatusUpdatedTime,
    coalesce(TWIN.ConnectionState, SYS.ConnectionState) as ConnectionState,
    sys.RSRP,  --Changes as Per AAIOT-3116 
    sys.RSRQ,  --Changes as Per AAIOT-3116 
    ifnull(LastActivityTime, sys.LastContactDtTmstmp) AS LastActivityTime,
    CloudToDeviceMessageCount,
    AuthenticationType,
    VersionNbr,
    Properties_Desired_Account,
    Properties_Desired_Key,
    Properties_Desired_FormatVersion,
    Properties_Desired_Packages,
    Properties_Desired_Upload,
    Properties_Desired_TwinPollingRate,
    Properties_Desired_SystemLock,
    Properties_Desired_DeviceEvents,
    Properties_Reported_AssignedIoTHub,
    Properties_Reported_FormatVersion,
    Properties_Reported_TotalChannels,
    Properties_Reported_PagerEnabled,
    Properties_Reported_SystemType,
    Properties_Reported_AliasName,
    Properties_Reported_Language,
    Properties_Reported_Packages,
    Properties_Reported_SystemLockStatus,
    Properties_Reported_ReleaseVersion,
    Properties_Reported_DeviceEvents,
    Properties_Reported_FluidFilter,
    Properties_Reported_AirFilter,
    Properties_Reported_HandPiece,
    Properties_Reported_ElectrodeConnector,
    Capabilities_IOTEdge,
    App1Version,
    App2Version,
    AuxiliaryAppVersion,
    CCBVersion,
    ConfiguratorVersion,
    CoolerVersion,
    ENCAppVersion,
    InstallerVersion,
    LogExtractorVersion,
    LogExtractorUIVersion,
    LogSVCVersion,
    MaestroVersion,
    MedAppVersion,
    MedAppUIVersion,
    OSVersion,
    PIBFPGAVersion,
    PIBTec1FPGAVersion,
    PIBTec2FPGAVersion,
    PibVersion,
    PsbHubServiceVersion,
    SecurityServiceVersion,
    Vac1Version,
    Vac2Version,
    DevicePowerManagerVersion,
    FMSVersion,
    PPS_MCUVersion,
    PPS_DSPVersion,
    PhoneHomeZMajorVerCd,
    PhoneHomeZMinorVerCd,
    row_number() over (
        PARTITION BY coalesce(TWIN.ExternalSerialNbr, sys.ExternalSerialNbr)
        order by coalesce(sys.LastContactDtTmstmp, LastActivityTime) desc
      ) as rk
  FROM
    device AS SYS
      FULL JOIN DeviceTwin AS TWIN
        ON sys.InternalSerialNbr = TWIN.DeviceID
)
select
  ExternalSerialNbr,
  InternalSerialNbr,
  HdrTimeGeneratedTmstmp,
  DeviceTwinDtTmstmp,
  DeviceTwinDt,
  SysDeviceTypeCd,
  LatestSoftwareVersionNbr,
  IntMobileEquipID,
  IntMobileSubscriberID,
  ProviderNetworkNm,
  LocalizationAreaCd,
  CellID,
  CrespID,
  ScrespID,
  CountryNbrCd,
  OperatorCd,
  CoolDeviceSerialNbr,
  IoTHubNm,
  EndPointNm,
  TPMKey,
  SIMCardID,
  ModemStatusCd,
  ModemStatusDesc,
  SIMCardLastReportedDtTmstmp,
  Etag,
  StatusFlag,
  StatusUpdatedTime,
  ConnectionState,
  RSRP,   --Changes as Per AAIOT-3116 
  RSRQ,   --Changes as Per AAIOT-3116 
  LastActivityTime,
  CloudToDeviceMessageCount,
  AuthenticationType,
  VersionNbr,
  Properties_Desired_Account,
  Properties_Desired_Key,
  Properties_Desired_FormatVersion,
  Properties_Desired_Packages,
  Properties_Desired_Upload,
  Properties_Desired_TwinPollingRate,
  Properties_Desired_SystemLock,
  Properties_Desired_DeviceEvents,
  Properties_Reported_AssignedIoTHub,
  Properties_Reported_FormatVersion,
  Properties_Reported_TotalChannels,
  Properties_Reported_PagerEnabled,
  Properties_Reported_SystemType,
  Properties_Reported_AliasName,
  Properties_Reported_Language,
  Properties_Reported_Packages,
  Properties_Reported_SystemLockStatus,
  Properties_Reported_ReleaseVersion,
  Properties_Reported_DeviceEvents,
  Properties_Reported_FluidFilter,
  Properties_Reported_AirFilter,
  Properties_Reported_HandPiece,
  Properties_Reported_ElectrodeConnector,
  Capabilities_IOTEdge,
  App1Version,
  App2Version,
  AuxiliaryAppVersion,
  CCBVersion,
  ConfiguratorVersion,
  CoolerVersion,
  ENCAppVersion,
  InstallerVersion,
  LogExtractorVersion,
  LogExtractorUIVersion,
  LogSVCVersion,
  MaestroVersion,
  MedAppVersion,
  MedAppUIVersion,
  OSVersion,
  PIBFPGAVersion,
  PIBTec1FPGAVersion,
  PIBTec2FPGAVersion,
  PibVersion,
  PsbHubServiceVersion,
  SecurityServiceVersion,
  Vac1Version,
  Vac2Version,
  DevicePowerManagerVersion,
  FMSVersion,
  PPS_MCUVersion,
  PPS_DSPVersion,
  PhoneHomeZMajorVerCd,
  PhoneHomeZMinorVerCd
from
  device_v
where
  rk = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.mllogassociation_v

-- COMMAND ----------

CREATE  OR REPLACE VIEW  cd_dev.goldzone.mllogassociation_v (
  CycleID,
  InternalSerialNbr,
  ExternalSerialNbr,
  FileNameUUID,
  SourceFileName,
  FileNameDeviceTypeCd,
  HdrFormatVersionCd,
  HdrStartDateGeneratedDt,
  HdrStartTimeGeneratedTmstmp,
  HdrLogtypeCd,
  HdrAppHeadNbr,
  FileNameApplicatorPortCd,
  HdrDestinationSubSystemCd,
  HdrSourceSubSystemCd,
  HdrCommandCD,
  HdrDataStoreID,
  EventStartTmstmp,
  EventEndTmstmp,
  SBCVersionNbr,
  PibVersionNbr,
  PibFactoryCRC,
  ApplicatorSerialNbr,
  PibInternalSerialNbr,
  CardSPSerialNbr,
  ContourProfileNbr,
  ContourProfileTreatmentTm,
  ContourProfileTreatmentTemp,
  History,
  StartFlg,
  PrevCTCnt,
  CycleErrorZCd,
  PIBTecVerNbr,
  ApplicatorConfigurantionNbr,
  Blob0CRCCd,
  Blob1CRCCd,
  PIBTecFactoryCRC,
  CurrentCycleNbr,
  ApplicatorInternalSerialNbr,
  CardPartNbr,
  AppSPAppletVerNbr,
  CardSPAppletVerNbr,
  PIBSPAppletVerNbr,
  controlChannelCd,
  PatientNextSameFlg,
  PatientGenderCd,
  PatientBodyPartNm,
  PatientNewFlg,
  BrilliantDistinctionID,
  CoolSculptingID,
  BDIDEncryptdFlg,
  AlleIDDecryptedFlg,
  AlleCertDt,
  MsrmntLogTreatmentFileNm,
  CycleStartDt,
  CycleStartTm,
  SwVersionNbr,
  Applicator1SerialNbr,
  Applicator2SerialNbr,
  ChsnTrtmntDurationTm,
  TrtmntPrtclCd,
  EQUIPCardPartNbr,
  SmartCardShippedInd,
  CoolDeviceShippedInd,
  SoldToAccountID,
  ShipToAccountID,
  UnqCyclSeq,
  DupCyclDateSeq,
  DupCyclUPMSeq,
  DupCyclDateUPMSeq,
  UnsoldFraudFlg,
  DuplicateCycleFlg,
  CycleUtilizationFraudFlg,
  CycleUsedCnt,
  CycleFinalStatusReasonDesc,
  CycleFinalStatusDesc,
  CycleEndDt,
  CycleEndTm,
  InterruptStart,
  CycleErrorCd,
  MaxCoolTmp,
  MaxApplicator1Tmp,
  MaxApplicator2Tmp,
  TtlTrtmntDurationTm,
  MsrmntLogTreatmentFileURLTxt,
  CompletedCycleUsedCnt,
  SmartCardShipDt,
  IsFraudCycleFlg,
  SourceFilePath,
  ULLogsUUID,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt,
  RowNbr,
  PreMLFileNameUUID,
  PreMLSourceFileName,
  PreMLSourceFilePath,
  PostMLFileNameUUID,
  PostMLSourceFileName,
  PostMLSourceFilePath,
  ROWNUM)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1684038419')
AS WITH associationData AS (
  Select FileNameUUID,InternalSerialNbr,ExternalSerialNbr,FileNameDeviceTypeCd,FileNameMessageTypeCd,UpdatedDt,LogStartDate,FileNameApplicatorPortCd,SourceFileName,SourceFilePath,
      ifnull(lag(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'1900-01-01T00:00:00.000+0000') AS PreviousLogStartDate,
      ifnull(lead(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'9999-12-31T00:00:00.000+0000') AS NextLogStartDate                            
  FROM   (Select *
          FROM cd_dev.goldzone.sourcefilespath_v AS LFL
          WHERE LFL.FileNameDeviceTypeCd IN ('COM3','CT','COM2')
            AND LFL.UniqueSequence = 1
            AND LFL.FileNameMessageTypeCd = 'ML') AS GFL
)
Select *
FROM
(Select  ul.*,
         PreML.FileNameUUID AS PreMLFileNameUUID,
         PreML.SourceFileName AS PreMLSourceFileName,
         PreML.SourceFilePath AS PreMLSourceFilePath,
         PostML.FileNameUUID AS PostMLFileNameUUID,
         PostML.SourceFileName AS PostMLSourceFileName,
         PostML.SourceFilePath AS PostMLSourceFilePath,
         row_number() OVER (PARTITION BY ul.ULLogsUUID ORDER BY PreML.LogStartDate DESC,PostML.LogStartDate ASC) AS ROWNUM
FROM    cd_dev.silverzone.factullogs AS ul
  INNER JOIN cd_dev.goldzone.sourcefilespath_v AS slf
          ON ul.FileNameUUID = slf.FileNameUUID
  LEFT JOIN associationData AS PreML
         ON  upper(slf.ExternalSerialNbr) = upper(PreML.ExternalSerialNbr)
         AND upper(slf.InternalSerialNbr) = upper(PreML.InternalSerialNbr)
         AND slf.FileNameApplicatorPortCd = PreML.FileNameApplicatorPortCd
         AND ul.HdrStartTimeGeneratedTmstmp >= PreML.LogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PreML.NextLogStartDate
  LEFT JOIN associationData AS PostML
         ON upper(slf.ExternalSerialNbr) = upper(PostML.ExternalSerialNbr)
         AND upper(slf.InternalSerialNbr) = upper(PostML.InternalSerialNbr)
         AND slf.FileNameApplicatorPortCd = PostML.FileNameApplicatorPortCd
         AND ul.HdrStartTimeGeneratedTmstmp >= PostML.PreviousLogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PostML.LogStartDate

) AS T        
Where T.rownum = 1
ORDER BY T.InternalSerialNbr desc



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. syslogassociation_v

-- COMMAND ----------

CREATE  OR REPLACE VIEW  cd_dev.goldzone.syslogassociation_v (
  CycleID,
  InternalSerialNbr,
  ExternalSerialNbr,
  FileNameUUID,
  SourceFileName,
  FileNameDeviceTypeCd,
  HdrFormatVersionCd,
  HdrStartDateGeneratedDt,
  HdrStartTimeGeneratedTmstmp,
  HdrLogtypeCd,
  HdrAppHeadNbr,
  FileNameApplicatorPortCd,
  HdrDestinationSubSystemCd,
  HdrSourceSubSystemCd,
  HdrCommandCD,
  HdrDataStoreID,
  EventStartTmstmp,
  EventEndTmstmp,
  SBCVersionNbr,
  PibVersionNbr,
  PibFactoryCRC,
  ApplicatorSerialNbr,
  PibInternalSerialNbr,
  CardSPSerialNbr,
  ContourProfileNbr,
  ContourProfileTreatmentTm,
  ContourProfileTreatmentTemp,
  History,
  StartFlg,
  PrevCTCnt,
  CycleErrorZCd,
  PIBTecVerNbr,
  ApplicatorConfigurantionNbr,
  Blob0CRCCd,
  Blob1CRCCd,
  PIBTecFactoryCRC,
  CurrentCycleNbr,
  ApplicatorInternalSerialNbr,
  CardPartNbr,
  AppSPAppletVerNbr,
  CardSPAppletVerNbr,
  PIBSPAppletVerNbr,
  controlChannelCd,
  PatientNextSameFlg,
  PatientGenderCd,
  PatientBodyPartNm,
  PatientNewFlg,
  BrilliantDistinctionID,
  CoolSculptingID,
  BDIDEncryptdFlg,
  AlleIDDecryptedFlg,
  AlleCertDt,
  MsrmntLogTreatmentFileNm,
  CycleStartDt,
  CycleStartTm,
  SwVersionNbr,
  Applicator1SerialNbr,
  Applicator2SerialNbr,
  ChsnTrtmntDurationTm,
  TrtmntPrtclCd,
  EQUIPCardPartNbr,
  SmartCardShippedInd,
  CoolDeviceShippedInd,
  SoldToAccountID,
  ShipToAccountID,
  UnqCyclSeq,
  DupCyclDateSeq,
  DupCyclUPMSeq,
  DupCyclDateUPMSeq,
  UnsoldFraudFlg,
  DuplicateCycleFlg,
  CycleUtilizationFraudFlg,
  CycleUsedCnt,
  CycleFinalStatusReasonDesc,
  CycleFinalStatusDesc,
  CycleEndDt,
  CycleEndTm,
  InterruptStart,
  CycleErrorCd,
  MaxCoolTmp,
  MaxApplicator1Tmp,
  MaxApplicator2Tmp,
  TtlTrtmntDurationTm,
  MsrmntLogTreatmentFileURLTxt,
  CompletedCycleUsedCnt,
  SmartCardShipDt,
  IsFraudCycleFlg,
  SourceFilePath,
  ULLogsUUID,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt,
  RowNbr,
  PreSYSFileNameUUID,
  PreSYSSourceFileName,
  PreSYSSourceFilePath,
  PostSYSFileNameUUID,
  PostSYSSourceFileName,
  PostSYSSourceFilePath,
  ROWNUM)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1684038420')
AS WITH associationData AS (
  Select FileNameUUID,InternalSerialNbr,ExternalSerialNbr,FileNameDeviceTypeCd,FileNameMessageTypeCd,UpdatedDt,LogStartDate,FileNameApplicatorPortCd,SourceFileName,SourceFilePath,
      ifnull(lag(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'1900-01-01T00:00:00.000+0000') AS PreviousLogStartDate,
      ifnull(lead(LogStartDate) OVER (PARTITION BY ExternalSerialNbr,FileNameMessageTypeCd,FileNameApplicatorPortCd 
                            ORDER BY GFL.LogStartDate ASC,GFL.UpdatedDt DESC),'9999-12-31T00:00:00.000+0000') AS NextLogStartDate                            
  FROM   (Select *
          FROM cd_dev.goldzone.sourcefilespath_v AS LFL
          WHERE LFL.FileNameDeviceTypeCd IN ('COM3','CT','COM2')
            AND LFL.UniqueSequence = 1
            AND LFL.FileNameMessageTypeCd = 'SYS') AS GFL
)
Select *
FROM
(Select  ul.*,
         PreSYS.FileNameUUID AS PreSYSFileNameUUID,
         PreSYS.SourceFileName AS PreSYSSourceFileName,
         PreSYS.SourceFilePath AS PreSYSSourceFilePath,
         PostSYS.FileNameUUID AS PostSYSFileNameUUID,
         PostSYS.SourceFileName AS PostSYSSourceFileName,
         PostSYS.SourceFilePath AS PostSYSSourceFilePath,
         row_number() OVER (PARTITION BY ul.ULLogsUUID ORDER BY PreSYS.LogStartDate DESC,PostSYS.LogStartDate ASC) AS ROWNUM
FROM    cd_dev.silverzone.factullogs AS ul
  INNER JOIN cd_dev.goldzone.sourcefilespath_v AS slf
          ON ul.FileNameUUID = slf.FileNameUUID
  LEFT JOIN associationData AS PreSYS
         ON  upper(slf.ExternalSerialNbr) = upper(PreSYS.ExternalSerialNbr)
         AND ul.HdrStartTimeGeneratedTmstmp >= PreSYS.LogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PreSYS.NextLogStartDate
  LEFT JOIN associationData AS PostSYS
         ON upper(slf.ExternalSerialNbr) = upper(PostSYS.ExternalSerialNbr)
         AND ul.HdrStartTimeGeneratedTmstmp >= PostSYS.PreviousLogStartDate AND ul.HdrStartTimeGeneratedTmstmp < PostSYS.LogStartDate

) AS T        
Where T.rownum = 1
ORDER BY T.InternalSerialNbr desc



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. uelogassociation_v

-- COMMAND ----------

CREATE  OR REPLACE VIEW  cd_dev.goldzone.uelogassociation_v (
  CycleID,
  InternalSerialNbr,
  ExternalSerialNbr,
  FileNameUUID,
  SourceFileName,
  FileNameDeviceTypeCd,
  HdrFormatVersionCd,
  HdrStartDateGeneratedDt,
  HdrStartTimeGeneratedTmstmp,
  HdrLogtypeCd,
  HdrAppHeadNbr,
  FileNameApplicatorPortCd,
  HdrDestinationSubSystemCd,
  HdrSourceSubSystemCd,
  HdrCommandCD,
  HdrDataStoreID,
  EventStartTmstmp,
  EventEndTmstmp,
  SBCVersionNbr,
  PibVersionNbr,
  PibFactoryCRC,
  ApplicatorSerialNbr,
  PibInternalSerialNbr,
  CardSPSerialNbr,
  ContourProfileNbr,
  ContourProfileTreatmentTm,
  ContourProfileTreatmentTemp,
  History,
  StartFlg,
  PrevCTCnt,
  CycleErrorZCd,
  PIBTecVerNbr,
  ApplicatorConfigurantionNbr,
  Blob0CRCCd,
  Blob1CRCCd,
  PIBTecFactoryCRC,
  CurrentCycleNbr,
  ApplicatorInternalSerialNbr,
  CardPartNbr,
  AppSPAppletVerNbr,
  CardSPAppletVerNbr,
  PIBSPAppletVerNbr,
  controlChannelCd,
  PatientNextSameFlg,
  PatientGenderCd,
  PatientBodyPartNm,
  PatientNewFlg,
  BrilliantDistinctionID,
  CoolSculptingID,
  BDIDEncryptdFlg,
  AlleIDDecryptedFlg,
  AlleCertDt,
  MsrmntLogTreatmentFileNm,
  CycleStartDt,
  CycleStartTm,
  SwVersionNbr,
  Applicator1SerialNbr,
  Applicator2SerialNbr,
  ChsnTrtmntDurationTm,
  TrtmntPrtclCd,
  EQUIPCardPartNbr,
  SmartCardShippedInd,
  CoolDeviceShippedInd,
  SoldToAccountID,
  ShipToAccountID,
  UnqCyclSeq,
  DupCyclDateSeq,
  DupCyclUPMSeq,
  DupCyclDateUPMSeq,
  UnsoldFraudFlg,
  DuplicateCycleFlg,
  CycleUtilizationFraudFlg,
  CycleUsedCnt,
  CycleFinalStatusReasonDesc,
  CycleFinalStatusDesc,
  CycleEndDt,
  CycleEndTm,
  InterruptStart,
  CycleErrorCd,
  MaxCoolTmp,
  MaxApplicator1Tmp,
  MaxApplicator2Tmp,
  TtlTrtmntDurationTm,
  MsrmntLogTreatmentFileURLTxt,
  CompletedCycleUsedCnt,
  SmartCardShipDt,
  IsFraudCycleFlg,
  SourceFilePath,
  ULLogsUUID,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt,
  RowNbr,
  TreatmentUEFileNameUUID,
  TreatmentUESourceFileName,
  TreatmentUESourceFilePath,
  PreUEFileNameUUID,
  PreUESourceFileName,
  PreUESourceFilePath,
  PostueFileNameUUID,
  PostueSourceFileName,
  PostueSourceFilePath,
  ROWNUM)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1684038422')
AS WITH associationData AS (
  Select GFL.*,
      ifnull(lag(HdrTimeGeneratedTmstmp) OVER (PARTITION BY GFL.ExternalSerialNbr,GFL.InternalSerialNbr,GFL.FileNameDeviceTypeCd,GFL.HdrAppHeadNbr 
                            ORDER BY GFL.HdrTimeGeneratedTmstmp ASC),'1900-01-01T00:00:00.000+0000') AS PreviousHdrTmstamp,
      ifnull(lead(HdrTimeGeneratedTmstmp) OVER (PARTITION BY GFL.ExternalSerialNbr,GFL.InternalSerialNbr,GFL.FileNameDeviceTypeCd,GFL.HdrAppHeadNbr 
                            ORDER BY GFL.HdrTimeGeneratedTmstmp ASC),'9999-12-31T00:00:00.000+0000') AS NextHdrTmstamp,  
      LF.SourceFileName,
      LF.SourceFilePath
  FROM   cd_dev.silverzone.factuelogs AS GFL
  LEFT JOIN  cd_dev.goldzone.sourcefilespath_v AS LF
        ON GFL.FileNameUUID = LF.FileNameUUID
)
Select *
FROM
(Select ul.*,
         ue.FileNameUUID AS TreatmentUEFileNameUUID,
         ue.SourceFileName AS TreatmentUESourceFileName,
         ue.SourceFilePath AS TreatmentUESourceFilePath,
         Preue.FileNameUUID AS PreUEFileNameUUID,
         Preue.SourceFileName AS PreUESourceFileName,
         Preue.SourceFilePath AS PreUESourceFilePath,
         Postue.FileNameUUID AS PostueFileNameUUID,
         Postue.SourceFileName AS PostueSourceFileName,
         Postue.SourceFilePath AS PostueSourceFilePath,
         row_number() OVER (PARTITION BY ul.ULLogsUUID ORDER BY ue.HdrTimeGeneratedTmstmp DESC,
                                                                Preue.HdrTimeGeneratedTmstmp DESC,
                                                                postue.HdrTimeGeneratedTmstmp ASC) AS ROWNUM
FROM    cd_dev.silverzone.factullogs AS ul
  LEFT join associationData As ue
                  On upper(ue.ExternalSerialNbr) = upper(ul.ExternalSerialNbr)
                  AND upper(ue.InternalSerialNbr) = upper(ul.InternalSerialNbr)
                  AND ue.FileNameDeviceTypeCd = ul.FileNameDeviceTypeCd
                  AND CASE WHEN ifnull(ue.HdrAppHeadNbr,0) = 0 THEN 'Y' WHEN ue.HdrAppHeadNbr = ul.HdrAppHeadNbr THEN 'Y' ELSE 'N' END = 'Y'
                  AND ue.HdrTimeGeneratedTmstmp between ul.EventStartTmstmp AND ul.EventEndTmstmp
  LEFT join associationData As Preue
                  On upper(Preue.ExternalSerialNbr) = upper(ul.ExternalSerialNbr)
                  AND upper(Preue.InternalSerialNbr) = upper(ul.InternalSerialNbr)
                  AND Preue.FileNameDeviceTypeCd = ul.FileNameDeviceTypeCd
                  AND CASE WHEN ifnull(Preue.HdrAppHeadNbr,0) = 0 THEN 'Y' WHEN Preue.HdrAppHeadNbr = ul.HdrAppHeadNbr THEN 'Y' ELSE 'N' END = 'Y'
                  AND ul.HdrStartTimeGeneratedTmstmp between Preue.HdrTimeGeneratedTmstmp AND Preue.NextHdrTmstamp
  LEFT join associationData As Postue
                  On upper(Postue.ExternalSerialNbr) = upper(ul.ExternalSerialNbr)
                  AND upper(Postue.InternalSerialNbr) = upper(ul.InternalSerialNbr)
                  AND Postue.FileNameDeviceTypeCd = ul.FileNameDeviceTypeCd
                  AND CASE WHEN ifnull(Postue.HdrAppHeadNbr,0) = 0 THEN 'Y' WHEN Postue.HdrAppHeadNbr = ul.HdrAppHeadNbr THEN 'Y' ELSE 'N' END = 'Y'
                  AND ul.HdrStartTimeGeneratedTmstmp between Postue.PreviousHdrTmstamp AND Postue.HdrTimeGeneratedTmstmp

) AS T        
Where T.rownum = 1
ORDER BY T.InternalSerialNbr desc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9.Missing Cycles Report

-- COMMAND ----------

-- CREATE OR REPLACE VIEW cd_dev.goldzone.MissingCyclesReport_v AS
-- with COM1_CTE as (
-- select 'COM1' as DeviceType,SBC_SN,BeginTime,EZCard,CyclesLeft, ifnull(lag(CyclesLeft) over(partition by EZCard order by CyclesLeft desc),CyclesLeft) as previousCyclesleft  , 
-- min(CyclesLeft) over(partition by EZCard) as MinCycle,
-- Max(CyclesLeft) over(partition by EZCard) as MaxCycle,
-- min(BeginTime) over(partition by EZCard) as MinBeginTime,
-- Max(BeginTime) over(partition by EZCard) as MaxBeginTime
-- from cd_dev.goldzone.cyclesdatacom1_v  
-- where  Ezcard in (select distinct(Ezcard) as EZcard  from cd_dev.goldzone.cyclesdatacom1_v  where createddt>=date_add(current_date(),-6*31 ) and EZCard is not null)
-- ),
-- COM1CyclesMissing as
-- (select *,explode(sequence(CyclesLeft+1, previousCyclesleft-1)) as MissingCycle
--  from COM1_CTE 
--  where (previousCyclesleft-CyclesLeft)>1),
-- COM3_CTE as (
-- select ProductTypeCd as DeviceType,SBC_SN,BeginTime,EZCard,CyclesLeft, ifnull(lag(CyclesLeft) over(partition by EZCard order by CyclesLeft desc),CyclesLeft) as previousCyclesleft  , 
-- min(CyclesLeft) over(partition by EZCard) as MinCycle,
-- Max(CyclesLeft) over(partition by EZCard) as MaxCycle,
-- min(BeginTime) over(partition by EZCard) as MinBeginTime,
-- Max(BeginTime) over(partition by EZCard) as MaxBeginTime
-- from cd_dev.goldzone.cyclesdata_v  
-- where  Ezcard in (select distinct(Ezcard) as EZcard  from cd_dev.goldzone.cyclesdata_v  where createddt>=date_add(current_date(),-6*31 ) and EZCard is not null)
-- ),
-- COM3CyclesMissing as
-- (select *,explode(sequence(CyclesLeft+1, previousCyclesleft-1)) as MissingCycle  
-- from COM3_CTE 
-- where (previousCyclesleft-CyclesLeft)>1),
-- COM3_COM1_MissingCysle as (
-- select 
-- DeviceType,
-- EZCard,
-- MissingCycle,
-- MaxBeginTime,
-- MinBeginTime,
-- MaxCycle,
-- MinCycle
-- from COM1CyclesMissing 
-- union 
-- select 
-- DeviceType,
-- EZCard,
-- MissingCycle,
-- MaxBeginTime,
-- MinBeginTime,
-- MaxCycle,
-- MinCycle
-- from COM3CyclesMissing )
 
-- select DeviceType,EZCard,MissingCycle,MaxBeginTime,MinBeginTime,MaxCycle,MinCycle,ShipToSalesRepNm as ShipToSalesRep,ShipToID from COM3_COM1_MissingCysle as MC
-- left Join cd_dev.goldzone.deviceownership_v as DOS on (MC.EZCard== DOS.SerialNumberNbr)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10.Data lineage view Operation dashboard

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ADLSGEN2URL = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ADLSGen2StorageURL")
-- MAGIC ADLSGEN2SASToken = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ADLSGEN2SASToken")
-- MAGIC
-- MAGIC spark.sql(''' 
-- MAGIC alter VIEW cd_dev.goldzone.DataLineage_v
-- MAGIC AS
-- MAGIC with logs_raw as 
-- MAGIC (SELECT l.SourceFileName, l.ErrorMessage, l.SourceContainerPath,
-- MAGIC         l.SourceFolderPath,l.DestinationFileName,l.SourceFileSize,l.LogType,
-- MAGIC         l.DestinationContainerPath,l.DestinationFolderPath,l.ConfigId,
-- MAGIC         l.CreatedDate, l.SourceTypeId,zipfileName,
-- MAGIC         CASE WHEN l.PipelineStatus IN ('Succeeded','Duplicate') THEN 'Succeeded'
-- MAGIC              WHEN l.PipelineStatus IN ('InProgress','Failed') THEN PipelineStatus
-- MAGIC              ELSE 'NotApplicable'
-- MAGIC              END  AS PipelineStatus_Derived,
-- MAGIC         PipelineStatus,     
-- MAGIC         row_number() over(partition by SourceContainerPath,SourceFolderPath,SourceFileName,
-- MAGIC                                        DestinationContainerPath,DestinationFolderPath,
-- MAGIC                                        DestinationFileName,SourceTypeId 
-- MAGIC                           Order By CASE WHEN l.PipelineStatus IN ('Succeeded') THEN 1
-- MAGIC              ELSE 2
-- MAGIC              END ASC,CreatedDate DESC) AS ROWNUM
-- MAGIC     FROM cd_dev.silverzone.factingestionlog AS l
-- MAGIC ),
-- MAGIC logs_silver as 
-- MAGIC (SELECT l.SourceFileName, l.ErrorMessage, 
-- MAGIC         l.SourceContainerPath,l.SourceFolderPath,l.DestinationFileName,l.SourceFileSize,
-- MAGIC         l.LogType,l.DestinationContainerPath,l.DestinationFolderPath,l.ConfigId,
-- MAGIC         l.CreatedDate, l.SourceTypeId,
-- MAGIC         CASE WHEN l.PipelineStatus IN ('Succeeded','Duplicate') THEN 'Succeeded'
-- MAGIC              WHEN ifnull(l.PipelineStatus,'NULL') IN ('InProgress','Failed','NULL') THEN PipelineStatus
-- MAGIC              ELSE 'NotApplicable'
-- MAGIC              END  AS PipelineStatus_Derived,
-- MAGIC         PipelineStatus,        
-- MAGIC         row_number() over(partition by SourceContainerPath,SourceFolderPath,SourceFileName,
-- MAGIC                                        DestinationContainerPath,DestinationFolderPath,
-- MAGIC                                        DestinationFileName,SourceTypeId 
-- MAGIC                           Order By CreatedDate DESC) AS ROWNUM
-- MAGIC     FROM cd_dev.silverzone.FactTransformationLog AS l
-- MAGIC
-- MAGIC )
-- MAGIC
-- MAGIC select *
-- MAGIC from
-- MAGIC (select rd.SourceContainerPath AS LandingContainer,
-- MAGIC         rd.SourceFolderPath AS LandingPath,
-- MAGIC         rd.SourceFileName as LandingFileName,
-- MAGIC         rd.zipfileName as LandingZipFileName,
-- MAGIC         rd.SourceFileSize as FileSize,
-- MAGIC         rd.LogType,
-- MAGIC         rd.PipelineStatus_Derived AS RawStatus,
-- MAGIC         rd.ErrorMessage as RawErrorDescription,
-- MAGIC         rd.CreatedDate AS RawCreatedDate, 
-- MAGIC
-- MAGIC         ifnull(rd.DestinationContainerPath,silver.SourceContainerPath) AS RawContainer,
-- MAGIC         ifnull(rd.DestinationFolderPath,silver.SourceFolderPath) AS RawFolderPath,
-- MAGIC         ifnull(rd.DestinationFileName,silver.SourceFileName) as RawFileName, 
-- MAGIC         silver.DestinationContainerPath AS SilverContainer,
-- MAGIC         silver.DestinationFolderPath AS SilverFolderPath,
-- MAGIC         
-- MAGIC         CASE WHEN rd.LogType NOT IN ('UL','UE','SYS','ENGR','Usage','Measurement',      
-- MAGIC                                      'UserException','Exception','Assert')
-- MAGIC                     OR rd.PipelineStatus_Derived = 'NotApplicable' 
-- MAGIC                 THEN 'NotApplicable'
-- MAGIC              ELSE silver.PipelineStatus_Derived    
-- MAGIC              END AS SilverStatus,
-- MAGIC
-- MAGIC         silver.ErrorMessage as SilverErrorDescription,
-- MAGIC         silver.CreatedDate As SilverCreatedDate,
-- MAGIC         rd.SourceTypeId AS SourceTypeID,
-- MAGIC 		concat('{0}','/',
-- MAGIC         ifnull(rd.DestinationContainerPath,silver.SourceContainerPath),
-- MAGIC         replace(ifnull(rd.DestinationFolderPath,silver.SourceFolderPath),'','%20'),
-- MAGIC         replace(ifnull(rd.DestinationFileName,silver.SourceFileName),'','%20'),'{1}') AS RAWFileURL
-- MAGIC from (Select * From logs_raw Where ROWNUM = 1) AS rd
-- MAGIC FULL Join (Select * From logs_silver Where ROWNUM = 1) AS silver
-- MAGIC ON rd.DestinationFolderPath  = silver.SourceFolderPath
-- MAGIC AND rd.DestinationFileName = silver.SourceFileName
-- MAGIC ) AS T
-- MAGIC           '''.format(ADLSGEN2URL,ADLSGEN2SASToken))
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##11.cd_dev.goldzone.cyclesdatarescart_v

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.cyclesdatarescart_v (
  CycleID,
  CartridgeSerialNbr,
  EventStartTmstmp,
  EventEndTmstmp,
  CartridgeType,
  PrimeTime,
  MaxDoseCount,
  UsedDoseCount,
  CurTreatDoseCount,
  CartridgeStartexpirationStatus,
  cartridgeDoseCountDelivered,
  CartridgeEndexpirationStatus,
  SourceFilePath,
  FileNameUUID,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1695143202')
AS (
select 
CycleID,
CartridgeSerialNbr,
EventStartTmstmp,
EventEndTmstmp,
CartridgeType,
PrimeTime,
MaxDoseCount,
UsedDoseCount,
CurTreatDoseCount,
CartridgeStartexpirationStatus,
cartridgeDoseCountDelivered,
CartridgeEndexpirationStatus,
SourceFilePath,
FileNameUUID,
CreatedBy,
CreatedDt,
UpdatedBy,
UpdatedDt  from cd_dev.silverzone.factulcartridgelogs_rs )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##12.cd_dev.goldzone.cyclesdatares_v

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.cyclesdatares_v (
  ULLogsUUID,
  CycleID,
  ExternalSerialNbr,
  EventEndTmstmp,
  EventStartTmstmp,
  AlleCertDt,
  AlleIDDecryptedFlg,
  ApplicatorSerialNbr,
  BDIDEncryptdFlg,
  BrilliantDistinctionID,
  CoolSculptingID,
  CycleErrorZCd,
  CycleFinalStatusDesc,
  CycleFinalStatusReasonDesc,
  FileNameApplicatorPortCd,
  FileNameDeviceTypeCd,
  FileNameUUID,
  HdrAppHeadNbr,
  History,
  InternalSerialNbr,
  MsrmntLogTreatmentFileNm,
  MsrmntLogTreatmentFileURLTxt,
  PatientBodyPartNm,
  PatientGenderCd,
  PatientNewFlg,
  PatientNextSameFlg,
  RS_CartridgeUsedCnt,
  RS_ConsoleSerialNbr,
  RS_FMSVersion,
  RS_HandPieceVersion,
  RS_Intensity,
  RS_PPSDSPVersion,
  RS_PPSMCUVersion,
  RS_StaffTrainingMode,
  RS_StaffTreatmentDesignation,
  RS_SubSystemSerialNbr,
  RS_TreatmentType,
  SBCVersionNbr,
  ShipToAccountID,
  SoldToAccountID,
  SourceFileName,
  SourceFilePath,
  TtlTrtmntDurationTm,
  CoolDeviceShippedInd,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1695143199')
AS (
  select
ULLogsUUID,
CycleID,
ExternalSerialNbr,
EventEndTmstmp,
EventStartTmstmp,
AlleCertDt,
AlleIDDecryptedFlg,
ApplicatorSerialNbr,
BDIDEncryptdFlg,
BrilliantDistinctionID,
CoolSculptingID,
CycleErrorZCd,
CycleFinalStatusDesc,
CycleFinalStatusReasonDesc,
FileNameApplicatorPortCd,
FileNameDeviceTypeCd,
FileNameUUID,
HdrAppHeadNbr,
History,
InternalSerialNbr,
MsrmntLogTreatmentFileNm,
MsrmntLogTreatmentFileURLTxt,
PatientBodyPartNm,
PatientGenderCd,
PatientNewFlg,
PatientNextSameFlg,
RS_CartridgeUsedCnt,
RS_ConsoleSerialNbr,
RS_FMSVersion,
RS_HandPieceVersion,
RS_Intensity,
RS_PPSDSPVersion,
RS_PPSMCUVersion,
RS_StaffTrainingMode,
RS_StaffTreatmentDesignation,
RS_SubSystemSerialNbr,
RS_TreatmentType,
SBCVersionNbr,
ShipToAccountID,
SoldToAccountID,
SourceFileName,
SourceFilePath,
TtlTrtmntDurationTm,
CoolDeviceShippedInd,
CreatedBy,
CreatedDt,
UpdatedBy,
UpdatedDt
from  cd_dev.silverzone.factullogs_rs 
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##13.cd_dev.goldzone.cyclesdata_v 

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.cyclesdata_v (
  CycleID,
  BeginTime,
  EndTime,
  SBC_sn,
  InternalSerialNbr,
  HdrAppHeadNbr,
  Applicator_sn,
  APP_sn,
  ssAPPident,
  EZCard,
  CyclesLeft,
  Status,
  ssWinCE,
  InterruptStart,
  InterruptStatus,
  InterruptError,
  SameNextPatient,
  ReturningPatient,
  PatientType,
  TreatmentBodyPart,
  CoolSculptingID2,
  CoolSculptingID,
  UpperModuleNumber,
  ProductTypeCd,
  Applicator2SerialNbr,
  MaxCoolTmp,
  MaxApplicator1Tmp,
  MaxApplicator2Tmp,
  TtlTrtmntDurationTm,
  TrtmntPrtclCd,
  ChsnTrtmntDurationTm,
  AlleIDEncryptedFlg,
  AlleIDDecryptedFlg,
  ApplicatorFrndlyNm,
  ApplicatorEngnrNm,
  AlleCertDt,
  FileNameUUID,
  ErrorDeviceScreen,
  ErrorDescription,
  TimeZoneDesc,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1695664695')
AS (

with startendparameter as (
  Select FIlenameUUID,ExternalSerialNbr,TimeZoneDesc
  FROM cd_dev.silverzone.dimstartendparameter 
  group by all
) 

SELECT
CYC.CycleID, 
cast(date_format(cyc.EventStartTmstmp,'yyyy-MM-dd HH:mm:ss') as TIMESTAMP) as BeginTime,
cast(date_format(cyc.EventEndTmstmp ,'yyyy-MM-dd HH:mm:ss') as TIMESTAMP) as  EndTime,
CYC.InternalSerialNbr AS SBC_sn,
CYC.InternalSerialNbr,
CYC.HdrAppHeadNbr,
ifnull(CYC.Applicator1SerialNbr,CYC.ApplicatorSerialNbr) as Applicator_sn,
CYC.ApplicatorInternalSerialNbr as APP_sn,
CYC.ApplicatorConfigurantionNbr as ssAPPident,
CYC.CardSPSerialNbr as EZCard,
CYC.CurrentCycleNbr as CyclesLeft,
CYC.CycleFinalStatusReasonDesc as Status,
ifnull(CYC.SwVersionNbr,SBCVersionNbr) as ssWinCE,

CYC.InterruptStart,
CYC.CycleFinalStatusReasonDesc as InterruptStatus,
ifnull(CYC.CycleErrorCd,CYC.CycleErrorZCd)  as InterruptError,
CYC.PatientNextSameFlg as SameNextPatient,
CYC.PatientNewFlg as ReturningPatient,
CYC.PatientGenderCd as PatientType,
CYC.PatientBodyPartNm as TreatmentBodyPart,
CYC.BrilliantDistinctionID as CoolSculptingID2,
CYC.CoolSculptingID, 
CYC.ExternalSerialNbr as UpperModuleNumber,
CYC.FileNameDeviceTypeCd as ProductTypeCd,
CYC.Applicator2SerialNbr,
CYC.MaxCoolTmp,
CYC.MaxApplicator1Tmp,
CYC.MaxApplicator2Tmp,
CYC.TtlTrtmntDurationTm,
CYC.TrtmntPrtclCd,
CYC.ChsnTrtmntDurationTm,
CYC.BDIDEncryptdFlg AS AlleIDEncryptedFlg,
CYC.AlleIDDecryptedFlg, 
AppVer.ApplicatorFrndlyNm,
Appver.ApplicatorEngnrNm,
CYC.AlleCertDt,
CYC.FileNameUUID,
err.EventText as ErrorDeviceScreen,
err.ErrorDescription,
startend.TimeZoneDesc,
CYC.CreatedBy,
CYC.CreatedDt,
CYC.UpdatedBy,
CYC.UpdatedDt
FROM cd_dev.silverzone.FACTULLogs CYC
LEFT JOIN cd_dev.silverzone.DIMApplicatorVerCd AppVer
ON AppVer.SSAPPIdentID = Left(CYC.ApplicatorConfigurantionNbr,7)
left join startendparameter AS startend
on CYC.FIlenameUUID = startend.FIlenameUUID
and startend.ExternalSerialNbr = cyc.ExternalSerialNbr

left join cd_dev.silverzone.errordescription Err  
on iff(err.DeviceType= 'CT', err.ErrorCode , substr(err.ErrorCode,2)) = ifnull(CYC.CycleErrorCd,CYC.CycleErrorZCd)   and cyc.FileNameDeviceTypeCd = err.DeviceType
Where CYC.ExternalSerialNbr <> 'Unkonwn'
and CYC.EventStartTmstmp is not Null
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##14.cd_dev.goldzone.cyclesdatacom1_v

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.cyclesdatacom1_v (
  ULLogsUUID,
  CycleID,
  SBC_sn,
  EZCard,
  CyclesLeft,
  BeginTime,
  InterruptStart,
  InterruptStatus,
  InterruptError,
  InterruptEnd,
  EndTime,
  Status,
  ErrorMessage,
  precycle,
  PostCycle,
  ssWinCE,
  ssCIB,
  ssAPP,
  ssTEC,
  ssAPPident,
  ProfilePN,
  ProfileIndex,
  SameNextPatient,
  ReturningPatient,
  PatientType,
  TreatmentBodyPart,
  TreatmentType,
  ReturningCoolSculpting,
  APP_sn,
  CIB_sn,
  DTC,
  ProfileTime,
  ProfileTemp,
  AlledID,
  TreatmentPlanNumber,
  UpperModuleNumber,
  EncryptedAlleID,
  Applicator_sn,
  Contour,
  Adapter_sn,
  AppBlobCRC,
  AppBlobVersion,
  SoftwareVerTxt,
  UnsoldFraudFlg,
  SoldToAccountID,
  ShipToAccountID,
  EQUIPCardPartNbr,
  CoolSculptingID,
  CoolSculptingID2,
  ssAdapter,
  DuplicateCycleFlg,
  FileNameUUID,
  SourceFilePath,
  SourceFileName,
  ErrorDeviceScreen,
  ErrorDescription,
  CreatedBy,
  CreatedDt,
  UpdatedBy,
  UpdatedDt)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1695664689')
AS SELECT 
                                                                        CYC.ULLogsUUID,
                                                                        CYC.CycleID,
                                                                        CYC.SBC_sn,
                                                                        CYC.EZCard,
                                                                        CYC.CyclesLeft,
                                                                        CYC.BeginTime,
                                                                        CYC.InterruptStart,
                                                                        CYC.InterruptStatus,
                                                                        CYC.InterruptError,
                                                                        CYC.InterruptEnd,
                                                                        CYC.EndTime,
                                                                        CYC.Status,
                                                                        CYC.ErrorMessage,
                                                                        CYC.precycle,
                                                                        CYC.PostCycle,
                                                                        CYC.ssWinCE,
                                                                        CYC.ssCIB,
                                                                        CYC.ssAPP,
                                                                        CYC.ssTEC,
                                                                        CYC.ssAPPident,
                                                                        CYC.ProfilePN,
                                                                        CYC.ProfileIndex,
                                                                        CYC.SameNextPatient,
                                                                        CYC.ReturningPatient,
                                                                        CYC.PatientType,
                                                                        CYC.TreatmentBodyPart,
                                                                        CYC.TreatmentType,
                                                                        CYC.ReturningCoolSculpting,
                                                                        CYC.APP_sn,
                                                                        CYC.CIB_sn,
                                                                        CYC.DTC,
                                                                        CYC.ProfileTime,
                                                                        CYC.ProfileTemp,
                                                                        CYC.AlledID,
                                                                        CYC.TreatmentPlanNumber,
                                                                        CYC.ExternalSerialNbr as UpperModuleNumber,
                                                                        CYC.EncryptedAlleID,
                                                                        CYC.Applicator_sn,
                                                                        CYC.Contour,
                                                                        case when Adapter_sn='Unknown' then 'U' else Adapter_sn end as Adapter_sn,
                                                                        CYC.AppBlobCRC,
                                                                        CYC.AppBlobVersion,
                                                                        CYC.SoftwareVerTxt,
                                                                        CYC.UnsoldFraudFlg,
                                                                        CYC.SoldToAccountID,
                                                                        CYC.ShipToAccountID,
                                                                        CYC.EQUIPCardPartNbr,
                                                                        CYC.CoolSculptingID,
                                                                        CYC.CoolSculptingID2,
                                                                        CYC.ssAdapter,
                                                                        CYC.DuplicateCycleFlg,
                                                                        CYC.FileNameUUID,
                                                                        CYC.SourceFilePath,
                                                                        CYC.SourceFileName,
                                                                        err.EventText as ErrorDeviceScreen,
                                                                        err.ErrorDescription,
                                                                        CYC.CreatedBy,
                                                                        CYC.CreatedDt,
                                                                        CYC.UpdatedBy,
                                                                        CYC.UpdatedDt
                                                                        
                                                                        
 FROM cd_dev.silverzone.factullogs_com1 CYC
 left join cd_dev.silverzone.errordescription Err  
on Err.ErrorCode = CYC.ErrorMessage and Err.DeviceType = 'COM1'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 15.cd_dev.goldzone.deviceownership_v

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.deviceownership_v (
  ExternalSerialNbr,
  SerialNumberNbr,
  MaterialCd,
  ProductName,
  SalesOrgNbr,
  ShipToID,
  SoldToID,
  ShipStartDt,
  ShipEndDt,
  EquipmentTransitStatusNm,
  SoldToAccountPrimaryNm,
  SoldToAccountCityNm,
  SoldToAcctStateProvinceCd,
  SoldToAccountCountryAlpha2Cd,
  ShipToAccountPrimaryNm,
  ShipToAccountCityNm,
  ShipToAccountCity2Nm,
  ShipToAcctStateProvinceCd,
  ShipToAccountCountryAlpha2Cd,
  ShipToAcctStreetAddress,
  ShipToAcctPostalCode,
  ShipToSalesTerritoryDesc,
  ShipToSalesAreaDesc,
  ShipToSalesRegionDesc,
  ShipToSalesRepNm,
  ShipToSalesRepPreferredNm,
  ShipToSalesAreaMngrPreferredNm,
  ShipToSalesRegionMngrPreferredNm)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1700013879')
AS WITH EquipmentMaster AS (
	SELECT SoldToID
  			,ShipToID
				,ShipStartDt
				,ShipEndDt
				,SerialNumberNbr
				,MaterialCd
			  ,L2ProductLineNm
				,SalesOrgNbr
				,EquipmentTransitStatusNm
				,IFF(SerialNumberNbr rlike '^[0-9]*$',ltrim('0',SerialNumberNbr),SerialNumberNbr) AS ExternalSerialNbr
				,row_number() OVER (PARTITION BY SerialNumberNbr ORDER BY ShipStartDt DESC) AS ROWNUM
	FROM 	cd_dev.silverzone.dimequipmentmaster
)
SELECT
	EQ.ExternalSerialNbr
	,EQ.SerialNumberNbr
	,EQ.MaterialCd
	,EQ.L2ProductLineNm AS ProductName
	,EQ.SalesOrgNbr
  ,EQ.ShipToID AS ShipToID
	,EQ.SoldToID AS SoldToID
	,EQ.ShipStartDt
	,EQ.ShipEndDt
	,EQ.EquipmentTransitStatusNm
	,SDC.AccountPrimaryNm AS SoldToAccountPrimaryNm
	,SDC.AccountCityNm SoldToAccountCityNm
	,SDC.AcctStateProvinceCd AS SoldToAcctStateProvinceCd
	,SDC.AccountCountryAlpha2Cd AS SoldToAccountCountryAlpha2Cd
  ,SHC.AccountPrimaryNm AS ShipToAccountPrimaryNm
	,SHC.AccountCityNm AS ShipToAccountCityNm
	,SHC.AccountCity2Nm AS ShipToAccountCity2Nm
	,SHC.AcctStateProvinceCd AS ShipToAcctStateProvinceCd
	,SHC.AccountCountryAlpha2Cd AS ShipToAccountCountryAlpha2Cd
  ,SHC.AccountStreetAddressTxt AS ShipToAcctStreetAddress
  ,SHC.AccountPostalCd AS ShipToAcctPostalCode
  ,TER.SalesTerritoryDesc AS ShipToSalesTerritoryDesc
	,TER.SalesAreaDesc AS ShipToSalesAreaDesc
	,TER.SalesRegionDesc AS ShipToSalesRegionDesc
	,concat_ws(',',TER.SalesRepLastNm,TER.SalesRepFirstNm) AS ShipToSalesRepNm
	,TER.SalesRepPreferredNm AS ShipToSalesRepPreferredNm
	,TER.SalesAreaMngrPreferredNm AS ShipToSalesAreaMngrPreferredNm
	,TER.SalesRegionMngrPreferredNm AS ShipToSalesRegionMngrPreferredNm
	
FROM EquipmentMaster AS EQ
JOIN cd_dev.silverzone.dimproducthierarchy AS PH ON EQ.MaterialCd = PH.ProductCd
LEFT JOIN cd_dev.silverzone.dimcustomermaster AS SDC  ON EQ.SoldToID = SDC.AccountID
LEFT JOIN cd_dev.silverzone.dimcustomermaster AS SHC ON EQ.ShiptoID = SHC.AccountID
LEFT join cd_dev.silverzone.dimalignmentdata  AS TER  ON SHC.AccountID = TER.AccountID
Where ROWNUM = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 16. cd_dev.goldzone.InvoiceAddendum_v

-- COMMAND ----------

CREATE OR REPLACE VIEW cd_dev.goldzone.InvoiceAddendum_v
AS
Select DA.ShipToAccountId,
       DA.MoxieShipToAccountId,
       DA.EffectiveDate,
       FUL.ShipToAccountUUID,
       DA.ShipToAccountName,
       DP.DisplayName as PatientClassificationName,
       DP.PatientClassificationGroupName as PatientClassificationGroupName,
       DP.InvoiceImpactFlag,
       DA.TerminationDate,
       coalesce(FUL.TotalCycleCount,0) * DP.InvoiceMultiplier as TotalCycleCount,
       coalesce(FUL.TotalVisitCount,0) * DP.InvoiceMultiplier as TotalVisitCount,
       coalesce(FUL.InvoiceAmount,0) * DP.InvoiceMultiplier as InvoiceAmount,
       coalesce(DP.ListPrice,0) * DP.InvoiceMultiplier as ListPrice,
       coalesce(case 
       when DP.InvoiceCalculationType = 'PerSubscription' then FUL.TotalVisitCount
       when DP.InvoiceCalculationType = 'PerCycle' then FUL.TotalCycleCount End,0) * DP.InvoiceMultiplier as Quantity,
       
       case 
       when DP.PatientClassificationName = 'PerPatientFee' then 1
       when DP.PatientClassificationName = 'PerPatientFeeExceptionPreInvoice' then 2

       when DP.PatientClassificationName = 'PerCycleFee' then 3
       when DP.PatientClassificationName = 'PerCycleFeeExceptionPreInvoice' then 4

       when DP.PatientClassificationName = 'NoPatientAssociationFee' then 5
       when DP.PatientClassificationName = 'NoPatientAssociationFeeExceptionPreInvoice' then 6
       when DP.PatientClassificationName = 'NoPatientAssociationFeePerCycleException' then 7
       when DP.PatientClassificationName = 'FollowUpVisit' then 8
       when DP.PatientClassificationName = 'PerPatientFeeExceptionPostInvoice' then 9

       when DP.PatientClassificationName = 'PerCycleFeeExceptionPostInvoice' then 10       
       
       when DP.PatientClassificationName = 'NoPatientAssociationFeeExceptionPostInvoice' then 11
       when DP.PatientClassificationName = 'PerPatientFeeMapViolation' then 12
       when DP.PatientClassificationName = 'NonP3PatientFraudViolation' then 13
       when DP.PatientClassificationName = 'NonP3Fee' then 14
       when DP.PatientClassificationName = 'MidwayPatient' then 15
       else 16
       End as PatientClassificationOrder,
       case
       when DP.PatientClassificationGroupName in ('Per Patient Fee Invoice Sub Total') then 1
       when DP.PatientClassificationGroupName in ('Per Cycle Fee Invoice Sub Total') then 2
       when DP.PatientClassificationGroupName in ('Per Patient Fee Map Violation Invoice Sub Total') then 3
       when DP.PatientClassificationGroupName in ('Non-P3 Patient Fraud Violation Invoice Sub Total') then 4
       when DP.PatientClassificationGroupName in ('Non-P3 Patient Invoice Sub Total') then 5
       when DP.PatientClassificationGroupName in ('No Patient Association Fee Sub Total')  then 6
       when DP.PatientClassificationGroupName in ('Per Cycle Exception Fee')  then 7
       when DP.PatientClassificationGroupName in ('Follow-up Visit', 'Per Patient Fee Exception Post-Invoice', 'No Patient Association Fee Exception Post-Invoice')  then 8
       else 9
       End as PatientClassificationGroupOrder,
       DP.SKUCode,
       FUL.InvoiceDate as InvoiceCalculationDate,
       ICM.CalendarStartTimeStamp AS BillingPeriodStartDate,
       ICM.CalendarEndTimeStamp AS BillingPeriodEndDate,
       case when cast(FUL.InvoiceDate as date)>cast('2024-07-29' as date) and COALESCE(FUL.InvoiceFileName,'')<>'' then
       replace(PM.InvoiceAddendumFilePath,'<FilePath>',FUL.InvoiceFileName)
       else 
       replace(PM.InvoiceAddendumFilePath,'<FilePath>',concat(DA.ShipToAccountId,'/',date_format(FUL.InvoiceDate, 'yyyy-MM'),'/', replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(DA.ShipToAccountId,' ', replace(DA.ShipToAccountName,'/','-'), ' ', date_format(FUL.InvoiceDate, 'MM.dd.yyyy'), ' Summary.xlsx'), ' ', '%20'), '!', '%21'), '#', '%23'), '$', '%24'),'&', '%26'),  '(', '%28'), ')', '%29'), '*', '%2A'), '+', '%2B'), ',', '%2C'), '/', '%2F'), ':', '%3A')))
       end  AS InvoiceAddendumSummaryFileURL,       
       replace(PM.InvoiceAddendumFilePath,'<FilePath>',concat(DA.ShipToAccountId,'/',date_format(FUL.InvoiceDate, 'yyyy-MM'),'/', replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(FUL.InvoiceFileName, ' ', '%20'), '!', '%21'), '#', '%23'), '$', '%24'),'&', '%26'), '(', '%28'), ')', '%29'), '*', '%2A'), '+', '%2B'), ',', '%2C'), '/', '%2F'), ':', '%3A')))  AS InvoiceAddendumDetailsFileURL,  
       FUL.InvoiceFileName AS InvoiceFileName,
       FUL.SalesOrderNumber,
       FUL.InvoiceNumber,
       PM.PromotionName,
       FUL.PromotionUUID,
       PM.CountryAlpha2Cd,
       FUL.SoldToAccountID,
       replace(PM.SoldToInvoiceAddendumFlePath,'<FilePath>',concat(FUL.SoldToAccountID,'/',date_format(FUL.InvoiceDate, 'yyyy-MM'),'/', replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(FUL.SoldToInvoiceFileName, ' ', '%20'), '!', '%21'), '#', '%23'), '$', '%24'),'&', '%26'), '(', '%28'), ')', '%29'), '*', '%2A'), '+', '%2B'), ',', '%2C'), '/', '%2F'), ':', '%3A')))  AS SoldToInvoiceAddendumFileURL,  
       DCS.AccountPrimaryNm as SoldToAccountName,
       FUL.SoldToInvoiceFileName,
       case
       when DP.PatientClassificationGroupName in ('Per Patient Fee Invoice Sub Total', 'Per Cycle Fee Invoice Sub Total', 'No Patient Association Fee Sub Total') then 1
       else 0
       End as IsSubTotalRow
From cd_dev.promotion.FACT_InvoiceAddendum AS FUL
LEFT JOIn cd_dev.promotion.dim_account AS DA
      ON FUL.ShipToAccountUUID = DA.ShipToAccountUUID
INNER JOIN cd_dev.promotion.DIM_PatientClassification AS DP
      ON FUL.PatientClassificationUUID = DP.PatientClassificationUUID
      AND DP.DisplayInd = 1
LEFT JOIN cd_dev.promotion.dim_invoicecyclemonth  AS ICM 
      ON FUL.InvoiceDate = ICM.InvoiceDate 
INNER JOIN cd_dev.promotion.dim_promotion AS PM 
      ON FUL.PromotionUUID = PM.PromotionUUID
LEFT JOIN cd_dev.silverzone.dimcustomermaster AS DCS 
      ON FUL.SoldToAccountID = DCS.AccountID     

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 17.cd_dev.goldzone.InvoiceAddendumDetails_v

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.invoiceaddendumdetails_v 
AS 
with consumerSubscription AS (
      select *,lag(SubscriptionStartDate) OVER (PARTITION BY(ShipToAccountUUID, CoolSculptingID) ORDER BY SubscriptionStartDate ASC) AS Previous_SubscriptionStartDate
      FROM cd_dev.promotion.FACT_ConsumerSubscription
      where VersionID = 1
)
Select DA.ShipToAccountId,
       DA.ShipToAccountName,
       DA.EffectiveDate As P3EffectiveDate,
       case 
        when len(FUL.CoolSculptingID) = 10 then Concat(Substr(FUL.CoolSculptingID,1,3),'-',Substr(FUL.CoolSculptingID,4,3),'-',Substr(FUL.CoolSculptingID,7,4))
        Else FUL.CoolSculptingID
       end as `PhoneNumber`,
       FUL.SoldToAccountID,
       SDC.AccountPrimaryNm AS SoldToAccountName,
       FUL.CycleDate AS TreatmentVisitDate,
       CASE WHEN DP.InvoiceCalculationType = 'PerCycle' THEN null ELSE FC.SubscriptionStartDate END AS InitialVisitDate,
       CASE WHEN DP.InvoiceCalculationType = 'PerCycle' THEN null ELSE FC.SubscriptionEndDate END AS FinalEligibleVisitDate,
       FUL.CycleCount,
       DS.ScannedID,
       DP.DisplayName AS PatientClassificationName,
       case 
       when DP.PatientClassificationName = 'PerPatientFee' then 1
       when DP.PatientClassificationName = 'PerPatientFeeExceptionPreInvoice' then 2
       
       when DP.PatientClassificationName = 'PerCycleFee' then 3
       when DP.PatientClassificationName = 'PerCycleFeeExceptionPreInvoice' then 4

       when DP.PatientClassificationName = 'NoPatientAssociationFee' then 5
       when DP.PatientClassificationName = 'NoPatientAssociationFeeExceptionPreInvoice' then 6
       when DP.PatientClassificationName = 'NoPatientAssociationFeePerCycleException' then 7
       when DP.PatientClassificationName = 'FollowUpVisit' then 8
       when DP.PatientClassificationName = 'PerPatientFeeExceptionPostInvoice' then 9

       when DP.PatientClassificationName = 'PerCycleFeeExceptionPostInvoice' then 10

       when DP.PatientClassificationName = 'NoPatientAssociationFeeExceptionPostInvoice' then 11
       when DP.PatientClassificationName = 'PerPatientFeeMapViolation' then 12
       when DP.PatientClassificationName = 'NonP3PatientFraudViolation' then 13
       when DP.PatientClassificationName = 'NonP3Patient' then 14
       when DP.PatientClassificationName = 'MidwayPatient' then 15
       else 16
       End as PatientClassificationOrder,
       FUL.IncrementalInvoiceCharge * DP.InvoiceMultiplier AS IncrementalInvoiceCharge,
       DP.ListPrice * DP.InvoiceMultiplier AS ListPrice,
       DATE_FORMAT(FUL.CycleDate, 'MMM-yy') AS DataPeriod,
       DATE_FORMAT(FUL.CycleDate, 'MMM-yyyy') AS DataPeriod1,
       DCM.CommentDesription,
      replace(
            replace(
                  replace(
                        replace(
                              replace(
                                    replace(DCM.CommentDesription,'<MoxieCaseNumber>',ifnull(FUL.ExceptionMoxieCaseNumber,'')),
                              '<ExceptionCreditAmount>',FORMAT_NUMBER(CAST(ifnull(FUL.ExceptionCreditAmount,'0') AS NUMERIC),0)),
                        '<ExceptionDate>',date_format(FUL.CreatedDate,'MM-dd-yyyy')),
                  '<DifferentShipToNumber>',ifnull(Different.ShipToAccountID,'')),
            '<DifferentShipToName>',ifnull(Different.ShipToAccountName, '')),
      '<ListPrice>',ifnull(DP.ListPrice,0)) AS Comments,
       FUL.CreatedDate,
       FUL.UpdatedDate,
       IA.InvoiceDate as InvoiceCalculationDate,
       ICM.CalendarStartTimeStamp AS BillingPeriodStartDate,
       ICM.CalendarEndTimeStamp AS BillingPeriodEndDate,
       DA.MoxieShipToAccountId,
       FUL.ShipToAccountUUID,
       FC.Previous_SubscriptionStartDate,
       DA.PerPatientPricingFlag,
       PM.PromotionName,
       PM.CountryAlpha2Cd,
       FUL.SmartCardSerialNumber
From cd_dev.promotion.FACT_InvoiceAddendumDetails AS FUL
LEFT JOIN cd_dev.promotion.dim_account AS DA
      ON FUL.ShipToAccountUUID = DA.ShipToAccountUUID
LEFT JOIN cd_dev.promotion.dim_consumer AS DS
      ON FUL.CoolSculptingID = DS.CoolSculptingID
LEFT JOIN consumerSubscription AS FC
      ON FUL.ConsumerSubscriptionUUID = FC.ConsumerSubscriptionUUID
LEFT JOIN cd_dev.promotion.dim_account AS Different
      ON FC.ShipToAccountUUID = Different.ShipToAccountUUID
INNER JOIN cd_dev.promotion.DIM_PatientClassification AS DP
      ON FUL.PatientClassificationUUID = DP.PatientClassificationUUID
      AND DP.DisplayInd = 1
LEFT JOIN cd_dev.promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails As IAIAD
      ON FUL.InvoiceAddendumDetailsUUID = IAIAD.InvoiceAddendumDetailsUUID
      AND IAIAD.VersionID = 1
LEFT JOIN cd_dev.promotion.FACT_InvoiceAddendum As IA
      ON IA.InvoiceAddendumUUID = IAIAD.InvoiceAddendumUUID      
LEFT JOIN cd_dev.promotion.DIM_InvoiceCycleMonth AS ICM
      ON IA.InvoiceDate = ICM.InvoiceDate   
LEFT JOIN cd_dev.promotion.DIM_Comments AS DCM
      ON DCM.Comments = FUL.Comments  
LEFT JOIN cd_dev.silverzone.dimcustomermaster AS SDC  ON FUL.SoldToAccountID = SDC.AccountID     
INNER JOIN cd_dev.promotion.dim_promotion AS PM 
      ON FUL.PromotionUUID = PM.PromotionUUID 

Where FUL.VersionID = 1 
Order By DA.ShipToAccountId,DA.ShipToAccountName,InitialVisitDate,PhoneNumber,TreatmentVisitDate ASC


-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.invoiceaddendumdetailssnapshot_v 
AS 


Select *
From cd_dev.promotion.FACT_InvoiceAddendumDetailsSnapshot
Order By SnapshotInvoiceDate DESC,ShipToAccountId,ShipToAccountName,InitialVisitDate,PhoneNumber,TreatmentVisitDate ASC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##18.cd_dev.goldzone.p3_smartcards_v
-- MAGIC

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.p3_smartcards_v 
AS (
select sc.SmartCardSerialNumber , sc.MaterialCd,sc.MaterialDescription, 
sc.EffectiveDate, sc.TerminationDate ,sc.UpdatedDate, PM.PromotionName,
       PM.CountryAlpha2Cd from cd_dev.promotion.dim_smartcard  sc INNER JOIN cd_dev.promotion.dim_promotion AS PM 
      ON sc.PromotionUUID = PM.PromotionUUID )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##19.cd_dev.goldzone.promotionNoncompliancereport_v
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW cd_dev.goldzone.promotionNoncompliancereport_v AS
(
    SELECT *
    FROM
    (
        SELECT ncd.SoldToAccountID AS `Sold To`,
               dcm.AccountPrimaryNm AS `Sold To Name`,
               da.ShipToAccountId AS `Ship To`,
               da.ShipToAccountName AS `Ship To Name`,
               dac.SHipToAccountId AS CycleShipToAccountId,               
            CASE 
                   WHEN len(ncd.CoolSculptingID) = 10 THEN Concat(Substr(ncd.CoolSculptingID,1,3),'-',Substr(ncd.CoolSculptingID,4,3),'-',Substr(ncd.CoolSculptingID,7,4))
                   ELSE ncd.CoolSculptingID
               END AS `PhoneNumber`,
               fc.BaseThresholdLimit,
               fc.HighPerformerThresholdLimit,
               fc.RuleDetails AS `Rule`,
               fc.RuleName,
               fc.RuleType,
               fc.RuleFrequency,
               fc.RuleCategory,
               fc.EffectiveDate,
               fc.TerminationDate,
               fc.DisplayFlag,
               ncd.CycleCount,
               ncd.ShipToRuleValue,
               ncd.ShipToNumber,          
               ncd.CycleDate AS `Date Threshold Exceeded (Flag Creation Date)`,
               dc.ScannedId AS `Scanned ID`,
               ncd.ConsumerSubscriptionUUID,
               DP.PromotionName AS `Promotion Name`,
               DP.CountryAlpha2Cd As `Country Code`,
               ncd.ShipToAccountUUID,
               ncd.CycleShipToAccountUUID               
        FROM cd_dev.promotion.fact_noncompliancedetails AS ncd
        inner JOIN cd_dev.promotion.fact_consumersubscription AS cs
                on NCD.ConsumerSubscriptionUUID = CS.ConsumerSubscriptionUUID
                and CS.VersionID = 1
                and ncd.Active = 1 
        INNER JOIN cd_dev.promotion.dim_promotion AS DP
                ON DP.PromotionUUID = NCD.PromotionUUID
        INNER JOIN cd_dev.promotion.dim_account AS da ON Da.ShipToAccountUUID = ncd.ShipToAccountUUID        
        INNER JOIN cd_dev.promotion.dim_fraudclassification AS fc ON ncd.FraudClassificationUUID = fc.FraudClassificationUUID        
        INNER JOIN cd_dev.promotion.dim_consumer AS dc ON dc.CoolsculptingId = ncd.CoolSculptingID        
        LEFT JOIN cd_dev.silverzone.dimcustomermaster AS dcm ON ncd.SoldToAccountID = dcm.AccountID
        INNER JOIN cd_dev.promotion.dim_account AS dac ON dac.ShipToAccountUUID = ncd.CycleShipToAccountUUID
    ) AS view
    ORDER BY  `Ship To`,`Date Threshold Exceeded (Flag Creation Date)`
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##20.cd_dev.goldzone.scanned_id_v
-- MAGIC

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.scannedid_v 
AS (
select CoolsculptingId , ScannedId ,CreatedDate, UpdatedDate from cd_dev.promotion.dim_consumer)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##21.cd_dev.goldzone.LocationTracking_v
-- MAGIC

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.locationtracking_v  as  (
select distinct fd.Externalserialnbr ,
fd.FileNameDeviceTypeCd as DeviceType,
fd.HdrDateGeneratedDt as DateOfConnection,
concat(fd.ExternalSerialNbr,"_", fd.HdrDateGeneratedDt) as SN_Date,
fd.DeviceLatitude,
fd.DeviceLongitude,
fd.DeviceStreetAddress,
fd.DeviceMunicipality,
fd.DeviceState,
fd.DeviceCountry,
fd.DeviceZipCode,
fd.SAPLatitude,
fd.SAPLongitude,
fd.SAPShiptoID,
case when da.ShipToAccountId is not null   and fd.FileNameDeviceTypeCd  = 'COM3' then 'P3' else 'Non-P3' end as P3_Flag,
fd.SAPShiptoName,
fd.SAPStreetAddress,
fd.SAPCity,
fd.SAPCountry,
fd.SAPPostalCode,
cast (fd.DistanceMeters as decimal),
cast (fd.DistanceMiles as decimal) ,
case when DistanceMiles >=10 then 'Red'  
when DistanceMiles >=0  and DistanceMiles <10 then 'Green'
else 'NA' end as Status,
fd.Equipment_Status,
row_number() over(partition by ExternalSerialNbr, HdrDateGeneratedDt order by DeviceStreetAddress desc ) rn
from device_location.Fact_DeviceLocationDistance fd
left join cd_dev.promotion.dim_account da
on fd.SAPShiptoID = da.ShipToAccountId 
and fd.HdrDateGeneratedDt BETWEEN da.EffectiveDate AND da.TerminationDate
and da.PerPatientPricingFlag = True
qualify rn =1
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##22.cd_dev.goldzone.Locationtag_v
-- MAGIC

-- COMMAND ----------

CREATE or replace VIEW cd_dev.goldzone.Locationtag_v  as  (
select
 ExternalSerialNbr, DateOfConnection, Location, Label, concat(ExternalSerialNbr,"_", DateOfConnection) as SN_Date,Equipment_Status
from (
select  ExternalSerialNbr , DateOfConnection ,DeviceStreetAddress  as Location,
'Device Location' as Label,Equipment_Status,
row_number() over(partition by ExternalSerialNbr, DateOfConnection order by DeviceStreetAddress desc ) rn
from cd_dev.goldzone.LocationTracking_v
qualify rn = 1
union
select  ExternalSerialNbr , DateOfConnection ,SAPStreetAddress   as Location,
'SAP Location' as Label,Equipment_Status,
row_number() over(partition by ExternalSerialNbr, DateOfConnection order by DeviceStreetAddress desc ) rn
from cd_dev.goldzone.LocationTracking_v
qualify rn = 1)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##23.cd_dev.goldzone.cyclescom1_ods_v

-- COMMAND ----------

Create or replace VIEW cd_dev.goldzone.cyclescom1_ods_v as (
select
CycleID,
ShipToAccountID,
customer.AccountPrimaryNm as ShiptoAccountName,
ExternalSerialNbr, 
SBC_sn as InternalSerialNumber, 
'UTC' as TimeZone, 
BeginTime as StartTime,
EndTime as EndTime,
APP_sn as ApplicatorSerialNumber,
EZCard as EZCardNumber,
ErrorMessage as Error_Code, 
PatientType as Gender,
ReturningPatient as ReturningPatient,
TreatmentBodyPart as TreatmentBodyPart,
customer.AccountCountryAlpha2Cd as CountryCd,
com1.CreatedDt as CreatedDate,
com1.UpdatedDt as UpdatedDate
from cd_dev.silverzone.factullogs_com1 com1 
left join cd_dev.silverzone.dimcustomermaster customer 
on customer.AccountID = com1.ShipToAccountID
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##24.cd_dev.goldzone.cyclescom3_ods_v

-- COMMAND ----------

Create or replace view cd_dev.goldzone.cyclescom3_ods_v AS (
select
c.CycleID,
c.ShipToAccountID,
cust.AccountPrimaryNm as ShiptoAccountName,
c.FileNameDeviceTypeCd as ProductType,
c.ExternalSerialNbr as ExternalSerialNumber,
c.InternalSerialNbr as InternalSerialNumber,
d.LatestSoftwareVersionNbr as SoftwareVersion,
d.OSVersion as OSVersion,
d.Properties_Desired_FormatVersion as FormatVersion,
d.PibVersion as PibTecVersion,
'UTC' as TimeZone,
c.EventStartTmstmp as StartTime,
c.EventEndTmstmp as EndTime,
AppVer.ApplicatorFrndlyNm as Applicator_Name,
Appver.ApplicatorEngnrNm as Applicator_Type,
case
  when c.FileNameDeviceTypeCd = 'CT' then c.Applicator1SerialNbr
  else c.ApplicatorSerialNbr
end as Applicator_SN,
c.Applicator2SerialNbr as Applicator_SN2,
c.CardSPSerialNbr as EZCard_Number,
ifnull(c.CycleErrorCd,c.CycleErrorZCd) as Cycle_Error_Code,
c.PatientGenderCd  as Gender,
c.PatientNewFlg  as  ReturningPatient,
c.PatientBodyPartNm  as TreatmentBodyPart,
cust.AccountCountryAlpha2Cd as CountryCd,
c.CreatedDt as CreatedDate,
c.UpdatedDt as UpdatedDate
from cd_dev.silverzone.factullogs c  
left join cd_dev.goldzone.device_v d
on c.InternalSerialNbr = d.InternalSerialNbr
left join cd_dev.silverzone.dimcustomermaster cust  
on c.ShipToAccountID = cust.AccountID
left join cd_dev.silverzone.DIMApplicatorVerCd AppVer
on AppVer.SSAPPIdentID = Left(c.ApplicatorConfigurantionNbr,7)
where c.FileNameDeviceTypeCd in ('COM3','CT')
order by c.EventStartTmstmp desc
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##25.cd_dev.goldzone.PostInvoice_P3_Metrics_Report

-- COMMAND ----------

CREATE OR REPLACE VIEW cd_dev.goldzone.P3_Metrics_Report AS (
        SELECT 
            InvoiceDate,
            DP.PromotionName,
            DP.CountryAlpha2Cd AS CountryName,
            PatientClassificationName,
            COUNT(DISTINCT FA.ShipToAccountUUID) AS `No.Of ShipTo`,
            COUNT(SalesOrderNumber) AS `No.Of Sales Orders`,
            COUNT(FA.InvoiceNumber) AS `No.Of Invoice Numbers`,
            COUNT(FA.MoxieCaseNumber) AS `No.Of Moxie Cases`,
            SUM(InvoiceAmount) AS `Total Invoice Amount`
        FROM 
            cd_dev.promotion.fact_invoiceaddendum AS FA
        INNER JOIN 
            cd_dev.promotion.dim_promotion AS DP ON DP.PromotionUUID = FA.PromotionUUID
        INNER JOIN 
            cd_dev.promotion.dim_account AS DA ON DA.ShipToAccountUUID = FA.ShipToAccountUUID
        INNER JOIN 
            cd_dev.promotion.dim_patientclassification AS DPC ON DPC.PatientClassificationUUID = FA.PatientClassificationUUID AND DPC.DisplayInd = 1
        WHERE 
            DPC.SKUCode IS NOT NULL
            AND (FA.InvoiceNumber IS NULL OR FA.InvoiceNumber IS NOT NULL)
        GROUP BY 
            InvoiceDate, DP.PromotionName, DP.CountryAlpha2Cd, PatientClassificationName
        ORDER BY 
            InvoiceDate DESC, DP.PromotionName DESC, DPC.PatientClassificationName DESC
)

-- COMMAND ----------

-- DBTITLE 1,differentshipto_smartcards_US_v
CREATE OR REPLACE VIEW cd_dev.goldzone.differentshipto_smartcards_US_v AS (
select 
SmartCard as `P3Card Serial Number`,
ExternalSerialNbr as `Device External Serial Number`,
CycleDate as `Treatment Visit Date`,
Cycles as Cycles,
ShipToofusage as `ShipToAccountID of CardUsage`,
SoldToofusage as `SoldToToAccountID of CardUsage`,
SoldToAccountNameofusage as `SoldToAccountName of CardUsage`,
ShipToAccountNameofusage as `ShipToAccountName of CardUsage`,
ShipToAccountCityNameofusage as `ShipToAccountCity of CardUsage`,
ShipToAccountStateCdofusage as `ShipToAccountStateCd of CardUsage`,

ShipToAccountCardSent as `ShipToAccountID of CardSentTo`,
SoldToAccountIDCardSentto as `SoldToToAccountID of CardSentTo`,
SoldToAccountNameCardSentto as `SoldToAccountName of CardSentTo`,
ShipToAccountNameCardSentto as `ShipToAccountName of CardSentTo`,
ShipToAccountCityNameCardSentto as `ShipToAccountCity of CardSentTo`,
ShipToAccountStateCdCardSentto as `ShipToAccountState of CardSentTo`,
ShipToofusageP3Flag as `ShipToAccount Usage P3Flag`,
ShipToAccountCardSenttoFlag as `ShipToAccount CardSentTo P3Flag`,
InvestigationMoxieCaseNumber,
InvestigationMoxieCaseStatus,
InvestigationMoxieCaseDate,
PenaltyMoxieCaseNumber,
PenaltyMoxieCaseStatus,
PenaltyMoxieCaseDate,
PromotionName,
Country
from cd_dev.promotion.fact_differentshipto_smartcards
Where Country = 'US')

-- COMMAND ----------

CREATE OR REPLACE VIEW cd_dev.goldzone.differentshipto_smartcards_OUS_v AS (
select 
SmartCard as `P3Card Serial Number`,
ExternalSerialNbr as `Device External Serial Number`,
CycleDate as `Treatment Visit Date`,
Cycles as Cycles,
ShipToofusage as `ShipToAccountID of CardUsage`,
SoldToofusage as `SoldToToAccountID of CardUsage`,
SoldToAccountNameofusage as `SoldToAccountName of CardUsage`,
ShipToAccountNameofusage as `ShipToAccountName of CardUsage`,
ShipToAccountCityNameofusage as `ShipToAccountCity of CardUsage`,
ShipToAccountStateCdofusage as `ShipToAccountStateCd of CardUsage`,

ShipToAccountCardSent as `ShipToAccountID of CardSentTo`,
SoldToAccountIDCardSentto as `SoldToToAccountID of CardSentTo`,
SoldToAccountNameCardSentto as `SoldToAccountName of CardSentTo`,
ShipToAccountNameCardSentto as `ShipToAccountName of CardSentTo`,
ShipToAccountCityNameCardSentto as `ShipToAccountCity of CardSentTo`,
ShipToAccountStateCdCardSentto as `ShipToAccountState of CardSentTo`,
ShipToofusageP3Flag as `ShipToAccount Usage P3Flag`,
ShipToAccountCardSenttoFlag as `ShipToAccount CardSentTo P3Flag`,
InvestigationMoxieCaseNumber,
InvestigationMoxieCaseStatus,
InvestigationMoxieCaseDate,
PenaltyMoxieCaseNumber,
PenaltyMoxieCaseStatus,
PenaltyMoxieCaseDate,
PromotionName,
Country
from cd_dev.promotion.fact_differentshipto_smartcards
Where Country <> 'US')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 26. cd_dev.goldzone.log_cyclesvalidation_v

-- COMMAND ----------

CREATE OR REPLACE VIEW cd_dev.goldzone.log_cyclesvalidation_v
as
select CycleID,
CycleDate,
CycleMonth,
P3EligibilityFlag,
CreatedDate
 from 
(select *,row_number() over(partition by CycleID,CycleDate order by P3EligibilityFlag desc,CreatedDate desc) as rk from cd_dev.promotion.log_cyclesvalidation qualify rk =1 )

-- COMMAND ----------

-- DBTITLE 1,goldzone.P3EligibleCycles_Subscription_v
CREATE  OR REPLACE VIEW cd_dev.goldzone.P3EligibleCycles_Subscription_v AS
with startEndParamter as (
  Select   FileNameUUID,
                     CASE 
                        WHEN TimeZoneDesc = '' THEN '+00:00'
                        WHEN TimeZoneDesc = 'Pacific Standard Time' THEN '-08:00'
                        WHEN TimeZoneDesc = 'Pacific Daylight Time' THEN '-07:00'
                        WHEN SUBSTRING(TimeZoneDesc, LENGTH(TimeZoneDesc) - INSTR(REVERSE(TimeZoneDesc), '(') + 5) like '-%' then replace(SUBSTRING(TimeZoneDesc, LENGTH(TimeZoneDesc) - INSTR(REVERSE(TimeZoneDesc), '(') + 5),':00)','')
                        WHEN SUBSTRING(TimeZoneDesc, LENGTH(TimeZoneDesc) - INSTR(REVERSE(TimeZoneDesc), '(') + 5) not like '-%' then concat('+', replace(SUBSTRING(TimeZoneDesc, LENGTH(TimeZoneDesc) - INSTR(REVERSE(TimeZoneDesc), '(') + 5),':00)',''))
                    END AS AdjustedTimeZoneDesc
            from cd_dev.silverzone.dimstartendparameter 
),
ullogs as (
  select  ul.CycleID,ul.SoldToAccountID,ul.ShipToAccountID,ul.PatientBodyPartNm,ul.PatientGenderCd,ul.HdrStartDateGeneratedDt AS CycleDate,dc.ScannedId,
        ul.CardSPSerialNbr AS SmartCardSerialNumber,ul.CoolSculptingID,ul.FileNameDeviceTypeCd,ul.CycleErrorZCd,
        ul.CycleErrorCd,ul.ExternalSerialNbr,
        cast(CASE WHEN coalesce(daco.Timezone,'No Results') != 'No Results' THEN from_utc_timestamp(ul.HdrStartTimeGeneratedTmstmp, timezone) 
                  WHEN dsp.AdjustedTimeZoneDesc IS NOT NULL THEN from_utc_timestamp(HdrStartTimeGeneratedTmstmp, AdjustedTimeZoneDesc) 
                  ELSE HdrStartTimeGeneratedTmstmp END AS DATE) as CycleDate_Local
From cd_dev.silverzone.factullogs AS ul
left join cd_dev.silverzone.dimcustomermaster AS c on c.AccountId = ul.ShipToAccountID
left join device_location.dim_addresscoordinates AS daco on  concat(coalesce(c.AccountStreetAddressTxt,''),' ',coalesce(c.AccountCityNm,''), ' ', coalesce(c.AcctStateProvinceCd,''),' ',coalesce(c.AccountCountryAlpha2Cd,''),' ',coalesce(c.AccountPostalCd,'')) = daco.SAPStreetAddress
left join cd_dev.promotion.dim_consumer AS dc on ul.CoolSculptingID = dc.CoolSculptingID
left join startEndParamter AS dsp On ul.FileNameUUID = dsp.FileNameUUID
Where COALESCE(COALESCE(ul.CycleErrorZCd, ul.CycleErrorCd), 'NA') = 'NA'
),
Cycle_shipto as (
select cd.*,da.ShipToAccountUUID,row_number() over (partition by cd.CycleID order by da.TerminationDate desc) as rn
From  ullogs cd
INNER JOIN  cd_dev.promotion.dim_account da 
              where cd.ShipToAccountID = da.ShipToAccountID
              AND cd.cycledate BETWEEN da.EffectiveDate AND da.TerminationDate
qualify rn =1              
),

validP3Cycles as (
  SELECT 
    cd.CycleID,
    dp.PromotionName,
    dp.CountryAlpha2Cd As `CountryCode`,
    cd.CycleDate,    
    cd.CycleDate_Local,
    cd.CoolSculptingID,
    cd.ScannedID,
    cd.SmartCardSerialNumber,
    cd.ExternalSerialNbr,
    cs.SoldToAccountID,
    cd.ShipToAccountID AS CycleShipToAccountID, -- Added CycleShipToAccountUUID column
    csa.ShipToAccountID AS SubscriptionShipToAccountID, -- Renamed column
    cs.SubscriptionStartDate,
    cs.SubscriptionEndDate,
    cd.PatientBodyPartNm,
    cd.PatientGenderCd,
    cd.ShipToAccountUUID AS CycleShipToAccountUUID,
    cs.ShipToAccountUUID AS SubscriptionShipToAccountUUID,
    dp.PromotionUUID,
    cs.consumersubscriptionUUID,
    cs.InvoiceExceptionFlag,
    row_number() over (partition by cd.CycleID order by cs.InvoiceExceptionFlag ASC,cs.SubscriptionEndDate desc) as rn
FROM Cycle_shipto cd
INNER JOIN cd_dev.promotion.FACT_ConsumerSubscription AS cs
    ON cd.CoolSculptingID = cs.CoolSculptingID
    AND cd.cycledate BETWEEN cs.SubscriptionStartDate AND cs.SubscriptionEndDate
    AND VersionId = 1
INNER JOIN cd_dev.promotion.dim_Account csa
    ON cs.ShipToAccountUUID = csa.ShipToAccountUUID
    AND csa.PromotionUUID = cs.PromotionUUID
    AND case when cd.shiptoaccountid = csa.ShipToAccountID THEN 1
             when cd.shiptoaccountid != csa.ShipToAccountID and cd.Soldtoaccountid = cs.SoldToAccountID THEN 1
             ELSE 0 END = 1
INNER JOIN cd_dev.promotion.dim_promotion AS dp
    ON cd.FileNameDeviceTypeCd = dp.DeviceTypeCd
    AND dp.PromotionUUID = cs.PromotionUUID
    AND cd.cycledate BETWEEN dp.EffectiveDate AND dp.TerminationDate
INNER JOIN cd_dev.promotion.dim_SmartCard ds
    ON cd.SmartCardSerialNumber = ds.SmartCardSerialNumber
    and ds.PromotionUUID = cs.PromotionUUID
    AND cd.cycledate BETWEEN ds.EffectiveDate AND ds.TerminationDate
qualify rn = 1
)

Select  *
From validP3Cycles



-- COMMAND ----------

-- DBTITLE 1,goldzone.P3DifferentSoldTo
CREATE  OR REPLACE VIEW cd_dev.goldzone.P3DifferentSoldTo_v AS
with CSID as 
( 
Select CoolSculptingID,ConsumerSubscriptionUUID,count(Distinct da.ShipToAccountId) AS ShipToCount,count(distinct IAD.PatientClassificationUUID) As PatientClassCount,
        count(Distinct IAD.SoldToAccountID) AS SoldToCount, Min(IAD.CycleDate) As Min_CycleDate
From cd_dev.promotion.fact_invoiceaddendumdetails AS IAD
inner join cd_dev.promotion.dim_patientclassification as pc         
        on pc.PatientClassificationUUID = IAD.PatientClassificationUUID
inner join cd_dev.promotion.dim_account as da         
        on IAD.ShipToAccountUUID = da.ShipToAccountUUID 
Where pc.InvoiceCalculationType = 'PerSubscription' 
AND ifnull(ExceptionMoxieCaseNumber,"") = ""
AND VersionID = 1
group by all Having count(Distinct da.ShipToAccountId) > 1 )

Select  CoolSculptingID,ScannedId,ShipToAccountId,SoldToAccountID,PatientClassificationName,CycleDate,CycleCount,
        InitialVisitShipToAccountId,InitialVisitSoldToAccountId,InitialVisitDate,FinalVisitDate,SoldToComment
FROM
(
SELECT  CASE WHEN da_cs.ShipToAccountId <> da.ShipToAccountId AND ia.SoldToAccountId <> csb.SoldToAccountId THEN "DifferentShipToDifferentSoldTo"
             WHEN ia.SoldToAccountId <> csb.SoldToAccountId THEN "SameShipToDifferentSoldTo"
             WHEN da_cs.ShipToAccountId <> da.ShipToAccountId THEN "DifferentShipToSameSoldTo"                
             WHEN da_cs.ShipToAccountId == da.ShipToAccountId AND ia.SoldToAccountId == csb.SoldToAccountId THEN 'SameShipToSameSoldTo'
             ELSE 'Unknown' END AS SoldToComment,
        Ia.CoolSculptingID,dc.ScannedId,
        da_cs.ShipToAccountId AS InitialVisitShipToAccountId,CSB.SoldToAccountID AS InitialVisitSoldToAccountId,CSB.SubscriptionStartDate AS InitialVisitDate,CSB.SubscriptionEndDate As FinalVisitDate,
        da.ShipToAccountId,Ia.SoldToAccountID,pc.PatientClassificationName,Ia.CycleDate,Ia.CycleCount,CSB.UpdatedBy,Min_CycleDate,
        row_number() over (Partition by Ia.CoolSculptingID,Ia.SoldToAccountID  Order BY CSB.SubscriptionEndDate DESC, Ia.CycleCount ASC) AS ROWNUM
From cd_dev.promotion.fact_invoiceaddendumdetails As Ia 
Left join cd_dev.promotion.dim_consumer AS dc        
        ON dc.CoolsculptingId = IA.CoolSculptingID 
left join cd_dev.promotion.fact_consumersubscription AS CSB
        ON Ia.ConsumerSubscriptionUUID = CSB.ConsumerSubscriptionUUID
        AND CSB.VersionID = 1        
inner join cd_dev.promotion.dim_account as da         
        on Ia.ShipToAccountUUID = da.ShipToAccountUUID 
inner join cd_dev.promotion.dim_account as da_cs
        on CSB.ShipToAccountUUID = da_cs.ShipToAccountUUID         
inner join cd_dev.promotion.dim_patientclassification as pc         
        on pc.PatientClassificationUUID = Ia.PatientClassificationUUID        
inner join CSID AS CS         
        on CS.CoolSculptingID = Ia.CoolSculptingID 
        AND CS.consumersubscriptionuuid = IA.consumersubscriptionuuid
Where Ia.VersionID = 1 
AND SoldToCount > 1
)

ORDER BY initialvisitdate ASC,ScannedId ASC,CycleDate ASC,SoldToAccountID

