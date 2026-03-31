-- Databricks notebook source
-- DBTITLE 1,cyclesdata_v
grant select on  goldzone.cyclesdata_v  to readonlyusers;
grant read_metadata on  goldzone.cyclesdata_v  to readonlyusers;
grant select on  goldzone.scannedid_v  to readonlyusers;
grant read_metadata on  goldzone.scannedid_v  to readonlyusers; 

grant select on  Silverzone.FACTULLogs  to readonlyusers;
grant read_metadata on  Silverzone.FACTULLogs to readonlyusers;
grant select on  Silverzone.DIMApplicatorVerCd   to readonlyusers;
grant read_metadata on  Silverzone.DIMApplicatorVerCd  to readonlyusers;
grant select on  Silverzone.errordescription   to readonlyusers;
grant read_metadata on  Silverzone.errordescription  to readonlyusers;
grant select on  promotion.dim_consumer  to readonlyusers;
grant read_metadata on  promotion.dim_consumer  to readonlyusers;

grant usage on database silverzone to readonlyusers;
grant usage on database goldzone to readonlyusers;


-- COMMAND ----------

-- DBTITLE 1,cyclesdatacom1_v
grant select on  goldzone.cyclesdatacom1_v  to readonlyusers;
grant read_metadata on  goldzone.cyclesdatacom1_v  to readonlyusers;

grant select on  silverzone.factullogs_com1  to readonlyusers;
grant read_metadata on  silverzone.factullogs_com1  to readonlyusers;

grant select on  silverzone.errordescription  to readonlyusers;
grant read_metadata on  silverzone.errordescription  to readonlyusers;

grant usage on database silverzone to readonlyusers;
grant usage on database goldzone to readonlyusers;


-- COMMAND ----------

-- DBTITLE 1,invoiceaddendumdetails_v
GRANT read_metadata on Promotion.FACT_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on Promotion.FACT_InvoiceAddendumDetails  TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account  TO readonlyusers;;
GRANT read_metadata on goldzone.invoiceaddendumdetails_v TO readonlyusers;;
GRANT select  on  goldzone.invoiceaddendumdetails_v TO readonlyusers;;
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select  on  promotion.dim_consumer TO readonlyusers;;
GRANT read_metadata on promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT select  on  promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT read_metadata on promotion.DIM_PatientClassification TO readonlyusers;;
GRANT select  on  promotion.DIM_PatientClassification TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT select  on  Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;

-- COMMAND ----------

-- DBTITLE 1,invoiceaddendum_v
GRANT read_metadata on goldzone.invoiceaddendum_v TO readonlyusers;;
GRANT select  on  goldzone.invoiceaddendum_v TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on Promotion.FACT_InvoiceAddendumDetails  TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account  TO readonlyusers;;
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select  on  promotion.dim_consumer TO readonlyusers;;
GRANT read_metadata on promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT select  on  promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT read_metadata on promotion.DIM_PatientClassification TO readonlyusers;;
GRANT select  on  promotion.DIM_PatientClassification TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT select  on  Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;

GRANT read_metadata on promotion.fact_invoiceaddendum TO readonlyusers;;
GRANT select  on promotion.fact_invoiceaddendum TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;


-- COMMAND ----------

-- DBTITLE 1,invoiceaddendumdetailssnapshot_v
GRANT read_metadata on Promotion.FACT_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on Promotion.FACT_InvoiceAddendumDetails  TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account  TO readonlyusers;;
GRANT read_metadata on goldzone.invoiceaddendumdetailssnapshot_v TO readonlyusers;;
GRANT select  on  goldzone.invoiceaddendumdetailssnapshot_v TO readonlyusers;;
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select  on  promotion.dim_consumer TO readonlyusers;;
GRANT read_metadata on promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT select  on  promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO readonlyusers;;
GRANT read_metadata on promotion.DIM_PatientClassification TO readonlyusers;;
GRANT select  on  promotion.DIM_PatientClassification TO readonlyusers;;
GRANT read_metadata on Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT select  on  Promotion.FACT_InvoiceAddendum TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT select  on  Promotion.DIM_InvoiceCycleMonth TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;
GRANT read_metadata on Promotion.DIM_Comments TO readonlyusers;;
GRANT select  on  Promotion.DIM_Comments TO readonlyusers;;

-- COMMAND ----------

-- DBTITLE 1,NonComplianceDetails
GRANT read_metadata on goldzone.promotionNoncompliancereport_v TO readonlyusers;;
GRANT select  on goldzone.promotionNoncompliancereport_v  TO readonlyusers;;
GRANT read_metadata on promotion.fact_noncompliancedetails TO readonlyusers;;
GRANT select  on promotion.fact_noncompliancedetails TO readonlyusers;;
GRANT read_metadata on promotion.fact_consumersubscription TO readonlyusers;;
GRANT select  on promotion.fact_consumersubscription TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account TO readonlyusers;;
GRANT read_metadata on promotion.dim_fraudclassification TO readonlyusers;;
GRANT select  on promotion.dim_fraudclassification TO readonlyusers;;
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select  on promotion.dim_consumer TO readonlyusers;;
GRANT read_metadata on silverzone.dimcustomermaster TO readonlyusers;;
GRANT select  on silverzone.dimcustomermaster TO readonlyusers;;

-- COMMAND ----------

-- DBTITLE 1,location tracking Views
GRANT read_metadata on goldzone.locationtracking_v TO readonlyusers;;
GRANT select  on goldzone.locationtracking_v TO readonlyusers;;
GRANT read_metadata on device_location.Fact_DeviceLocationDistance TO readonlyusers;;
GRANT select  on device_location.Fact_DeviceLocationDistance TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account TO readonlyusers;;
GRANT read_metadata on goldzone.Locationtag_v TO readonlyusers;;
GRANT select  on goldzone.Locationtag_v TO readonlyusers;;
 


-- COMMAND ----------

-- DBTITLE 1,promotion.fact_differentshipto_smartcards
GRANT read_metadata on goldzone.differentshipto_smartcards_US_v TO readonlyusers;;
GRANT select  on goldzone.differentshipto_smartcards_US_v TO readonlyusers;;

GRANT read_metadata on goldzone.differentshipto_smartcards_OUS_v TO readonlyusers;;
GRANT select  on goldzone.differentshipto_smartcards_OUS_v TO readonlyusers;;

GRANT read_metadata on promotion.fact_differentshipto_smartcards TO readonlyusers;;
GRANT select  on promotion.fact_differentshipto_smartcards TO readonlyusers;;


-- COMMAND ----------

-- DBTITLE 1,goldzone.log_cyclesvalidation_v
GRANT read_metadata on goldzone.log_cyclesvalidation_v TO readonlyusers;;
GRANT select  on goldzone.log_cyclesvalidation_v TO readonlyusers;;

GRANT read_metadata on promotion.log_cyclesvalidation TO readonlyusers;;
GRANT select  on promotion.log_cyclesvalidation TO readonlyusers;;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## International Cool Connect - OUSUsers

-- COMMAND ----------

--goldzone.cyclesdata_v 
grant usage on database silverzone to OUSUsers;
grant usage on database goldzone to OUSUsers;
grant usage on database promotion to OUSUsers;
grant select on  goldzone.cyclesdata_v  to OUSUsers;
grant read_metadata on  goldzone.cyclesdata_v  to OUSUsers;
grant select on  Silverzone.FACTULLogs  to OUSUsers;
grant read_metadata on  Silverzone.FACTULLogs to OUSUsers;
grant select on  Silverzone.DIMApplicatorVerCd   to OUSUsers;
grant read_metadata on  Silverzone.DIMApplicatorVerCd  to OUSUsers;
grant select on  Silverzone.errordescription   to OUSUsers;
grant read_metadata on  Silverzone.errordescription  to OUSUsers;
grant select on  promotion.dim_consumer  to OUSUsers;
grant read_metadata on  promotion.dim_consumer  to OUSUsers;
grant select on  silverzone.dimstartendparameter   to OUSUsers;
grant read_metadata on  silverzone.dimstartendparameter   to OUSUsers;

--goldzone.cyclesdatacom1_v
grant select on  goldzone.cyclesdatacom1_v  to OUSUsers;
grant read_metadata on  goldzone.cyclesdatacom1_v  to OUSUsers;
grant select on  silverzone.factullogs_com1  to OUSUsers;
grant read_metadata on  silverzone.factullogs_com1  to OUSUsers;
grant select on  silverzone.errordescription  to OUSUsers;
grant read_metadata on  silverzone.errordescription  to OUSUsers;
grant usage on database silverzone to OUSUsers;
grant usage on database goldzone to OUSUsers;

--goldzone.device_v
GRANT read_metadata on goldzone.device_v TO OUSUsers;;
GRANT select  on goldzone.device_v TO OUSUsers;;
GRANT read_metadata on Silverzone.FACTSysLogs TO OUSUsers;;
GRANT select  on Silverzone.FACTSysLogs TO OUSUsers;;
GRANT read_metadata on silverzone.factlastcontact_com1 TO OUSUsers;;
GRANT select  on silverzone.factlastcontact_com1 TO OUSUsers;;
GRANT read_metadata on Silverzone.FACTDeviceTwin TO OUSUsers;;
GRANT select  on Silverzone.FACTDeviceTwin TO OUSUsers;;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ODS - ODSUsers

-- COMMAND ----------

--silverzone.factullogs_com1
grant usage on database silverzone to ODSUsers;
grant usage on database goldzone to ODSUsers;
GRANT read_metadata on silverzone.factullogs_com1 TO ODSUsers;;
GRANT select  on silverzone.factullogs_com1 TO ODSUsers;;
GRANT read_metadata on silverzone.dimcustomermaster TO ODSUsers;;
GRANT select  on silverzone.dimcustomermaster TO ODSUsers;;

GRANT read_metadata on goldzone.cyclescom1_ods_v TO ODSUsers;;
GRANT select  on goldzone.cyclescom1_ods_v TO ODSUsers;;

--goldzone.cyclescom3_ods_v
GRANT read_metadata on silverzone.factullogs TO ODSUsers;;
GRANT select  on silverzone.factullogs TO ODSUsers;;
GRANT read_metadata on silverzone.dimcustomermaster TO ODSUsers;;
GRANT select  on silverzone.dimcustomermaster TO ODSUsers;;
GRANT read_metadata on Silverzone.DIMApplicatorVerCd TO ODSUsers;;
GRANT select  on Silverzone.DIMApplicatorVerCd TO ODSUsers;;
GRANT read_metadata on goldzone.device_v TO ODSUsers;;
GRANT select  on goldzone.device_v TO ODSUsers;;
GRANT read_metadata on Silverzone.FACTSysLogs TO ODSUsers;;
GRANT select  on Silverzone.FACTSysLogs TO ODSUsers;;
GRANT read_metadata on silverzone.factlastcontact_com1 TO ODSUsers;;
GRANT select  on silverzone.factlastcontact_com1 TO ODSUsers;;
GRANT read_metadata on Silverzone.FACTDeviceTwin TO ODSUsers;;
GRANT select  on Silverzone.FACTDeviceTwin TO ODSUsers;;

GRANT read_metadata on goldzone.cyclescom3_ods_v TO ODSUsers;;
GRANT select  on goldzone.cyclescom3_ods_v TO ODSUsers;;

-- COMMAND ----------

-- %md
-- ## US-SnowflakeUsers
 

-- COMMAND ----------

-- --goldzone.cyclesdata_v 
-- grant usage on database silverzone to SnowflakeUsers;
-- grant usage on database goldzone to SnowflakeUsers;
-- grant usage on database promotion to SnowflakeUsers;
-- grant select on  goldzone.cyclesdata_v  to SnowflakeUsers;
-- grant read_metadata on  goldzone.cyclesdata_v  to SnowflakeUsers;
-- grant select on  Silverzone.FACTULLogs  to SnowflakeUsers;
-- grant read_metadata on  Silverzone.FACTULLogs to SnowflakeUsers;
-- grant select on  Silverzone.DIMApplicatorVerCd   to SnowflakeUsers;
-- grant read_metadata on  Silverzone.DIMApplicatorVerCd  to SnowflakeUsers;
-- grant select on  Silverzone.errordescription   to SnowflakeUsers;
-- grant read_metadata on  Silverzone.errordescription  to SnowflakeUsers;
-- grant select on  promotion.dim_consumer  to SnowflakeUsers;
-- grant read_metadata on  promotion.dim_consumer  to SnowflakeUsers;
-- grant select on  silverzone.dimstartendparameter   to SnowflakeUsers;
-- grant read_metadata on  silverzone.dimstartendparameter   to SnowflakeUsers;

-- --goldzone.cyclesdatacom1_v
-- grant select on  goldzone.cyclesdatacom1_v  to SnowflakeUsers;
-- grant read_metadata on  goldzone.cyclesdatacom1_v  to SnowflakeUsers;
-- grant select on  silverzone.factullogs_com1  to SnowflakeUsers;
-- grant read_metadata on  silverzone.factullogs_com1  to SnowflakeUsers;
-- grant select on  silverzone.errordescription  to SnowflakeUsers;
-- grant read_metadata on  silverzone.errordescription  to SnowflakeUsers;
-- grant usage on database silverzone to SnowflakeUsers;
-- grant usage on database goldzone to SnowflakeUsers;

-- --goldzone.p3_smartcards_v
-- grant usage on database promotion to SnowflakeUsers;
-- grant usage on database goldzone to SnowflakeUsers;
-- grant select on  goldzone.p3_smartcards_v  to SnowflakeUsers;
-- grant read_metadata on  goldzone.p3_smartcards_v  to SnowflakeUsers;
-- grant select on  promotion.dim_smartcard  to SnowflakeUsers;
-- grant read_metadata on  promotion.dim_smartcard  to SnowflakeUsers;
-- grant select on  promotion.dim_promotion  to SnowflakeUsers;
-- grant read_metadata on  promotion.dim_promotion  to SnowflakeUsers;

-- --goldzone.invoiceaddendumdetails_v
-- grant usage on database silverzone to SnowflakeUsers;
-- grant usage on database goldzone to SnowflakeUsers;
-- grant usage on database promotion to SnowflakeUsers;
-- GRANT read_metadata on Promotion.FACT_InvoiceAddendumDetails TO SnowflakeUsers;
-- GRANT select  on Promotion.FACT_InvoiceAddendumDetails  TO SnowflakeUsers;
-- GRANT read_metadata on promotion.dim_account TO SnowflakeUsers;
-- GRANT select  on promotion.dim_account  TO SnowflakeUsers;
-- GRANT read_metadata on goldzone.invoiceaddendumdetails_v TO SnowflakeUsers;
-- GRANT select  on  goldzone.invoiceaddendumdetails_v TO SnowflakeUsers;
-- GRANT read_metadata on promotion.dim_consumer TO SnowflakeUsers;
-- GRANT select  on  promotion.dim_consumer TO SnowflakeUsers;
-- GRANT read_metadata on promotion.FACT_ConsumerSubscription TO SnowflakeUsers;
-- GRANT select  on  promotion.FACT_ConsumerSubscription TO SnowflakeUsers;
-- GRANT read_metadata on Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO SnowflakeUsers;
-- GRANT select  on  Promotion.FACT_InvoiceAddendum_InvoiceAddendumDetails TO SnowflakeUsers;
-- GRANT read_metadata on promotion.DIM_PatientClassification TO SnowflakeUsers;
-- GRANT select  on  promotion.DIM_PatientClassification TO SnowflakeUsers;
-- GRANT read_metadata on Promotion.FACT_InvoiceAddendum TO SnowflakeUsers;
-- GRANT select  on  Promotion.FACT_InvoiceAddendum TO SnowflakeUsers;
-- GRANT read_metadata on Promotion.DIM_InvoiceCycleMonth TO SnowflakeUsers;
-- GRANT select  on  Promotion.DIM_InvoiceCycleMonth TO SnowflakeUsers;
-- GRANT read_metadata on silverzone.dimcustomermaster TO SnowflakeUsers;
-- GRANT select  on  silverzone.dimcustomermaster TO SnowflakeUsers;
-- GRANT read_metadata on Promotion.DIM_Comments TO SnowflakeUsers;
-- GRANT select  on  Promotion.DIM_Comments TO SnowflakeUsers;
-- GRANT read_metadata on promotion.dim_promotion TO SnowflakeUsers;
-- GRANT select  on  promotion.dim_promotion TO SnowflakeUsers;

-- --goldzone.device_v
-- GRANT read_metadata on goldzone.device_v TO SnowflakeUsers;
-- GRANT select  on goldzone.device_v TO SnowflakeUsers;
-- GRANT read_metadata on Silverzone.FACTSysLogs TO SnowflakeUsers;
-- GRANT select  on Silverzone.FACTSysLogs TO SnowflakeUsers;
-- GRANT read_metadata on silverzone.factlastcontact_com1 TO SnowflakeUsers;
-- GRANT select  on silverzone.factlastcontact_com1 TO SnowflakeUsers;
-- GRANT read_metadata on Silverzone.FACTDeviceTwin TO SnowflakeUsers;
-- GRANT select  on Silverzone.FACTDeviceTwin TO SnowflakeUsers;

-- COMMAND ----------

GRANT read_metadata on goldzone.P3EligibleCycles_Subscription_v TO readonlyusers;;
GRANT select  on goldzone.P3EligibleCycles_Subscription_v TO readonlyusers;;

GRANT read_metadata on silverzone.dimstartendparameter TO readonlyusers;;
GRANT select  on silverzone.dimstartendparameter TO readonlyusers;;

GRANT read_metadata on silverzone.factullogs TO readonlyusers;;
GRANT select  on silverzone.factullogs TO readonlyusers;;

GRANT read_metadata on silverzone.dimcustomermaster TO readonlyusers;;
GRANT select  on silverzone.dimcustomermaster TO readonlyusers;;

GRANT read_metadata on device_location.dim_addresscoordinates TO readonlyusers;;
GRANT select  on device_location.dim_addresscoordinates TO readonlyusers;;
 
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select  on promotion.dim_consumer TO readonlyusers;;

GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select  on promotion.dim_account TO readonlyusers;;

GRANT read_metadata on promotion.FACT_ConsumerSubscription TO readonlyusers;;
GRANT select  on promotion.FACT_ConsumerSubscription TO readonlyusers;;

GRANT read_metadata on promotion.dim_promotion TO readonlyusers;;
GRANT select  on promotion.dim_promotion TO readonlyusers;;

GRANT read_metadata on promotion.dim_SmartCard TO readonlyusers;;
GRANT select  on promotion.dim_SmartCard TO readonlyusers;;




-- COMMAND ----------

-- DBTITLE 1,goldzone.P3DifferentSoldTo_v
GRANT read_metadata on goldzone.P3DifferentSoldTo_v TO readonlyusers;;
GRANT select on goldzone.P3DifferentSoldTo_v TO readonlyusers;;
GRANT read_metadata on promotion.fact_invoiceaddendumdetails TO readonlyusers;;
GRANT select on promotion.fact_invoiceaddendumdetails TO readonlyusers;;
GRANT read_metadata on promotion.dim_patientclassification TO readonlyusers;;
GRANT select on promotion.dim_patientclassification TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select on promotion.dim_account TO readonlyusers;;
GRANT read_metadata on promotion.fact_invoiceaddendumdetails TO readonlyusers;;
GRANT select on promotion.fact_invoiceaddendumdetails TO readonlyusers;;
GRANT read_metadata on promotion.dim_consumer TO readonlyusers;;
GRANT select on promotion.dim_consumer TO readonlyusers;;
GRANT read_metadata on promotion.fact_consumersubscription TO readonlyusers;;
GRANT select on promotion.fact_consumersubscription TO readonlyusers;;
GRANT read_metadata on promotion.dim_account TO readonlyusers;;
GRANT select on promotion.dim_account TO readonlyusers;;
GRANT read_metadata on promotion.dim_patientclassification TO readonlyusers;;
GRANT select on promotion.dim_patientclassification TO readonlyusers;;