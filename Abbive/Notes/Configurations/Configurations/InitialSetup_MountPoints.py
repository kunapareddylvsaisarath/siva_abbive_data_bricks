# Databricks notebook source
subscriptionId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-SubscriptionId")
tenantId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-TenantId")
clientId = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-ClientId")
clientSecret = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-agnadb-SPN-Secret")
resourceGroup = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-ResourceGroup")
QueueConnectionString = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","ABV-QueueConnectionString")


# COMMAND ----------

# dbutils.fs.unmount("/mnt/raw")
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": clientId,
          "fs.azure.account.oauth2.client.secret": clientSecret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenantId+"/oauth2/token"}


# COMMAND ----------

ADLSGen2ConnectionString = dbutils.secrets.get(scope="ABV_AKV_ADB_SCOPE", key="ADLSGen2ConnectionString")
adlsGen2Name = ADLSGen2ConnectionString.split(';')[1].split('=')[1]

# COMMAND ----------

ADLSContainerList = ['raw','silver','gold','invoicereports']
for container in ADLSContainerList:
    if not any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
          source = "abfss://{}@{}.dfs.core.windows.net/".format(container,adlsGen2Name),
          mount_point = "/mnt/{}".format(container),
          extra_configs = configs)
        print("/mnt/{}   Mounted".format(container))

# COMMAND ----------

BlobStorageConnectionString = dbutils.secrets.get(scope="ABV_AKV_ADB_SCOPE", key="BlobStorageConnectionString")
BlobStorageAccountName = BlobStorageConnectionString.split(';')[1].split('=')[1]
BlobStorageAccountKey = BlobStorageConnectionString.split(';')[2].replace('AccountKey=','')
print(BlobStorageAccountName)

spark.conf.set("fs.azure.account.key."+BlobStorageAccountName+".dfs.core.windows.net",BlobStorageAccountKey)

# COMMAND ----------

BlobContainerList = ['usbdecrypted-zip','logs','logs-com1-raw','logs-com1-legacy-processed','lastcontactreport', 'logs-scanned']
for container in BlobContainerList:
    if not any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
          source = "wasbs://{}@{}.blob.core.windows.net".format(container,BlobStorageAccountName),
          mount_point = "/mnt/"+container,
          extra_configs = {"fs.azure.account.key.{}.blob.core.windows.net".format(BlobStorageAccountName):BlobStorageAccountKey})
        print("/mnt/{}   Mounted".format(container))  

# COMMAND ----------

access_key = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","S3-PDM-AccessKey")
secret_key = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","S3-CoolConnect")

encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "agn-dw-prod-ingest/outbound/CoolConnect"
mount_name = "s3_pdm"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
# display(dbutils.fs.ls(f"/mnt/{mount_name}"))