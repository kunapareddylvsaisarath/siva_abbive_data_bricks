# Databricks notebook source
import requests
import json
import time
from pyspark.sql.functions import *

# COMMAND ----------

api_token = dbutils.secrets.get(scope="ABV_AKV_ADB_SCOPE", key="DatabricksTokenForWorkflows")
url = spark.conf.get("spark.databricks.workspaceUrl")
workspace_url = f"https://{url}/api/2.1"
print(workspace_url)

# COMMAND ----------

headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

# COMMAND ----------

API_RETRY_LIMIT = 5

# COMMAND ----------

def RunJob(job_id):
    endpoint = f"{workspace_url}/jobs/run-now"

    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

    data = {
        'job_id': job_id
    }

    response = requests.post(endpoint, headers=headers, json=data)

    if response.status_code == 200:
        print(f"Job with ID {job_id} was Started.")
    else:
        print(f"Failed to start a job with ID {job_id}. Error: {response.text}")
        return False


# COMMAND ----------

def CancelJobRun(job_id):
    endpoint = f"{workspace_url}/jobs/runs/cancel-all"

    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

    data = {
        'job_id': job_id
    }

    response = requests.post(endpoint, headers=headers, json=data)

    if response.status_code == 200:
        print(f"Job with ID {job_id} was successfully canceled.")
    else:
        print(f"Failed to cancel job with ID {job_id}. Error: {response.text}")
        return False


# COMMAND ----------

def listJobDetails():
    endpoint = f"{workspace_url}/jobs/list"
    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
      data = {
        'limit': 50
      }
      response = requests.get(endpoint, headers=headers, json = data)
      if(response.status_code == 200): 
          print(f"Successfully retrieved the job details.")
          response_data = response.json()
          return response_data
      elif(500 <= response.status_code <= 599 or response.status_code == 429):
        print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
        print("Waiting for 90 seconds...")
        sleep(90)
        retry_count += 1
      else:
        raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')
    raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')

# COMMAND ----------

def getJobDetails(job_id):
    endpoint = f"{workspace_url}/jobs/get"


    data = {
    'job_id': job_id
            }

    response = requests.get(endpoint, headers=headers, json=data)

    if response.status_code == 200:
        print(f"Successfully got the Job {job_id} ID details.")
        response_data = response.json()
        return response_data
    else:
        print(f"Failed to get Job Details for the job with ID {job_id}. Error: {response.text}")
        return False


# COMMAND ----------

def updateJobSchedule(extract_dict):

    endpoint = f"{workspace_url}/jobs/update"
    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
        data = {
            "job_id": extract_dict['job_id'],
            "new_settings": {
            "schedule": extract_dict['schedule']
            }
        }

        response = requests.post(endpoint, headers=headers, json=data)

        if response.status_code == 200:
            print(f"Job with ID {extract_dict['job_id']} was successfully {extract_dict['schedule']['pause_status']}.")
            IsException = False
            return IsException
        elif(500 <= response.status_code <= 599 or response.status_code == 429):
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            print("Waiting for 90 seconds...")
            sleep(90)
            retry_count += 1
        else:
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            IsException = True
            return IsException
        IsException = True
        return IsException

# COMMAND ----------

def getJobRunDetails(job_id):

    endpoint = f"{workspace_url}/jobs/runs/list"
    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
        data = {
            'job_id': job_id,
            'limit' : '25',
            'expand_tasks' : True
        }

        response = requests.get(endpoint, headers=headers, json=data)
        status = response.text

        if response.status_code == 200:
            print(f"Job with ID {job_id} run details was collected Successfully")
            response_data = response.json()
            IsException = False
            return response_data,IsException
        elif(500 <= response.status_code <= 599 or response.status_code == 429):
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            print("Waiting for 90 seconds...")
            sleep(90)
            retry_count += 1
        else:
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            IsException = True
            return status,IsException
        IsException = True
        return status,IsException


# COMMAND ----------

def restartCluster(cluster_id):
    workspace_url = f"https://{url}/api/2.0"
    endpoint = f"{workspace_url}/clusters/restart"

    data = {
        'cluster_id':cluster_id
    }

    response = requests.post(endpoint, headers=headers, json=data)

    if response.status_code == 200:
        print(f"Cluster with Cluster ID {cluster_id} was successfully restarted.")
        response_data = response.json()
        return response_data
    else:
        print(f"Failed to restart the cluster with ID {cluster_id}. Error: {response.text}")


# COMMAND ----------

def listClusters():
    workspace_url = f"https://{url}/api/2.0"
    endpoint = f"{workspace_url}/clusters/list"
    retry_count = 0
    while (retry_count <= API_RETRY_LIMIT):
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            return response_data
        elif(500 <= response.status_code <= 599 or response.status_code == 429):
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            print("Waiting for 90 seconds...")
            sleep(90)
            retry_count += 1
        else:
            raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')
        raise Exception(f'url:{endpoint};', f'response_code:{response.status_code};' f'error_message:{response.text}')

# COMMAND ----------

def terminateCluster(cluster_id):
    workspace_url = f"https://{url}/api/2.0"
    endpoint = f"{workspace_url}/clusters/delete"
    retry_count = 0
    IsException = True
    while (retry_count <= API_RETRY_LIMIT):
        data = {
            'cluster_id':cluster_id
        }

        response = requests.post(endpoint, headers=headers, json=data)

        if response.status_code == 200:
            print(f"Cluster with cluster {cluster_id} ID was successfully terminated.")
            IsException = False
            return IsException
        elif(500 <= response.status_code <= 599 or response.status_code == 429):
            print(f'url:{url};', f'response_code:{response.status_code};' f'error_message:{response.text}')
            print("Waiting for 90 seconds...")
            sleep(90)
            retry_count += 1
        else:
            return IsException
        return IsException


# COMMAND ----------

def startCluster(cluster_id):
    workspace_url = f"https://{url}/api/2.0"
    endpoint = f"{workspace_url}/clusters/start"

    data = {
        'cluster_id':cluster_id
    }

    response = requests.post(endpoint, headers=headers, json=data)

    if response.status_code == 200:
        print(f"Cluster with cluster {cluster_id} ID was successfully started.")
        response_data = response.json()
        return response_data
    else:
        print(f"Failed to start the cluster with ID {cluster_id}. Error: {response.text}")
        print(f"Status Code:{response.status_code}")
        return False


# COMMAND ----------

# import time
# def graceStop(query,status_flag):
#     while status_flag == 1:
#         status_flag = spark.sql("Select StreamFlag from test.streamFlag where TaskName = 'Test_stream'").collect()[0][0]
#         print(f"Status of a Stream Flag:{status_flag}")
#         print(f"Status of a stream query:{query.status}")
#         print("Sleep for 60 Sec")
#         if status_flag == 1 :
#             sleep(60)
#     if (status_flag == 0):
#         #Stop_Process=1
#         print(query.status.get('isTriggerActive'))
#         if not query.status.get('isTriggerActive'):
#             print("Job Terminated")
#             query.stop()
#         else:
#             return 0

# COMMAND ----------

# def batchEnd(query,stop_process):
#     if stop_process == 0:
#         query.stop()

# COMMAND ----------

