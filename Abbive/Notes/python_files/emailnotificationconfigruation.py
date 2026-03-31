# Databricks notebook source
# MAGIC %pip install sendgrid
# MAGIC #--------dbutils.library.restartPython()

# COMMAND ----------

#import the packages
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys
import pandas as pd
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from pyspark.sql.functions import *

# COMMAND ----------

SendGridAPIClient = dbutils.secrets.get("ABV_AKV_ADB_SCOPE","SendGridAPIClient")

# COMMAND ----------

def SendNotification(sender_email,recipient_emails,cc_emails,query,subject,HTMLContent,sortByColumn,sortByAscending):

    to_emailsList = recipient_emails.split(',')
    cc_emailsList = cc_emails.split(',')
    sortByColumnList = sortByColumn.split(',')
    sortByAscendingList = sortByAscending.split(',')
    sortByAscendingList_Bool = [eval(x) for x in sortByAscendingList]

    spark_df = spark.sql(query)
    pd_df = spark_df.toPandas().sort_values(by=sortByColumnList, ascending=sortByAscendingList_Bool)
    HTMLContent = HTMLContent.replace('<basic_query>',pd_df.to_html(index=False))
    
    for email in to_emailsList:
        if email in cc_emailsList:
            cc_emailsList.remove(email)
    
    message = Mail(
        from_email=sender_email,
        to_emails=to_emailsList,
        subject=subject,
        html_content = HTMLContent
    )


    for ccemail in cc_emailsList:
        message.add_cc(cc_email=ccemail)

    sg = SendGridAPIClient(SendGridAPIClient)
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)

# COMMAND ----------

def streamEmailNotification(EmailNotificationID,df_source):
    max_retries = 5
    num_retries = 0
    while True:
        try:
            #Retrieve EmailConfiguration Details
            EmailNotificationConfiguration = spark.sql(f"SELECT * FROM promotion.DIM_EmailNotification where EmailNotificationID = '{EmailNotificationID}' and Active = True").collect()[0]

            recipient_emails = EmailNotificationConfiguration['RecipientEmails']
            cc_emails = EmailNotificationConfiguration['CCEmails']
            sender_email = EmailNotificationConfiguration['SenderEmail']
            subject = EmailNotificationConfiguration['EmailSubject']
            HTMLContent = EmailNotificationConfiguration['HTMLContent']

            to_emailsList = recipient_emails.split(',')
            cc_emailsList = cc_emails.split(',')

            #Removing PII information from Dataframe
            cols_toremove = ['coolsculptingid','phonenumber']
            source_columns = df_source.columns
            existing_columns_to_drop = [col for col in source_columns if col.lower() in cols_toremove]
            df_source = df_source.drop(*existing_columns_to_drop)
            df_pd = df_source.toPandas()

            HTMLContent = HTMLContent.replace('<dataframe_data>',df_pd.to_html(index=False))

            for email in to_emailsList:
                if email in cc_emailsList:
                    cc_emailsList.remove(email)
            
            message = Mail(
                from_email=sender_email,
                to_emails=to_emailsList,
                subject=subject,
                html_content = HTMLContent
            )

            for ccemail in cc_emailsList:
                message.add_cc(cc_email=ccemail)

            #Send Email
            sg = SendGridAPIClient(SendGridAPIClient)
            response = sg.send(message)

            print(response.status_code)
            return True
        except Exception as e:
            print("Failed to Trigger Email"+str(e))
            if num_retries > max_retries:
                print("Stream Email Notification Failed")
                return True
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(60)

# COMMAND ----------

def streamLogEmailNotification(EmailNotificationID,df_source,pipelinename,Env):
    max_retries = 5
    num_retries = 0
    while True:
        try:
            #Retrieve EmailConfiguration Details
            EmailNotificationConfiguration = spark.sql(f"SELECT * FROM promotion.DIM_EmailNotification where EmailNotificationID = '{EmailNotificationID}' and Active = True").collect()[0]

            html = f'''
                    <html> 
                    <head> 
                    </head>
                    <body>
                        Hi Team,  <br> <br>
                        Log Processing streaming for CoolSculpting Task {pipelinename} needs review while processing the below records in the streaming task. Please analyze the records listed below:<br> <br>
                        <dataframe_data>
                        <br>
                        Thanks,<br>
                            AGN-DL-CoolConnectApp-Support 
                        </p>
                        </body>
                    </html>
                    '''

            recipient_emails = EmailNotificationConfiguration['RecipientEmails']
            cc_emails = EmailNotificationConfiguration['CCEmails']
            sender_email = EmailNotificationConfiguration['SenderEmail']
            subject = f'{Env} - {pipelinename} Needs review'
            HTMLContent = html

            to_emailsList = recipient_emails.split(',')
            cc_emailsList = cc_emails.split(',')

            #Removing PII information from Dataframe
            cols_toremove = ['coolsculptingid','phonenumber']
            source_columns = df_source.columns
            existing_columns_to_drop = [col for col in source_columns if col.lower() in cols_toremove]
            df_source = df_source.drop(*existing_columns_to_drop)
            df_pd = df_source.toPandas()
            

            HTMLContent = HTMLContent.replace('<dataframe_data>',df_pd.to_html(index=False))

            for email in to_emailsList:
                if email in cc_emailsList:
                    cc_emailsList.remove(email)
            
            message = Mail(
                from_email=sender_email,
                to_emails=to_emailsList,
                subject=subject,
                html_content = HTMLContent
            )

            for ccemail in cc_emailsList:
                message.add_cc(cc_email=ccemail)

            #Send Email
            sg = SendGridAPIClient(SendGridAPIClient)
            response = sg.send(message)

            print(response.status_code)
            return True
        except Exception as e:
            print("Failed to Trigger Email"+str(e))
            if num_retries > max_retries:
                print("Stream Email Notification Failed")
                return True
            else:
                print("Retrying error", e)
                num_retries += 1
                sleep(60)
