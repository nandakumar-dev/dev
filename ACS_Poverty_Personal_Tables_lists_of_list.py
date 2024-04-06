#!/usr/bin/env python
# coding: utf-8

# ## ACS_Poverty_Personal_Tables_lists_of_list
# 
# 
# 

# In[8]:


import requests
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re


# In[9]:


input_url_blob_path='api.census.gov/ACS_Poverty_Family_Tables_to_bronze.csv'


# In[10]:


BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'bronze'
input_container_name='bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
source_blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)
container_client = source_blob_service_client.get_container_client(input_container_name)
data_folder_name='api.census.gov/data'
blobs = container_client.list_blobs(name_starts_with=data_folder_name)


# In[11]:


input_container_name='ingestion-meta'
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (input_container_name, BLOB_ACCOUNT_NAME,input_url_blob_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (input_container_name, BLOB_ACCOUNT_NAME), BLOB_SAS_TOKEN) 

input_txt_file_df = spark.read.csv(wasbs_path,header=True)
input_txt_file_df = input_txt_file_df.select(*[regexp_replace(col_name, ' ','%20').alias(col_name) for col_name in input_txt_file_df.columns])
new_column_names = [col_name.upper() for col_name in input_txt_file_df.columns]
input_txt_file_df = input_txt_file_df.select([col(col_name).alias(new_col_name) for col_name, new_col_name in zip(input_txt_file_df.columns, new_column_names)])
url_df = input_txt_file_df.select("URL")
url_list = url_df.rdd.map(lambda row: row[0]).collect()


# In[12]:


url_list=[url.replace('https://','') for url in url_list]


# In[13]:


# Azure storage access info 
blob_account_name = 'usafactssilver'
blob_container_name = 'silver'
linked_service_name = 'silver' 

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 

# Allow SPARK to access from Blob remotely
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 


# In[14]:


for blob in blobs:
    if blob.name in url_list: 
        blob_client = container_client.get_blob_client(blob) 
        blob_data = blob_client.download_blob().readall()
        json_data = json.loads(blob_data)
        df = pd.DataFrame(json_data[1:], columns=json_data[0],dtype=str)
        spark_schema = StructType([StructField(col, StringType(), True) for col in df.columns])
        spark_df = spark.createDataFrame(df, schema=spark_schema)
        special_characters = r'[^a-zA-Z0-9_/.]'
        file_path = re.sub(special_characters, '_', blob.name)
        wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name,file_path) 
        spark_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",wasbs_path).save()
        print('file_uploaded:', file_path)

