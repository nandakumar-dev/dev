#!/usr/bin/env python
# coding: utf-8

# ## DL-296_POPEST OCP 1990-2000_codebook_silver
# 
# 
# 

# In[1]:


codebook_file_path = 'www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/1990-2000/stch-intercensal_layout.txt'


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import tempfile
import re
from pyspark.sql.window import Window


# In[3]:


BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'Bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)
container_name='bronze'


# In[4]:


wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (container_name, BLOB_ACCOUNT_NAME,codebook_file_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (container_name, BLOB_ACCOUNT_NAME), BLOB_SAS_TOKEN) 

df = spark.read .option("delimiter", "/n").option("header", "false").option('multiline','true').csv(wasbs_path, inferSchema=True)


# In[5]:


data_list=df.collect()


# In[7]:


data_map = []
current_d = None
current_v = None

# Iterate through the list of rows
for row in data_list:
    line = row._c0.strip()

    if (line !='' and line != None and line[0].isalpha() and line.endswith(':')):
        current_d=line
        current_d = re.sub(r'The key for year code is as follows:', 'Year', current_d)
        current_d = re.sub(r'The key for Age group code is as follows:', 'Age_Group', current_d)
        current_d = re.sub(r'The key for the race-sex group is as follows:', 'Race-Sex', current_d)
        current_d = re.sub(r'The key for the Ethnic origin code is as follows:', 'Ethnic_origin', current_d)   

    elif (line !='' and line != None  and line[0].isdigit() and current_d is not None ):
        current_v='\t'.join(line.split('='))
        value=current_d+'\t'+current_v
        value=value.split('\t')
        if len(value) == 3:
            value_x = tuple(value2.strip() for value2 in value if value2.strip())
            data_map.append((value_x))


# In[8]:


schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("Description", StringType(), True)])

final_df=spark.createDataFrame(data_map, schema=schema)


# In[9]:


ouput_file_path=codebook_file_path.split('.')[:-1]
ouput_file_path='.'.join(ouput_file_path)+'.csv'

pandas_df=final_df.toPandas()
csv_data=pandas_df.to_csv(index=False)
# Create a blob client for the new file
blob_client = blob_service_client.get_blob_client(container=container_name, blob=ouput_file_path)
# Upload the CSV data to the blob
blob_client.upload_blob(csv_data, overwrite=True)
print("CSV file uploaded successfully:",ouput_file_path)


# In[10]:


# silver storage access
# Azure storage access info 
blob_account_name = 'usafactssilver'
blob_container_name = 'silver'
linked_service_name = 'silver' 
blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 
ouput_file_path=codebook_file_path.split('.')[:-1]
ouput_file_path='.'.join(ouput_file_path)
# Allow SPARK to access from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, ouput_file_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
final_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",wasbs_path).save()
print('file_written: ',ouput_file_path)

