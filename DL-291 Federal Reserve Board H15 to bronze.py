#!/usr/bin/env python
# coding: utf-8

# ## DL-291 Federal Reserve Board H15 to bronze
# 
# 
# 

# In[111]:


pip install lxml


# In[112]:


from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
from pyspark.sql import SparkSession
from lxml import etree


# In[113]:


blob_name='www.federalreserve.gov/datadownload/'


# In[114]:


BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'Bronze'
container_name='bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
# Create a BlobServiceClient
blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)


# In[115]:


# Azure storage access info 
blob_account_name = 'usafactssilver' # replace with your blob name 
blob_container_name = 'silver' # replace with your container name 
linked_service_name = 'silver' # replace with your linked service name 

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/' % (blob_container_name, blob_account_name) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 


# In[123]:


container_client = blob_service_client.get_container_client(container_name)

# List blobs in the folder
blobs = container_client.list_blobs(name_starts_with=blob_name)

for blob in blobs:
    if blob.name.endswith('xml'):
        # Download the XML file to a local file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        local_file_path = "local_copy.xml"
        with open(local_file_path, "wb") as local_file:
            blob_data = blob_client.download_blob()
            blob_data.readinto(local_file)

        # Parse XML file
        tree = etree.parse(local_file_path)
        root = tree.getroot()
        
        try:

            data_lists = root.xpath('.//frb:DataSet', namespaces=root.nsmap)
            for data in data_lists:
                file_id = data.get('id')
                break

            # Use XPath with a wildcard for the namespace
            observations = root.xpath('.//*[local-name()="Obs"]')

            # Initialize a list of tuples to store rows of data
            rows = []

            for obs in observations:

                status = obs.get('OBS_STATUS')
                time_period = obs.get('TIME_PERIOD')
                obs_value = float(obs.get('OBS_VALUE'))
                rows.append((file_id,status,obs_value,time_period))

            columns = ['file_id',"OBS_STATUS","OBS_VALUE","TIME_PERIOD", ]

            # Create PySpark DataFrame
            df = spark.createDataFrame(rows, columns)
            df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",wasbs_path+blob_name).save()
            print('file_writen:',blob.name)
        
        except:
            # Extract all CodeList names and descriptions
            code_lists = root.xpath('.//structure:CodeList', namespaces=root.nsmap)

            data=[]
            for code_list in code_lists:

                code_list_id = code_list.get('id')
                code_list_agency =code_list.get('agency')
                code_list_name = code_list.xpath('./structure:Name', namespaces=root.nsmap)[0].text

                # Extract and print descriptions
                for code in code_list.xpath('./structure:Code', namespaces=root.nsmap):
                    code_value = code.get('value')
                    code_description = code.xpath('./structure:Description', namespaces=root.nsmap)[0].text
                    data.append((file_id,code_list_agency,code_list_id,code_list_name,code_value,code_description))

            df = spark.createDataFrame(data,schema=['file_id','agency','id','code_list_name','code_value','code_description'])
            df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",wasbs_path+blob_name).save()
            print('file_writen:',blob.name)

        # Remove the local file after reading
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file {local_file_path} removed successfully.")
        else:
            print(f"Local file {local_file_path} does not exist.")

