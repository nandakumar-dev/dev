#!/usr/bin/env python
# coding: utf-8

# ## DL-299_298_creating_url
# 
# 
# 

# In[43]:


from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd


# In[44]:


table_no='S1702'
ouput_file_path='api.census.gov/ACS_Poverty_Family_Tables_to_bronze.csv'


# In[45]:


BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'Bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)

container_name='ingestion-meta'


# In[46]:


url_list=[]
for year in range(2008,2023):
    url1=f'https://api.census.gov/data/{year}/acs/acs1/subject?get=group({table_no})&for=us&key=398b3cad84fc969d5a102ee6cf99b373fe71da10'
    url2=f'https://api.census.gov/data/{year}/acs/acs1/subject?get=group({table_no})&for=state:*&key=398b3cad84fc969d5a102ee6cf99b373fe71da10'
    url_list.append(url1)
    url_list.append(url2)
df=pd.DataFrame(url_list,columns=['URL'])


# In[47]:


csv_data=df.to_csv(index=False)

# Create a blob client for the new file
blob_client = blob_service_client.get_blob_client(container=container_name, blob=ouput_file_path)

# Upload the CSV data to the blob
blob_client.upload_blob(csv_data, overwrite=True)

print("CSV file uploaded successfully:",ouput_file_path)

