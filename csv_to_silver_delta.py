#!/usr/bin/env python
# coding: utf-8

# ## csv_to_silver_delta
# 
# 
# 

# In[20]:


blob_relative_path = 'www.cbp.gov/sites/default/files/assets/documents/'
date_runid_trigger=''
file_extension=''
header='True'


# Azure storage access info for Bronze

# In[21]:


from pyspark.sql.functions import col
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd


# In[22]:


blob_account_name = 'usafactsbronze' 
blob_container_name = 'bronze'  
linked_service_name = 'bronze'
blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
bronze_wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 


# get_file_paths function will read all sub folder and returns only csv files

# In[23]:


def get_file_paths(dir_path):
    file_paths= []
    files = mssparkutils.fs.ls(dir_path)
    for file in files:
        
        if file.isDir :
            file_paths.extend(get_file_paths(file.path))
        else:
            if (file_extension== None or file_extension=='' ) or file.path.endswith(f'{file_extension}') :
                path = file.path.split(bronze_wasbs_path)[-1]
                file_paths.append(path)

    return file_paths


# all_file_path contains all csv file paths

# In[24]:


all_file_path = get_file_paths(bronze_wasbs_path)


# Azure Storage Access info for Silver

# In[25]:


container_name='bronze'
BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'Bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)


# In[26]:


blob_account_name = 'usafactssilver'  
blob_container_name = 'silver'  
linked_service_name = 'silver'
blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
silver_wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 


# Reading all csv files from Bronze and writing into Silver

# In[27]:


error_list=[]
for file_path in all_file_path:
    try:
        spark_df = spark.read.option('multiLine','true').csv(bronze_wasbs_path+file_path,header = f'{header}')
        # spark_df = spark.read.option('multiLine','true').option("mode", "DROPMALFORMED").csv(bronze_wasbs_path+file_path,header = f'{header}')
        spark_df = spark_df.select([col(col_name).alias(col_name.replace(' ','_').replace('.','_').replace('(','_').replace(')','_')) for col_name in spark_df.columns])
        file_location = silver_wasbs_path+file_path
        spark_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location.replace('.csv','')).save()
        # display(spark_df)
        print("file uploaded sucessfully",file_location)
    except Exception as e:
        # Handle the exception
        print(f"An error occurred while processing {file_path}: {str(e)}")
        error_list.append((f'{file_path}', str(e)))
    


# In[28]:


if len(error_list) !=0:
    pandas_df = pd.DataFrame(error_list,columns=["URL","Reason"])
    filelocation = f'{blob_relative_path}'+'error_files'+'/'+ f"error_list_{date_runid_trigger}.csv"
    blob_client = blob_service_client.get_blob_client(container_name,f"{filelocation}")
    csv_file = pandas_df.to_csv(index=False)
    # blob_client.upload_blob(csv_file,overwrite=True)
    print('error_file_uploaded')

