#!/usr/bin/env python
# coding: utf-8

# ## DL-296_popest_to_silver
# 
# 
# 

# In[72]:


from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from azure.storage.blob import BlobServiceClient, BlobClient


# File Path

# In[73]:


file_blob_name = "www2.census.gov/programs-surveys/popest/tables/1990-2000/intercensal/st-co/"
code_book_blob_path='www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/1990-2000/stch-intercensal_layout.txt'
code_book_blob_path=code_book_blob_path.replace('.txt','.csv')


# Blob Connection

# In[74]:


# Azure storage access info 
BLOB_ACCOUNT_NAME = 'usafactsbronze'
LINKED_SERVICE_NAME = 'Bronze'
BLOB_SAS_TOKEN = mssparkutils.credentials.getConnectionStringOrCreds(LINKED_SERVICE_NAME)
blob_service_client = BlobServiceClient("https://{}.blob.core.windows.net".format(BLOB_ACCOUNT_NAME), credential=BLOB_SAS_TOKEN)
blob_container_name = 'bronze'


# In[75]:


# silver storage access
# Azure storage access info 
blob_account_name = 'usafactssilver'
silver_container_name = 'silver'
linked_service_name = 'silver' 

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name)


# Reading files from bronze

# In[76]:


def read_df(blob_path):
    # Allow SPARK to access from Blob remotely 
    wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, BLOB_ACCOUNT_NAME, blob_path) 
    spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, BLOB_ACCOUNT_NAME), BLOB_SAS_TOKEN) 
    return wasbs_path


# In[77]:


def write_df(output_blob_path):
    # Allow SPARK to access from Blob remotely
    wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (silver_container_name, blob_account_name, output_blob_path)
    spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (silver_container_name, blob_account_name), blob_sas_token)
    return wasbs_path


# In[78]:


def mapping_data(code_book_df):
    column_updates = {}

    # Iterate through the codebook DataFrame and store the updates in the dictionary
    for row in code_book_df.collect():
        value = row['Code']
        valuelabel = row['Description']
        variable_name = row['Name']

        if variable_name in final_df.columns:
            condition = (col(variable_name) == value)
            update_value = lit(valuelabel)

            if variable_name in column_updates:
                column_updates[variable_name] = when(condition, update_value).otherwise(column_updates[variable_name])
            else:
                column_updates[variable_name] = when(condition, update_value).otherwise(col(variable_name))
    return column_updates


# In[79]:


code_book_df= spark.read.option("multiLine","true").csv(read_df(code_book_blob_path),header=True)


# In[80]:


container_client = blob_service_client.get_container_client(blob_container_name)

# List blobs in the folder
blobs = container_client.list_blobs(name_starts_with=file_blob_name)

for blob in blobs:
    if blob.name.endswith(".txt"): 
        #output file path
        output_blob_path=blob.name.replace('.txt','')
        input_txt_file_df = spark.read.text(read_df(blob.name))
        df=input_txt_file_df.collect()
        value_list=[]
        for row in df:
            line = row.value.strip()
            line=line.split(' ')
            line=tuple(value.strip() for value in line if value.strip())
            value_list.append(line)
        schema = StructType([
            StructField("Year", StringType(), True),
            StructField("FIPS_code", StringType(), True),
            StructField("Age_Group", StringType(), True),
            StructField("Race-Sex", StringType(), True),
            StructField("Ethnic_origin", StringType(), True),
            StructField("POP", StringType(), True)])
        final_df=spark.createDataFrame(value_list, schema=schema)
        column_updates=mapping_data(code_book_df)
        # Apply the updates to the target DataFrame
        for column_name, update_expr in column_updates.items():
            final_df = final_df.withColumn(column_name, update_expr)
        final_df.write.format('delta').mode('overwrite').option("overwriteSchema",True).option("path",write_df(output_blob_path)).save()
        print('file_written: ', output_blob_path)
        

