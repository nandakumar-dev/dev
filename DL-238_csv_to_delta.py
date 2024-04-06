#!/usr/bin/env python
# coding: utf-8

# ## DL-238_csv_to_delta
# 
# 
# 

# In[2]:


blob_relative_path = 'bjs.ojp.gov/media/'


# In[3]:


import requests
from pyspark.sql.functions import split,col,lit,regexp_replace
from azure.storage.blob import BlobServiceClient
import pandas as pd
import re
from bs4 import BeautifulSoup
from io import StringIO
import urllib
import urllib.parse
import urllib.request
import re
from urllib.parse import urlparse
from pyspark.sql import functions as F 


# In[8]:


def none_replace(data):

    temp_list = []
    temp_value = None

    for value in data:
        if value is None:
            value = temp_value
        else:
            temp_value = value
        temp_list.append(value)

    return temp_list


# In[9]:


def concat_row(data,none_count):

    concat_list1 = []
    concat_list = []

    if none_count == 1:
        iter_range = 2
    else:
        iter_range = none_count
    for index in range(iter_range):
        if (none_count == 1 and index == 0) or (index+1 < none_count) :        
            temp_list = none_replace(data[index])
            if len(concat_list1) >= 1:
                concat_list1 = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,temp_list)]
            else:
                concat_list1 =  temp_list
        else:     
            if concat_list1[0] is None and data[index][0] is None :
                data[index] =  none_replace(data[index])
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,data[index])]
            data[index] = concat_list
            if none_count == 1 :
                data = data[index:]
            else:
                data = data[none_count-1:]
    return data


# In[10]:


def process_value(value):
    if value is not None:
        cleaned_value = re.sub(r'[,*().%]', '', str(value))  
        return not cleaned_value.isnumeric()
    else:
        return True  


# In[11]:


def filtered_data(clean_data):
    data = []
    header =[]
    footer =[]
    for row in clean_data:

        row = [None if value is not None and value.strip() == ''  else value for value in row ]
        row = [value.strip() if value is not None else value for value in row]  

        cleaned_row = list(filter(lambda values: values is not None , row))

        if  len(cleaned_row) >= 2  :
            cleaned_row=[val for val in cleaned_row if val!='�']
            cleaned_row=[val for val in cleaned_row if val!='!']
            cleaned_row=[val for val in cleaned_row if val!='*']
            data.append(cleaned_row)
            
        elif len(data) < 1 :
            header += cleaned_row 
        else:
            footer += cleaned_row
        none_count = 0 
        for value in data :
            if value.count(None) > 1 or (none_count <= 1 and value.count(None) == 1) or value[0] is None :
                none_count += 1
            else:
                break 
  
    if none_count > 1:
        data = concat_row(data,none_count) 

    data[0] = [None if value is not None and value.strip() == '' else value for value in data[0] ]
    data[1] = [None if value is not None and value.strip() == '' else value for value in data[1] ]
    if data[0][0] is None or data[0][0].strip()=='' :
        result = all(process_value(value) for value in data[1]) 
        if result :
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(data[0],data[1])]
            data[1] = concat_list
            data = data[1:] 
    if None in data[1]  or None in data[0]:
        result = all(process_value(value) for value in data[1]) 
        if result :
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(data[0],data[1])]
            data[1] = concat_list
            data = data[1:]
    return data,header,footer


# In[12]:


def replace_percent_with_none(value):
    if value == '%':
        return None
    return value


# In[13]:


def clean_df(df):
    df = df.applymap(replace_percent_with_none)
    df = df.dropna(axis=0, how='all')
    df = spark.createDataFrame(df,schema=None)
    empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count()]
    df = df.drop(*empty_columns)
    first_row = df.first()
    if  any(cell is None for cell in first_row) :
        raw_data = df.collect()

        data,header,footer = filtered_data(raw_data)
   
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip()) if value is not None else ' ' for value in data[0]]
        df = spark.createDataFrame(data[1:], cleaned_columns)

        emp_count = 0
        for c in df.columns:
            if ' ' in c:
                emp_count += 1
        col_count=0
        if emp_count > 1:
            df = df.drop(' ')
        empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count() and (column.strip() == '' )]
        df = df.drop(*empty_columns)
        if len(df.columns) != len(list(set(df.columns))):
            columns = []
            for ind,col in enumerate(df.columns):
                columns.append('col_'+str(ind))
            df = spark.createDataFrame(data,columns)
        
        if header:
            header = '. '.join(header)
            df = df.withColumn("Header",lit(header))
        if footer:
            footer ='. '.join(footer)
            df = df.withColumn("Footer",lit(footer))
    else:
        data = df.collect()
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip()) if value is not None else ' ' for value in data[0]]
        df = spark.createDataFrame(data[1:],cleaned_columns)
    for column in df.columns :
        if column.strip() == '':
            df = df.withColumnRenamed(column,'YEAR')

    return df


# In[14]:


def filtered_data_1(clean_data):
    data = []
    header =[]
    footer =[]
    for row in clean_data:
        row = [None if value is not None and value.strip() == ''  else value for value in row ]
        row = [value.strip() if value is not None else value for value in row]  
    
        cleaned_row = list(filter(lambda values: values is not None , row))

        if  len(cleaned_row) >= 2  :
            data.append(row)
            
        elif len(data) < 1 :
            header += cleaned_row 
        else:
            footer += cleaned_row
        none_count = 0 
        for value in data :
            if value.count(None) > 1 or (none_count <= 1 and value.count(None) == 1) or value[0] is None :
                none_count += 1
            else:
                break 

    if none_count > 1:
        data = concat_row(data,none_count) 

    data[0] = [None if value is not None and value.strip() == '' else value for value in data[0] ]
    data[1] = [None if value is not None and value.strip() == '' else value for value in data[1] ]
    if data[0][0] is None or data[0][0].strip()=='' :
        result = all(process_value(value) for value in data[1]) 
        if result :
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(data[0],data[1])]
            data[1] = concat_list
            data = data[1:] 
    if None in data[1]  or None in data[0]:
        result = all(process_value(value) for value in data[1]) 
        if result :
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(data[0],data[1])]
            data[1] = concat_list
            data = data[1:]
    return data,header,footer

def clean_df_1(df):

    df = df.applymap(replace_percent_with_none)
    df = df.dropna(axis=0, how='all')
    df = spark.createDataFrame(df)
    empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count()]
    df = df.drop(*empty_columns)
    first_row = df.first()
    if  any(cell is None for cell in first_row) :
        raw_data = df.collect()
        data,header,footer = filtered_data_1(raw_data)
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip()) if value is not None else ' ' for value in data[0]]
        df = spark.createDataFrame(data[1:], cleaned_columns)
        empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count() and (column.strip() == '' )]
        df = df.drop(*empty_columns)
        if len(df.columns) != len(list(set(df.columns))):
            columns = []
            for ind,col in enumerate(df.columns):
                columns.append('col_'+str(ind))
            df = spark.createDataFrame(data,columns)
        
        if header:
            header = '. '.join(header)
            df = df.withColumn("Header",lit(header))
        if footer:
            footer ='. '.join(footer)
            df = df.withColumn("Footer",lit(footer))
    else:
        data = df.collect()
        cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip()) if value is not None else ' ' for value in data[0]]
        df = spark.createDataFrame(data[1:],cleaned_columns)
        
    return df   

            


# In[17]:


blob_account_name = 'usafactsbronze' 
blob_container_name = 'bronze'
linked_service_name = 'bronze'

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 

# Allow SPARK to access from Blob remotely 
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 


# In[18]:


# Azure storage access info 
blob_account_name = 'usafactssilver'
blob_container_name = 'silver'
linked_service_name = 'silver'

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 

silver_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 


# In[19]:


def get_file_paths(dir_path):
    file_paths= []
    files = mssparkutils.fs.ls(dir_path)
    for file in files:
        allowed_extensions = ['.csv']
        if file.isDir :
            file_paths.extend(get_file_paths(file.path))
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            file_paths.append(file.path)

    return file_paths


# In[20]:


all_file_paths = get_file_paths(wasbs_path)


# In[32]:


for file in all_file_paths :
    try:
        df1 = spark.read.csv(file,header=False)
        df1 =df1.toPandas()
        file_location = silver_path +file.split(blob_relative_path)[-1]
        file_location='.'.join(file_location.split('.')[:-1])
        final_df = clean_df(df1)
        final_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
        print(file_location,'sucessfully uploaded')
    except Exception as e:
        try:      
            final_df = clean_df_1(df1)
            final_df = final_df.select([col(c).alias(c.replace('�', '')) for c in final_df.columns]).drop(col(' '))
            final_df = final_df.select([col_name for col_name in final_df.columns if final_df.filter(col(col_name).isNull()).count() == 0])
            final_df=final_df.coalesce(1)
            final_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
            print(file_location,'sucessfully uploaded')
        except Exception as e:
            print(f'error_file:{file}',str(e))

