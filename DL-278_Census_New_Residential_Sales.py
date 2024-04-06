#!/usr/bin/env python
# coding: utf-8

# ## DL-278
# 
# 
# 

# ## DL-259
# 
# 
# 

# In[103]:


blob_relative_path = "www.census.gov/construction/nrs/xls/"


# In[104]:


from notebookutils import mssparkutils  
from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
import re
import pandas as pd
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import *
# Azure storage access info 
blob_account_name = 'usafactsbronze' # replace with your blob name 
blob_container_name = 'bronze' # replace with your container name 
linked_service_name = 'bronze' # replace with your linked service name 

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 

# Allow SPARK to access from Blob remotely 
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 

blob_service_client = BlobServiceClient(account_url=f'https://{blob_account_name}.blob.core.windows.net/', credential=blob_sas_token)
container_client = blob_service_client.get_container_client(blob_container_name)


# In[105]:


pip install xlrd


# In[106]:


# Azure storage access info 
blob_account_name = 'usafactssilver' # replace with your blob name 
blob_container_name = 'silver' # replace with your container name 
linked_service_name = 'silver' # replace with your linked service name 

blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name) 

# Allow SPARK to access from Blob remotely 
target_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path) 
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token) 


# In[107]:


def get_file_paths(dir_path):
    file_paths= []
    files = mssparkutils.fs.ls(dir_path)
    for file in files:
        allowed_extensions = ['.xls', '.xlsx']
        if file.isDir :
            file_paths.extend(get_file_paths(file.path))
        if any(file.path.endswith(ext) for ext in allowed_extensions):
            file_paths.append(file.path)

    return file_paths


# In[108]:


all_file_paths = get_file_paths(wasbs_path)


# In[109]:


def concat_xls_row(data,none_count):
                
    concat_list1 = []
    concat_list = []
    if none_count == 1:
        iter_range = 2
    else:
        iter_range = none_count
    for index in range(iter_range):
        if (none_count == 1 and index == 0) or index+1 < none_count :
            temp_value = None
            temp_list =[]
            for value in data[index]:
                if value is None:
                    value = temp_value
                else:
                    temp_value = value
                temp_list.append(value)
            if len(concat_list1) >= 1:
                concat_list1 = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,temp_list)]
            else:
                concat_list1 =  temp_list
        else:
            concat_list = [(x if x is not None else ' ') +' '+ (y if y is not None else ' ') for x, y in zip(concat_list1,data[index])]
            data[index] = concat_list
            if none_count == 1 :
                data = data[index:]
            else:
                data = data[none_count-1:]
    return data


# In[110]:


def filtered_data(clean_data):
    data = []
    header =[]
    footer =[]
    temp_footer = ''
    for ind , row in enumerate(clean_data):
        row = [None if value =='nan' or value.strip() == '' else value.strip() for value in row ]
        cleaned_row = [value for value in row if value is not None]
        
        if len(cleaned_row) < 1:
            continue
        if len(cleaned_row ) > 1 or (len(cleaned_row) == 1 and row[0] is None):
            if temp_footer :
                if row[0] is None:
                    row[0] = ''
                row[0] = temp_footer +'  ' + row[0]
            data.append(row)
        elif len(data) < 1 :
            header += cleaned_row 
        else:
            temp_footer = cleaned_row[0]
            
            row = [None if value =='nan' or value.strip() == '' else value.strip() for value in clean_data[ind-1] ]
            prev_clean_row = [value for value in row if value is not None]
            if len(prev_clean_row) < 2 :
                footer +=cleaned_row
            else:
               footer = cleaned_row 
        none_count = 0 
        
        for value in data :
            if value.count(None) > 1 or (none_count <= 1 and value.count(None) == 1) or value[0] is None or not value[0][0].isdigit() :
                none_count += 1
            else:
                break 
   
    if none_count > 1:
        data = concat_xls_row(data,none_count)
 
    return data,header,footer


# In[111]:


filtered_data(raw_data)


# In[112]:


import warnings

# Ignore the specific FutureWarning related to iteritems
warnings.filterwarnings("ignore", category=FutureWarning, module="pyspark")


# In[113]:


from urllib.parse import quote
bad_records = []
for text_path in all_file_paths :
    try:
        file_path = text_path.split('.net/')[-1]
        file_path = quote(file_path, safe="/:")
        link = f'https://usafactsbronze.blob.core.windows.net/bronze/{file_path}'
        file_location = target_path + text_path.split(wasbs_path)[-1]
        df = pd.read_excel(link,header = None)
        df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.strip()=='' else x)
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        df = df.astype(str)
        df = spark.createDataFrame(df)
        first_row = [value for value in df.first()]
        if 'nan' in first_row :
            raw_data = df.collect()
            data,header,footer = filtered_data(raw_data)
            cleaned_columns = [re.sub(r'[^a-zA-Z0-9]', "_", value.strip().replace('.0','')) if value is not None else ('col_'+str(ind)) for ind,value in enumerate(data[0])]
            columns = []
            if len(cleaned_columns) !=  len(list(set(cleaned_columns))):
                for ind,column in enumerate(cleaned_columns) :
                    if column not in columns :
                        columns.append(column)
                    else:
                        columns.append(column+'_'+str(ind))
            else:
                columns = cleaned_columns
            schema = StructType([StructField(name, StringType(), True) for name in columns])
            
            df = spark.createDataFrame(data[1:], schema)
            empty_columns = [column for column in df.columns if df.filter(df[column].isNull()).count() == df.count() ]
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
        df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path",file_location).save()
        print(file_location,'uploaded sucessfully')

    except BaseException as e :
        bad_records.append((text_path,file_location.split('/')[-1],e))
        print(e,file_location)


# In[114]:


if len(bad_records)>= 1:
    pandas_df = pd.DataFrame(bad_records,columns=["URL","File_name","Reason"])
    bad_path = blob_relative_path+'bad_records/bad_record.csv'
    blob_client = container_client.get_blob_client(bad_path)
    csv_file = pandas_df.to_csv(index=False)
    blob_client.upload_blob(csv_file,overwrite=True)

