#!/usr/bin/env python
# coding: utf-8

# ## wild_url_scrap_updated
# 
# 
# 

# In[123]:


url='https://www.macpac.gov/publication/macstats-compiled/'
scrap_url_path =''
pipeline_name ='test'
run_id = '123'
patterns = '.csv$,.xlsx$'


# In[124]:


patterns = patterns.split(',')


# In[125]:


from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from bs4 import BeautifulSoup
import re
import requests
import pandas as pd
import concurrent.futures
from urllib.parse import urlparse
import urllib.parse
thread = concurrent.futures.ThreadPoolExecutor(max_workers=10)


# In[126]:


# headers = {'User-Agent': f'{pipeline_name} {run_id} (https://usafacts.org/; DataTeam@usafacts.org)'}
headers = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}


# In[127]:


linked_service_name = 'bronze'
container_name = 'ingestion-meta'
account_name = 'usafactsbronze'
blob_sas_token = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name)
blob_service_client = BlobServiceClient(account_url=f'https://{account_name}.blob.core.windows.net/', credential=blob_sas_token)
container_client = blob_service_client.get_container_client(container_name)


# In[128]:


def parse_url(seed_url):
    temp=urlparse(seed_url)
    base_url=(temp.scheme+'://'+temp.netloc)
    return base_url


# In[129]:


def scrap_sub_url(url,patterns):
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        regex_patterns = [re.compile(pattern) for pattern in patterns]
        if len(regex_patterns)>0:
            url_links = [a['href'] for a in soup.find_all('a') if 'href' in a.attrs and any(pattern.search(a['href']) for pattern in regex_patterns)]
        else:
            url_links = [a['href'] for a in soup.find_all('a') if 'href' in a.attrs]
        url_links=[urllib.parse.quote(url, safe=':/') for url in url_links]
        return url_links
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")


# In[130]:


def scrap_url(url,container_client=container_client,patterns=patterns):
        url_links=scrap_sub_url(url,'')
        regex_patterns = [re.compile(pattern) for pattern in patterns]
        concat_urls=[]
        for sub_url in url_links:
            if len(sub_url.split('/'))<=1 and any(pattern.search(sub_url) for pattern in regex_patterns):
                sub_url='/'.join(url.split('/')[:-1])+'/'+sub_url
                concat_urls.append(sub_url)
                
            elif sub_url.startswith('https://') and sub_url.endswith('/')  :
                
                sub_url=scrap_sub_url(sub_url,patterns)
                
                concat_urls.extend(sub_url)
            else:
                
                if any(pattern.search(sub_url) for pattern in regex_patterns):
                    concat_urls.append(sub_url)
        if len(concat_urls)>0:
            url_list=concat_urls
        if len(url_list) > 0 :
            base_url = parse_url(url)
            pandas_df = pd.DataFrame(url_list,columns=["URL"])
            pandas_df['URL'] = pandas_df['URL'].apply(lambda x: base_url + x if not x.startswith('https://') else x)
            file_path = base_url.replace('https://','') + '/' \
                + url.replace('https://','').replace('/','_').replace('.','_')+'.csv'
            blob_client = container_client.get_blob_client(file_path)
            csv_file = pandas_df.to_csv(index=False)
            blob_client.upload_blob(csv_file,overwrite=True)
            print(file_path,"uploaded Successfully")


# In[131]:


error_list = []
try:
    # Allow SPARK to access from Blob remotely 
    if not url :
        wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (linked_service_name, account_name,scrap_url_path) 
        spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (linked_service_name, account_name), blob_sas_token) 
        input_txt_file_df = spark.read.csv(wasbs_path,header=True)
        input_txt_file_df = input_txt_file_df.select(*[regexp_replace(col_name, ' ','%20').alias(col_name) for col_name in input_txt_file_df.columns])
        url_df = input_txt_file_df.select("URL")
        url_list = url_df.rdd.map(lambda row: row[0]).collect()
        if len(url_list) !=0:
            thread.map(scrap_url,url_list)
    else:
        scrap_url(url)
except Exception as e:
    error_list.append(str(e))
    print('error:',str(e))


# In[133]:


error_df=pd.DataFrame(error_list)
if not error_df.empty :
    if url :
        scrap_url_path = parse_url(url).replace('https://','')
    csv_filename = f'{scrap_url_path}'+'/'+'error_files'+'/'+ f"url_scrap_error_{run_id}.csv"
    output=error_df.to_csv(index=False)
    blob_client = container_client.get_blob_client(csv_filename)
    blob_client.upload_blob(output, overwrite=True)
    print('file_writed:',csv_filename)

