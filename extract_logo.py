import pandas as pd
from sqlalchemy import create_engine
import json
import time
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import requests
import shutil
import base64
import os


name="aak_maindb"
user="aak_maindbusr"
pwd="AakDBUsr#2021"
host="52.117.247.165"
client="mysql"
port="3306"
db_string=client+"://"+user+":"+pwd+"@"+host+":"+port+"/"+name


#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection_str = db_string
db_connection = create_engine(db_connection_str,connect_args= connect_args)

def get_company_url(): # to get url and name of companies
    sql="SELECT name,homepage_url FROM acp_companies"
    df = pd.read_sql(sql, db_connection)
    return df

df=get_company_url()

print("full df shape: ", df.shape)

print("unique names: ", len(df['name'].unique()))
print("unique urls: ", len(df['homepage_url'].unique()))
print("----")
print("unique names: ", len(set(df['name'].values)))
print("unique urls: ", len(set(df['homepage_url'].values)))

data={} # to store csv of images and companies
data['name']=[]
data['homepage_url']=[]
data['base64_img']=[]

try:
    os.mkdir("logo_images/")
except:
    pass

base_url="https://logo.clearbit.com/" # clearbit used to download logo

names_done=[]

with tqdm(total=len(df)) as pbar:
    for i,row in df.iterrows():
        url=row['homepage_url'].replace('http://','')
        url=url.replace('https://','') # clean the url
        new_url=base_url+url
        if row['name'] not in names_done:
            try:
                r = requests.get(url = new_url, stream=True) # get logo
                if r.status_code==200: # if logo found
                    with open('logo_images/'+row['name']+'.png', 'wb') as out_file:
                        shutil.copyfileobj(r.raw, out_file)
                    data['name'].append(row['name'])
                    data['homepage_url'].append(row['homepage_url'])

                    with open('logo_images/'+row['name']+'.png', "rb") as image_file:
                        encoded_string = base64.b64encode(image_file.read()) # get base64 from image
                    data['base64_img'].append(encoded_string)
                    names_done.append(row['name']) # to keep track of all the names
            except Exception as e:
                print("EXC: ", e)
                pass
        pbar.update(1)

df1=pd.DataFrame(data)
print("df1: ", df1.shape)

df1.to_csv("company_logos1.csv")

print("names found: ", names_done)