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


def write_b64(b64,file_name):
    imgdata = base64.b64decode(b64[1:])
    with open(file_name, 'wb') as f:
        f.write(imgdata)


dest="logos"

try:
    os.mkdir(dest)
except:
    pass

chunksize = 200000
with pd.read_csv("company_logos.csv", chunksize=chunksize) as reader:
    for chunk in reader:
        print("shape of chunk: ", chunk.shape)
        print("unique names: ", len(chunk['name'].unique()))
        '''
        with tqdm(total=len(chunk)) as pbar:
            for i,row in chunk.iterrows():
                b64=str(row['base64_img'])
                name=row['name']
                name=dest+"/"+name+".jpg"
                write_b64(b64,name)
                pbar.update(1)
        '''