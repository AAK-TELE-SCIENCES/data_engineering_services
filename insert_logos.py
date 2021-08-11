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


chunksize = 200000
with pd.read_csv("company_logos.csv", chunksize=chunksize) as reader:
    for chunk in reader:
        print("shape of chunk: ", chunk.shape)
        chunk.to_sql("company_logos", con=db_connection, if_exists="append", index=False)
        print("--chunk added--")