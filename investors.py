import pandas as pd
from sqlalchemy import create_engine
import datetime
import json
import time
import pickle
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

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

def get_stats_for_column(col_name):
    "returns the mean and std dev for the given column"
    sql="select "+col_name+" from investor_sic_profiling_majorfields"
    df=pd.read_sql(sql,db_connection)
    df=df.replace(0,np.nan).dropna(axis=1,how="all")
    df[col_name] = df[col_name].apply(pd.to_numeric, errors='coerce')
    return df[col_name].mean(), df[col_name].std()

def get_sci_name_from_column(col_name):
    sql="""
    select column_description from h2020_project_scientific_scores_major_and_minor_fields_col_dic
    where column_id= '"""+str(col_name)+"""'"""
    try:
        df1 = pd.read_sql(sql, db_connection)
        return df1['column_description'].values[0]
    except Exception as e:
        return ""

def get_investor_data(name=""):
    "returns investors data"
    if name=="":
        sql="""
        SELECT * FROM investor_sic_profiling_majorfields
        """
    else:
        sql="SELECT * FROM investor_sic_profiling_majorfields where name = '" + str(name) +"'"
    df = pd.read_sql(sql, db_connection)
    return df

def get_investors_sic_info(investor_name):
    "returns the sic info for the investors"
    df=get_investor_data(investor_name)
    df=df.replace(0,np.nan).dropna(axis=1,how="all")
    cols=df.columns
    
    data={}
    
    for col in cols:
        if col=="CIK":
            continue
        try:
            val=float(df[col].values[0])
            col_desc=get_sci_name_from_column(col)
            data[col_desc]={}
            data[col_desc]['value']=df[col].values[0]
            avg,std=get_stats_for_column(col)
            data[col_desc]['average']=avg
            data[col_desc]['std_dev']=std
        except:
            continue
    return data