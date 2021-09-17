import os
import pandas as pd
import numpy as np


from sqlalchemy import create_engine
import datetime

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

sql="select * from all_fp_projects"

df1=pd.read_sql(sql,db_connection)


def get_acronym_details(acr):
    "returns the details of acronym given for several years"
    df=df1.loc[df1['acronym']==acr]
    return df.to_dict('dict')

def get_acronym_details_timeseries(acr):
    "returns the details of acronym given for several years"
    df=df1.loc[df1['acronym']==acr]
    
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year

    data={}
    for i,row in df.iterrows():
        x={}
        x['start_year']=row['startYear']
        x['end_year']=row['endYear']
        x['title']=row['title']
        x['acronym']=row['acronym']
        x['total_cost']=row['totalCost']
        x['ec_max_contribution']=row['ecMaxContribution']
        
        try:
            y=data[row['acronym']]
            data[row['acronym']].append(x)
        except:
            data[row['acronym']]=[]
            data[row['acronym']].append(x)
    
    return data



def get_coordinator_details(coord):
    "returns the details of coordinator given for several years"
    df=df1.loc[df1['coordinator']==coord]
    return df.to_dict('dict')


def get_coordinator_details_timeseries(coord):
    "returns the details of coordinator given for several years"
    df=df1.loc[df1['coordinator']==coord]
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year

    data={}
    for i,row in df.iterrows():
        x={}
        x['start_year']=row['startYear']
        x['end_year']=row['endYear']
        x['title']=row['title']
        x['acronym']=row['acronym']
        x['total_cost']=row['totalCost']
        x['ec_max_contribution']=row['ecMaxContribution']
        
        try:
            y=data[row['acronym']]
            data[row['acronym']].append(x)
        except:
            data[row['acronym']]=[]
            data[row['acronym']].append(x)
    
    return data


