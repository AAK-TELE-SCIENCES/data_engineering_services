import os
import pandas as pd
import numpy as np

import copy
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


df1=pd.read_csv('all_projects.csv',index_col=False)


def get_acronym_data(acr):
    sql="select * from all_fp_projects where acronym='"+acr+"'"
    df1=pd.read_sql(sql,db_connection)
    return df1


def get_coord_data(coord):
    sql="select * from all_fp_projects where coordinator='"+coord+"'"
    df1=pd.read_sql(sql,db_connection)
    return df1


def get_acronym_details(acr):
    "returns the details of acronym given for several years"
    df=get_acronym_data(acr)
    return df.to_dict('dict')

def get_acronym_details_timeseries(acr):
    "returns the details of acronym given for several years"
    #df=get_acronym_data(acr)
    df=copy.deepcopy(df1)
    df=df.loc[df['acronym']==acr]
    
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
    df=get_coord_data(coord)
    return df.to_dict('dict')


def get_coordinator_details_timeseries(coord):
    "returns the details of coordinator given for several years"
    #df=get_coord_data(coord)
    df=copy.deepcopy(df1)
    df=df.loc[df['coordinator']==coord]
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



def get_stats_per_country(country):
    "returns the stats per country"
    data={}
    data['total']=len(df1)
    coord=df1.loc[df1['coordinatorCountry']==country].shape[0]
    data['coordinator']=coord

    parti=df1[df1['participantCountries'].astype(str).str.contains(country)].shape[0]
    parti=parti-coord # subtract where the country was coordinator as well
    data['participant']=parti
    
    return data


def get_totalcost_per_country(country):
    "returns the cumulative funding received by the country for the year"
    df=df1.loc[df1['coordinatorCountry']==country]
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year
    
    df=df.dropna(subset=['startYear'])
    df['startYear']=df['startYear'].astype(np.int64)
    df['totalCost']=df['totalCost'].fillna(0)
    df['totalCost']=pd.to_numeric(df['totalCost'])
    df2=df.groupby(['startYear'])['totalCost'].sum()
    return df2.to_dict()


def get_eccontribution_per_country(country):
    "returns the cumulative ec contribution received by the country for the year"
    df=df1.loc[df1['coordinatorCountry']==country]
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year
    
    df=df.dropna(subset=['startYear'])
    df['startYear']=df['startYear'].astype(np.int64)
    df['ecMaxContribution']=pd.to_numeric(df['ecMaxContribution'])
    
    df['ecMaxContribution']=df['ecMaxContribution'].fillna(0)
    df2=df.groupby(['startYear'])['ecMaxContribution'].sum()
    return df2.to_dict()




def get_totalcost_per_inst(inst):
    "returns the cumulative funding received by the inst for the year"
    df=df1.loc[df1['coordinator']==inst]
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year
    
    print(df.head())
    df=df.dropna(subset=['startYear'])
    df['startYear']=df['startYear'].astype(np.int64)
    df['totalCost']=df['totalCost'].fillna(0)
    df['totalCost']=pd.to_numeric(df['totalCost'])
    df2=df.groupby(['startYear'])['totalCost'].sum()
    return df2.to_dict()


def get_eccontribution_per_inst(inst):
    "returns the cumulative ec contribution received by the inst for the year"
    df=df1.loc[df1['coordinator']==inst]
    df=df[['title','acronym','startDate','endDate','totalCost','ecMaxContribution']]
    df['startYear']=pd.to_datetime(df['startDate']).dt.year
    df['endYear']=pd.to_datetime(df['endDate']).dt.year
    
    df=df.dropna(subset=['startYear'])
    df['startYear']=df['startYear'].astype(np.int64)
    df['ecMaxContribution']=pd.to_numeric(df['ecMaxContribution'])
    
    df['ecMaxContribution']=df['ecMaxContribution'].fillna(0)
    df2=df.groupby(['startYear'])['ecMaxContribution'].sum()
    return df2.to_dict()