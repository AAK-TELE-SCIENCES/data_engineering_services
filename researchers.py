## connect the investors with the universities or insts for several years of funding
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
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


def get_projects_organization_h2020(project_name):
    "returns the projects info"
    sql="SELECT * FROM h2020_projects_organization where projectAcronym='"+project_name+"'"
    df = pd.read_sql(sql, db_connection)
    return df

def get_researchers_data(project_id):    
    sql="SELECT * FROM demo_horizon where projectID="+str(project_id)
    df2 = pd.read_sql(sql, db_connection)
    return df2


def get_researchers_info_from_project(project_id):
    "returns the researchers and publications stats for the project id"
    df=get_researchers_data(project_id)
    df['publishedYear']=pd.to_numeric(df['publishedYear'],downcast='integer')
    df1=pd.read_csv('journal_stats.csv') # read journals info
    df1['year']=pd.to_numeric(df1['year'],downcast='integer') # convert to int
    
    global_stats={}
    
    data=[]
    total_researchers=[]
    total_publications=[]
    
    for i,row in df.iterrows():
        x={}
        x['id']=row['ID']
        x['publication_title']=row['title']
        x['authors']=row['authors'].replace('et al.','').strip().split(',') # clean authors
        total_researchers.append(x['authors'])
        total_publications.append(x['publication_title'])
        x['journal_title']=row['journalTitle']
        x['published_year']=int(row['publishedYear'])
        try: # get h_index of journal
            sub_df1=df1.loc[(df1['year']==row['publishedYear']) & (df1['title']==row['journalTitle'])]
            x['h_index']=int(sub_df1['h_index'].values[0])
        except Exception as e:
            try:
                sub_df1=df1.loc[(df1['year']==row['publishedYear']) & (df1['title']==row['journalTitle']+", "+str(row['publishedYear']))]
                x['h_index']=int(sub_df1['h_index'].values[0])
            except Exception as e:
                x['h_index']="None"
        x['published_as']=row['isPublishedAs']
        data.append(x)
    
    total_researchers=[item for sublist in total_researchers for item in sublist]
    global_stats['total_researchers']=int(len(set(total_researchers)))
    global_stats['total_publications']=int(len(set(total_publications)))
    # add other publication stats
    df1=pd.read_csv("total_publications.csv")
    global_stats['average_publications']=int(df1['count'].mean())
    global_stats['max_publications']=int(df1['count'].max())
    global_stats['min_publications']=int(df1['count'].min())
    global_stats['std_dev_publications']=float(df1['count'].std())

    return data,global_stats



def get_project_info_investors(project_acronym=''):
    "returns the total ec contribution for projects and link with inst"
    data={}
    sub_df=get_projects_organization_h2020(project_acronym)

    sub_df['ecContribution']=pd.to_numeric(sub_df['ecContribution'])
    # add avg sum investment
    df1=pd.read_csv("total_cost_projects.csv")
    data['avg_ecContribution']=float(df1['sum'].mean())
    data['std_dev_ecContribution']=float(df1['sum'].std())
    
    vals=sub_df['ecContribution'].values.tolist()
    vals= [x for x in vals if ~np.isnan(x)] # remove nans
    data['sum_ecContribution']=sum(vals) # total ec contribution
    
    data['project_id']=sub_df['projectID'].unique()[0]
    counts=sub_df['activityType'].value_counts().to_dict()  # total activityTypes count
    data['activity_type']={}
    for k,v in counts.items():
        data['activity_type'][k]=v
    
    
    counts=sub_df['role'].value_counts().to_dict()  # total roles count
    data['role']={}
    for k,v in counts.items():
        data['role'][k]=v
        
        
    counts=sub_df['country'].value_counts().to_dict() # total countries count
    data['country']={}
    for k,v in counts.items():
        data['country'][k]=v
    
    
    data['institutions']=[] # get inst info
    for i,row in sub_df.iterrows():
        x={}
        x['name']=row['name']
        x['role']=row['role']
        x['activity_type']=row['activityType']
        x['ec_contribution']=row['ecContribution']
        x['country']=row['country']
        data['institutions'].append(x)
    
    
    data['researchers'],data['global_researcher_stats']=get_researchers_info_from_project(data['project_id']) # get researchers and publications
    return data

print(get_project_info_investors("Rheform"))