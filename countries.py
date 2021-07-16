import pandas as pd
from sqlalchemy import create_engine
import datetime
import json
import time
import pickle, os
import numpy as np
from config import db_string as db_connection_str
from matplotlib import pyplot as plt
from tqdm import tqdm
import sci_per_country, sic_per_country
#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection = create_engine(db_connection_str,connect_args= connect_args)


def get_country_stats_for_field(field):
    "returns the average of country score for the given field"
    
    col,f=sci_per_country.get_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sci_scores_per_country where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        return df[col].mean(), df[col].std()

    col,f=sic_per_country.get_sic_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sic_scores_per_country where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        return df[col].mean(), df[col].std()

    

def get_sic_scores_per_country(country):
    sql="select * from h2020_sic_scores_per_country where country='"+country+"'"
    df = pd.read_sql(sql, db_connection)
    return df


def get_sci_scores_per_country(country):
    sql="select * from h2020_sci_scores_per_country where country='"+country+"'"
    df = pd.read_sql(sql, db_connection)
    return df


def get_sci_name_from_column(col_name):
    sql="""
    select column_description from h2020_project_scientific_scores_major_and_minor_fields_col_dic
    where column_id= '"""+str(col_name)+"""'"""
    try:
        df1 = pd.read_sql(sql, db_connection)
        return df1['column_description'].values[0]
    except Exception as e:
        #print("EXC: ", e)
        return ""
    
    
    
def get_sic_name_from_column(col_name):
    sql="""
    select column_description from h2020_project_sic_scores_major_and_minor_fields_col_dic
    where column_id= '"""+str(col_name)+"""'"""
    try:
        df1 = pd.read_sql(sql, db_connection)
        return df1['column_description'].values[0]
    except Exception as e:
        #print("EXC: ", e)
        return ""


def compare_countries(country_name=[],fields=[]):
    "takes various countries and compares the given fields"
    
    data={}
    
    for con in country_name:
        data[con]={}
        data[con]['sic']={}
        data[con]['sci']={}
        
        sic=get_sic_scores_per_country(con)
        sci=get_sci_scores_per_country(con)
        for f in fields: # for all the given fields
            col,field=sic_per_country.get_sic_column_data_from_field(f)
            if col!="" and field!="": # if sic, populate in sic json
                sic1=sic[[col]].values[0][0]
                try:
                    data[con]['sic'][field].append({f:sic1})
                except Exception as e:
                    data[con]['sic'][field]=[]
                    data[con]['sic'][field].append({f:sic1})
            
            col,field=sci_per_country.get_column_data_from_field(f)
            if col!="" and field!="": # if sci, populate in sci json
                sci1=sci[[col]].values[0][0]
                try:
                    data[con]['sci'][field].append({f:sci1})
                except:
                    data[con]['sci'][field]=[]
                    data[con]['sci'][field].append({f:sci1})
    
    data['averages']={}
    data['std_dev']={}
    for f in fields:
        avg,std=get_country_stats_for_field(f)
        data['averages'][f]=avg
        data['std_dev'][f]=std
    return data



# for a given country, make graph of all the fields that are non zero (can take fields as input)

def get_countries_info(con):
    sic=get_sic_scores_per_country(con)
    sci=get_sci_scores_per_country(con)
    data={}
    df1=sic_per_country.get_sic_scores_major_and_minor_fields_col_dic() # populate sic data
    
    minor_fields=df1.loc[df1['field_scope']=="Minor Field"]
    major_fields=df1.loc[df1['field_scope']=="Major Field"]
    
    minor_cols=minor_fields.column_id.values
    major_cols=major_fields.column_id.values
    
    sub_df=sic[minor_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sic']={}
    data['sic']['minor']={}
    
    for col in cols:
        data['sic']['minor'][get_sic_name_from_column(col)]=sub_df[col].values[0]
    
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sic']['major']={}
    for col in cols:
        data['sic']['major'][get_sic_name_from_column(col)]=sub_df[col].values[0]
    
    df1=sci_per_country.get_scientific_scores_major_and_minor_fields_col_dic() # populate sci data
    
    minor_fields=df1.loc[df1['field_scope']=="Minor Field"]
    major_fields=df1.loc[df1['field_scope']=="Major Field"]
    
    minor_cols=minor_fields.column_id.values
    major_cols=major_fields.column_id.values
    
    sub_df=sci[minor_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sci']={}
    data['sci']['minor']={}
    
    for col in cols:
        data['sci']['minor'][get_sic_name_from_column(col)]=sub_df[col].values[0]
        
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sci']['major']={}
    for col in cols:
        data['sci']['major'][get_sic_name_from_column(col)]=sub_df[col].values[0]
    
    return data


