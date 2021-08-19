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

#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection = create_engine(db_connection_str,connect_args= connect_args)

def get_sci_scores_per_country():
    sql="""
    select * from h2020_sci_scores_per_country
    """

    df = pd.read_sql(sql, db_connection)
    return df

def get_scientific_scores_major_and_minor_fields_col_dic():
    sql="""
    select * from h2020_project_scientific_scores_major_and_minor_fields_col_dic
    """

    df1 = pd.read_sql(sql, db_connection)
    return df1


def get_column_data_from_field(col):
    "returns the column and field type corresponding to the given field name"
    sql="""
    select column_id,field_scope from h2020_project_scientific_scores_major_and_minor_fields_col_dic
    where column_description= '"""+str(col)+"""'"""
    try:
        df1 = pd.read_sql(sql, db_connection)
        return df1['column_id'].values[0], df1['field_scope'].values[0]
    except Exception as e:
        #print("EXC: ", e)
        return "",""

def get_sci_score_from_country_and_field(country,field):
    df=get_sci_scores_per_country()
    df1=get_scientific_scores_major_and_minor_fields_col_dic()
    
    df=df.loc[df['country']==country]
    col,field_type=get_column_data_from_field(field)
    
    if col!="" and field_type!="":
        return float(df[col].values[0])


def get_all_sci_data_from_country():
    "gets all sci data for countries"
    sql="select * from h2020_sci_scores_per_country"
    df = pd.read_sql(sql, db_connection)
    df = df.iloc[1:]
    df = df[df.country != 'None']
    df = df[df.country != None]
    return df


def get_average_major_and_minor_score_per_country():
    df=get_sci_scores_per_country()
    df1=get_scientific_scores_major_and_minor_fields_col_dic()
    
    all_countries=list(df.country.unique())
    try:
        all_countries.remove("None")
    except:
        pass

    try:
        all_countries.remove(None)
    except:
        pass
    minor_fields=df1.loc[df1['field_scope']=="Minor Field"]
    major_fields=df1.loc[df1['field_scope']=="Major Field"]
    
    minor_cols=minor_fields.column_id.values
    major_cols=major_fields.column_id.values
    data={}
    
    minor_avg=[]
    major_avg=[]
    for country in all_countries:
        if country=="None" or country==None:
            continue
        sub_df=df.loc[df['country']==country]
        sub_df=sub_df[minor_cols]
        cols = sub_df.columns # convert columns to numeric
        sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')
        sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        
        sum_minor=sum(list(sub_df.values[0])) # get minor average
        len_minor=len(list(sub_df.values[0])) 
        
        data[country]={}
        try:
            data[country]['minor_average']=sum_minor/len_minor
            minor_avg.append(sum_minor/len_minor)
        except:
            data[country]['minor_average']=0
            minor_avg.append(0)
            
        
        sub_df=df.loc[df['country']==country]
        sub_df=sub_df[major_cols]
        cols = sub_df.columns # convert columns to numeric
        sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')
        sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        
        
        sum_major=sum(list(sub_df.values[0])) # get major average
        len_major=len(list(sub_df.values[0])) 
        
        try:
            data[country]['major_average']=sum_major/len_major
            major_avg.append(sum_major/len_major)
        except:
            data[country]['major_average']=0 
            major_avg.append(0)
    return data, sum(minor_avg)/len(minor_avg), sum(major_avg)/len(major_avg), np.std(minor_avg), np.std(major_avg)


def get_all_insts():
    sql="select distinct(inst_name) from h2020_sic_scores_per_inst"
    df = pd.read_sql(sql, db_connection)
    return df

def get_sci_scores_per_inst(inst_name):
    sql="""select * from h2020_sci_scores_per_inst 
    where replace(inst_name, '\"', '') ='"""+inst_name+"""'"""
    
    df = pd.read_sql(sql, db_connection)
    if len(df)>0:
        return df.iloc[0]
    return df


def get_average_major_and_minor_sci_for_all_insts():
    "returns average major and minor sci scores for all the insts"
    
    all_insts=get_all_insts()['inst_name'].values
    
    # get columns dict
    df1=get_scientific_scores_major_and_minor_fields_col_dic()
    
    minor_fields=df1.loc[df1['field_scope']=="Minor Field"]
    major_fields=df1.loc[df1['field_scope']=="Major Field"]
    
    minor_cols=minor_fields.column_id.values
    major_cols=major_fields.column_id.values
    
    data={}
    
    minor_avg=[]
    major_avg=[]
    with tqdm(total=len(all_insts)) as pbar:
        for inst in all_insts:
            sub_df=get_sci_scores_per_inst(inst)

            sub_df=sub_df[minor_cols]
            cols = sub_df.columns # convert columns to numeric
            sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')

            sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s

            sum_minor=sum(list(sub_df.values[0])) # get minor average
            len_minor=len(list(sub_df.values[0])) 

            data[inst]={}
            try:
                data[inst]['minor_average']=sum_minor/len_minor
                minor_avg.append(sum_minor/len_minor)
            except:
                data[inst]['minor_average']=0
                minor_avg.append(0)

            sub_df=get_sci_scores_per_inst(inst)
            sub_df=sub_df[major_cols]
            cols = sub_df.columns # convert columns to numeric
            sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')
            sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s


            sum_major=sum(list(sub_df.values[0])) # get major average
            len_major=len(list(sub_df.values[0])) 

            try:
                data[inst]['major_average']=sum_major/len_major
                major_avg.append(sum_major/len_major)
            except:
                data[inst]['major_average']=0 
                major_avg.append(0)
            pbar.update(1)
            print(data)
        return data,minor_avg,major_avg, sum(minor_avg)/len(minor_avg), sum(major_avg)/len(major_avg)
    

def get_average_major_and_minor_sci_per_inst(inst_name=[]):
    "returns for the given insts only"
    all_insts=get_all_insts()
    all_insts['inst_name']=all_insts['inst_name'].str.replace("'", '')
    all_insts['inst_name']=all_insts['inst_name'].str.replace('"', '')
    all_insts=all_insts['inst_name'].values
    
    # get columns dict
    df1=get_scientific_scores_major_and_minor_fields_col_dic()
    
    minor_fields=df1.loc[df1['field_scope']=="Minor Field"]
    major_fields=df1.loc[df1['field_scope']=="Major Field"]
    
    minor_cols=minor_fields.column_id.values
    major_cols=major_fields.column_id.values
    
    data={}
    
    minor_avg=[]
    major_avg=[]
    with tqdm(total=len(all_insts)) as pbar:
        for inst in all_insts:
            if inst not in inst_name and inst_name!=[]: # if selective insts are given
                continue
            sub_df=get_sci_scores_per_inst(inst)
            sub_df=sub_df[minor_cols]
            cols = sub_df.columns # convert columns to numeric
            sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')
            sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
            
            try:
                sum_minor=sum(list(sub_df.values[0])) # get minor average
                len_minor=len(list(sub_df.values[0])) 
            except:
                sum_minor=[]
                len_minor=0
            
            data[inst]={}
            try:
                data[inst]['minor_average']=sum_minor/len_minor
                minor_avg.append(sum_minor/len_minor)
            except:
                data[inst]['minor_average']=0
                minor_avg.append(0)

            sub_df=get_sci_scores_per_inst(inst)
            sub_df=sub_df[major_cols]
            cols = sub_df.columns # convert columns to numeric
            sub_df[cols] = sub_df[cols].apply(pd.to_numeric, errors='coerce')
            sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s

            try:
                sum_major=sum(list(sub_df.values[0])) # get major average
                len_major=len(list(sub_df.values[0])) 
            except:
                sum_major=[]
                len_major=0
            
            try:
                data[inst]['major_average']=sum_major/len_major
                major_avg.append(sum_major/len_major)
            except:
                data[inst]['major_average']=0 
                major_avg.append(0)
            pbar.update(1)
        return data, sum(minor_avg)/len(minor_avg), sum(major_avg)/len(major_avg)
