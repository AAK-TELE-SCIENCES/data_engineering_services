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
from itertools import islice
import copy
# get best countries in the given field
# get best inst in the given field
# get the country with most active insts in the given field


#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection = create_engine(db_connection_str,connect_args= connect_args)

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def get_sci_score_from_country_per_field(col_name):
    sql="select country,"+col_name+" from h2020_sci_scores_per_country"
    df = pd.read_sql(sql, db_connection)
    return df

def get_sic_score_from_country_per_field(col_name):
    sql="select country,"+col_name+" from h2020_sic_scores_per_country"
    df = pd.read_sql(sql, db_connection)
    return df


def get_sci_score_from_inst_per_field(col_name):
    sql="select inst_name,"+col_name+" from h2020_sci_scores_per_inst"
    df = pd.read_sql(sql, db_connection)
    return df

def get_sic_score_from_ins_per_field(col_name):
    sql="select inst_name,"+col_name+" from h2020_sic_scores_per_inst"
    df = pd.read_sql(sql, db_connection)
    return df

def get_country_of_inst(inst):
    sql="select country from eu_organizations where name='"+inst+"'"
    df = pd.read_sql(sql, db_connection)
    
    try:
        return df.values[0][0]
    except:
        return ""


def get_country_stats_for_field(field):
    "returns the average of country score for the given field"
    
    col,f=sci_per_country.get_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sci_scores_per_country where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        df[col]=pd.to_numeric(df[col])
        return df[col].mean(), df[col].std()

    col,f=sic_per_country.get_sic_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sic_scores_per_country where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        df[col]=pd.to_numeric(df[col])
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
    zero_vals={}
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
                if sic1==0: # to filter 0s
                    try:
                        zero_vals['sic'+"-"+field+"_"+f+"-"+str(sic1)]+=1
                    except:
                        zero_vals['sic'+"-"+field+"_"+f+"-"+str(sic1)]=0
                        zero_vals['sic'+"-"+field+"_"+f+"-"+str(sic1)]+=1
                
                try:
                    data[con]['sic'][field].append({f:sic1})
                except Exception as e:
                    data[con]['sic'][field]=[]
                    data[con]['sic'][field].append({f:sic1})
            
            col,field=sci_per_country.get_column_data_from_field(f)
            if col!="" and field!="": # if sci, populate in sci json
                sci1=sci[[col]].values[0][0]
                if sci1==0: # to filter 0s
                    try:
                        zero_vals['sci'+"-"+field+"_"+f+"-"+str(sci1)]+=1
                    except:
                        zero_vals['sci'+"-"+field+"_"+f+"-"+str(sci1)]=0
                        zero_vals['sci'+"-"+field+"_"+f+"-"+str(sci1)]+=1
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
    
    for k,v in zero_vals.items():
        if v==len(country_name): # all are zero, remove them
            for con in country_name:
                keys=k.split('_')
                path1=keys[0].split('-')
                path2=keys[1].split('-')
                x={path2[0]:float(path2[1])}
                data[con][path1[0]][path1[1]].remove(x)
                try:
                    del data['averages'][path2[0]]
                    del data['std_dev'][path2[0]]
                except:
                    pass
    
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
    data['sic']['minor_global_average']={}

    for col in cols:
        data['sic']['minor'][get_sic_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sic']['minor_global_average'][get_sic_name_from_column(col)]=get_country_stats_for_field(get_sic_name_from_column(col))[0]
    
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sic']['major']={}
    data['sic']['major_global_average']={}
    for col in cols:
        data['sic']['major'][get_sic_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sic']['major_global_average'][get_sic_name_from_column(col)]=get_country_stats_for_field(get_sic_name_from_column(col))[0]
    
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
    data['sci']['minor_global_average']={}
    
    for col in cols:
        data['sci']['minor'][get_sci_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sci']['minor_global_average'][get_sci_name_from_column(col)]=get_country_stats_for_field(get_sci_name_from_column(col))[0]
    
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sci']['major']={}
    data['sci']['major_global_average']={}
    for col in cols:
        data['sci']['major'][get_sci_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sci']['major_global_average'][get_sci_name_from_column(col)]=get_country_stats_for_field(get_sci_name_from_column(col))[0]
    return data

def get_top_countries_from_inst(values):
    country_scores={}
    for val in values: # inst and its score -> val
        con=get_country_of_inst(val[0])
        if con!="":
            try:
                country_scores[con]['num_insts']+=1
            except:
                country_scores[con]={}
                country_scores[con]['num_insts']=0
                country_scores[con]['num_insts']+=1
            try:
                country_scores[con]['average']=float(val[1])+float(country_scores[con]['average'])
            except:
                country_scores[con]['average']=float(val[1])
    for k,v in country_scores.items():
        country_scores[k]['average']=float(country_scores[k]['average'])/float(country_scores[k]['num_insts'])
    return country_scores

def get_best_insts(field_name):
    "returns best insts for the given field"
    col,field=sic_per_country.get_sic_column_data_from_field(field_name)
    country_scores={}
    if col=="": # sci
        col,field=sci_per_country.get_column_data_from_field(field_name)
        if col=="": # no data found
            return {}
        sci=get_sci_score_from_inst_per_field(col)
        sci=sci.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        sci=sci.dropna()
        sci=sci.sort_values(col,ascending=False) # sort in desc order
        sci=dict(sci.values)
        values= take(10, sci.items()) # take top 10 values
        cons=get_top_countries_from_inst(values)
        return values, cons
    elif col!="": # sic
        sic=sic_per_country.get_sic_score_from_inst_per_field(col)
        sic=sic.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        sic=sic.dropna()
        sic=sic.sort_values(col,ascending=False)
        sic=dict(sic.values)
        values= take(10, sic.items())  # take top 10 values
        cons=get_top_countries_from_inst(values)
        return values, cons
    else: # no data found
        return {}


def get_best_countries(field_name):
    "returns best countries for the given field"
    col,field=sic_per_country.get_sic_column_data_from_field(field_name)
    if col=="": # sci
        col,field=sci_per_country.get_column_data_from_field(field_name)
        if col=="": # no data found
            return {}
        sci=get_sci_score_from_country_per_field(col)
        sci=sci.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        sci=sci.dropna()
        sci=sci.sort_values(col,ascending=False)
        return dict(sci.values)
    elif col!="": # sic
        sic=get_sic_score_from_country_per_field(col)
        sic=sic.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        sic=sic.dropna()
        sic=sic.sort_values(col,ascending=False)
        return dict(sic.values)
    else: # no data found
        return {}


def get_best_countries_for_field(field_type,sort_by="average"):
    "returns best countries for the given type i.e. sic or sci after sorting"
    if field_type=="sci":
        df=sci_per_country.get_all_sci_data_from_country()
        sub_df=df.replace(0,np.nan)
        real_df=copy.deepcopy(sub_df)
        sub_df['count']=len(sub_df.columns)-1-sub_df.loc[:, sub_df.columns != 'country'].isnull().sum(axis=1)
        sub_df['average']=real_df.loc[:, real_df.columns != 'country'].mean(axis=1)
        if sort_by=="count":
            sub_df=sub_df.sort_values(by='count',ascending=False)
        else:
            sub_df=sub_df.sort_values(by='average',ascending=False)
        sub_df=sub_df[['country','average','count']][:30]
        sub_df=sub_df.T.to_dict('list')
        print(sub_df)
        return {v[0]:[v[1],v[2]] for k,v in sub_df.items()}
    elif field_type=="sic":
        df=sic_per_country.get_all_sic_data_from_country()
        sub_df=df.replace(0,np.nan)
        real_df=copy.deepcopy(sub_df)
        sub_df['count']=len(sub_df.columns)-1-sub_df.loc[:, sub_df.columns != 'country'].isnull().sum(axis=1)
        sub_df['average']=real_df.loc[:, real_df.columns != 'country'].mean(axis=1)
        if sort_by=="count":
            sub_df=sub_df.sort_values(by='count',ascending=False)
        else:
            sub_df=sub_df.sort_values(by='average',ascending=False)
        sub_df=sub_df[['country','average','count']][:30]
        sub_df=sub_df.T.to_dict('list')
        print(sub_df)
        return {v[0]:[v[1],v[2]] for k,v in sub_df.items()}
    else:
        return {}

def get_country_names():
    "returns the name of countries without abbreviations"
    df_con=pd.read_csv("new_data/wikipedia-iso-country-codes.csv") # read the country info csv
    try:
        sql="select DISTINCT(country) from h2020_sci_score_per_country"
        sci_con = pd.read_sql(sql, db_connection) 
        countries=sci_con['country'].values.tolist() # get all country codes
        df_con=df_con.loc[df_con['Alpha-2 code'].isin(countries)] 
        return df_con['English short name lower case'].values.tolist() # return corresponding full names
    except Exception as e:
        print("EXC: ", e)
        return []

def get_all_inst_per_countries(country_name):
    "returns the insts found in a country"
    df_con=pd.read_csv("new_data/wikipedia-iso-country-codes.csv")
    try: # get country code from the given country name
        country_code=df_con.loc[df_con['English short name lower case']==country_name]['Alpha-2 code'].values[0]
        sql="select name from eu_organizations where country='"+country_code+"'"
        df1 = pd.read_sql(sql, db_connection)
        return df1['name'].values.tolist() # return all inst against the country code found
    except Exception as e:
        print("EXC: ", e)
        return []
