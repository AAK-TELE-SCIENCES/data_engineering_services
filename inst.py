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

def get_inst_stats_for_field(field):
    "returns the average of inst score for the given field"
    
    col,f=sci_per_country.get_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sci_scores_per_inst where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        return df[col].mean(), df[col].std()

    col,f=sic_per_country.get_sic_column_data_from_field(field)
    if col!="":
        sql="select "+col+" from h2020_sic_scores_per_inst where "+col+"!=0"
        df = pd.read_sql(sql, db_connection)
        return df[col].mean(), df[col].std()


def compare_insts(inst_name=[],fields=[]):
    "takes various insts and compares the given fields"
    
    data={}
    zero_vals={}
    for inst in inst_name:
        data[inst]={}
        data[inst]['sic']={}
        data[inst]['sci']={}
        
        sic=sic_per_country.get_sic_scores_per_inst(inst)
        sci=sci_per_country.get_sci_scores_per_inst(inst)
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
                    data[inst]['sic'][field].append({f:sic1})
                except Exception as e:
                    data[inst]['sic'][field]=[]
                    data[inst]['sic'][field].append({f:sic1})
            
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
                    data[inst]['sci'][field].append({f:sci1})
                except:
                    data[inst]['sci'][field]=[]
                    data[inst]['sci'][field].append({f:sci1})
    
    data['averages']={}
    data['std_dev']={}
    for f in fields:
        avg,std=get_inst_stats_for_field(f)
        data['averages'][f]=avg
        data['std_dev'][f]=std

    for k,v in zero_vals.items():
        if v==len(inst_name): # all are zero, remove them
            for inst in inst_name:
                keys=k.split('_')
                path1=keys[0].split('-')
                path2=keys[1].split('-')
                x={path2[0]:float(path2[1])}
                data[inst][path1[0]][path1[1]].remove(x)
                try:
                    del data['averages'][path2[0]]
                    del data['std_dev'][path2[0]]
                except:
                    pass
    return data




def get_inst_info(inst):
    "get inst info"
    sic=sic_per_country.get_sic_scores_per_inst(inst)
    sci=sci_per_country.get_sci_scores_per_inst(inst)
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
        data['sic']['minor_global_average'][get_sic_name_from_column(col)]=get_inst_stats_for_field(get_sic_name_from_column(col))[0]
    
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sic']['major']={}
    data['sic']['major_global_average']={}
    for col in cols:
        data['sic']['major'][get_sic_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sic']['major_global_average'][get_sic_name_from_column(col)]=get_inst_stats_for_field(get_sic_name_from_column(col))[0]
    
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
        data['sci']['minor_global_average'][get_sci_name_from_column(col)]=get_inst_stats_for_field(get_sci_name_from_column(col))[0]
    
    sub_df=sic[major_cols]
    sub_df=sub_df.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
    cols=list(sub_df)
    data['sci']['major']={}
    data['sci']['major_global_average']={}
    for col in cols:
        data['sci']['major'][get_sci_name_from_column(col)]=sub_df[col].values[0]
        # add average per field as well
        data['sci']['major_global_average'][get_sci_name_from_column(col)]=get_inst_stats_for_field(get_sci_name_from_column(col))[0]
    
    return data


def get_inst_name_by_activity_type(act_type):
    "returns the insts of the given act_type"
    sql="""
    select * from eu_organizations
    """
    df = pd.read_sql(sql, db_connection)

    return df.loc[df['activity_type']==act_type].name.values.tolist()
