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
                try:
                    data[inst]['sic'][field].append({f:sic1})
                except Exception as e:
                    data[inst]['sic'][field]=[]
                    data[inst]['sic'][field].append({f:sic1})
            
            col,field=sci_per_country.get_column_data_from_field(f)
            if col!="" and field!="": # if sci, populate in sci json
                sci1=sci[[col]].values[0][0]
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
    return data
