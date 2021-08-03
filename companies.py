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
from itertools import islice
import copy

#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection = create_engine(db_connection_str,connect_args= connect_args)

df=pd.read_csv('companies.csv')


def get_countries_of_most_investment(year=2020):
    "returns the countries by year which got the most investment"
    sub_df=df.loc[df['Filing_Year']==year]
    sub_df=sub_df[sub_df.country_code.notnull()] # get rows where country is present
    
    grouped=sub_df.groupby(by=['country_code'],as_index=False)['best_match_score','Investment_Amount'].sum()
    
    # average of best match score
    avg_best_match_score=sub_df.groupby(by=['country_code'])['best_match_score'].mean().values
    grouped['average_best_match_score']=avg_best_match_score
    
    # get counts per country
    counts=sub_df.groupby(by=['country_code'])['best_match_score'].count().values
    grouped['counts']=counts
    
    #get avg
    avg_amount=sub_df.groupby(by=['country_code'])['Investment_Amount'].mean().values
    grouped['average_investment_amount']=avg_amount
    
    grouped = grouped.rename(columns={'best_match_score': 'sum_Investment_Amount', 'Investment_Amount': 'sum_Investment_Amount'})

    # sort
    grouped=grouped.sort_values(by=['counts','average_investment_amount'], ascending=[False,False])[:10]
    return grouped.set_index('country_code').T.to_dict('dict')
