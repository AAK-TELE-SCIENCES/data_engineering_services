import pandas as pd
from sqlalchemy import create_engine
import datetime
import json
import time
import pickle, os
import numpy as np
from config import db_string as db_connection_str
from matplotlib import pyplot as plt

#create connection
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}

db_connection = create_engine(db_connection_str,connect_args= connect_args)

try:
    base_folder="data/"
    os.mkdir(base_folder)
except:
    pass

def func(pct, allvalues):
    absolute = int(pct / 100.*np.sum(allvalues))
    return "{:.1f}%\n({:d})".format(pct, absolute)

def get_all_table_names():
    "returns the names of all the tables present"

    sql = """
    SELECT * FROM information_schema.tables
    """
    df = pd.read_sql(sql, db_connection)
    df.to_csv(base_folder+'table_names.csv')
    return base_folder+'table_names.csv'


def plot_cumulative_scores_per_country(countries):
    "plots the cumulative scores per country"
    print("-- plot_cumulative_scores_per_country --")
    
    sql="""
    select * from h2020_cumulative_scores_per_country
    """
    df = pd.read_sql(sql, db_connection)
    df=df[df['country'].isin(countries)]
    possible_explodes = np.array([0.01, 0.0, 0.02, 0.05, 0.03, 0.04])
    inds=np.random.randint(0,4,len(df))
    explodes=list(possible_explodes[inds])
    fig, ax = plt.subplots(figsize =(14, 14))
    wp = { 'linewidth' : 1, 'edgecolor' : "green" }

    wedges, texts, autotexts = ax.pie(df['sum(score.sci_minor_score)'].values, 
                                    autopct = lambda pct: func(pct, df['sum(score.sci_minor_score)'].values),
                                    explode = explodes, 
                                    labels = df.country.values,
                                    shadow = True,
                                    startangle = 90,
                                    wedgeprops = wp,
                                    textprops = dict(color ="black"))

    ax.legend(wedges, df.country.values,
            title ="Cumulative Minor Score",
            loc ="center left",
            bbox_to_anchor =(1, 0, 0.5, 1))
    
    plt.setp(autotexts, size = 8, weight ="bold")
    ax.set_title("Cumulative Minor Score")

    plt.savefig(base_folder+"pie.jpg")
    return base_folder+"pie.jpg"


def plot_h2020_sci_scores_per_country(countries):
    "plots the h2020_sci_scores_per_country"
    print("-- plot_h2020_sci_scores_per_country --")
    sql="""
    select * from h2020_sci_scores_per_country
    """
    df = pd.read_sql(sql, db_connection)
    #cols.append('country')
    
    plt.figure(figsize=(16,7))

    for country in countries:
        df1=df.loc[df['country']==country]
        df1 = df1.drop('country', 1)
        values=df1.values[0]
        plt.plot(np.arange(0,len(list(df))-1),values,label=country)
        
    plt.xlabel("Columns")
    plt.ylabel("Values")
    plt.legend()
    plt.title("Analysis of h2020_sci_scores_per_country")
    plt.savefig(base_folder+"plot_h2020_sci_scores_per_country.jpg")
    return base_folder+"plot_h2020_sci_scores_per_country.jpg"
