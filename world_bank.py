import os
import pandas as pd
import numpy as np
import os
import math

import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
connect_args={'ssl':{'fake_flag_to_enable_tls': True},
             'port': 3306}
# read full data once
df=pd.read_excel("new_data/Data_Extract_From_World_Development_Indicators.xlsx", engine='openpyxl')
df=df.replace('..',0) # replace '..' with 0s

def get_year_columns(df):
    "get years columns"
    df1=df.drop(['Country Name', 'Country Code', 'Series Name', 'Series Code'],axis=1)
    return list(df1.columns)

def trendline(index,data, order=1):
    "returns the trend or slope of data"
    print("index: ", index)
    print("data: ", data)
    
    
    coeffs = np.polyfit(index, list(data), order)
    print("coeffs: ", coeffs)
    print("--------------------")
    slope = coeffs[-2]
    return float(slope)

def get_country_profile(country):
    "returns the profile dict for the given country"
    sub_df=df.loc[df['Country Name']==country]
    series=sub_df['Series Name'].unique().tolist()
    years=get_year_columns(df)
    data={}
    for col in series:
        sub_df1=sub_df.loc[sub_df['Series Name']==col] # select for each column
        sub_df1=sub_df1[years]
        sub_df1=sub_df1.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        if sub_df1.empty==False:
            data[col]={}
            data[col]['values']=sub_df1.to_dict('records')
            data[col]['averages']=sub_df1.values.mean() # add average
            data[col]['std_dev']=sub_df1.values.std()
    return data

def get_best_countries(field):
    "returns the top best countries for the given field"
    years=get_year_columns(df)
    countries=df['Country Name'].unique()
    data={}
    avgs=[]
    for con in countries:
        sub_df=df.loc[df['Country Name']==con]
        sub_df1=sub_df.loc[sub_df['Series Name']==field] # select for the given column
        sub_df1=sub_df1[years]
        
        sub_df1=sub_df1.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        #print(sub_df1)
        #print("----")
        avg=sub_df1.values.mean()
        if math.isnan(avg):
            data[con]=0
        else:
            data[con]=avg
    return sorted(data.items(), key=lambda x: x[1], reverse=True)

def get_trend_of_data(country):
    "returns the trend of data"
    sub_df=df.loc[df['Country Name']==country]
    series=sub_df['Series Name'].unique().tolist()
    years=get_year_columns(df)
    data={}
    for col in series:
        sub_df1=sub_df.loc[sub_df['Series Name']==col] # select for each column
        sub_df1=sub_df1[years]
        sub_df1=sub_df1.replace(0,np.nan).dropna(axis=1,how="all")# removing 0s
        if sub_df1.empty==False and len(sub_df1.values[0])>1:
            data[col]={}
            index=np.arange(0,len(sub_df1.values[0]))
            slope=trendline(index,sub_df1.values[0]) # get slope
            data[col]['slope']=slope
            if slope>=50:
                data[col]['trend']="positive"
            elif slope<50 and slope>0:
                data[col]['trend']="constant"
            else:
                data[col]['trend']="negative"
            
            data[col]['averages']=sub_df1.values.mean() # add average
            data[col]['std_dev']=sub_df1.values.std()
    return data
