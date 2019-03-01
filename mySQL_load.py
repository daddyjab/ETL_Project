#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pandas.io import sql


# In[2]:


# Dependencies
import pandas as pd
from sqlalchemy import create_engine
from pprint import pprint

from splinter import Browser
from bs4 import BeautifulSoup
import requests
import pymongo

import time

# Support export to and import from JSON file
import json


# In[3]:


# Initialize PyMongo to work with MongoDBs
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)

# Define database and collection
db = client.etl_db

# Convert the items in MongoDB collection
# to a flat format permit import to DataFrame
iwp_info = db.iwp.find()

new_i_list = []

for i in iwp_info:
    # Create a new list of dicts with flat entries
    #print(i['incident_title'])
    new_dict = {
         'incident_title': i['incident_title'],
         'incident_date': i['incident_date'],
         'incident_url': i['incident_url'],
         'incident_report_type': i['incident_report_type'],
         'incident_symptoms': ", ".join(i['incident_symptoms']),
         'incident_misc': i['incident_misc'],
          'incident_address': i['incident_address_info']['incident_address'],
          'incident_address_standard': i['incident_address_info']['incident_address_standard'],
          'incident_address_street': i['incident_address_info']['incident_address_street'],
          'incident_address_street2': i['incident_address_info']['incident_address_street2'],
          'incident_address_street3': i['incident_address_info']['incident_address_street3'],
          'incident_address_city': i['incident_address_info']['incident_address_city'],
          'incident_address_state': i['incident_address_info']['incident_address_state'],
          'incident_address_zipcode': i['incident_address_info']['incident_address_zipcode'],
          'incident_address_country': i['incident_address_info']['incident_address_country'],
         'incident_description': i['incident_description']
         }
    
    new_i_list.append(new_dict)


# In[4]:


# Try to import the dictionary into pandas
iwp_df = pd.DataFrame(new_i_list)
iwp_df.head()


# In[5]:


# extract business name from incident_title and write back to dataframe
split_incident_df = iwp_df['incident_title'].str.split(",", n=0, expand = True)
iwp_df['business_name']=split_incident_df.iloc[:,0]

iwp_df.head(5)


# In[6]:


# import chicago inspections json file to pandas dataframe
path = 'Resources/inspections.json'
inspection_df=pd.read_json(path, lines=True, orient='records', encoding='utf-8')

inspection_df.head(10)


# In[7]:


# create dataframe to hold relevant analysis columns and location data

poison_loc_df = iwp_df[['business_name','incident_address','incident_address_city','incident_address_state','incident_address_zipcode']].copy()
poison_loc_df['lat']= 0
poison_loc_df['lng']= 0

poison_loc_df.head()


# In[8]:


# Change data type for location columns to support merge with inspection data
poison_loc_df.lat = poison_loc_df.lat.astype(float)
poison_loc_df.lng = poison_loc_df.lng.astype(float)
poison_loc_df.incident_address_zipcode= poison_loc_df.incident_address_zipcode.astype(str)

poison_loc_df.dtypes


# In[9]:


# Identify missing street and city data in poison submissions

#Summary information
null_columns=poison_loc_df.columns[poison_loc_df.isnull().any()]
print(poison_loc_df[null_columns].isnull().sum())

#Detail Information
null_rows = poison_loc_df[poison_loc_df.isnull().any(axis=1)]
null_rows.head(40)


# In[ ]:


# get longitude and Latitude via google location API call

import requests
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

# set url components
base = 'https://maps.googleapis.com/maps/api/geocode/json?'
key = '&key=AIzaSyBvfB8zwVRsyCYpi-dJc5ES2mtkrrODrU4'

# Loop through the incident DataFrame, perform an API call for data on each. Store data in lists.

i=0

for index, row in poison_loc_df.iterrows():
    
    address = poison_loc_df.iloc[i,2]
    address = address.replace(' ', '+')
    
    city = poison_loc_df.iloc[i,3]
    city = city.replace(' ', '+')
            
    state=poison_loc_df.iloc[i,4]
    state = state.replace(' ', '+')
            
    zipcode=poison_loc_df.iloc[i,5]
    #zipcode=zipcode.replace(' ', '+')

    #build and execute API call, save location results to list
    query_url = (f"{base}address={address},{city},{state},{zipcode}{key}")   
    response = requests.get(query_url).json()
 
    print(f"Processed: Record {i} {poison_loc_df.iloc[i,1]}")

    try:
        lat = response['results'][0]['geometry']['location']['lat']
        lng = response['results'][0]['geometry']['location']['lng']

        poison_loc_df.loc[i, 'lat'] = lat
        poison_loc_df.loc[i, 'lng'] = lng

    except IndexError: 
        print("IndexError")

    i=i+1


# In[ ]:


poison_loc_df.head(10)


# In[ ]:


from geopy.distance import lonlat, distance

i=0
for index, row in poison_loc_df.iterrows():
    poison = (poison_loc_df.iloc['lon'], poison_loc_df['lat'])
    inspection = (-81.695391, 41.499498)
    print(distance(lonlat(*newport_ri_xy), lonlat(*cleveland_oh_xy)).miles)
    538.3904453677203


# In[ ]:


# Merge data on business name
merge_df = pd.merge(poison_loc_df, inspection_df, how='inner', left_on=['lat','lng'], right_on = ['latitude','longitude'])
merge_df.head()


# In[ ]:


inspection_df


# In[ ]:


import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine

#Create Engine
#-----------------------------------
engine = create_engine('sqlite://', echo=False)

#Convert Pandas DataFrame into SQL
#-----------------------------------
#inspections_df.to_sql('inspections', con=engine)

#Perform a SQL Query
#-----------------------------------
engine.execute("SELECT Business_Name, Results FROM inspections").fetchall()


# In[ ]:


# SQL Alchemy
from sqlalchemy import create_engine

# PyMySQL 
import pymysql
pymysql.install_as_MySQLdb()

import sqlalchemy as db

engine = db.create_engine('sqlite:///food_inspections_chicago.sqlite') 
connection = engine.connect()
metadata = db.MetaData()

inspections = db.Table('merger', metadata,
               db.Column('index', db.String(255)),       
               db.Column('business_name', db.String(255)),
               db.Column('street', db.String(255)),
               db.Column('city', db.String(255)),
               db.Column('lat', db.String(255)),
               db.Column('lon', db.String(255)),
               db.Column('inspection_date', db.String(255)),
               db.Column('status', db.String(255)),
               db.Column('results', db.String(255))
              )

metadata.create_all(engine)

inspections_complete.to_sql('merger', engine)


# In[ ]:


# Create Engine and Pass in MySQL Connection
engine = create_engine("mysql://root@localhost/icecream_db")


# In[ ]:


#Dependency
#-----------------------------------
from sqlalchemy import create_engine

#Create Engine
#-----------------------------------
engine = create_engine('sqlite://', echo=False)

#Convert Pandas DataFrame into SQL
#-----------------------------------
merge_df.to_sql('merge', con=engine)

#Perform a SQL Query
#-----------------------------------
engine.execute("SELECT Business_Name, Results FROM Correlation").fetchall()


# In[ ]:


# Show tables from connected database
data = engine.execute("SHOW TABLES")

