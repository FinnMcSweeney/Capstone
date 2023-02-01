# Databricks notebook source
# MAGIC %pip install confluent-kafka
# MAGIC %pip install geopy

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime 
import json
import jmespath
import uuid, confluent_kafka as kafka
from confluent_kafka.admin import AdminClient, NewTopic
from geopy.geocoders import Nominatim


# COMMAND ----------

# MAGIC %md
# MAGIC key = 'd2263a5e868ce0475110b97b77c85248'
# MAGIC location = f'http://api.openweathermap.org/geo/1.0/direct?q=London&limit=5&appid={key}'
# MAGIC 
# MAGIC lat = 40.980690
# MAGIC lon = -73.683769
# MAGIC start = 1606488670
# MAGIC end = 1606747870

# COMMAND ----------

# MAGIC %md
# MAGIC current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'
# MAGIC forecast = f'http://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={lat}&lon={lon}&appid={key}'
# MAGIC historic = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={key}'

# COMMAND ----------

# MAGIC %md
# MAGIC response = requests.get(current)
# MAGIC 
# MAGIC data = response.json()
# MAGIC 
# MAGIC data

# COMMAND ----------

# MAGIC %md
# MAGIC data.keys()
# MAGIC lat_val = data['coord']['lat']
# MAGIC lon_val = data['coord']['lon']
# MAGIC inner_dict = data['list'][0]
# MAGIC inner_dict['components'].values()
# MAGIC aqi = inner_dict['main']['aqi']
# MAGIC co = inner_dict['components']['co']
# MAGIC co = inner_dict['components']['co']
# MAGIC no = inner_dict['components']['no']
# MAGIC no2 = inner_dict['components']['no2']
# MAGIC o3 = inner_dict['components']['o3']
# MAGIC so2 = inner_dict['components']['so2']
# MAGIC pm2_5 = inner_dict['components']['pm2_5']
# MAGIC pm10 = inner_dict['components']['pm10']
# MAGIC nh3 = inner_dict['components']['nh3']
# MAGIC dt = inner_dict['dt']
# MAGIC dt = datetime.utcfromtimestamp(dt).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC data2 = {'latitude': [lat],
# MAGIC         'longitude': [lon],
# MAGIC         'aqi': [aqi],
# MAGIC         'co': [co],
# MAGIC         'no': [no],
# MAGIC         'no2': [no2],
# MAGIC         'o3': [o3],
# MAGIC         'so2': [so2],
# MAGIC         'pm2.5': [pm2_5],
# MAGIC         'pm10': [pm10],
# MAGIC         'nh3': [nh3],
# MAGIC         'date': [dt]}
# MAGIC 
# MAGIC   
# MAGIC # Create DataFrame
# MAGIC df = pd.DataFrame(data2)
# MAGIC df

# COMMAND ----------


key = 'd2263a5e868ce0475110b97b77c85248'


# In[3]:


location = f'http://api.openweathermap.org/geo/1.0/direct?q=London&limit=5&appid={key}'


# In[4]:


lat = 40.7128
lon = -74.0060

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response = requests.get(current)
data = response.json()

ny1 = pd.json_normalize(data['list'])

ny1["latitude"] = (40.7128)
ny1["longitude"] = (-74.0060)

ny1['dt'] = pd.to_datetime(ny1['dt'],unit='s')

ny1 = ny1.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
ny1['city'] = ny1.apply(get_city, axis=1)


# In[5]:


lat = 34.0522
lon = -118.2437

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response2 = requests.get(current)
data2 = response2.json()

la1 = pd.json_normalize(data2['list'])

la1["latitude"] = (34.0522)
la1["longitude"] = (-118.2437)

la1['dt'] = pd.to_datetime(la1['dt'],unit='s')

la1 = la1.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
la1['city'] = la1.apply(get_city, axis=1)


# In[6]:


lat = 41.8781
lon = -87.6298

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response3 = requests.get(current)
data3 = response3.json()

chi1 = pd.json_normalize(data3['list'])

chi1["latitude"] = (41.8781)
chi1["longitude"] = (-87.6298)

chi1['dt'] = pd.to_datetime(chi1['dt'],unit='s')

chi1 = chi1.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
chi1['city'] = chi1.apply(get_city, axis=1)


# In[7]:


lat = 29.7604
lon = -95.3698

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response4 = requests.get(current)
data4 = response4.json()

hou = pd.json_normalize(data4['list'])

hou["latitude"] = (29.7604)
hou["longitude"] = (-95.3698)

hou['dt'] = pd.to_datetime(hou['dt'],unit='s')

hou = hou.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
hou['city'] = hou.apply(get_city, axis=1)


# In[8]:


lat = 33.4484
lon = -112.0740

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response5 = requests.get(current)
data5 = response5.json()

phx = pd.json_normalize(data5['list'])

phx["latitude"] = (33.4484)
phx["longitude"] = (-112.0740)

phx['dt'] = pd.to_datetime(phx['dt'],unit='s')

phx = phx.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
phx['city'] = phx.apply(get_city, axis=1)


# In[9]:


lat = 39.9526
lon = -75.1652

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response6 = requests.get(current)
data6 = response6.json()

phi = pd.json_normalize(data6['list'])

phi["latitude"] = (39.9526)
phi["longitude"] = (-75.1652)

phi['dt'] = pd.to_datetime(phi['dt'],unit='s')

phi = phi.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
phi['city'] = phi.apply(get_city, axis=1)


# In[10]:


lat = 29.4252
lon = -98.4946

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response7 = requests.get(current)
data7 = response7.json()

sa = pd.json_normalize(data7['list'])

sa["latitude"] = (29.4252)
sa["longitude"] = (-98.4946)

sa['dt'] = pd.to_datetime(sa['dt'],unit='s')

sa = sa.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
sa['city'] = sa.apply(get_city, axis=1)


# In[11]:


lat = 32.7157
lon = -117.1611

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response8 = requests.get(current)
data8 = response8.json()

sd = pd.json_normalize(data8['list'])

sd["latitude"] = (32.7157)
sd["longitude"] = (-117.1611)

sd['dt'] = pd.to_datetime(sd['dt'],unit='s')

sd = sd.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
sd['city'] = sd.apply(get_city, axis=1)


# In[12]:


lat = 32.7767
lon = -96.7970

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response9 = requests.get(current)
data9 = response9.json()

dal = pd.json_normalize(data9['list'])

dal["latitude"] = (32.7767)
dal["longitude"] = (-96.7970)

dal['dt'] = pd.to_datetime(dal['dt'],unit='s')

dal = dal.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
dal['city'] = dal.apply(get_city, axis=1)


# In[19]:


lat = 37.3387
lon = -121.8853

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response10 = requests.get(current)
data10 = response10.json()

sj = pd.json_normalize(data10['list'])

sj["latitude"] = (37.3387)
sj["longitude"] = (-121.8853)

sj['dt'] = pd.to_datetime(sj['dt'],unit='s')

sj = sj.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
sj['city'] = sj.apply(get_city, axis=1)


# In[20]:


PopulationDF = pd.concat([ny1, la1, chi1, hou, phx, phi, sa, sd, dal, sj], ignore_index = True)


# In[21]:


PopulationDF


# In[22]:


lat = 41.4531
lon = -90.5721

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response11 = requests.get(current)
data11 = response11.json()

mil = pd.json_normalize(data11['list'])

mil["latitude"] = (41.4531)
mil["longitude"] = (-90.5721)

mil['dt'] = pd.to_datetime(mil['dt'],unit='s')

mil = mil.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
mil['city'] = mil.apply(get_city, axis=1)


# In[24]:


mil


# In[25]:


lat = 38.6369
lon = -90.5721

current = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}'

response12 = requests.get(current)
data12 = response12.json()

wp = pd.json_normalize(data12['list'])

wp["latitude"] = (38.6369)
wp["longitude"] = (-90.5721)

wp['dt'] = pd.to_datetime(wp['dt'],unit='s')

wp = wp.rename(columns= {'dt' : 'date','main.aqi': 'aqi', 'components.co': 'co', 'components.no':'no',
                         'components.no2':'no2', 'components.o3':'o3', 'components.so2':'so2', 'components.pm2_5': 'pm2.5',
                         'components.pm10': 'pm10', 'components.nh3':'nh3'})

def get_city(row):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", exactly_one=True)
    try:
        return location.raw['address']['city']
    except KeyError:
        try:
            return location.raw['address']['town']
        except KeyError:
            return location.raw['address']['county']

# apply the function to the dataframe
wp['city'] = wp.apply(get_city, axis=1)


# In[26]:


wp

# COMMAND ----------

PopulationDF

# COMMAND ----------

confluentBootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
confluentApiKey = "UGZQZXBMU4ITW4TE"
confluentSecret = "vWTRzZdjZxnZu9NTpgBctijiq6lxNmMMcZ3tZejvVftLTvEjS2eTb1OSeKPVww4F"

admin_client = AdminClient({
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'auto.offset.reset': 'earliest',
    'bootstrap.servers': confluentBootstrapServers,
    'sasl.username': confluentApiKey,
    'sasl.password': confluentSecret,
    'group.id': str(uuid.uuid1())
}) 
producer = kafka.Producer({
    'bootstrap.servers': confluentBootstrapServers,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': confluentApiKey,
    'sasl.password': confluentSecret,
    'group.id': '134534',  # this will create a new consumer group on each invocation.
    'auto.offset.reset': 'earliest'
})

topic = 'group-3'
topics = [NewTopic(topic, 1, 3)]
admin_client.create_topics(topics)

# COMMAND ----------

# MAGIC %md
# MAGIC import time, json
# MAGIC final_json = PopulationDF.to_json()
# MAGIC x = [1]
# MAGIC 
# MAGIC output = {'successes':[],'failures':[]}
# MAGIC for row in x:
# MAGIC     msg = json.dumps(final_json)
# MAGIC     try:
# MAGIC         producer.produce(topic, msg)
# MAGIC         producer.flush()
# MAGIC         output['successes'].append(msg)
# MAGIC     except Exception as e:
# MAGIC         output['failures'].append(e)
# MAGIC     finally:
# MAGIC         time.sleep(.5)
# MAGIC 
# MAGIC # Print relevant messages ===========================
# MAGIC cnt_fail = len(output['failures'])
# MAGIC if cnt_fail > 0:
# MAGIC     print(f'{cnt_fail} failure(s): {output["failures"][0]}')
# MAGIC else:
# MAGIC     cnt_succ = len(output['successes'])
# MAGIC     print(f'{cnt_succ} success(es)! E.g.:\n')
# MAGIC     for msg in output['successes'][:4]:
# MAGIC         print(type(msg),'\t',msg,'\n')

# COMMAND ----------


import time
final_json = PopulationDF.to_json()

error_cnt = 0
for i in range(0,1):
    try:
        x = json.dumps(final_json)
        producer.produce(topic, x)
        producer.flush()
        
        output = 'SUCCESS: ' + str(x)
    except Exception as e:
        error_cnt += 1
        output = 'ERROR: ' + str(e)[:150]
        print(output,'\n')
    finally:
#         print(output,'\n')
        time.sleep(1)

if error_cnt == 0:
    print('SUCCESS!!')

# COMMAND ----------

print(final_json)


# COMMAND ----------


