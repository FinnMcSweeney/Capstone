# Databricks notebook source
# MAGIC %pip install confluent-kafka
# MAGIC %pip install mysql-connector-python 

# COMMAND ----------

from confluent_kafka import Consumer
import pyodbc 

# COMMAND ----------

props = {
    'bootstrap.servers':'pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':'UGZQZXBMU4ITW4TE',
    'sasl.password':'vWTRzZdjZxnZu9NTpgBctijiq6lxNmMMcZ3tZejvVftLTvEjS2eTb1OSeKPVww4F',

    # Best practice for higher availability in librdkafka clients prior to 1.7
    'session.timeout.ms':45000,

    # Required connection configs for Confluent Cloud Schema Registry
#     schema.registry.url=https://{{ SR_ENDPOINT }}
#     basic.auth.credentials.source=USER_INFO
#     basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}
    
    # Unsure if these are needed
    'group.id' : 'python-group-1345',
    'auto.offset.reset' : 'earliest'
}

# COMMAND ----------

from confluent_kafka import Consumer

consumer = Consumer(props)

topic_names = ['group-3']
consumer.subscribe(topic_names)

print(consumer)

# COMMAND ----------

messages = []
try:
    while len(messages) < 5:
        msg = consumer.poll(10)
        #consumer.seek_to_end()
        if msg is None:
            raise Exception('Message is None')
        elif msg.error() is not None:
            raise Exception(msg.error())
        else:
            messages.append(msg)
except Exception as e:
    print(f'Exception: {str(e)[:100]}')
finally:
    consumer.close()

# COMMAND ----------

#print(messages[0].value().decode('utf-8'))

# COMMAND ----------

import json
from datetime import datetime

columns = (json.loads(messages[0].value().decode('utf-8')))

# COMMAND ----------

print(columns)

# COMMAND ----------

import pandas as pd
df = pd.read_json(columns)

# COMMAND ----------

df

# COMMAND ----------

SAS_TOKEN = 'sp=racwdlmeop&st=2023-01-23T17:22:16Z&se=2023-02-07T01:22:16Z&spr=https&sv=2021-06-08&sr=c&sig=2T1LclHjRP7znPYIfTIVAJcp6TsqU%2BzMj1NcNI9ueFE%3D'

CONTAINER = 'dnr'

STOR_ACCT = 'cohort40storage'

ROOT_PATH = f'wasbs://{CONTAINER}@{STOR_ACCT}.blob.core.windows.net/'

spark.conf.set(f'fs.azure.sas.{CONTAINER}.{STOR_ACCT}.blob.core.windows.net', SAS_TOKEN)

# COMMAND ----------


sparkDF = spark.createDataFrame(df) 
write_path = f'{ROOT_PATH}capstone/weather.json'
sparkDF.write.mode('overwrite').json(write_path)

# COMMAND ----------

server = "cohort40-sql-2.database.windows.net"
database = "test-dnr"
table = "dbo.Capstone"

# specific credentials
user = "testuserdnr"
password = "T3STPASSWORD!!"

# global user/password
# user = "myadminuser"
# password = "NhVtMnMaCtRi5States"



# COMMAND ----------

sparkDF.write.format("jdbc").option(
    "url", f"jdbc:sqlserver://{server}:1433;databaseName={database};"
    ) \
    .mode("overwrite") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

# COMMAND ----------

jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{server}:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()


jdbcDF.show()

# COMMAND ----------


