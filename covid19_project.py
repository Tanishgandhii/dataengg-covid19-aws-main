#!/usr/bin/env python
# coding: utf-8

# # COVID-19 Data Engineering Project

# Performing data modeling, data wrangling and extract-load-transform on the COVID-19 Data Lake available on registry of open data AWS using various AWS tools such as boto3, Glue, S3, Athena and Redshift.

# ## Code

# In[1]:


import boto3
import pandas as pd
from io import StringIO
import time


# In[2]:


AWS_ACCESS_KEY='  '
AWS_SECRET_KEY='   '
AWS_REGION='ap-south-1'
SCHEMA_NAME='covid_19'
S3_STAGING_DIR='s3://tanish-test-bucket/output/'
S3_BUCKET_NAME='tanish-test-bucket'
S3_OUTPUT_DIRECTORY='output'


# In[ ]:


athena_client = boto3.client("athena",
                            aws_access_key_id = AWS_ACCESS_KEY,
                            aws_secret_access_key_id = AWS_SECRET_KEY,
                            region_name = AWS_REGION,)


# In[ ]:


#Gets the data from athena and converts it into pandas dataframe
Dict = {}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            client.get_query_results(QueryExecutionId=query_response["QueryExecutionId"])
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.01)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client("s3",
                            aws_access_key_id = AWS_ACCESS_KEY,
                            aws_secret_access_key_id = AWS_SECRET_KEY,
                            region_name = AWS_REGION,)
    s3_client.download_file(S3_BUCKET_NAME,
                           f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
                           temp_file_location,)
    return pd.read_csv(temp_file_location)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

enigma_jhud = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_county",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

nytimes_data_in_usa_us_county = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usaus_states",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

nytimes_data_in_usaus_states = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM countypopulation",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

countypopulation = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_usa_hospital_beds",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

rearc_usa_hospital_beds = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM state_abv",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

state_abv = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM states_daily",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

states_daily = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_daily",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

us_daily = download_and_load_query_results(athena_client, response)


# In[ ]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_total_latest",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={"OutputLocation":S3_STAGING_DIR
                        "EncryptionConfiguration":{"EncryptionOption":"SSE_S3"},
                        },
)

us_total_latest = download_and_load_query_results(athena_client, response)


# In[ ]:


us_total_latest.head() #Checking the dataframe


# ## Building Dimensional Model

# In[ ]:


factCovid_1 = enigma_jhud[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2 = us_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1,factCovid_2,on='fips',how='inner')


# In[ ]:


factCovid.shape


# In[ ]:


dimRegion_1 = enigma_jhud[['fips','province_state','country_region','latitude','longitude']]
dimRegion_2 = nytimes_data_in_usa_us_county[['fips','county','state']]
dimRegion = pd.merge(dimRegion_1,dimRegion_2,on='fips',how='inner')


# In[ ]:


dimHospital = rearc_usa_hospital_beds[['fips','state_name','latitude','longitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]


# In[ ]:


dimDate = states_daily[['fips','date']]


# In[ ]:


dimDate.head()


# In[ ]:


dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')


# In[ ]:


dimDate['year'] = dimDate['date'].dt.year
dimDate['date'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek


# ## Saving to S3

# In[ ]:


bucket = 'oovk-covid-project-output-buck'


# In[ ]:


csv_buffer = StringIO()
factCovid.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket,'output/factCovid.csv').put(Body=csv_buffer.getvalue())


# In[ ]:


csv_buffer.getvalue()


# ## Extracting schema from dataset

# In[ ]:


dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(),'dimDate')
print(''.join(dimDatesql))


# In[ ]:


factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(),'factCovid')
print(''.join(factCovidsql))


# In[ ]:


dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(),'dimRegion')
print(''.join(dimRegionsql))


# In[ ]:


dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(),'dimHospital')
print(''.join(dimHospitalsql))


# ## Redshift Connector

# In[64]:


import redshift_connector


# In[ ]:


conn = redshift_connector.connect(
    host='',
    databse='dev',
    user='awsuser',
    password='Passw0rd123'
)


# In[ ]:


conn.autocommit = True


# In[ ]:


cursor=redshift_connector.Cursor = conn.cursor()


# In[ ]:


cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
"fips" INTEGER,
"date" TIMESTAMP,
"year" INTEGER,
"month" INTEGER,
"day_of_week" INTEGER
)
""")


# In[ ]:


cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
"fips" REAL,
"state_name" TEXT,
"longitutde" REAL,
"latitude" REAL,
"hq_address" TEXT,
"hospital_name" TEXT,
"hospital_type" TEXT,
"hq_city" TEXT,
"hq_state" TEXT,
)
""")

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
"fips" REAL,
"province_state" TEXT,
"country_region" TEXT,
"confirmed" REAL,
"deaths" REAL,
"recovered" REAL,
"active" REAL,
"date" INTEGER,
"positive" REAL,
"negative" REAL,
"hospitalizedcurrently" REAL,
"hospitalized" REAL,
"hospitalizeddischarged" REAL
)
""")


# In[ ]:


cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
"fips" REAL,
"province_state" TEXT,
"country_region" TEXT,
"latitude" REAL,
"longititude" REAL,
"county" TEXT,
"state" TEXT
)
""")


# In[ ]:


cursor.execute("""
copy dimDate from 's3_uri'
credentials 'aws_iam_role=arn:aws:iam:iamrole'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")


# 

# In[ ]:




