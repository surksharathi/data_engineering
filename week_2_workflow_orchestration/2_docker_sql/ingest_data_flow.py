import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector



#df= pd.read_csv("trips_data_all.csv", nrows=100)

@task(log_prints=True)
def extract_data():
     df_iter = pd.read_csv("green_tripdata_2020-01.csv", iterator=True, chunksize=100000)
     df = next(df_iter)
     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
     return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count:{df['passenger_count'].isin([0]).sum()}")
    df=df[df['passenger_count']!=0]
    print(f"post:missing the pasenger count:{df['passenger_count'].isin([0]).sum()}")
    return df

    
@task(log_prints=True,retries=3)    
def load_data(table_name,df):
    connection_block=SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
      df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
      df.to_sql(name=table_name, con=engine, if_exists='append')
@flow(name="Subflow",log_prints=True)
def log_subflow(table_name:str):
    print(f"Logging subflow for: {table_name}")
      
@flow(name="Ingest Data")
def main_flow(table_name:str = "green_taxi_trips"):
    log_subflow(table_name)
    raw_data=extract_data()
    data=transform_data(raw_data)
    load_data(table_name,data)
if __name__ == '__main__':
     main_flow(table_name = "green_taxi_trips")






# docker run -it \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v /Users/suraksha/Documents/Data-Engineering/data-engineering-zoomcamp/week_1_basics_n_setup/      2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
#   -p 5432:5432 \
# --network=pg-network \
#   --name pg-database \
#    postgres:13

# Run pgAdmin
# docker run -it \
#   -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
#   -e PGADMIN_DEFAULT_PASSWORD="root" \
#   -p 8080:80 \
#   --network=pg-network \
#   --name pgadmin-2 \
#   dpage/pgadmin4

# python ingest_data.py \
# --user=root \
# --password=root \
# --host=localhost \
# --port=5432 \
# --db=ny_taxi \
# --table_name=yellow_taxi_trips \

























