import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine



df= pd.read_csv("yellow-tripdata-2021.csv", nrows=100)



def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    #url = params.url
    #csv_name = 'output.csv'

    #os.system(f"wget {url} -O {csv_name}")
#yellow-tripdata-2021.csv
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv("yellow-tripdata-2021.csv", iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)

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























