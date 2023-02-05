from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True,retries=3)
def extract_from_gcs(color:str,year:int,month:int)->Path:
   """Download data from GCS"""
   gcs_path=f"data/{color}/{color}_tripdata_{year}-0{month}.parquet"
   gcs_block= GcsBucket.load("zoom-gcs")
   gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
   return Path(f"./data/{gcs_path}")

@task(log_prints=True)
def df_transform(path:Path)->pd.DataFrame:
  """Data Cleaning example"""
  df= pd.read_parquet(path)
  print(f"pre:missing passenger count: {df['passenger_count'].isna().sum()}")
  df['passenger_count'].fillna(0, inplace=True)
  print(f"post:missing passenger count: {df['passenger_count'].isna().sum()}")
  return df
   
@task(log_prints=True)
def write_bq(df:pd.DataFrame)->None:
  """Write the data into Big query"""
  gcp_credentials_block = GcpCredentials.load("zoom-gap-creds")
  df.to_gbq(
    destination_table="trips_data_all.rides",
    project_id="pacific-formula-375906",
    credentials=gcp_credentials_block.get_credentials_from_service_account(),
    chunksize=500_000,
    if_exists="append"
)


@flow(log_prints=True)
def etl_gcs_to_bg():
  """Main etl flow to load data from clod storage to big  query data warehouse"""
  color="yellow"
  year=2021
  month=1

  path= extract_from_gcs(color,year,month)
  print(f"path is:----->{path}")
  df= df_transform(path)
  write_bq(df)

if __name__ == '__main__':
  etl_gcs_to_bg()