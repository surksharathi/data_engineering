FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2
RUN pip install pgcli

WORKDIR /app
COPY yellow-tripdata-2021.csv yellow-tripdata-2021.csv
COPY ingest_data.py ingest_data.py


ENTRYPOINT [ "python", "ingest_data.py" ]


  