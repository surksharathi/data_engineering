FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2
RUN pip install pgcli

WORKDIR /app
COPY trips_data_all.csv trips_data_all.csv
COPY ingest_data.py ingest_data.py


ENTRYPOINT [ "python", "ingest_data.py" ]


  