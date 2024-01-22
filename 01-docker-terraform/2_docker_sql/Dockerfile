FROM python:3.9

RUN apt-get install wget
RUN pip install pyarrow pandas sqlalchemy psycopg2

WORKDIR /app
COPY green_taxi_data.py green_taxi_data.py 

ENTRYPOINT [ "python", "green_taxi_data.py" ]
