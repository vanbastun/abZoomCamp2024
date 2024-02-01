FROM python:3.9

RUN apt-get install wget
RUN pip install pyarrow pandas sqlalchemy psycopg2

WORKDIR /app
COPY zone_lookup.py zone_lookup.py 

ENTRYPOINT [ "python", "zone_lookup.py" ]
