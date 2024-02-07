import pyarrow as pa 
import pyarrow.parquet as pq
from pandas import DataFrame 
import os 


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# set variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/abZoomCamp2024/02-mage/personal-gcp.json/gcp-keys.json'
project_id = 'vocal-tempo-411407'
bucket_name = 'ab-zoomcamp2024-green-taxi-bucket'
object_key = 'green_taxi_data.parquet'
table_name = 'green_taxi'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to GCP bucket.

    Write data as Parquet files, partioned by lpep_pickup_date
    """
    table  = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )
    

