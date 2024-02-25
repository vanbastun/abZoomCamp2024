import os
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'bastun',
    'start_date': datetime(2024, 2, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


def download_files():
    """Download the For Hire Vehicle Trip Records Parquet Files."""

    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    output_folder = "/home/ubuser/airflow/tmp"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    parquet_links = soup.find_all("a", attrs={
        "title": lambda x: x and "For-Hire Vehicle Trip Records" in x and "High Volume" not in x})

    # Download Parquet files for 2019 with title 'For-Hire Vehicle Trip Records'
    for link in parquet_links:
        file_url = link["href"]
        file_name = file_url.split("/")[-1]
        if "2019" in file_name:
            response = requests.get(file_url)
            file_path = os.path.join(output_folder, file_name)
            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"{file_name} downloaded successfully.")
            

def update_schema():
    """Update the schema and set proper datatypes for each Parquet file."""

    folder_path = "/home/ubuser/airflow/tmp"  # Folder containing the Parquet files

    # Get the list of Parquet files in the folder
    file_list = [file for file in os.listdir(folder_path) if file.startswith("fhv")]

    # Iterate over the Parquet files
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)

        # Create a PyArrow dataset from the Parquet file
        dataset = pq.ParquetDataset(file_path)

        # Read the Parquet file into a PyArrow table
        table = dataset.read()

        # Define the target schema with proper datatypes and matching field names
        target_schema = pa.schema([
            ('dispatching_base_num', pa.string()),
            ('pickup_datetime', pa.timestamp('s')),
            ('dropOff_datetime', pa.timestamp('s')),
            ('PUlocationID', pa.int64()),
            ('DOlocationID', pa.int64()),
            ('SR_Flag', pa.int64()),
            ('Affiliated_base_number', pa.string())
        ])

        # Apply schema modifications
        table = table.cast(target_schema)

        # Write the updated table back to the Parquet file, overwriting the old file
        pq.write_table(table, file_path)

        print(f"{file_name} updated successfully.")

    print("Schema update complete.")


def upload_files():
    """Upload Parquet Files to GCS bucket."""

    bucket_name = "abzoomcamp2024-bucket"
    gcs_hook = GoogleCloudStorageHook()
    files = os.scandir("/home/ubuser/airflow/tmp/")
    for file_name in files:
        destination_blob_name = f"dbt/{file_name.name}"
        gcs_hook.upload(
            bucket_name,
            destination_blob_name,
            file_name.path
        )
        print(f"{file_name.name} uploaded successfully.")


def cleanup_temp_folder():
    """Clean local folder."""

    path = "/home/ubuser/airflow/tmp"
    for file in os.scandir(path):
        if "tripdata" in file.name:
            os.unlink(file.path)


with DAG('fhv_data_from_api_to_gcs_dag', default_args=default_args, schedule_interval='@once') as dag:
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_files
    )
    
    schema_task = PythonOperator(
        task_id='update_schema',
        python_callable=update_schema
    )

    upload_task = PythonOperator(
        task_id='upload_files',
        python_callable=upload_files
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_folder',
        python_callable=cleanup_temp_folder
    )

    download_task >> schema_task >> upload_task >> cleanup_task
