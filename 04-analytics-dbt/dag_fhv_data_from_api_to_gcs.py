import os
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

    upload_task = PythonOperator(
        task_id='upload_files',
        python_callable=upload_files
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_folder',
        python_callable=cleanup_temp_folder
    )

    download_task >> upload_task >> cleanup_task
