import io
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load green taxi data from API
    
    for the final quarter of 2020
    """

    # defining schema
    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }
    green_taxi_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    green_taxi_df = pd.DataFrame() #create empty dataframe

    #read necessary data month by month and concat it 
    for month in range (10, 13):
        green_taxi_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{str(month)}.csv.gz"

        df = pd.read_csv(green_taxi_url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=green_taxi_dates)
        green_taxi_df = pd.concat([green_taxi_df, df])

    return green_taxi_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
