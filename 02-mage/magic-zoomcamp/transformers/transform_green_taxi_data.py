if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Transform data according requirements.

    1. Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    2. Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    3. Rename columns in Camel Case to Snake Case.
    """
    #q4 answer
    print(list(data['VendorID'].unique()))

    print(list(data.columns))
    #  transformation 1
    cleaned_data = data[ (data['passenger_count'] > 0) & (data['trip_distance'] > 0) ]

    #  transformation 2
    cleaned_data['lpep_pickup_date'] = cleaned_data['lpep_pickup_datetime'].dt.date

    #  transformation 3

    snake_names = ['vendor_id', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',

       'store_and_fwd_flag', 'ratecode_id', 'pu_location_id', 'do_location_id',

       'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',

       'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',

       'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge', 'lpep_pickup_date']
    
    cleaned_data = cleaned_data.rename(columns=dict(zip(cleaned_data.columns, snake_names)))

    return cleaned_data


@test
def test_output(output, *args) -> None:
    """
    Testing the output of the block.
    """
    assert 'vendor_id' in list(output.columns), 'Check the columns names.'
    assert output['passenger_count'].ge(0).all(), 'Some trips with zero passengers are in the dataset!'
    assert output['trip_distance'].ge(0).all(), 'Some trips with no distance are in the dataset!'
