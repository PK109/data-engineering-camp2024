import io
import pandas as pd
import os
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Script for loading data from GitHub storage 
    """
    taxi_type = kwargs['taxi_type']
    match taxi_type:
        case 'fhv':
            parse_dates = ["pickup_datetime","dropOff_datetime"]
        case 'yellow':
            parse_dates = ["tpep_pickup_datetime","tpep_dropoff_datetime"]
        case 'green':
            parse_dates = ["lpep_pickup_datetime","lpep_dropoff_datetime"]
        case _:
            parse_dates = None
            raise Exception('Wrong taxi data type.')
    
    years = kwargs['year_range']
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{:s}/{:s}_tripdata_{:4d}-{:02d}.csv.gz'
    
    taxi_dtypes = {
        'VendorID': pd.Int32Dtype(),
        'passenger_count': pd.Int32Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int32Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int32Dtype(),
        'DOLocationID': pd.Int32Dtype(),
        'payment_type': pd.Int32Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float,
        'ehail_fee': float,
        'trip_type': pd.Int32Dtype()

    }
    target_directory = os.path.join(os.getcwd(), 'data', taxi_type)
    print(f"Target folder for uploading data is {target_directory}")

    if (os.path.exists(target_directory)) == False:
        os.mkdir(target_directory)

    for year in years:
        for month in range(1,13):
            download_url = url.format(taxi_type, taxi_type, year, month)
            print(f"Download link:\n {download_url}")
            local_name = download_url.split("/")[-1]
            print (f'Obtaining file: {local_name}',)
            os.system(f"wget -c {download_url} -O {target_directory}/{local_name} >/dev/null 2>&1")
            # df = pd.read_csv(f"{target_directory}/{local_name}", sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
            print (f"\tFile saved as: {target_directory}/{local_name.replace('.csv.gz', '.parquet')}")
            # df.to_parquet(f"{target_directory}/{local_name.replace('.csv.gz', '.parquet')}")
            # del df
    return target_directory

@test
def test_output(output, *args, **kwargs) -> None:
    """
    Template code for testing the output of the block.
    """
    expected_count = len(kwargs['year_range']) * 12
    assert len(os.listdir(output)) == expected_count, f'The output is missing files. Found {len(os.listdir(output))} files instead of {len(kwargs["year_range"]) * 12}'