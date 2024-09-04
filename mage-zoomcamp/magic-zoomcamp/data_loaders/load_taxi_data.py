from mage_ai.io.file import FileIO
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz']
    df_list = []
    for url in urls:
        print ('Obtaining file:\t', url.split('/')[-1])
        df_list.append(pd.read_csv(url))



    return pd.concat(df_list, axis=0)


@test
def test_output(output, *args) -> None:
    time_min = output['lpep_pickup_datetime'].min()
    time_max = output['lpep_pickup_datetime'].max()

    print(f'Taxi data is located between {time_min} and {time_max}.')
    assert output is not None, 'The output is undefined'
