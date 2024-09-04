import io
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    df_list = []

    # refer to this page for data
    # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    url_template = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{:02d}.parquet'
    for iter in range(1,13):
        df = pd.read_parquet(url_template.format(iter))
        print(f"{iter}) Data loaded - imported {len(df)} rows.")
        df_list.append(df)
    df_output = pd.concat(df_list)
    return df_output


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    # assert len(output) == 12, 'The output has not correct file count'
    
