if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import pandas as pd


@transformer
def transform(data, *args, **kwargs):
    data = '/home/src/taxi_data/green'
    for pq_file in os.listdir(data):
        file_path = os.path.join(data, pq_file)
        print(f"Cleaning {pq_file}...")
        df = pd.read_parquet(file_path)
        df.columns = (df.columns
                        .str.replace(' ','_')
                        .str.lower()
        )   
        df.to_parquet(file_path)
        del df
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
