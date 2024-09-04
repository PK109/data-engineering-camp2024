if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
import pyarrow as pa
import pyarrow.parquet as pq
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/keys/my-keys.json'

bucket_name = 'lateral-attic-426206-c4-mage-demo'
table_name = 'green_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    
    table = pa.Table.from_pandas(data)
    
    gcs = pa.fs.GcsFileSystem()
    # local = pa.fs.LocalFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )