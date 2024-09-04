from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(data, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = 'lateral-attic-426206-c4-ny-taxi-data'
    taxi_type = kwargs['taxi_type']

    for pq_file in os.listdir(data):
        file_path = os.path.join(data, pq_file)
        object_key = taxi_type +'/'+ pq_file
        print(f"Exporting {pq_file}...")
        df = pd.read_parquet(file_path)
        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df,
            bucket_name,
            object_key
        )