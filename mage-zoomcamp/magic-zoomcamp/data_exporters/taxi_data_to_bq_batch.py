from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_bigquery(data, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    taxi_type = kwargs['taxi_type']

    table_id = 'lateral-attic-426206-c4.dbt_zoomcamp.{:s}_trip_data'.format(taxi_type)
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    # print (os.listdir(data))

    # Initializing table
    file_path = os.path.join(data, os.listdir(data)[10])
    df = pd.read_parquet(file_path)
    print(file_path)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df.head(1),
            table_id,
            if_exists = 'replace'
        )

    for index,pq_file in enumerate(os.listdir(data)):
        file_path = os.path.join(data, pq_file)
        print(f"\n\t{index+1})Exporting {pq_file}...")
        df = pd.read_parquet(file_path)
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df,
            table_id,
            if_exists = 'append'
        )
        del df