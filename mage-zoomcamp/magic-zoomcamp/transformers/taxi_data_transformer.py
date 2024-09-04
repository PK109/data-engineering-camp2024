from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
import pandas as pd
import re
from pandas import DataFrame


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    df = df[df['passenger_count']>0]
    df = df[df['trip_distance']>0]

    df['lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date
    df.drop(['lpep_pickup_datetime'], axis = 1, inplace = True)

    new_columns = []
    modify_count = 0
    for column in df:
        new_column = re.sub(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', '_', column).lower()
        new_columns.append(new_column)
        if new_column != column:
            modify_count += 1
    print("Count of modified columns is: ", modify_count)
    df.columns = new_columns
    print(df['vendor_id'].unique())
    return df

@test
def test_output(output, *args) -> None:

    assert len(output[output['passenger_count']==0]) == 0, 'There are error data in passenger count'
    assert len(output[output['trip_distance']==0]) == 0,'There are error data in trip distance'
    assert output is not None, 'The output is undefined'
