if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd


@transformer
def transform(data, *args, **kwargs):
    """
    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data['lpep_pickup_month'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date.apply(lambda x: str(x.timetuple()[0])+'-'+str(x.timetuple()[1]).format(':05d'))
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    print(output['lpep_pickup_month'].unique())
    assert output is not None, 'The output is undefined'
