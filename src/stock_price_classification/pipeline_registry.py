"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline


from stock_price_classification.pipelines import pull_stock_data, feature_engineering


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """

    data_pull_pipeline = pull_stock_data.create_pipeline()
    feature_engineering_pipeline = feature_engineering.create_pipeline()

    return {
        "__default__": feature_engineering_pipeline,
        "full_run" : data_pull_pipeline + feature_engineering_pipeline,
        # individual pipelines:
        "data_pull" : data_pull_pipeline,
        "feature_engineering" : feature_engineering_pipeline
    }
