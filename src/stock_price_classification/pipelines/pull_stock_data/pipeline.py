"""
This is a boilerplate pipeline 'pull_stock_data'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import pull_stock_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=pull_stock_data,
                inputs=["params:stock_pull_settings"],
                outputs="raw_combined_equity_data",
                name="pull-in-stock-data",
            ),
        ]
    )
