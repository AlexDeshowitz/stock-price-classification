"""
This is a boilerplate pipeline 'classification_model'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import train_models


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
            node(
                    func=train_models,
                    inputs=["X_train", "y_train", "params:modeling_settings"],
                    outputs=["results_df" , "aggregated_results_df"],
                    name="train-classifiers",
            )


    ])
