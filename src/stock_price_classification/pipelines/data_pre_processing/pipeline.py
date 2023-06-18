"""
This is a boilerplate pipeline 'data_pre_processing'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import clean_stock_data, identify_fields_to_standardize, standardize_continuous_features


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
            node(
                    func=clean_stock_data,
                    inputs=["combined_modeling_input"],
                    outputs="filtered_modeling_data",
                    name="Add-filtering-preprocessing",
                ),
                
            node(
                    func=standardize_continuous_features,
                    inputs=["filtered_modeling_data", "params:moving_average_settings", "params:modeling_settings"],
                    outputs="standardized_modeling_data",
                    name="Add-standardization-preprocessing",
                ),




    ])
