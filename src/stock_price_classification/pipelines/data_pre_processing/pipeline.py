"""
This is a boilerplate pipeline 'data_pre_processing'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import clean_stock_data, identify_fields_to_standardize, standardize_continuous_features, one_hot_encode_tickers, create_training_test_splits


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

             node(
                    func=one_hot_encode_tickers,
                    inputs=["standardized_modeling_data", "params:moving_average_settings"],
                    outputs="one-hot-encoded-tickers",
                    name="one-hot-encoding",
                ),

             node(
                    func=create_training_test_splits,
                    inputs=["standardized_modeling_data", "params:modeling_settings"],
                    outputs=["X_train", "X_test", "y_train", "y_test"],
                    name="train-test-split",
                ),




    ])
