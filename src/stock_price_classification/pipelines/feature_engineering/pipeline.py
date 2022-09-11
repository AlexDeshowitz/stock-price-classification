"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import calculate_rolling_means, calculate_rolling_standard_deviations, create_above_below_indicator_fields


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
            node(
                func=calculate_rolling_means,
                inputs=["raw_combined_equity_data", "params:moving_average_settings"],
                outputs="combined_equity_data_moving_averages",
                name="Add-moving-averages",
            ),

            node(
                func=calculate_rolling_standard_deviations,
                inputs=["combined_equity_data_moving_averages", "params:moving_average_settings"],
                outputs="combined_equity_data_standard_deviations",
                name="Add-standard-deviations",
            ),

             node(
                func=create_above_below_indicator_fields,
                inputs=["combined_equity_data_standard_deviations", "params:moving_average_settings"],
                outputs="test_output_data",
                name="Add-indicator-fields",
            ),
        ]
    )
