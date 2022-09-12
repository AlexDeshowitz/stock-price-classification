"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.2
"""

import pandas as pd
import numpy as np



def calculate_rolling_means(dataframe: pd.DataFrame, parameters: dict) -> pd.DataFrame: 

    
    '''return a dataframe that includes all of the rolling means either straight or exponential appended to the overall dataset
        
        Args:
            dataframe: main dataset (from raw to be fed in or specified in the modeling catalog
            date field: field containing the trading date of the equity
            field: field to use on which to calculate the rolling standard deviations
            day_ranges: list of the days over which the rolling mean is to be calculates (e.g., 6, 7, 15)
            exponential: If True will calculate exponential moving averages instead of simple moving averages
        
        Returns: Dataframe containing the user-specified moving averages
        
    '''
    dataframe = dataframe.sort_values(by =[parameters['stock_field'], parameters['date_field']]  )

    for days in parameters['day_ranges']: # loop through each day range and append the new column after running for each security
        
        moving_averages = []

        if parameters['exponential'] == True:

            for equity in dataframe[parameters['stock_field']].unique():
                temp = dataframe[dataframe[parameters['stock_field']] == equity]
                stock_moving_average = temp[parameters['calculation_field']].ewm(span = days, min_periods = days).mean()
                moving_averages.extend(stock_moving_average)
                del temp

            dataframe[str(days) +'_' + parameters['calculation_field'] + '_' + 'ema'] = moving_averages
        
        else:
            for equity in dataframe[parameters['stock_field']].unique():
                temp = dataframe[dataframe[parameters['stock_field']] == equity].reset_index(drop = True)
                stock_moving_average = temp[parameters['calculation_field']].rolling(days).mean()
                moving_averages.extend(stock_moving_average)
                del temp
            
            dataframe[str(days) +'_' + parameters['calculation_field'] + '_' + 'sma'] = moving_averages
    
    return dataframe


def calculate_rolling_standard_deviations(dataframe: pd.DataFrame, parameters: dict) -> pd.DataFrame:

    '''Function that calculates the standard deviations of the specified fields
    
    
    Args:
        Dataframe: pandas dataframe that contains a series of values over which the standard deviations are to be calculated
    
    Returns: Dataframe with standard deviation values added as columns in the dataset
    '''

    #TODO: come back and remove this nested for loop with more efficient process: pyspark likely a better option

    for days in parameters['day_ranges']:

        standard_deviations = []

        for equity in dataframe[parameters['stock_field']].unique():

            temp = dataframe[dataframe[parameters['stock_field']] == equity].reset_index(drop = True)
            standard_deviations.extend(temp[parameters['calculation_field']].rolling(days).std())
            print(len(standard_deviations))
        
        dataframe[str(days) +'_' + parameters['calculation_field'] + '_' + 'std'] = standard_deviations

        del standard_deviations

    return dataframe


def create_above_below_indicator_fields(dataframe: pd.DataFrame, 
                                        parameters: dict) -> pd.DataFrame:

    #TODO: Come back and format functions to be in proper format with black/linting with proper documentation

    '''Function that adds indicator fields or calculates percentage differences or both depending on arguments
    
    Args:
        dataframe: input as a pandas dataframe with fields already included for calculation

    Returns: dataframe that includes representative fields for the functionality specified by the user
    '''

    target_columns = dataframe.columns[dataframe.columns.str.contains("|".join(['sma', 'ema']))]

    for column in target_columns:

        if parameters['indicator_return_type'] == 'boolean':

            dataframe['above_'+ column + '_ind'] = np.where(dataframe[column].isna(), 
            np.nan, np.where(dataframe[column] < dataframe[parameters['calculation_field']], 1, 0))
        
        elif parameters['indicator_return_type'] == 'percentage':

            dataframe[column + '_pct_diff'] = np.where(dataframe[column].isna(), 
            np.nan,  dataframe[column] / dataframe[parameters['calculation_field']] -1)
        
        else:

            dataframe['above_'+ column + '_ind'] = np.where(dataframe[column].isna(), 
            np.nan, np.where(dataframe[column] < dataframe[parameters['calculation_field']], 1, 0))

            dataframe[column + '_pct_diff'] = np.where(dataframe[column].isna(), 
            np.nan,  dataframe[column] / dataframe[parameters['calculation_field']] -1)

    return dataframe

            


   








