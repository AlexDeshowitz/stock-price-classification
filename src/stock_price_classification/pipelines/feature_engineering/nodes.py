"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.2
"""

import pandas as pd




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







