"""
This is a boilerplate pipeline 'pull_stock_data'
generated using Kedro 0.18.2
"""

from typing import Any, Dict, Tuple

from utils import fix_columns
import pandas as pd
import pandas_datareader as pdr
#import yfinance as yf
import time as tm
import logging
import random
import os 

logger = logging.getLogger(__name__)



def pull_stock_data(data_pull_parameters: dict) -> pd.DataFrame:
    
    start = tm.time()
    
    if data_pull_parameters['single_dataframe'] == True:
        pulls = 0

        for stock in data_pull_parameters['equities']:
            print(f'retrieving: {stock.strip()}')

            if pulls == 0:
                df = pdr.get_data_yahoo(stock.strip(), start = data_pull_parameters['modeling_start'], end = data_pull_parameters['modeling_end'])
                pulls+=1

                df = df.reset_index()
                df['ticker'] = stock

                df.columns = fix_columns(df.columns)
                
                sleep_time = random.randint(data_pull_parameters['sleep_min'], data_pull_parameters['sleep_max'])
                print(f"sleeping for: {sleep_time} seconds")
                # sleep between pulls so to not arouse suspicion:
                tm.sleep(sleep_time)

            else:
                temp = pdr.get_data_yahoo(stock.strip(), start = data_pull_parameters['modeling_start'], end = data_pull_parameters['modeling_end'])
                pulls+=1

                temp = temp.reset_index()
                temp['ticker'] = stock

                temp.columns = fix_columns(temp.columns)
    
                # union-all into the main dataframe:
                df = pd.concat([df, temp], ignore_index= True)

                del temp
                sleep_time = random.randint(data_pull_parameters['sleep_min'], data_pull_parameters['sleep_max'])

                print(f"sleeping for: {sleep_time} seconds")
                # sleep between pulls so to not arouse suspicion:
                tm.sleep(sleep_time)

        del pulls, sleep_time


        return df

    else:

        for stock in data_pull_parameters['equities']:

            df = pdr.get_data_yahoo(stock.strip(), start = data_pull_parameters['modeling_start'], end = data_pull_parameters['modeling_end'])
            df['ticker'] = stock
            df = df.reset_index()
          
            df.columns = fix_columns(df.columns)
            df.to_csv(os.path.join('data/01_raw/separate_stock_pulls/', stock.strip() +'.csv'), index = False)
            sleep_time = random.randint(data_pull_parameters['sleep_min'], data_pull_parameters['sleep_max'])
            tm.sleep(sleep_time)
            print(f"sleeping for: {sleep_time} seconds")
            print('saving: ', stock, ' data to: ', os.path.join('../data/01_raw/separate_stock_pulls/', stock.strip() +'.csv'))
        
        return df # only returns the last df to the catalogue ***
        
