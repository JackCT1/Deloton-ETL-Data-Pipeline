from datetime import date, datetime
import logging
import os
from time import strptime

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

def get_logger(log_level: str) -> logging.Logger:
        """
        Returns:
        - formatted logger
        """
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s: %(levelname)s: %(message)s'
        )
        logger = logging.getLogger()                    
        return logger

def extract_staging_data() -> dict:
        """
        Writes SQL table into dataframe

        Returns:
        Dictionary with dataframes:
        - user dataframe
        - user_ride dataframe
        - metrics dataframe
        """
        logging.info('EXTRACTING DATA...') # - check
        
        user_df = pd.read_sql_query(f'SELECT * FROM {STAGING_SCHEMA}.user_table',con=engine)
        user_ride_df = pd.read_sql_query(f'SELECT * FROM {STAGING_SCHEMA}.user_ride',con=engine)
        metrics_df = pd.read_sql_query(f'SELECT * FROM {STAGING_SCHEMA}.metrics_table',con=engine)

        dfs_dict = {
            "user_df": user_df,
            "user_ride_df": user_ride_df,
            "metrics_df": metrics_df
        }

        return dfs_dict

def calculate_age(date_of_birth: str) -> int:
    """
    Calculates user age from date_of_birth, which is a string in the
    format '%Y-%m-%d %H:%M:%S'
    
    Returns:
    - age (years)
    """
    today = date.today()
    
    dob_time = strptime(date_of_birth, '%Y-%m-%d %H:%M:%S')

    year = dob_time.tm_year
    month = dob_time.tm_mon
    day = dob_time.tm_mday

    diff_years = today.year - year
    is_before_birthday = (today.month, today.day) < (month, day)
    age = diff_years - is_before_birthday

    return age

def calculate_bmi(weight_kg: int, height_cm: int) -> float:
    """
    Turns weight & height data into BMI

    Args:
        weight in kg
        height in cm

    Returns:
        - BMI (1 decimal place)
    """
    bmi = 0.0
    if weight_kg and height_cm:
        weight = weight_kg
        height = height_cm/100            
        bmi = round(weight/(height)**2, 1)
    
    return bmi
