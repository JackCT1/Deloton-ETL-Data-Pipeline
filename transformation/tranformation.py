from datetime import date, datetime
import logging
import os
from time import strptime

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

# Credentials
load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

STAGING_SCHEMA = 'zuckerberg_staging'
PRODUCTION_SCHEMA = 'zuckerberg_production'

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

def clean_dataframes(df_dict: dict) -> pd:
    """
    Combines & Transforms staging dataframes

    Returns:
    - Single transformed dataframe
    """

    user_df = df_dict.get("user_df")
    user_ride = df_dict.get("user_ride_df")
    metrics_df = df_dict.get("metrics_df")

    # extract relevant information from user_df
    updated_user = pd.DataFrame()
    updated_user['user_id'] = user_df['user_id']
    updated_user['first_name'] = user_df['first_name']
    updated_user['last_name'] = user_df['last_name']
    updated_user['gender'] = user_df['gender']
    updated_user['age'] = user_df['date_of_birth'].apply(calculate_age)
    updated_user['bmi'] = user_df.apply(lambda row: calculate_bmi(row.weight_kg, row.height_cm), axis=1)
    updated_user['postcode'] = user_df['postcode']
    updated_user['account_creation'] = user_df['account_creation']

    # merge updated_user + user_ride
    user_merge_df = user_ride.merge(updated_user, on='user_id', how='left', sort=False)
    
    # extract relevant information from metrics_df
    grouped_metrics = metrics_df.groupby('ride_id').agg({
        'time':'first',
        'bike_model':'first',
        'resistance':'mean',
        'heart_rate':['mean','min','max'],
        'rpm':['mean','min','max'],
        'power':['sum','mean','min','max'],
        'duration_seconds':'max'
    }).reset_index()

    updated_metrics = pd.DataFrame()
    updated_metrics['ride_id'] = grouped_metrics['ride_id']
    updated_metrics['time'] = grouped_metrics['time']['first']
    updated_metrics['bike_model'] = grouped_metrics['bike_model']['first']
    updated_metrics['resistance_avg'] = grouped_metrics['resistance']['mean']
    updated_metrics['heart_rate_avg'] = grouped_metrics['heart_rate']['mean']
    updated_metrics['heart_rate_min'] = grouped_metrics['heart_rate']['min']
    updated_metrics['heart_rate_max'] = grouped_metrics['heart_rate']['max']
    updated_metrics['rpm_avg'] = grouped_metrics['rpm']['mean']
    updated_metrics['rpm_min'] = grouped_metrics['rpm']['min']
    updated_metrics['rpm_max'] = grouped_metrics['rpm']['max']
    updated_metrics['power_total'] = grouped_metrics['power']['sum']
    updated_metrics['power_avg'] = grouped_metrics['power']['mean']
    updated_metrics['power_min'] = grouped_metrics['power']['min']
    updated_metrics['power_max'] = grouped_metrics['power']['max']
    updated_metrics['duration_seconds'] = grouped_metrics['duration_seconds']['max']
    
    # merge user_df + updated_metrics
    ride_df = user_merge_df.merge(updated_metrics, on='ride_id', how='left', sort=False)
    log.info('CREATED RIDE DATAFRAME') # - check

    return ride_df

def sql_conversion() -> None:
    """
    Write dataframe into SQL table
    """

    logging.info('SCHEMA: %s', PRODUCTION_SCHEMA) # - check
    df_dict = extract_staging_data()
    clean_df = clean_dataframes(df_dict)
    clean_df.to_sql(
        'dash_table',
        con=engine,
        schema=PRODUCTION_SCHEMA,
        if_exists='replace',
        index=False
    )
    logging.info('...COMPLETE!') # - check

def handler(event, context):
    """
    AWS Handler for Lambda Function
    """

    # Make logger
    log = get_logger(logging.INFO)


    # Run Script
    log.info('STARTING...') # - check
    full_date = datetime.now()
    log.info('DATE: %s', full_date) # - check

    engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')    
    sql_conversion()