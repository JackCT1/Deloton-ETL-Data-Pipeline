from datetime import datetime, timedelta
from dotenv import load_dotenv

import numpy as np
import os
import pandas as pd
import plotly.express as px
import sqlalchemy
# import psycopg2

load_dotenv(override=True)
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

STAGING_SCHEMA = 'zuckerberg_staging'
PRODUCTION_SCHEMA = 'zuckerberg_production'
TEST1_SCHEMA = 'zuckerberg_test_1'
TEST2_SCHEMA = 'zuckerberg_test_2'

engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

daily = datetime.strftime(datetime.today(), '%Y-%m-%d %H:%M:%S')
twelve_hours = datetime.strftime(datetime.now() - timedelta(hours=12), '%Y-%m-%d %H:%M:%S')

df = pd.read_sql_query(
    f"""
        SELECT * FROM {PRODUCTION_SCHEMA}.dash_table
        WHERE time >= '{twelve_hours}'
    """
,con=engine)

# changing account creation and time to datetime format
df['account_creation'] = pd.to_datetime(df['account_creation'])
df['time'] = pd.to_datetime(df['time'])

# creating bins for age groups and ride duration in seconds
age_group_bins = pd.cut(df['age'], bins = [18, 25, 35, 45, 55, 65, np.inf], labels = ['18-25', '25-35', '35-45', '45-55', '55-65', '65 or Above'])
duration_bins = pd.cut(df['duration_seconds'], bins = [0, 300, 400, 500, 600, np.inf],
labels=['0-300', '300-400', '400-500', '500-600', '600 or Above'])

# dataframes for age insights
rides_by_age_df = df.groupby([age_group_bins])[['ride_id']].count()
duration_by_age_df = df.groupby([age_group_bins])[['duration_seconds']].sum()

duration_rides_df = df.groupby([duration_bins])[['ride_id']].count()

# dataframes for gender insights
rides_by_gender_df = df.groupby(['gender'])[['ride_id']].count()
duration_by_gender_df = df.groupby(['gender'])[['duration_seconds']].sum()

def get_indexes(df):
    '''
    Returns separate indexes in a multi index data frame
    '''
    first_index_list = []
    second_index_list = []
    for index in df.index:
        first_index_list.append(index[0])
        second_index_list.append(index[1])
    return first_index_list, second_index_list

df['hour'] = df['time'].dt.hour
rides_by_hour_df = df.groupby(['hour', 'gender'])[['ride_id']].count()
duration_by_hour_df = df.groupby(['hour', 'gender'])[['duration_seconds']].sum()