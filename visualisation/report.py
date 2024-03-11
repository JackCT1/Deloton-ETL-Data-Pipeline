import base64
from datetime import date, datetime, timedelta
import logging
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import plotly.express as px
import sqlalchemy

load_dotenv()
REGION = os.getenv('REGION')

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

SES_SENDER_ADDRESS = os.getenv('SES_SENDER_ADDRESS')
CEO = os.getenv('CEO')

FROM_EMAIL = os.getenv('FROM_EMAIL')
TO_EMAIL = os.getenv('TO_EMAIL')

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

def extract_production_data() -> pd.DataFrame:
    """
    Writes SQL table into dataframe

    Returns:
    - Dataframe containing the last 24hrs of ride data
    """
    logging.info('EXTRACTING DATA...') # - check

    daily = date.strftime(date.today(), '%Y-%m-%d %H:%M:%S')
    daily_df = pd.read_sql_query(
        f"""
            SELECT * FROM {PRODUCTION_SCHEMA}.dash_table
            WHERE time >= '{daily}'
        """
    ,con=engine)

    return daily_df

def create_image_file_base64_string(filepath: str) -> str:
    """
    Converts png into a string
    
    Returns:
    - image html uri
    """
    data_uri = base64.b64encode(open(filepath, 'rb').read()).decode('utf-8')
    return data_uri

def create_email() -> dict:
    """
    Visualises data
    Creates HTML format SES message

    Returns:
    - message dictionary
    """
    daily_df = extract_production_data()

    # stat: average heart rate
    heart_rate = daily_df[['ride_id', 'gender', 'heart_rate_avg', 'heart_rate_min', 'heart_rate_max']]
    avg_heart_rate_avg = heart_rate['heart_rate_avg'].mean()

    # stat: average bmi
    bmi = daily_df[['ride_id', 'gender', 'bmi']]
    avg_bmi = bmi['bmi'].mean()

    # stat: average bmi (by gender)
    avg_bmi_gender = bmi.groupby('gender').agg({'bmi':'mean'}).reset_index()
    avg_female_bmi = avg_bmi_gender[avg_bmi_gender['gender']=='female']['bmi'].max()
    avg_male_bmi = avg_bmi_gender[avg_bmi_gender['gender']=='male']['bmi'].max()

    # stat: average power
    power = daily_df[['ride_id', 'gender', 'power_total', 'power_avg', 'power_min', 'power_max']]
    total_power_total = power['power_total'].sum()
    avg_power_avg = power['power_avg'].mean()

    # visual: number of rides (by gender)
    total_rides = daily_df['ride_id'].count()

    total_rides_gender = daily_df[['ride_id', 'gender']]
    total_rides_gender = total_rides_gender.groupby('gender').count()

    fig_total_rides = px.pie(
        total_rides_gender,
        values='ride_id',
        title='Total Rides (by gender)',
        names=total_rides_gender.index,
        color_discrete_sequence=['#7CC37C', '#343434'],
        hole=.4,
        height=600,
        width=600
    )

    fig_total_rides.add_annotation(text=f'Total Rides: {total_rides}', showarrow=False)
    fig_total_rides.update_traces(
        text=total_rides_gender['ride_id'].map('{:}'.format),
        textinfo='percent+text'
    )
    fig_total_rides.write_image('/tmp/total_rides.png')
    
    # visual: total duration (by gender)
    total_duration = int(daily_df['duration_seconds'].sum())

    total_duration_gender = daily_df[['duration_seconds', 'gender']]
    total_duration_gender['duration_seconds'] = total_duration_gender['duration_seconds'].astype(int)
    total_duration_gender = total_duration_gender.groupby('gender').sum()

    fig_total_duration = px.pie(
        total_duration_gender,
        values='duration_seconds',
        title='Total Duration in Seconds (by gender)',
        names=total_duration_gender.index,
        color_discrete_sequence=['#7CC37C', '#343434'],
        hole=.4,
        height=600,
        width=600
    )

    fig_total_duration.add_annotation(text=f'Total Duration: {total_duration}', showarrow=False)
    fig_total_duration.update_traces(
        text=total_duration_gender['duration_seconds'].map('{:}'.format),
        textinfo='percent+text'
    )
    fig_total_duration.write_image('/tmp/total_duration.png')

    # visual: ages
    ages = daily_df[['ride_id', 'gender', 'age']]

    age_bins = pd.cut(
        ages['age'],
        bins=[18, 25, 35, 45, 55, 65, np.Infinity],
        labels=[
            '18 - 25',
            '26 - 35',
            '36 - 45',
            '46 - 55',
            '56 - 65',
            '65+'])

    ages_split = ages.groupby(age_bins).agg({'ride_id': 'count'})

    color = dict(enumerate([f'{row}' for row in ages_split.index],start=1))

    fig_ages = px.bar(
        ages_split,
        x=ages_split.index,
        y='ride_id',
        title='Number of Rides (by ages)',
        labels={
            'ride_id':'Number of Rides',
            'age':'Age'
        },
        text_auto=True,
        color_discrete_sequence=['#7CC37C', '#343434'],
        color=color,
        height=600,
        width=1200
    )

    fig_ages.update_traces(textposition='outside', showlegend=True)
    fig_ages.update_layout(
        # paper_bgcolor='rgba(0,0,0,0)',
        # plot_bgcolor='rgba(0,0,0,0)'
        legend_title_text='Ages')
    fig_ages.write_image('/tmp/ages.png')

    # visual: ages (by gender)
    ages_gender_split = ages.groupby(['gender', age_bins]).agg({'ride_id': 'count'})
    ages_gender_pivot = ages_gender_split.reset_index()
    ages_gender_pivot = ages_gender_pivot.pivot(index='age', columns='gender', values='ride_id')

    fig_ages_gender = px.bar(
        ages_gender_pivot,
        x=ages_gender_pivot.index,
        y=['female', 'male'],
        title='Gender Split of Rides (by ages)',
        labels={
            'value':'Number of Rides',
            'age':'Age'
        },
        text_auto=True,
        color_discrete_sequence=['#7CC37C', '#343434'],
        barmode='group',
        height=600,
        width=1200
    )

    fig_ages_gender.update_traces(textposition='outside', showlegend=True)
    fig_ages_gender.update_layout(
        # paper_bgcolor='rgba(0,0,0,0)',
        # plot_bgcolor='rgba(0,0,0,0)'
        legend_title_text='Gender')
    fig_ages_gender.write_image('/tmp/ages_gender.png')

    # email
    charset = 'UTF-8'

    html = f"""
            <html>
                <p>&nbsp;</p>
                <div>
                <h2 style="text-align: center;"><span style="color: #ff0000;"><img style="color: #000000; font-size: 14px;" src="https://user-images.githubusercontent.com/5181870/188019461-4a27a045-9301-4931-910c-b367f7b2709a.png" alt="fullwidth" width="380" height="141" /></span></h2>
                <h2 style="text-align: center;"><span style="color: #ff0000;">Daily Report!</span></h2>
                </div>
                <div>&nbsp;</div>
                <div>
                <h4 style="text-align: center;"><span style="color: #000000;">Statistics</span></h2>
                </div>
                <div>
                <p>&nbsp;</p>
                <p>Average Heart Rate: {avg_heart_rate_avg}</p>
                </div>
                <div>
                <p>&nbsp;</p>
                <p>Average BMI: {avg_bmi}</p>
                </div>
                <div>
                <p>&nbsp;</p>
                <p>Average Female BMI: {avg_female_bmi}</p>
                <p>Average Male BMI: {avg_male_bmi}</p>
                </div>
                <div>
                <p>&nbsp;</p>
                <p>Total Power: {total_power_total}</p>
                <p>Average Power: {avg_power_avg}</p>
                </div>
                <div>&nbsp;</div>
                <div>
                <h4 style="text-align: center;"><span style="color: #000000;">Graphs</span></h2>
                </div>
                <div>
                <p>&nbsp;</p>
                <p>Number of Rides (by gender)</p>
                </div>
                <img src="data:image/png;base64,{create_image_file_base64_string('/tmp/total_rides.png')}" width="400px" align ="left">
                <div>
                <p>&nbsp;</p>
                <p>Total Duration (by gender)</p>
                </div>
                <img src="data:image/png;base64,{create_image_file_base64_string('/tmp/total_duration.png')}" width="400px" align ="left">
                <div>
                <p>&nbsp;</p>
                <p>Ages</p>
                </div>
                <img src="data:image/png;base64,{create_image_file_base64_string('/tmp/ages.png')}" width="400px" align ="left">
                <div>
                <p>&nbsp;</p>
                <p>Age (by gender)</p>
                </div>
                <img src="data:image/png;base64,{create_image_file_base64_string('/tmp/ages_gender.png')}" width="400px" align ="left">
            </html>
        """
    
    message = {
                "Body": {
                    "Html": {
                        "Charset": charset,
                        "Data": html,
                    }
                },
                "Subject": {
                    "Charset": charset,
                    "Data": "Daily Report",
                },
            }
    
    return message

def send_report() -> bool:
    """
    Connects to SES via boto3 client
    Sends SES message for daily report

    Returns:
    - True if successfully sent
    - False if error occurred in attempting to send email
    """
    message = create_email()
    try:
        ses_client = boto3.client('ses', REGION)

        ses_client.send_email(
            Destination={
                "ToAddresses": [
                    TO_EMAIL,
                ],
            },
            Message=message,
            Source=FROM_EMAIL,
        )
        return True
    except Exception as e:
        logging.error("""
            Error occurred whilst trying to send email FROM %s
            TO %s with SES:
            %s
            """, FROM_EMAIL, TO_EMAIL, e)
        return False
    
def handler(event, context):
    """
    AWS Handler for Lambda Function
    """
    log = get_logger(logging.INFO)

    # Run Script
    log.info('STARTING...') # - check
    full_date = datetime.now()
    log.info('DATE: %s', full_date) # - check

    engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')    
    send_report()