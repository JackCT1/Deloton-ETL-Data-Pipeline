"""Current Rides helpers:
Contains Helper functions to generate figures and values to display on current rides page"""
import logging
import os
from datetime import datetime

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy
from dash import dcc, html
from dotenv import load_dotenv


def get_deleton_engine() -> sqlalchemy.engine.Engine:
    """Get a sqlalchemy engine that is connected to the deleton database

    Returns:
        Engine: sql alchemy engine that can be used to query
    """
    load_dotenv()
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')

    zuck_engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    return zuck_engine

def extract_latest_log_info(engine: sqlalchemy.engine.Engine) -> pd.Series:
    """
    Queries the staging schema to retrieve the metrics
    and user details of the most recent entry

    Arguments:
    - engine: a sqlalchemy engine that is connected to the deloton database

    Returns:
    - Pandas series corresponding to the result of the query.
    - An empty series if no result or there is an error whilst querying. 
    """

    query = '''
        with latest as
        (select *
        from zuckerberg_staging.metrics_table
        order by "time" desc
        limit 1)
        select *
        from latest 
        join zuckerberg_staging.user_ride ur using(ride_id)
        join zuckerberg_staging.user_table ut using(user_id)
        '''

    logging.info('EXTRACTING CURRENT RIDE INFO...') # - check
    
    try:
        result_df = pd.read_sql_query(query,con=engine)
    except Exception as e:
        logging.error("Error whilst querying database: %s",e)
        return pd.Series()
    
    result_series = result_df.squeeze()

    return result_series

def extract_recent_heart_rates(engine: sqlalchemy.engine.Engine) -> pd.DataFrame:
    """
    Queries the staging schema to retrieve the heart rate data for the
    last 30 seconds\n
    Only retireves heart rate data from the current ride.

    Arguments:
    - engine: a sqlalchemy engine that is connected to the deloton database

    Returns:
    - Pandas dataframe corresponding to the result of the query.
    - An empty dataframe if no result or there is an error whilst querying. 
    """

    query = """
        with latest as
            (select ride_id
            from zuckerberg_staging.metrics_table
            order by "time" desc
            limit 1)
        select heart_rate, "time" 
        from zuckerberg_staging.metrics_table mt
        join latest using(ride_id)
        where time::timestamp >= (
            now() at time zone('utc') - interval  '30 seconds'
            )
        order by time desc  
        limit 100
    """

    logging.info('EXTRACTING RECENTS HEART RATE INFO...') # - check
    
    try:
        result_df = pd.read_sql_query(query,con=engine)
    except Exception as e:
        logging.error("Error whilst querying database: %s",e)
        return pd.DataFrame()

    return result_df

def calculate_age(dob: datetime) -> int:
    """
    Calculates user age from dob, which is in a datetime object
    
    Returns:
    - age (years)
    """
    today = datetime.now()

    year = dob.year
    month = dob.month
    day = dob.day

    diff_years = today.year - year
    is_before_birthday = (today.month, today.day) < (month, day)
    age = diff_years - is_before_birthday
    return age

def check_heart_rate(age: int, heart_rate: int) -> bool:
    """
    Checks heart_rate against safe range
    - age (years)
    - heart_rate (bpm)
    Returns:
    - True if Safe
    - False if Unsafe
    """
    max_heart_rate = 220 - age
    lower_limit = max_heart_rate * 0.5
    upper_limit = max_heart_rate * 0.7

    is_safe = lower_limit <= heart_rate <= upper_limit or heart_rate == 0
    return is_safe

def get_heart_rate_limits(age: int) -> dict:
    """
    Calculates the safe limits for heart rate whilst 
    exercising based on age

    Arguments:
    age (years)

    Returns:
    A dictionary containing the limits with keys:
    - "upper" (int)
    - "lower" (int)
    """
    max_heart_rate = 220 - age
    lower_limit = max_heart_rate * 0.5
    upper_limit = max_heart_rate * 0.7

    limits = {
        "upper": upper_limit,
        "lower": lower_limit
    }

    return limits

def generate_heart_rate_figure(limited_heart_rate_df: pd.DataFrame) -> go.Figure:
    """generates and formats current heart rate chart

    Args:
        limited_heart_rate_df (pd.DataFrame): df with columns heart_rate, time, lower_limit and upper_limit
        for the current user over the last 30 seconds.

    Returns:
        go.Figure: plotly line chart with heart rate from last 30 seconds and horizontal limit lines to show
        safe range for heart rate
    """

    y_tick_vals = [x for x in range(201) if not x%20]

    fig = px.line(
        limited_heart_rate_df,
        y='heart_rate',
        x = 'time',
        title='Live Heart Rate',
        labels={
            'heart_rate':'Heart Rate',
            'time':'Seconds from Now',
            }
        ).update_yaxes(
            range=[0, 200]
        ).update_layout( 
            # Set y axis to be on right hand side
            yaxis={
                'side': 'right',
                'tickvals': y_tick_vals
                }  
        ).update_traces(
            #change heart rate line colour
            line_color='#EE0000',
            line_width=5
        ).add_trace(
            go.Scatter(
                x=limited_heart_rate_df.time,
                y=limited_heart_rate_df.upper_limit,
                name='Upper Safe Limit',
                line=dict(
                    color='firebrick',
                    width=3,
                    dash='dash'
                    ) # dash options include 'dash', 'dot', and 'dashdot'
            )
        ).add_trace(
            go.Scatter(
                x=limited_heart_rate_df.time,
                y=limited_heart_rate_df.lower_limit,
                name='Lower Safe Limit',
                line=dict(
                    color='mediumslateblue',
                    width=3,
                    dash='dot'
                    ) # dash options include 'dash', 'dot', and 'dashdot'
            )
        ).update_layout(
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            paper_bgcolor='rgba(0, 0, 0, 0)',
            template='plotly_dark',
            title_x=0.5,
            title_font_size=30
        )

        # fig2.update_layout({'template':'plotly_dark','plot_bgcolor': 'rgba(0, 0, 0, 0)',
        # 'paper_bgcolor': 'rgba(0, 0, 0, 0)'})
    return fig

def add_safe_limits_to_dataframe(heart_rate_df: pd.DataFrame, heart_rate_limits: dict) -> pd.DataFrame:
    """Appends columns with upper and lower safe heart rate limits from the given dictionary
    to the recent heart rate dataframe

    Args:
        heart_rate_df (pd.DataFrame): dataframe with columns heart_rate and time
        heart_rate_limits (dict): dictionary containing "upper" and "lower" safe heart rates

    Returns:
        pd.DataFrame: dataframe with appended columns
    """
    df_with_limits = heart_rate_df.assign(
        upper_limit = heart_rate_limits['upper'],
        lower_limit = heart_rate_limits['lower']
    )

    return df_with_limits

def get_heart_rate_plot(latest_log_info: pd.Series, recent_heart_rate_df: pd.DataFrame) -> go.Figure:
    """ Utilises the current rider info to augment the recent heart rate data with safe limits,
    then call the functions to generate the plot displaying this info

    Args:
        latest_log_info (pd.Series): Summary of user info with most recent metrics log
        recent_heart_rate_df (pd.DataFrame): table containing heart rates from last 30 seconds for
        the current rider

    Returns:
        go.Figure: plotly line chart with heart rate from last 30 seconds and horizontal limit lines to show
        safe range for heart rate
    """

    current_dob = latest_log_info.date_of_birth

    recent_heart_rate_df.time = pd.to_datetime(recent_heart_rate_df.time, format='%Y-%m-%d %H:%M:%S')

    time_now = datetime.utcnow()

    recent_heart_rate_df.time = recent_heart_rate_df.time.apply(lambda x: -(time_now - x).seconds)

    current_dob = latest_log_info.date_of_birth

    current_age = get_age_from_dob_str(current_dob)

    current_user_heart_rate_limits = get_heart_rate_limits(current_age)

    recent_heart_rate_df_with_limits = add_safe_limits_to_dataframe(recent_heart_rate_df, current_user_heart_rate_limits)

    heart_rate_figure = generate_heart_rate_figure(recent_heart_rate_df_with_limits)
    
    return heart_rate_figure

def get_age_from_dob_str(current_dob: str) -> int:
    """given dob as a string in below format, calculate the age

    Args:
        current_dob (_type_): users dob in string format '%Y-%m-%d %H:%M:%S'

    Returns:
        current_age (int): current age in years 
    """
    current_dob_dt = datetime.strptime(current_dob, '%Y-%m-%d %H:%M:%S')

    current_age = calculate_age(current_dob_dt)
    return current_age

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


def get_live_ride_data() -> dict:

    """Gets a dictionary of the most recent ride and user info.

    Returns:
        current_live_ride_data (dict): dictionary containing latest
            - user_info
            - metrics
            - heart_rate_fig
    """
    engine = get_deleton_engine()

    recent_heart_rate_df = extract_recent_heart_rates(engine)

    latest_log_info = extract_latest_log_info(engine)

    heart_rate_figure = get_heart_rate_plot(latest_log_info,recent_heart_rate_df)

    user_name = latest_log_info.first_name + " " + latest_log_info.last_name
    user_age = get_age_from_dob_str(latest_log_info.date_of_birth)
    user_weight = latest_log_info.weight_kg
    user_height = latest_log_info.height_cm
    user_bmi = calculate_bmi(user_weight,user_height)
    user_gender = latest_log_info.gender

    user_info = {
        "name": user_name,
        "age": user_age,
        "weight": user_weight,
        "height": user_height,
        "bmi": user_bmi,
        "gender": user_gender
    }

    live_heart_rate = latest_log_info.heart_rate
    live_power = latest_log_info.power
    live_duration = latest_log_info.duration_seconds

    metrics = {
        "heart_rate": live_heart_rate,
        "duration": live_duration,
        "power": live_power
    }

    current_ride_live_data = {
        "heart_rate_fig": heart_rate_figure,
        "user_info": user_info,
        "metrics": metrics
    }

    return current_ride_live_data

def get_current_ride_layout(live_data: dict) -> list:

    """Build the html elements that make up the current rides dash page

    Arguments:
        live_data (dict): Contains the necessary live data for the current rides page
        - "heart_rate_fig"
        - "user_info"
        - "metrics"

    Returns:
        div_children (list): layout components
    """

    user_info = live_data["user_info"]
    metrics = live_data["metrics"]
    heart_rate_fig = live_data["heart_rate_fig"]

    is_heart_rate_safe = check_heart_rate(user_info["age"],metrics["heart_rate"])

    heart_rate_div_colour = 'primary'
    heart_rate_warning_text = '(Safe)'

    if metrics["heart_rate"] == 0:
        heart_rate_warning_text = '(Not Detected)'
    elif not is_heart_rate_safe:
        heart_rate_div_colour = 'danger'
        heart_rate_warning_text = '(UNSAFE)'

    div_children = [
        html.H3('Current Ride', style={'textAlign': 'center'}),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(
                        html.H3('User Profile', className="text-success")
                    ),
                    dbc.CardBody(children=[
                        html.H4(f'Name: {user_info["name"]}', className="card-title"),
                        html.B(),
                        html.H4(f'Age: {user_info["age"]}', className="card-title"),
                        html.B(),
                        html.H4(f'Weight: {user_info["weight"]} kg', className="card-title"),
                        html.B(),
                        html.H4(f'Height: {user_info["height"]} cm', className="card-title"),
                        html.B(),
                        html.H4(f'BMI: {user_info["bmi"]}', className="card-title"),
                        html.B(),
                        html.H4(f'Gender: {user_info["gender"]}', className="card-title")
                    ])
                ])
            ],
            width=3),
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader(
                                html.H3('Heart Rate', className="text-success")
                            ),
                            dbc.CardBody(children=[
                                html.H4(f'{metrics["heart_rate"]} bpm {heart_rate_warning_text}', className="card-title")
                            ])
                        ],
                        color=heart_rate_div_colour)
                    ]),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader(
                                html.H3('Duration', className="text-success")
                            ),
                            dbc.CardBody(children=[
                                html.H4(f'{metrics["duration"]} seconds', className="card-title")
                            ])
                        ])
                    ]),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader(
                                html.H3(f'Power', className="text-success")
                            ),
                            dbc.CardBody(children=[
                                html.H4(f'{metrics["power"]} W', className="card-title")
                            ])
                        ])
                    ])
                ]),
                dbc.Row([
                    html.Br()
                ]),
                dbc.Row([
                    dcc.Graph(figure=heart_rate_fig)
                ],
                align='centre')
            ])
        ])       
    ]

    return div_children