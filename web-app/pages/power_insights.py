import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc

from db import df

# variables for power insights

total_power = df['power_total'].sum()
average_power = df['power_avg'].mean()

average_heart_rate = df['heart_rate_avg'].mean()

average_rpm = df['rpm_avg'].mean()

average_bmi = df['bmi'].mean()

power_layout = html.Div(children=[
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Mean Heart Rate', className="text-success")
                ),
                dbc.CardBody(children=[
                    html.H3(f'{round(average_heart_rate, 1)} beats per minute', className="card-title")
                ])
            ])
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Total Power', className="text-success")
                ),
                dbc.CardBody(children=[
                    html.H3(f'{round(total_power, 1)} Watts', className='card-title')
                ])
            ])
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Mean RPM', className="text-success")
                ),
                dbc.CardBody(children=[
                    html.H3(f'{round(average_rpm, 1)} revolutions per minute', className='card-title')
                ])
            ])
        ])
    ])
])