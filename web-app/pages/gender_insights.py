import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px

from db import rides_by_gender_df, duration_by_gender_df, get_indexes, rides_by_hour_df, duration_by_hour_df, df

fig1 = px.pie(rides_by_gender_df, values='ride_id', names=rides_by_gender_df.index,
title='Number of Rides Split by Gender', color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig2 = px.pie(duration_by_gender_df, values='duration_seconds', names=duration_by_gender_df.index,
title='Total Ride Time Split by Gender', color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig3 = px.bar(rides_by_hour_df, x=get_indexes(rides_by_hour_df)[0], y='ride_id', color=get_indexes(rides_by_hour_df)[1], title='Number of Rides each Hour for the Last 12 hours',
labels={'ride_id':'Number of Rides', 'x':'Hour of the Day'}, color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig4 = px.bar(duration_by_hour_df, x=get_indexes(duration_by_hour_df)[0], y='duration_seconds', color=get_indexes(duration_by_hour_df)[1], title='Total Ride Duration each Hour for the Last 12 hours',
labels={'duration_seconds':'Total Duration', 'x':'Hour of the Day'}, color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig1.update_layout({'template':'plotly_dark','plot_bgcolor': 'rgba(0, 0, 0, 0)',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

fig2.update_layout({'template':'plotly_dark','plot_bgcolor': 'rgba(0, 0, 0, 0)',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

fig3.update_layout({'template':'plotly_dark',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

fig4.update_layout({'template':'plotly_dark',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

male_df = df[df['gender'] == 'male']
female_df = df[df['gender'] == 'female']

average_male_duration = male_df['duration_seconds'].mean()
average_female_duration = female_df['duration_seconds'].mean()

gender_layout = html.Div(children=[
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Total Ride Duration', className="text-success")
                ),
                dbc.CardBody(
                    dcc.Graph(
                        figure=fig1
                    )
                ),
                dbc.CardHeader(
                    html.H3('Number of Rides', className="text-success")
                ),
                dbc.CardBody(
                    dcc.Graph(
                        figure=fig2
                    )
                )
            ])
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Average Time per Ride', className="text-success")
                ),
                dbc.CardBody(children=[
                    html.H3(f"Male: {round(average_male_duration, 1)} seconds"),
                    html.H3(f"Female: {round(average_female_duration, 1)} seconds")
                ])
            ])
        ]),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H3('Ride Duration by Hour', className="text-success")
                ),
                dbc.CardBody(
                    dcc.Graph(
                        figure=fig3
                    )
                ),
                dbc.CardHeader(
                    html.H3('Number of Rides by Hour', className="text-success")
                ),
                dbc.CardBody(
                    dcc.Graph(
                        figure=fig4
                    )
                )
            ])
        ])
    ])
])