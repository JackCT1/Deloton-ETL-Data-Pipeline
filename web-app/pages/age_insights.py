import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px

from db import rides_by_age_df, duration_by_age_df

fig1 = px.bar(rides_by_age_df, y='ride_id', barmode='group', title='Number of Rides Across Different Age groups',
labels={'ride_id':'Number of Rides', 'age':'Age'}, color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig2 = px.bar(duration_by_age_df, y='duration_seconds', barmode='group', title='Total Ride Time Across Age Groups',
labels={'duration_seconds':'Ride Duration', 'age':'Age'}, color_discrete_sequence=['#7CC37C', '#E6E6D9'])

fig1.update_layout({'template':'plotly_dark',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

fig2.update_layout({'template':'plotly_dark',
'paper_bgcolor': 'rgba(0, 0, 0, 0)'})

age_layout = html.Div(children=[
    dbc.Card([
        dbc.CardHeader(
            html.H3("Number of Rides per Age Group", className="text-success")
        ),
        dbc.CardBody(
            dcc.Graph(
                figure=fig1
            )
        )
    ]),
    dbc.Card([
        dbc.CardHeader(
            html.H3("Ride Duration Across Age Groups", className="text-success")
        ),
        dbc.CardBody(
            dcc.Graph(
                figure=fig2
            )
        )
    ])
])