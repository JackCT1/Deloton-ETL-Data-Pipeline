import dash
from dash import html, dcc, callback, Input, Output, page_registry
import dash_bootstrap_components as dbc

from pages.current_ride import current_ride_layout
from pages.recent_rides import recent_rides_layout

dash.register_page(__name__, path='/')

layout = html.Div(children=[
    html.H1('Deloton Dashboard', style={'textAlign': 'center'}),
    html.Div(
    [   
        dbc.Tabs(
            [
                dbc.Tab(label="Current Ride", tabClassName="flex-grow-1 text-center", tab_id="tab-1"),
                dbc.Tab(label="Recent Rides", tabClassName="flex-grow-1 text-center", tab_id="tab-2"),
            ],
            id="tabs",
            active_tab="tab-1",
        ),
        html.Div(id="content"),
    ])
])


@dash.callback(Output("content", "children"), [Input("tabs", "active_tab")])
def switch_tab(at):
    if at == "tab-1":
        return current_ride_layout
    elif at == "tab-2":
        return recent_rides_layout
    return html.P("This shouldn't ever be displayed...")

"tab_style={'marginLeft': '41%'}"