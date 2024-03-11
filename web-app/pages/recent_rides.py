import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc

from pages.age_insights import age_layout
from pages.gender_insights import gender_layout
from pages.power_insights import power_layout

recent_rides_layout = html.Div(children=[
    html.H3('Recent Rides', style={'textAlign': 'center'}),
    html.Div(
    [
        dbc.Tabs(
            [
                dbc.Tab(label="Age Insights", tabClassName="flex-grow-1 text-center", tab_id="tab-1"),
                dbc.Tab(label="Gender Insights", tabClassName="flex-grow-1 text-center", tab_id="tab-2"),
                dbc.Tab(label="Telemetry Insights", tabClassName="flex-grow-1 text-center", tab_id="tab-3")
            ],
            id="sub-tabs",
            active_tab="tab-1",
        ),
        html.Div(id="tab-content"),
    ])
])


@dash.callback(Output("tab-content", "children"), [Input("sub-tabs", "active_tab")])
def switch_tab(at):
    if at == "tab-1":
        return age_layout
    elif at == "tab-2":
        return gender_layout
    elif at == "tab-3":
        return power_layout
    return html.P("This shouldn't ever be displayed...")