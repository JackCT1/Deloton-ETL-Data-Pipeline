import helpers
from dash import Input, Output, callback, dcc, html

current_ride_layout = html.Div([
    html.Div(id='live-update-layout'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000, # in milliseconds
        n_intervals=0
    )
])

@callback(
    Output('live-update-layout', 'children'),
    Input('interval-component', 'n_intervals')
    )
def get_live_layout(n):
    live_ride_data = helpers.get_live_ride_data()
    live_layout_children = helpers.get_current_ride_layout(live_ride_data)
    return live_layout_children