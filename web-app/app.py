import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, dcc, html, page_container, page_registry

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.DARKLY])

app.layout = html.Div(children=[
    page_container
    ])

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", debug=True, port=8080)