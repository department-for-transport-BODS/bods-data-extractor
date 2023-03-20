# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 15:04:24 2022

@author: -loper-irai
"""

# Useful documentation: https://realpython.com/python-dash/

import dash  # initialise application
import dash_core_components as dcc  # interative components, dropdowns
from dash import html, Input, Output
import pandas as pd
from dash import dash_table

from extractor import TimetableExtractor
# from BODSDataExtractor import otc_db_download
import os

api = os.environ.get('bods_api_key')

my_bus_data_object = TimetableExtractor(api_key=api
                                ,status = 'published' # Only view published datasets
                                ,service_line_level=True # True if you require Service line data
                                ,stop_level=False # True if you require stop level data
                                )

# Raise following 2 in github
# ISSUE - versioning causing issues in services_on_bods_or_otc_by_area() method - we don't have the revision field available there we don't know which servicecode-linename combo is the most recent
# BUG - have to run code seperately and then load to csv and read from csv
# BUG - local authority = 'None' - why?
services_by_area = my_bus_data_object.services_on_bods_or_otc_by_area() #Generates a dataframe of all service codes published on BODS and/or in the OTC database, indicates whether they are published on both or one of BODS and OTC, and provides the admin area the services has stops within
services_by_area.to_csv("dashboard_data.csv")

# taken from 'maintenance_function.py'
services_not_maintained = pd.read_csv("Services_not_maintained.csv")

services_by_area = pd.read_csv("dashboard_data.csv")

# renaming columns to enable join with services_by_area
services_not_maintained = services_not_maintained.rename(
    columns={"ServiceCode": "service_code", "LineName": "LineName_bods", "ATCO Code": "atco_code"})

services_by_area_with_maintenance = pd.merge(services_by_area, services_not_maintained[
    ['service_code', 'LineName_bods', 'Admin Area Name associated with ATCO Code', 'atco_code', 'not_maintained']],
                                             how='left', on=['service_code', 'LineName_bods',
                                                             'Admin Area Name associated with ATCO Code',
                                                             'atco_code']).drop_duplicates()

services_by_area_with_maintenance_df = pd.DataFrame(services_by_area_with_maintenance)

services_by_area_with_maintenance_df = services_by_area_with_maintenance_df.reset_index()

# gets the number of unique services which have not been updated in last 42 days for each la - check logic
services_by_area_with_maintenance_df_filtered = services_by_area_with_maintenance_df.loc[
    services_by_area_with_maintenance_df['not_maintained'] == True]
maintenance_issues_by_la = \
services_by_area_with_maintenance_df_filtered.groupby('Admin Area Name associated with ATCO Code')[
    'service_code'].nunique()
maintenance_issues_by_la = maintenance_issues_by_la.reset_index()

# get the number of unique service codes per authority
no_services_by_la = services_by_area_with_maintenance_df.groupby('Admin Area Name associated with ATCO Code')[
    'service_code'].nunique()
no_services_by_la = no_services_by_la.reset_index()

# get the number of unique services which are in bods per la
no_of_services_in_bods = services_by_area_with_maintenance_df.loc[services_by_area_with_maintenance_df['in_bods'] == 1]
no_of_services_in_bods_per_la = no_of_services_in_bods.groupby('Admin Area Name associated with ATCO Code')[
    'service_code'].nunique()
no_of_services_in_bods_per_la = no_of_services_in_bods_per_la.reset_index()

# get unique admin areas for dropdown
admin_areas = services_by_area_with_maintenance_df['Admin Area Name associated with ATCO Code'].drop_duplicates()

# see http://localhost:8050/
Local_Authority_App = dash.Dash(__name__)

Local_Authority_App.layout = html.Div(
    children=[
        # title and header
        html.Div(
            children=[
                html.H1(children="Local Authority dashboard", className="header-title"),
            ],
            className="header"  # top header
        ),
        # drop down menu
        html.Div(
            children=[
                html.P(
                    children="Select an area using the below dropdown:", className="header-desc",

                ),
                dcc.Dropdown(admin_areas, 'Greater Manchester', id='admin-area-filter', className="dropdown2"),
                html.Div(id='dd-output-container'),

            ],
            className="dropdown-box"
        ),
        # bar chart
        html.Div(
            children=dcc.Graph(
                id="services-chart",
                # id="services-chart", config={"displayModeBar": False},
            ),
            className="graph-box",
        ),
        # table of services
        dash_table.DataTable(id="table-chart", style_cell={'fontSize': 13, 'font-family': '"Lato", sans-serif'})
    ],
)


@Local_Authority_App.callback(
    [Output('services-chart', 'figure'), Output('table-chart', 'data')],
    Input('admin-area-filter', 'value')
)
def update_output(value):
    services_maintenance_count = maintenance_issues_by_la.loc[
        maintenance_issues_by_la['Admin Area Name associated with ATCO Code'] == value]
    services_in_bods_count = no_of_services_in_bods_per_la.loc[
        no_of_services_in_bods_per_la['Admin Area Name associated with ATCO Code'] == value]
    services_count = no_services_by_la.loc[no_services_by_la['Admin Area Name associated with ATCO Code'] == value]
    detailed_services = services_by_area_with_maintenance_df.loc[
        services_by_area_with_maintenance_df['Admin Area Name associated with ATCO Code'] == value]
    services_chart_figure = {
        "data": [
            {
                "x": services_count['Admin Area Name associated with ATCO Code'],
                "y": services_count['service_code'],
                "name": "all services",
                "type": "bar",
                "marker": {"color": "#135285"},
            },
            {
                "x": services_in_bods_count['Admin Area Name associated with ATCO Code'],
                "y": services_in_bods_count['service_code'],
                "name": "services in BODS",
                "type": "bar",
                "marker": {"color": "#1C74BC"},
            },
            {
                "x": services_maintenance_count['Admin Area Name associated with ATCO Code'],
                "y": services_maintenance_count['service_code'],
                "name": "services not updated",
                "type": "bar",
                "marker": {"color": "#8BC1ED"},
            },
        ],
        "layout": {"title": "Number of services per Local Authority"},
    }

    return services_chart_figure, detailed_services.to_dict('records')


if __name__ == "__main__":
    Local_Authority_App.run_server(debug=True)  # re-runs by itself