from extractor import TimetableExtractor
import pandas as pd
import datetime as dt
import numpy as np
import os

api = os.environ.get('bods_api_key')

my_bus_data_object = TimetableExtractor(api_key=api
                                        , status='published'
                                        , service_line_level=True
                                        , stop_level=False
                                        )

# This function looks to determine which operators are not maintaining their data.
# How has this been determined? We should have all service published from now until 42 days from now.
# For each service, get the most recently published dataset and check if the service end date is less than 42 days from now. If its less than 42 days
# from now they haven’t been maintaining their data.
# Limitation: We can only measure this where services have an end date

la_lookup = pd.read_csv('ATCO_code_to_LA_lookup.csv', dtype={'ATCO Code': str})

# Analytical timetable data includes Local Authority information -  this creates duplicates (each Service can have many Local Authorities)
analytical_timetable_data_with_duplicates = my_bus_data_object.analytical_timetable_data()

# analytical_timetable_data_with_duplicates.to_csv("check duplicates")

# Merging timetable data with Admin Area and ATCO code
timetable_la_merge = analytical_timetable_data_with_duplicates[
    ['ServiceCode', 'LineName', 'la_code', 'OperatorName', 'OperatingPeriodEndDate', 'RevisionNumber']].merge(
    la_lookup[['Admin Area Name associated with ATCO Code', 'ATCO Code']], how='left', right_on='ATCO Code',
    left_on='la_code').drop_duplicates()

# Remove any values where the end date of the service has not been specified
timetable_la_merge_with_end_date = timetable_la_merge[~timetable_la_merge['OperatingPeriodEndDate'].isna()]

# All operators should be publishing their data up until 42 days from now.
todays_date = pd.to_datetime('today').normalize()
expected_final_published_date = todays_date + pd.Timedelta(days=42)

# Convert end date to datetime
timetable_la_merge_with_end_date['converted end date to datetime'] = pd.to_datetime(
    timetable_la_merge_with_end_date['OperatingPeriodEndDate'])

# Check if the service end date is less than 42 days from now. If its less than 42 days, the operator is not maintaining their data
timetable_la_merge_with_end_date['not_maintained'] = timetable_la_merge_with_end_date[
                                                         'converted end date to datetime'] < expected_final_published_date

# Filter out rows where the data has been maintained
bods_la_merge_remove_compliant = timetable_la_merge_with_end_date[
    timetable_la_merge_with_end_date.not_maintained != False]

# Save data in file
bods_la_merge_remove_compliant.to_csv("Services_not_maintained.csv")