import pandas as pd
import datetime as dt
import numpy as np
import os
from datetime import datetime, timedelta
import helpers

def create_calendar_dataframe(dataframe, days_lookahead: int = 0):

    df_with_date_columns = helpers.append_date_columns_to_dataframe(dataframe, days_lookahead)
    calendar_df = helpers.assign_timetable_file_validity_for_each_date(df_with_date_columns, days_lookahead)

    return calendar_df

def refactor_operating_period(dataframe):

    refactored_dataframe = helpers.convert_dates(dataframe)

    return refactored_dataframe

def add_days_group_column(dataframe):

    dataframe_with_grouping = helpers.check_operating_days(dataframe)

    return dataframe_with_grouping

def add_file_validity_for_each_date(dataframe, dates):

    operator_df, report_df = helpers.determine_file_validity(dataframe,dates)

    return operator_df, report_df

def remove_invalid_files(dataframe):

    indexes_to_delete = helpers.collect_index_numbers_to_delete(dataframe)

    df_with_invalid_files_removed = helpers.remove_invalid_files(dataframe,indexes_to_delete)

    return df_with_invalid_files_removed

if __name__ == '__main__':
    from extractor import TimetableExtractor

    # edit this with api key
    api = os.environ.get('bods_api_key')

    my_bus_data_object = TimetableExtractor(api_key=api
                                            , status='published'
                                            , service_line_level=True
                                            , stop_level=False
                                            , nocs=['YEOC']
                                            )

    # Returns a copy of the service line level data suitable for analysis and filters columns
    # analytical_timetable_data_without_duplicates = my_bus_data_object.analytical_timetable_data_analysis()
    my_bus_data_object.analytical_timetable_data_analysis()
    analytical_timetable_data_without_duplicates = my_bus_data_object.service_line_extract
    timetable_df = analytical_timetable_data_without_duplicates[['DatasetID', 'OperatorName', 'FileName', 'TradingName',
                                                                'ServiceCode', 'LineName', 'OperatingPeriodStartDate',
                                                                'OperatingPeriodEndDate', 'RevisionNumber',
                                                                'OperatingDays']]

    calendar_dataframe = create_calendar_dataframe(timetable_df,0)

    calendar_dataframe_refactored = refactor_operating_period(calendar_dataframe)

    calendar_with_days_group = add_days_group_column(calendar_dataframe_refactored)

    columns = []
    for date in timetable_df.columns:
        columns.append(date)
    dates = columns[11:]

    operator_df, report_df = add_file_validity_for_each_date(calendar_with_days_group,dates)
    
    # For testing - remember to remove
    #operator_df.to_csv('operator_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))

    # amended_operator_df = operator_df.dropna(subset=dates, how='all')

    # for testing remember to remove
    #amended_operator_df.to_csv('amended_operator_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))



