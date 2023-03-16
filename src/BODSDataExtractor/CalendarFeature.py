# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 12:37:43 2023

@author: -loper-irai
"""

# Versioning
# version: 1.3

from extractor import TimetableExtractor
import pandas as pd
import datetime as dt
import numpy as np
import os
from datetime import datetime, timedelta

# edit this with api key
api = os.environ.get('bods_api_key')

my_bus_data_object = TimetableExtractor(api_key=api
                                        , status='published'
                                        , service_line_level=True
                                        , stop_level=False
                                        )

# Returns a copy of the service line level data suitable for analysis and filters columns
analytical_timetable_data_without_duplicates = my_bus_data_object.analytical_timetable_data_analysis()
timetable_df = analytical_timetable_data_without_duplicates[['DatasetID', 'OperatorName', 'FileName', 'TradingName',
                                                             'ServiceCode', 'LineName', 'OperatingPeriodStartDate',
                                                             'OperatingPeriodEndDate', 'RevisionNumber',
                                                             'OperatingDays']]


# for testing
# calendar_df.to_csv('calendar_df_original_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def produce_calendar_structure(dataframe):
    """
    This function adds columns to the end of the dataframe. Each column is assigned a date starting from today's date
    up to 42 days from now. It then assigns the value true for the dates within the operating period.

    Parameters:
    dataframe (df): dataframe of timetable data

    Returns:
    calendar_df (df): dataframe with calendar dates added with true values indicating the operating period dates.
    """
    current_date = pd.to_datetime('today').normalize()
    expected_final_published_date = current_date + pd.Timedelta(days=42)

    calendar_df = dataframe
    delta = dt.timedelta(days=1)
    while current_date <= expected_final_published_date:
        calendar_df[current_date] = None
        print(current_date, end='\n')
        current_date += delta

    calendar_df['OperatingPeriodStartDate'] = pd.to_datetime(calendar_df['OperatingPeriodStartDate'])
    calendar_df['OperatingPeriodEndDate'] = pd.to_datetime(calendar_df['OperatingPeriodEndDate'])
    calendar_df.reset_index(inplace=True)

    # update this to make it generic!
    for calendar_date in range(11, 54):
        for row in calendar_df.itertuples():
            print(calendar_df.columns[calendar_date])
            print(getattr(row, 'OperatingPeriodStartDate'))
            if (getattr(row, 'OperatingPeriodEndDate') is pd.NaT) or (getattr(row, 'OperatingPeriodEndDate') is None):
                print(calendar_df.columns[calendar_date])
                print(getattr(row, 'OperatingPeriodStartDate'))
                if calendar_df.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate'):
                    calendar_df[calendar_df.columns[calendar_date]][getattr(row, 'Index')] = "True"
            elif (calendar_df.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate')) and \
                    (calendar_df.columns[calendar_date] <= getattr(row, 'OperatingPeriodEndDate')):
                calendar_df[calendar_df.columns[calendar_date]][getattr(row, 'Index')] = "True"
    return calendar_df


# Adding Calendar dates
calendar_df_structure = produce_calendar_structure(timetable_df)


# for testing
# calendar_df_structure.to_csv('calendar_df_structure_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def convert_dates(dataframe):
    """
    This function substitutes the operating period start date to include all dates
    E.g. Monday-Friday -> Monday, Tuesday, Wednesday, Thursday, Friday

    Parameters:
        dataframe (df): dataframe with operating period column

    Returns:
        refactored_dataframe (df): dataframe with operating period refactored.
    """
    dataframe = dataframe.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays']
                                      , ascending=True)

    refactored_dataframe = dataframe
    refactored_dataframe = refactored_dataframe.reset_index()

    # Converts dates with dashes e.g. Monday-Friday to Monday, Tuesday, Wednesday, Thursday, Friday to enable comparison
    for operatingDay in refactored_dataframe['OperatingDays']:
        if "-" in operatingDay:
            first_date = operatingDay[0:operatingDay.find('-')]
            second_date = operatingDay[operatingDay.find('-') + 1:]
            days = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}
            new_output = []
            for i in range(days[first_date], days[second_date] + 1):
                new_output.append(list(days.keys())[list(days.values()).index(i)])
            new_output = ",".join(new_output)
            refactored_dataframe.replace(operatingDay, new_output, inplace=True)
    return refactored_dataframe


# Converting format of dates
calendar_df_refactored = convert_dates(calendar_df_structure)


# for testing
# calendar_df_refactored.to_csv('calendar_df_refactored_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def check_operating_days(dataframe):
    """
    This function creates a new column to group rows where the operating days for a service-line combo is a subset of
    an equal or higher revision number file's operating days.

    Logic: It iterates through each row in the group (current_index) and for each row it iterates through the next rows
    (next_index). It checks if all the operating days for the current_index are in the next_index's operating days.
    If they are a subset, it assigns both rows the same counter in the 'DaysGroup' column. This will form the new group.

    Parameters:
        dataframe (df): dataframe with service, line and operating days

    Returns:
        dataframe_with_grouping (df): dataframe with new column called 'DaysGroup' which tells us the groups for
        checking which file is valid for a specific date.
    """
    dataframe_with_grouping = dataframe
    dataframe_with_grouping['DaysGroup'] = ""
    dataframe_with_grouping.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber'])
    groups = dataframe_with_grouping.groupby(['ServiceCode', 'LineName'])

    counter = 0
    for name, group in groups:
        for current_index in group.index:
            counter += 1
            if group.RevisionNumber.nunique() == 1:
                calculated_next_index = group.iloc[0].name
            else:
                calculated_next_index = current_index + 1
            if calculated_next_index in group.index:
                last_index = group.iloc[-1].name
                for next_index in range(calculated_next_index, last_index + 1):
                    if next_index in group.index:
                        operating_days_in_current_index = group.loc[current_index, 'OperatingDays']
                        operating_days_next_index = group.loc[next_index, 'OperatingDays']
                        operating_days_in_current_index = operating_days_in_current_index.split(",")
                        operating_days_next_index = operating_days_next_index.split(",")
                        if all([item in operating_days_next_index for item in operating_days_in_current_index]):
                            if (dataframe_with_grouping.loc[next_index, 'DaysGroup'] == "") & (
                                    dataframe_with_grouping.loc[current_index, 'DaysGroup'] == ""):
                                dataframe_with_grouping.at[current_index, 'DaysGroup'] = counter
                                dataframe_with_grouping.at[next_index, 'DaysGroup'] = counter
                            elif (dataframe_with_grouping.loc[next_index, 'DaysGroup'] == "") & (
                                    dataframe_with_grouping.loc[current_index, 'DaysGroup'] != ""):
                                dataframe_with_grouping.at[next_index, 'DaysGroup'] = \
                                    dataframe_with_grouping.loc[current_index, 'DaysGroup']
                            else:
                                dataframe_with_grouping.at[current_index, 'DaysGroup'] = \
                                    dataframe_with_grouping.loc[next_index, 'DaysGroup']
                                group.at[current_index, 'DaysGroup'] = group.loc[next_index, 'DaysGroup']
            if dataframe_with_grouping.loc[current_index, 'DaysGroup'] == "":
                dataframe_with_grouping.loc[current_index, 'DaysGroup'] = counter
    return dataframe_with_grouping


# Determine groups for validity check
calendar_df_grouped = check_operating_days(calendar_df_refactored)
# for testing
# calendar_df_grouped.to_csv('calendar_df_grouped_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))

# Gets a list of all dates from calendar_df
columns = []
for date in timetable_df.columns:
    columns.append(date)
dates = columns[11:]


def determine_file_validity(dataframe):
    """
    This function groups by the ServiceCode, LineName and DaysGroups and iterates through each date column to check
    if the files are valid. If they are valid the row will be given a value of True. If they are not valid they will
    be given no value (empty).

    Parameters:
        dataframe (df): dataframe with calendar dates (including service code, line name and days group)
        It relies on the 'check_operating_days' function.

    Returns:
        operator_df (df): dataframe with values in calendar_df updated with empty values where a file is not valid
        for a specific date
        report_df (df): summarised view which identifies for each service-line-operating days
        combination, if there is a missing 42-day lookahead or if there are multiple valid files on a specific date.
    """
    report_df = pd.DataFrame(
        columns=['LicenseNumber', 'ServiceCode', 'LineName', 'Look_ahead_missing_flag', 'Dates_for_missing_lookahead',
                 'multiple_valid_files_issue_flag', 'Dates_for_multiple_valid_files', 'OperatingDays', 'DatasetID'])

    operator_df = dataframe
    groups = operator_df.groupby(['ServiceCode', 'LineName', 'DaysGroup'])
    for name, group in groups:
        # Find the index of the max revision number
        max_revision = group['RevisionNumber'].astype('int').idxmax()

        look_ahead_missing_flag = False
        issue_dates = []
        multiple_valid_files_issue_flag = False
        dates_for_multiple_valid_files = []
        # Iterate over the column dates
        for col in dates:
            # Get the number of true values in the group
            num_true = (group[col] == 'True').sum()
            # Check if any value in the column is True, the group has more than one row and the number of true rows
            # is greater than one
            if (group[col].any() == True) & (len(group) > 1) & (num_true > 1):
                # get the rows where the values are equal to 'True'
                true_rows = group[group[col] == 'True']
                # iterate through each row which has a 'True' value
                for index in true_rows.index:
                    true_rows = group[group[col] == 'True']
                    true_rows_max_revision_number_index = true_rows['RevisionNumber'].astype('int').idxmax()
                    true_rows_max_revision_number = true_rows['RevisionNumber'].astype('int').max()
                    # if the index of the row is less than the index of the row with the greater revision number
                    # for only 'True' rows, then set this value to empty
                    if index < true_rows_max_revision_number_index:
                        operator_df.loc[index, col] = ''
                    # if the revision number of the row is equal to the greatest revision number of the 'True' rows
                    # then there are multiple valid files for the specified date
                    if (operator_df.loc[index, 'RevisionNumber'] == str(true_rows_max_revision_number)) & \
                            (len(true_rows[true_rows['RevisionNumber'] == str(true_rows_max_revision_number)]) > 1):
                        multiple_valid_files_issue_flag = True
                        dates_for_multiple_valid_files.append(col)
            # Check if all values in the column are False or NaN. If so then the look ahead is missing
            if (group[col].all() == False) or (group[col].isnull().all()):
                look_ahead_missing_flag = True
                issue_dates.append(col)
        dates_for_multiple_valid_files = np.unique(dates_for_multiple_valid_files)
        datasets = []
        for x in group['DatasetID']:
            datasets.append(x)
        datasets = np.unique(datasets)  # causing no commas
        temp_df = pd.DataFrame(
            {'LicenseNumber': [str(group['ServiceCode'].iloc[0])[0:str(group['ServiceCode'].iloc[0]).find(':')]],
             'ServiceCode': [group['ServiceCode'].iloc[0]], 'LineName': [group['LineName'].iloc[0]],
             'Look_ahead_missing_flag': [look_ahead_missing_flag],
             'Dates_for_missing_lookahead': [issue_dates],
             'multiple_valid_files_issue_flag': [multiple_valid_files_issue_flag],
             'Dates_for_multiple_valid_files': [dates_for_multiple_valid_files],
             'OperatingDays': [group['OperatingDays'].iloc[-1]],
             'DatasetID': [datasets]})
        report_df = pd.concat([report_df, temp_df], ignore_index=True)
    return operator_df, report_df


# Determine which file is valid on a specific date
calendar_operator_df, calendar_report_df = determine_file_validity(calendar_df_grouped)
calendar_operator_df.to_csv('operator_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))
consumer_df = calendar_operator_df.dropna(subset=dates, how='all')
consumer_df.to_csv('consumer_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))
calendar_report_df.to_csv('calendar_report_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


# consumer to query
def get_valid_file(date_for_file, service_code, line_name, operating_days):
    """
    This function allows a consumer to query a date, service code, line name and operating days and receive the
    corresponding valid file if there is one. Otherwise, it will return 'No valid file' or an error if there is a
    problem with the input.

    Parameters:
        date_for_file: the date you want to find the valid file for
        service_code: Service Code
        line_name: Line Name
        operating_days: Operating Days

    Returns:
        response: corresponding valid file(s) or 'No valid file' or error if input is incorrect.
    """
    date_for_file = pd.to_datetime(date_for_file)
    try:
        rows_to_check = consumer_df.loc[((consumer_df['ServiceCode'] == service_code) &
                                         (consumer_df['LineName'] == line_name)
                                         & (consumer_df['OperatingDays'] == operating_days))]
        new_rows_to_check = consumer_df.loc[((consumer_df['ServiceCode'] == service_code) &
                                             (consumer_df['LineName'] == line_name)
                                             & (consumer_df['DaysGroup'] == rows_to_check['DaysGroup'].iloc[0]))]
        result_df = new_rows_to_check.loc[new_rows_to_check[date_for_file] == 'True']
        if result_df.empty:
            response = 'No valid file'
        else:
            response = result_df['FileName']
        return response
    except IndexError:
        raise IndexError("No record found for provided input.")


# update below inputs
result = get_valid_file('16/03/2023  00:00:00', 'PB0000815:112', '541',
                        'Tuesday,Wednesday,Thursday,Friday,Saturday')
print(result)
