# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 12:37:43 2023

@author: -loper-irai
"""

# Versioning
# version: 1.1

from extractor import TimetableExtractor
import pandas as pd
import datetime as dt
import numpy as np
import os
from datetime import datetime, timedelta

# edit this
api = os.environ.get('bods_api_key')

my_bus_data_object = TimetableExtractor(api_key=api
                                        , status='published'
                                        , service_line_level=True
                                        , stop_level=False
                                        )

# Returns a copy of the service line level data suitable for analysis and filters columns
analytical_timetable_data_without_duplicates = my_bus_data_object.analytical_timetable_data_analysis()
calendar_df = analytical_timetable_data_without_duplicates[['DatasetID', 'OperatorName', 'FileName', 'TradingName',
                                                            'ServiceCode', 'LineName', 'OperatingPeriodStartDate',
                                                            'OperatingPeriodEndDate', 'RevisionNumber',
                                                            'OperatingDays']]

calendar_df.to_csv('calendar_df_original_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def produce_calendar_structure(dataframe):
    """
    This function adds columns to the end of the dataframe. Each column is assigned a date going from today's date to
    42 days from now. It then assigns true for the operating period.

    Parameters:
    dataframe (df): dataframe of timetable data

    Returns:
    dataframe: dataframe with calendar dates added with true values to show the operating period.
    """
    current_date = pd.to_datetime('today').normalize()
    expected_final_published_date = current_date + pd.Timedelta(days=42)

    # Add the dates to the calendar_df and assign all values as empty
    delta = dt.timedelta(days=1)
    while current_date <= expected_final_published_date:
        dataframe[current_date] = None
        print(current_date, end='\n')
        current_date += delta

    # Convert data types for analysis
    dataframe['OperatingPeriodStartDate'] = pd.to_datetime(dataframe['OperatingPeriodStartDate'])
    dataframe['OperatingPeriodEndDate'] = pd.to_datetime(dataframe['OperatingPeriodEndDate'])
    dataframe.reset_index(inplace=True)

    # Iterate through each calendar date column. For each date iterate through each row and assign value 'true'
    # where calendar date is between or equal to the operating period start and end date
    for calendar_date in range(11, 54):
        for row in dataframe.itertuples():
            print(dataframe.columns[calendar_date])
            print(getattr(row, 'OperatingPeriodStartDate'))
            if (getattr(row, 'OperatingPeriodEndDate') is pd.NaT) or (getattr(row, 'OperatingPeriodEndDate') is None):
                print(dataframe.columns[calendar_date])
                print(getattr(row, 'OperatingPeriodStartDate'))
                if dataframe.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate'):
                    dataframe[dataframe.columns[calendar_date]][getattr(row, 'Index')] = "True"
            elif (dataframe.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate')) and \
                    (dataframe.columns[calendar_date] <= getattr(row, 'OperatingPeriodEndDate')):
                dataframe[dataframe.columns[calendar_date]][getattr(row, 'Index')] = "True"
    return dataframe


# Adding Calendar dates
calendar_df_structure = produce_calendar_structure(calendar_df)
calendar_df_structure.to_csv('calendar_df_structure_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def convert_dates(dataframe):
    """
    This function substitutes the operating period start date, where it has been summarised to include all dates
    E.g. Monday-Friday -> Monday, Tuesday, Wednesday, Thursday, Friday

    Parameters:
    dataframe (df): dataframe with operating period column

    Returns:
    dataframe: dataframe with operating period refactored.
    """
    dataframe = dataframe.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays']
                                        , ascending=True)

    dataframe = dataframe.reset_index()

    # Converts dates with dashes e.g. Monday-Friday to Monday, Tuesday, Wednesday, Thursday, Friday to enable comparison
    for operatingDay in dataframe['OperatingDays']:
        if "-" in operatingDay:
            first_date = operatingDay[0:operatingDay.find('-')]
            second_date = operatingDay[operatingDay.find('-') + 1:]
            days = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}
            new_output = []
            for i in range(days[first_date], days[second_date] + 1):
                new_output.append(list(days.keys())[list(days.values()).index(i)])
            new_output = ",".join(new_output)
            dataframe.replace(operatingDay, new_output, inplace=True)
    return dataframe


# Converting format of dates
calendar_df_refactored = convert_dates(calendar_df_structure)
calendar_df_refactored.to_csv('calendar_df_refactored_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))


def check_operating_days(dataframe):
    # Create new column to group rows where the operating period for a service-line combo is a subset of an equal/higher
    # revision number file
    dataframe['DaysGroup'] = ""
    dataframe.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber'])
    groups = dataframe.groupby(['ServiceCode', 'LineName'])

    # Iterate through each row in the group (current_index) and for each row iterate through the next rows (next_index).
    # Check if all the operating days for the current_index are in the next_index's operating days. If they are a subset
    # assign both rows the same counter in the 'DaysGroup' column
    counter = 0
    for name, group in groups:
        for current_index in group.index:
            counter += 1
            # check if the revision numbers are all the same. If they are the same then 'next_index' goes through all
            # the rows. If they are not the same then only go through the rows with a higher revision number.
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
                            if (dataframe.loc[next_index, 'DaysGroup'] == "") & (
                                    dataframe.loc[current_index, 'DaysGroup'] == ""):
                                dataframe.at[current_index, 'DaysGroup'] = counter
                                dataframe.at[next_index, 'DaysGroup'] = counter
                                group.at[current_index, 'DaysGroup'] = counter
                                group.at[next_index, 'DaysGroup'] = counter
                            elif (dataframe.loc[next_index, 'DaysGroup'] == "") & (
                                    dataframe.loc[current_index, 'DaysGroup'] != ""):
                                dataframe.at[next_index, 'DaysGroup'] = dataframe.loc[current_index, 'DaysGroup']
                                group.at[next_index, 'DaysGroup'] = group.loc[current_index, 'DaysGroup']
                            else:
                                dataframe.at[current_index, 'DaysGroup'] = dataframe.loc[next_index, 'DaysGroup']
                                group.at[current_index, 'DaysGroup'] = group.loc[next_index, 'DaysGroup']
            if dataframe.loc[current_index, 'DaysGroup'] == "":
                dataframe.loc[current_index, 'DaysGroup'] = counter
                group.loc[current_index, 'DaysGroup'] = counter
    return dataframe


# Determine groups for validity check
calendar_df_grouped = check_operating_days(calendar_df_refactored)
calendar_df_grouped.to_csv('calendar_df_grouped_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))

# Gets a list of all dates from calendar_df
columns = []
for date in calendar_df.columns:
    columns.append(date)
dates = columns[11:]


def determine_file_validity(dataframe):
    # Create an empty report_df DataFrame to store the results
    report_df = pd.DataFrame(
        columns=['LicenseNumber', 'ServiceCode', 'LineName', 'Look_ahead_missing_flag', 'Dates_for_missing_lookahead',
                 'multiple_valid_files_issue_flag', 'Dates_for_multiple_valid_files', 'OperatingDays', 'DatasetID'])

    # Group operator_df by ServiceCode, LineName and DaysGroup to determine which file is valid for each date.
    groups = dataframe.groupby(['ServiceCode', 'LineName', 'DaysGroup'])
    # Iterate over the groups, iterate through each date column and determine which file(s) are valid. If not valid,
    # assign column date value to null
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
                        dataframe.loc[index, col] = ''
                    # if the revision number of the row is equal to the greatest revision number of the 'True' rows
                    # then there are multiple valid files for the specified date
                    if (dataframe.loc[index, 'RevisionNumber'] == str(true_rows_max_revision_number)) & \
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
    return dataframe, report_df


# Determine which file is valid on a specific date
operator_df, calendar_report_df = determine_file_validity(calendar_df_grouped)
operator_df.to_csv('operator_df_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))
consumer_df = operator_df.dropna(subset=dates, how='all')
consumer_df.to_csv('consumer_df_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))
calendar_report_df.to_csv('calendar_report_df_{}.csv'.format(pd.to_datetime('today').strftime("%Y-%m-%d %Hh%Mm%Ss")))

# consumer to query
# def getValidFile(date, serviceCode, lineName, operatingDays): date = pd.to_datetime(date) try:
# position = consumer_df.loc[((consumer_df['ServiceCode'] == serviceCode) & (consumer_df['LineName'] == lineName) & (
# consumer_df['OperatingDays'] == operatingDays)), date] if position.iloc[0] == 'True': result = consumer_df.loc[
# position.index, 'FileName'] else: result = 'No valid file' return result except IndexError: raise IndexError("No
# record found for provided input.")
#
#
# result = getValidFile('13/02/2023  00:00:00', 'PB0001746:1', '3', 'Monday-Sunday')
# print(result)