# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 12:37:43 2023

@author: -loper-irai
"""

from extractor import TimetableExtractor
import pandas as pd
import datetime as dt
import numpy as np
import os
from datetime import datetime, timedelta

api = os.environ.get('bods_api_key')

my_bus_data_object = TimetableExtractor(api_key=api
                                        , status='published'
                                        , service_line_level=True
                                        , stop_level=False
                                        )


analytical_timetable_data_without_duplicates = my_bus_data_object.analytical_timetable_data_analysis()

# get relevant fields
calendar_df = analytical_timetable_data_without_duplicates[['DatasetID', 'OperatorName', 'FileName', 'TradingName', 'ServiceCode', 'LineName', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate','RevisionNumber', 'OperatingDays']]

#calendar_df.to_csv('calendar_df_27_01_2023.csv')

#calendar_df = pd.read_csv('C:\\Users\\irai\\OneDrive - KPMG\\BODS\\BDE Package\\src\\BODSDataExtractor\\calendar_df_27_01_2023.csv')
# calendar_df = calendar_df[calendar_df['ServiceCode'] == 'PB0000328:006']
# calendar_df = calendar_df[calendar_df['LineName'] == '75']
# calendar_df = calendar_df[calendar_df['OperatingDays'] == 'Monday-Friday']

# get dates 42 days from current date
todays_date = pd.to_datetime('today').normalize()
expected_final_published_date = todays_date + pd.Timedelta(days=42)

delta = dt.timedelta(days=1)
while todays_date <= expected_final_published_date:
    calendar_df[todays_date] = None
    print(todays_date, end='\n')
    todays_date += delta

# convert data types for analysis
calendar_df['OperatingPeriodStartDate'] = pd.to_datetime(calendar_df['OperatingPeriodStartDate'])
calendar_df['OperatingPeriodEndDate'] = pd.to_datetime(calendar_df['OperatingPeriodEndDate'])
calendar_df.reset_index(inplace=True)

# put assign value 'true' where calendar date is between or equal to the operating period start and end date
for j in range(11, 54):
    for row in calendar_df.itertuples():
        print(calendar_df.columns[j])
        print(getattr(row, 'OperatingPeriodStartDate'))
        if(getattr(row, 'OperatingPeriodEndDate') is pd.NaT) or (getattr(row,'OperatingPeriodEndDate') is None):
            print(calendar_df.columns[j])
            print(getattr(row, 'OperatingPeriodStartDate'))
            if(calendar_df.columns[j] >= getattr(row, 'OperatingPeriodStartDate')):
                calendar_df[calendar_df.columns[j]][getattr(row, 'Index')] = "True"
        elif (calendar_df.columns[j] >= getattr(row, 'OperatingPeriodStartDate')) and \
                (calendar_df.columns[j] <= getattr(row, 'OperatingPeriodEndDate')):
            calendar_df[calendar_df.columns[j]][getattr(row, 'Index')] = "True"


calendar_df_sorted = calendar_df.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays']
                                             , ascending=True)
#calendar_df_sorted.to_csv('C:\\Users\\irai\\OneDrive - KPMG\\BODS\\BDE Package\\src\\BODSDataExtractor\\calendar_df_sorted_1.csv')

# collect all dates from column names 11 onwards
columns = []
for date in calendar_df.columns:
    columns.append(date)
dates = columns[11:]

operator_df = calendar_df_sorted.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays']
                                             , ascending=True)

# Create an empty report_df DataFrame to store the results
report_df = pd.DataFrame(columns=['ServiceCode', 'LineName', 'Look_ahead_missing_flag', 'Dates_for_missing_lookahead',
                                  'multiple_valid_files_issue_flag', 'Dates_for_multiple_valid_files', 'OperatingDays'])

operator_df = operator_df.reset_index()
# Group operator_df by ServiceCode, LineName, and OperatingDays
groups = operator_df.groupby(['ServiceCode', 'LineName', 'OperatingDays'])


# Iterate over the groups and process them
for name, group in groups:
    # Find the index of the max revision number
    max_revision = group['RevisionNumber'].astype('int').idxmax()

    Look_ahead_missing_flag = False
    IssueDates = []
    multiple_valid_files_issue_flag = False
    Dates_for_multiple_valid_files = []
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
                # for only 'True' rows, then set this value to 'False'
                if index < true_rows_max_revision_number_index:
                    operator_df.loc[index, col] = 'False'
                # if the revision number of the row is equal to the greatest revision number of the 'True' rows
                # then there are multiple valid files for the specified date
                if (operator_df.loc[index, 'RevisionNumber'] == str(true_rows_max_revision_number)) & \
                        (len(true_rows[true_rows['RevisionNumber'] == str(true_rows_max_revision_number)]) > 1):
                    multiple_valid_files_issue_flag = True
                    Dates_for_multiple_valid_files.append(col)
        # Check if all values in the column are False or NaN. If so then the look ahead is missing
        if (group[col].all() == False) or (group[col].isnull().all()):
            Look_ahead_missing_flag = True
            IssueDates.append(col)
    Dates_for_multiple_valid_files = np.unique(Dates_for_multiple_valid_files)
    report_df = report_df.append({'ServiceCode': group['ServiceCode'].iloc[0], 'LineName': group['LineName'].iloc[0],
                                  'Look_ahead_missing_flag': Look_ahead_missing_flag,
                                  'Dates_for_missing_lookahead': IssueDates,
                                  'multiple_valid_files_issue_flag': multiple_valid_files_issue_flag,
                                  'Dates_for_multiple_valid_files': Dates_for_multiple_valid_files,
                                  'OperatingDays': group['OperatingDays'].iloc[0]},
                                   ignore_index=True)



# Edit below paths
operator_df.to_csv('C:\\Users\\irai\\OneDrive - KPMG\\BODS\\BDE Package\\src\\BODSDataExtractor\\operator_df1.csv')
consumer_df = operator_df.dropna(subset=dates, how='all') # do we want to drop n/a values for the consumer??
consumer_df.to_csv('C:\\Users\\irai\\OneDrive - KPMG\\BODS\\BDE Package\\src\\BODSDataExtractor\\consumer_df1.csv')
report_df.to_csv('C:\\Users\\irai\\OneDrive - KPMG\\BODS\\BDE Package\\src\\BODSDataExtractor\\report_df1.csv')


# consumer to query
# def getValidFile(date, serviceCode, lineName, operatingDays):
#     date = pd.to_datetime(date)
#     try:
#         position = consumer_df.loc[((consumer_df['ServiceCode'] == serviceCode) & (consumer_df['LineName'] == lineName) &
#                      (consumer_df['OperatingDays'] == operatingDays)), date]
#         if position.iloc[0] == 'True':
#             result = consumer_df.loc[position.index, 'FileName']
#         else:
#             result = 'No valid file'
#         return result
#     except IndexError:
#         raise IndexError("No record found for provided input.")
#
#
# result = getValidFile('27/01/2023  00:00:00', 'PB0000328:001', '56', 'Monday-Friday')
# print(result)