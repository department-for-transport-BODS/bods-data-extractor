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
                               #   , limit= 1
                                 ,status = 'published' # IS THIS PUBLISHED AND REGISTERED?
                                ,nocs=['NADS', 'RBUS']
                                 ,service_line_level=True
                                 ,stop_level=False
                                 )


analytical_timetable_data_without_duplicates = my_bus_data_object.analytical_timetable_data_analysis()

# filter for dataset id and service code
analytical_timetable_data_without_duplicates_filtered = analytical_timetable_data_without_duplicates
#2139
#analytical_timetable_data_without_duplicates_filtered_2 = analytical_timetable_data_without_duplicates[analytical_timetable_data_without_duplicates['DatasetID'] == 10168]
# #analytical_timetable_data_without_duplicates_filtered = analytical_timetable_data_without_duplicates[analytical_timetable_data_without_duplicates['ServiceCode'] == 'PB0000328:001']
#analytical_timetable_data_without_duplicates_filtered = analytical_timetable_data_without_duplicates[analytical_timetable_data_without_duplicates['ServiceCode'] == 'PD0000480:11']
#analytical_timetable_data_without_duplicates_filtered = analytical_timetable_data_without_duplicates[analytical_timetable_data_without_duplicates['ServiceCode'] == 'PH0005856:10']


# get relevant fields
calendar_df = analytical_timetable_data_without_duplicates_filtered[['DatasetID', 'OperatorName', 'FileName', 'TradingName', 'ServiceCode', 'LineName', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate','RevisionNumber', 'OperatingDays']]

calendar_df.to_csv('calendar_df.csv')

#calendar_df = pd.read_csv('calendar_df.csv')
calendar_df = calendar_df[calendar_df['ServiceCode'] == 'PB0000815:24']


# get days 42 days from now
todays_date = pd.to_datetime('today').normalize()
expected_final_published_date = todays_date + pd.Timedelta(days=42)

delta = dt.timedelta(days=1)
while (todays_date <= expected_final_published_date):
    calendar_df[todays_date] = None
    print(todays_date, end='\n')
    todays_date += delta

# convert data types for analysis
calendar_df['OperatingPeriodStartDate'] = pd.to_datetime(calendar_df['OperatingPeriodStartDate'])
calendar_df['OperatingPeriodEndDate'] = pd.to_datetime(calendar_df['OperatingPeriodEndDate'])
calendar_df.reset_index(inplace= True)

# put true where calendar date is between or equal to the operating period start and end date
for j in range(11,54):
    for row in calendar_df.itertuples():
        print(calendar_df.columns[j])
        print(getattr(row,'OperatingPeriodStartDate'))

        if((getattr(row,'OperatingPeriodEndDate') is pd.NaT) or (getattr(row,'OperatingPeriodEndDate') == None)):
            print(calendar_df.columns[j])
            print(getattr(row, 'OperatingPeriodStartDate'))
            if(calendar_df.columns[j] >= getattr(row, 'OperatingPeriodStartDate')):
                calendar_df[calendar_df.columns[j]][getattr(row, 'Index')] = "True"
        elif ((calendar_df.columns[j] >= getattr(row,'OperatingPeriodStartDate')) and (calendar_df.columns[j] <= getattr(row,'OperatingPeriodEndDate'))):
            calendar_df[calendar_df.columns[j]][getattr(row, 'Index')] = "True"

calendar_df_sorted = calendar_df.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays'], ascending=True)
calendar_df_sorted.to_csv('calendar_df_sorted TAKE2.csv')
# select columns to remove n/a values where all columns are n/a
columns = []
for col in calendar_df.columns:
    columns.append(col)
print(columns)
df_columns = columns[11:]

# remove rows where calendar days are all n/a
#new_df_two = calendar_df_sorted.dropna(subset=df_columns, how='all')
operator_df = calendar_df_sorted.sort_values(by=['ServiceCode', 'LineName', 'RevisionNumber', 'OperatingDays'], ascending=True)

#=============================================================

#calendar_df_sorted = calendar_df_sorted.drop(columns, axis=1) # remove columns from calendar_df_sorted where all values are 'True'
# Create an empty report_df DataFrame to store the results
report_df = pd.DataFrame(columns=['ServiceCode', 'LineName', 'Look_ahead_missing_flag', 'Dates_for_missing_lookahead',
                                  'multiple_valid_files_issue_flag', 'Dates_for_multiple_valid_files'])

# Group operator_df by ServiceCode, LineName, and OperatingDays
groups = operator_df.groupby(['ServiceCode', 'LineName', 'OperatingDays'])

# Iterate over the groups and process them
for name, group in groups:
    # Find the index of the max revision number
    max_revision = group['RevisionNumber'].astype('int').idxmax()

    # Initialize flags and lists for storing results
    Look_ahead_missing_flag = False
    IssueDates = []
    multiple_valid_files_issue_flag = False
    Dates_for_multiple_valid_files = []

    # Iterate over the columns in df_columns
    for col in df_columns:
        # Check if any value in the column is True and the group has more than one row
        num_true = (group[col] == 'True').sum()
        print("TEST 1")
        print(group[col] == 'True')
        if (group[col].any() == True) & (len(group) > 1) & (num_true>1):
            # Iterate over the indices in the group
            for index in group.index:
                # If the index is less than the max revision, set the value in the operator_df to 'False'
                # ONLY MAX REVISION NUMBER OF THE TRUE ROWS!!!!!!!!!!
                group[col].all() == 'True'
                if index < max_revision:
                    operator_df.loc[index, col] = 'False'
                # If the index is equal to the max revision, set the flag for multiple valid files and append the date to the list
               #num_true = (group[col] == 'True').sum()
                #print("hhhh")
               # print(group[col] == 'True')
                # IF THERE IS MORE THAN ONE TRUE VALUE IN THE GROUP
                if (index == max_revision) & (num_true>1):
                    multiple_valid_files_issue_flag = True
                    Dates_for_multiple_valid_files.append(col)
        # Check if all values in the column are False or NaN
        if (group[col].all() == False) or (group[col].isnull().all()):
            Look_ahead_missing_flag = True
            IssueDates.append(col)
    # Append the results to the report_df DataFrame
    report_df = report_df.append({'ServiceCode': group['ServiceCode'].iloc[0], 'LineName': group['LineName'].iloc[0],
                                  'Look_ahead_missing_flag': Look_ahead_missing_flag,
                                  'Dates_for_missing_lookahead': IssueDates,
                                  'multiple_valid_files_issue_flag': multiple_valid_files_issue_flag,
                                  'Dates_for_multiple_valid_files': Dates_for_multiple_valid_files},
                                 ignore_index=True)

# UNCOMMENT THESE
#operator_df.to_csv('operator_df.csv')
#report_df.to_csv('report_df.csv')


# PB0000815:24, 7S
# include operating profile in output
#consumer_df = operator_df.dropna(subset=df_columns, how='all') # do we want to drop n/a values for the consumer??
#operator_df.to_csv('consumer_df.csv')



# EFFICIENCY ISSUE HERE - report_df not running for whole dataset
# report_df = pd.DataFrame()
# Look_ahead_missing_flag = False
# IssueDates = []
# multiple_valid_files_issue_flag = False
# Dates_for_multiple_valid_files = []
# groups = operator_df.groupby(['ServiceCode', 'LineName', 'OperatingDays'])
# for name, group in groups:
#     max_revision = group['RevisionNumber'].astype('int').idxmax()
#     for col in df_columns:
#         if (group[col].any() == True) & (len(group) > 1):
#             for index in group.index:
#                 if index < max_revision:
#                     operator_df.loc[index, col] = 'False'
#                 elif index == max_revision:
#                     multiple_valid_files_issue_flag = True
#                     Dates_for_multiple_valid_files.append(col)
#         if (group[col].all() == False) or (group[col].isnull().all()):
#             Look_ahead_missing_flag = True
#             IssueDates.append(col)
#     report_df = report_df.append({'ServiceCode': group['ServiceCode'].iloc[0], 'LineName': group['LineName'].iloc[0],
#                                    'Look_ahead_missing_flag': Look_ahead_missing_flag,
#                                     'Dates_for_missing_lookahead': IssueDates,
#                                     'multiple_valid_files_issue_flag': multiple_valid_files_issue_flag,
#                                     'Dates_for_multiple_valid_files': Dates_for_multiple_valid_files},
#                                  ignore_index=True)
#












# group by service code, line name and operating days. If there is more than one row for a particular date, make the one
# with the lowest revision number false

# columns = []
# df_columns = analyse_df.columns[11:] # assign columns starting from the 11th index to df_columns
# for col in df_columns: # iterate through columns in df_columns
#     print(analyse_df[col])
#     print(type(analyse_df[col]))
#     if (analyse_df[col].all() == True):  # Output: True
#     #if all(analyse_df[col] == True): # check if all values in the column are equal to 'True'
#         columns.append(col) # append column to columns list if all values are 'True'

# lookahead = True
# for col in df_columns:
#     groups = analyse_df.groupby(['ServiceCode', 'LineName', 'OperatingDays'])
#     for name, group in groups:
#         print(group[col])
#         print(type(group[col]))
#         if (group[col].all() == True):
#             print(group[col])
#             print("lalalla")
#             print(type(group['RevisionNumber'].min()))
#             analyse_df.loc[group['RevisionNumber'].astype('int').idxmin(), col] = 'False'
#         if (group[col].all() == False):
#             lookahead = False
#


# for name, group in grouped:
#     if all(group[df_columns].eq('False').all(axis=1)):
#             response = "missing valid data for 42 day look-ahead for "
#     response = "look ahead provided"
#     if len(group) > 1:
#         if all(group[df_columns].eq('True').all(axis=1)):
#             analyse_df.loc[group['RevisionNumber'].astype('int').idxmin(), df_columns] = 'False'
#             row = analyse_df.loc[group['RevisionNumber'].astype('int').idxmin(), df_columns] = 'False'
#             df = pd.DataFrame({'ServiceCode': row['ServiceCode'],
#                            'LineName': row['LineName'],
#                            'RevisionNumber': row['RevisionNumber'],
#                            'OperatingDays': row['OperatingDays'],
#                            'lookup?': response})

# grouped_df = analyse_df.groupby(['ServiceCode', 'LineName', 'OperatingDays']).size().reset_index(name='counts')
# grouped_df = grouped_df[grouped_df['counts'] > 1]
# grouped_df = grouped_df.merge(calendar_df, on=['ServiceCode', 'LineName', 'OperatingDays'])
#
# for group, data in grouped_df.groupby(['ServiceCode', 'LineName', 'OperatingDays']):
#     true_count = data[data.iloc[:,11:54] == 'True'].shape[0]
#     if true_count == data.shape[0]:
#         min_rev_num = data['RevisionNumber'].min()
#         analyse_df.loc[(analyse_df['ServiceCode'] == group[0]) &
#                         (analyse_df['LineName'] == group[1]) &
#                         (analyse_df['OperatingDays'] == group[2]) &
#                         (analyse_df['RevisionNumber'] == min_rev_num), 'RevisionNumber'] = 'False'