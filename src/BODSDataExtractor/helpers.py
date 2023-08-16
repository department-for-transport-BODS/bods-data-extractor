import pandas as pd
import datetime as dt
import numpy as np

def append_date_columns_to_dataframe(dataframe, days_lookahead: int):
    """
    This function adds columns to the end of the dataframe. Each column is assigned a date starting from today's date. The number of columns
    is determined by the input given. The default is 0, producing a dataframe with only a column for the current date.
    """
    current_date = pd.to_datetime('today').normalize()
    expected_final_published_date = current_date + pd.Timedelta(days=days_lookahead)

    df_with_date_columns = dataframe
    delta = dt.timedelta(days=1)
    while current_date <= expected_final_published_date:
        df_with_date_columns[current_date] = None
        print(current_date, end='\n')
        current_date += delta

    df_with_date_columns['OperatingPeriodStartDate'] = pd.to_datetime(df_with_date_columns['OperatingPeriodStartDate'])
    df_with_date_columns['OperatingPeriodEndDate'] = pd.to_datetime(df_with_date_columns['OperatingPeriodEndDate'])
    df_with_date_columns.reset_index(inplace=True)

    return df_with_date_columns

def assign_timetable_file_validity_for_each_date(df_with_date_columns, days_lookahead: int):
    """
    This function iterates through the rows and columns, assigning a "True" value if the file is valid for that date.
    """
    LOWER_RANGE = 11
    UPPER_RANGE = LOWER_RANGE + 1 + days_lookahead

    for calendar_date in range(LOWER_RANGE, UPPER_RANGE): 
        for row in df_with_date_columns.itertuples():
            #TODO Add in False values as default for column
            print(df_with_date_columns.columns[calendar_date])
            print(getattr(row, 'OperatingPeriodStartDate'))
            if (getattr(row, 'OperatingPeriodEndDate') is pd.NaT) or (getattr(row, 'OperatingPeriodEndDate') is None):
                print(df_with_date_columns.columns[calendar_date])
                print(getattr(row, 'OperatingPeriodStartDate'))
                if df_with_date_columns.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate'):
                    df_with_date_columns[df_with_date_columns.columns[calendar_date]][getattr(row, 'Index')] = "True" # Shall we change this to boolean instead of string boolean
            elif (df_with_date_columns.columns[calendar_date] >= getattr(row, 'OperatingPeriodStartDate')) and \
                    (df_with_date_columns.columns[calendar_date] <= getattr(row, 'OperatingPeriodEndDate')):
                df_with_date_columns[df_with_date_columns.columns[calendar_date]][getattr(row, 'Index')] = "True"

    return df_with_date_columns

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

def determine_file_validity(dataframe, dates):
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

def collect_index_numbers_to_delete(dataframe):
    current_date = pd.to_datetime('today').normalize()
    column_to_retrieve = 'index'

    filtered_rows = dataframe.loc[dataframe[current_date].isnull()]
    indexes_to_delete = filtered_rows[column_to_retrieve].tolist()
    
    return indexes_to_delete

def remove_invalid_files(dataframe, indexes_to_delete: list[int]):
    amended_service_line_extract = dataframe.drop(indexes_to_delete)

    return amended_service_line_extract