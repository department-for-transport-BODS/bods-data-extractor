#This file contains functions to download and save the database of the traffic commissioner to view registered bus services. Currently England only
import pandas as pd
from datetime import date
import requests
import io
import os
from pathlib import Path
from sys import platform

west_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_H.csv'
west_midlands = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_D.csv'
london_south_east = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_K.csv'
north_west_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_C.csv'
north_east_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_B.csv'
east_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_F.csv'

otc_db_files = [west_england, west_midlands, london_south_east, north_west_england, north_east_england, east_england]

today = str(date.today())


def get_user_downloads_folder():
    if platform == "win32":
        downloads_folder = str(Path.home() / "Downloads")

    elif platform == "darwin" or "linux":
        # for macOS or linux
        downloads_folder = str(os.path.join(Path.home(), "Downloads"))

    else:
        print("Unrecognised OS, cannot locate downloads folder")
        downloads_folder = ""

    return downloads_folder


def create_today_folder():
    '''
    Create a folder, named with the days data, so that timetables can be saved locally
    '''

    today = str(date.today())
    downloads_folder = get_user_downloads_folder()

    # list out the file names in the downloads folder
    files = os.listdir(downloads_folder)

    # create the path for today folder in downloads
    today_folder_path = downloads_folder + '/' + today

    # if timetable output folder is not in downloads, create
    if today not in files:
        os.mkdir(today_folder_path)

    else:
        print('file with todays date already exists')

    return today_folder_path


def save_otc_db():
    # instantiate a list in which each regions dataframe will reside
    otc_regions = []

    # loop through regions as per OTC DB page
    for region in otc_db_files:
        # print(f'Downloading region: {region}...')
        # get the raw text from the link, should be a csv file
        text_out = requests.get(region).content

        # convert to a dataframe
        df = pd.read_csv(io.StringIO(text_out.decode('utf-8-sig')))

        # append current region df to regions list
        otc_regions.append(df)

    # combine to a single dataframe
    print('Merging files...')
    otc_db = pd.concat(otc_regions)
    # otc_db.drop(otc_db.columns[0], axis = 1, inplace = True)
    otc_db['service_code'] = otc_db['Reg_No'].str.replace('/', ':')

    # postgresql does not like uppercase or spaces - removing from column titles
    otc_db.columns = [c.lower() for c in otc_db.columns]
    otc_db.columns = [c.replace(" ", "_") for c in otc_db.columns]

    # remove duplicate rows
    otc_db = otc_db.drop_duplicates()

    create_today_folder()
    downloads_folder = get_user_downloads_folder()

    save_loc = downloads_folder + '/' + today + f'/otc_db_{today}.csv'

    otc_db.to_csv(save_loc, index=False)   
    return otc_db


def fetch_otc_db():
    # instantiate a list in which each regions dataframe will reside
    otc_regions = []
    print(f'Downloading otc database...\n')
    # loop through regions as per OTC DB page
    for region in otc_db_files:
        # print(f'Downloading region: {region}...')
        # get the raw text from the link, should be a csv file
        text_out = requests.get(region).content

        # convert to a dataframe
        df = pd.read_csv(io.StringIO(text_out.decode('utf-8-sig')))

        # append current region df to regions list
        otc_regions.append(df)

    # combine to a single dataframe
    print('Merging files...')
    otc_db = pd.concat(otc_regions)
    # otc_db.drop(otc_db.columns[0], axis = 1, inplace = True)
    otc_db['service_code'] = otc_db['Reg_No'].str.replace('/', ':')

    # postgresql does not like uppercase or spaces - removing from column titles
    otc_db.columns = [c.lower() for c in otc_db.columns]
    otc_db.columns = [c.replace(" ", "_") for c in otc_db.columns]

    # remove duplicate rows
    otc_db = otc_db.drop_duplicates()

    return otc_db

if __name__ == "__main__":
    save_otc_db()
