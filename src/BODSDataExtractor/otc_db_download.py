#This file contains functions to download and save the database of the traffic commissioner to view registered bus services. Currently England only
import pandas as pd
from datetime import date
import requests
from io import BytesIO
import os
from pathlib import Path
from sys import platform


OTC_BASE_URL = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/'
OTC_DB_FILES = [
    f'{OTC_BASE_URL}Bus_RegisteredOnly_H.csv', # West England
    f'{OTC_BASE_URL}Bus_RegisteredOnly_D.csv', # West Midlands
    f'{OTC_BASE_URL}Bus_RegisteredOnly_K.csv', # London, South East
    f'{OTC_BASE_URL}Bus_RegisteredOnly_C.csv', # North West England
    f'{OTC_BASE_URL}Bus_RegisteredOnly_B.csv', # North East England
    f'{OTC_BASE_URL}Bus_RegisteredOnly_F.csv'  # East England
]


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
    otc_db = fetch_otc_db()
    create_today_folder()
    downloads_folder = get_user_downloads_folder()
    save_loc = downloads_folder + '/' + today + f'/otc_db_{today}.csv'
    otc_db.to_csv(save_loc, index=False)


def fetch_otc_db():
    """Returns a pandas dataframe of the OTC database (all regions combined)."""
    print('Downloading OTC database...')
    otc_regions = []

    for region in OTC_DB_FILES:
        text_out = requests.get(region).content
        df = pd.read_csv(BytesIO(text_out))
        otc_regions.append(df)

    print('Merging OTC files...')
    otc_db = pd.concat(otc_regions)
    otc_db['service_code'] = otc_db['Reg_No'].str.replace('/', ':')
    # postgresql does not like uppercase or spaces
    otc_db.columns = [c.lower().replace(" ", "_") for c in otc_db.columns]
    return otc_db.drop_duplicates()


if __name__ == "__main__":
    save_otc_db()
