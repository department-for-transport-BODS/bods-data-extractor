import requests
import zipfile
import io
import os
from bods_client.client import BODSClient
from bods_client.models import timetables
from bods_client.models.base import APIError as BodsApiError
from bods_client.models.timetables import TimetableResponse
import lxml.etree as ET
import itertools
from itertools import zip_longest
from pathlib import Path
from sys import platform
import re
import concurrent.futures

try:
    import BODSDataExtractor.otc_db_download as otc_db_download
except:
    import otc_db_download
from datetime import date
from collections import Counter
import importlib.resources
from shapely.geometry import Point
from geopandas import GeoDataFrame
import pandas as pd
from dacite import from_dict
import xmltodict
import datetime
from classes import *


class TimetableExtractor:
    error_list = []

    def __init__(self, api_key, limit=10_000, offset=0, nocs=None, status='published',
                 search=None, bods_compliant=True, atco_code=None, service_line_level=False,
                 stop_level=False, threaded=False):
        self.api_key = api_key
        self.limit = limit
        self.offset = offset
        self.nocs = nocs
        self.status = status
        self.search = search
        self.bods_compliant = bods_compliant
        self.atco_code = atco_code
        self.service_line_level = service_line_level
        self.stop_level = stop_level
        self.threaded = threaded

        self.pull_timetable_data()

        if self.metadata is None:
            return  # early return if no results to process

        self.otc_db = otc_db_download.fetch_otc_db()

        if service_line_level or stop_level:
            self.analytical_timetable_data()
            self.analytical_timetable_data_analysis()

        if stop_level:
            self.generate_timetable()

    def create_metadata_df(self, timetable_api_response):
        """Converts BODS Timetable API results into a Pandas dataframe."""
        df = pd.DataFrame([vars(t_dataset) for t_dataset in timetable_api_response.results])
        df['filetype'] = df['extension']
        return df

    def extract_dataset_level_atco_codes(self):

        # initiate list for atco codes
        atco_codes = []
        # extract atco code from admin_area list of dicts on each row
        for r in self.metadata['admin_areas']:
            atco_codes.append([d['atco_code'] for d in r])

        # flatten list of lists to list, and ensure only unique values
        atco_codes = list(set((itertools.chain(*atco_codes))))

        return atco_codes

    def _handle_api_response(self, response: Union[TimetableResponse, BodsApiError]):
        """Handle and validate the API response. Inform the user of any issues."""
        if type(response) is BodsApiError:
            if response.status_code == 401:
                self.metadata = None
                print('Invalid API token used.')
                return
            elif response.status_code == 504:
                self.metadata = None
                print('Gateway error, please check BODS website.')
                return
            else:
                raise ValueError(repr(response))

        if type(response) is TimetableResponse and response.count == 0:
            self.metadata = None
            print('No results returned from BODS Timetable API. Please check input parameters.')
            return

        return response

    def _get_timetable_datasets(self):
        """Queries the BODS Timetable API as per the parameters set at instance
        initialisation.
        """
        bods = BODSClient(api_key=self.api_key)
        params = timetables.TimetableParams(limit=self.limit,
                                            offset=self.offset,
                                            nocs=self.nocs,
                                            status=self.status,
                                            admin_areas=self.atco_code,
                                            search=self.search)
        api_response = bods.get_timetable_datasets(params=params)
        return self._handle_api_response(api_response)

    def pull_timetable_data(self):
        """Creates the timetable dataset metadata dataframe and assigns to
        self.metadata.

        Will return early with self.metadata set to None if there
        are no datasets to work with.
        """
        print(f"Fetching timetable metadata for up to {self.limit:,} datasets...")
        timetable_datasets = self._get_timetable_datasets()
        if not timetable_datasets:
            return
        self.metadata = self.create_metadata_df(timetable_datasets)

        if self.bods_compliant:
            self.metadata = self.metadata[self.metadata['bods_compliance'] == True]

        print(f"Metadata downloaded for {self.metadata.shape[0]:,} datasets.")

    def xml_metadata(self, url, error_list):

        '''' This function assumes the file at the end of the URL is an xml file.
             If so, it returns the filename, size and url link as a tuple. If
             file type is invalid it will print an error and skip.

             Arguments: URL and a list in which to pass urls which could not be treated as xmls
        '''

        try:
            resp = requests.get(url)

            # create a temporary file in the local directory to get the file size
            with open(r'temp.xml', 'w', encoding="utf-8") as file:
                file.write(resp.text)
                size = os.path.getsize(r'temp.xml')

                # dig into the headers of the file to pull the file name
                meta = resp.headers
                filename = str(meta["Content-Disposition"].split('"')[1])

            print("xml file found and appended to list\n")
            return (filename, size, url)


        except:
            # if we reach this then the filetype may not be xml or zip and needs investigating
            print(f"*****Error in dataset. Please check filetype: {url}*****\n")
            TimetableExtractor.error_list.append(url)
            pass

    def _dataset_filetype(self, response_headers):
        """Determines the filetype of the dataset served up by a dataset url.
        Returns None if it can't be determined.
        """
        pattern = r'(\.\w+)"'
        m = re.search(pattern, response_headers['Content-Disposition'])
        try:
            return m.group(1)
        except AttributeError:
            return None

    def download_extract_txc(self, url):
        """Download the txc data from a dataset url (can be zip or single xml) and
        extracts the data into a Pandas dataframe."""
        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise e
        filetype = self._dataset_filetype(response.headers)

        if filetype == '.zip':
            print(f'Fetching zip file from {url}...')
            txc_df = self._extract_zip(response)
        elif filetype == '.xml':
            print(f'Fetching xml file from {url}...')
            xml = io.BytesIO(response.content)
            txc_df = self._extract_xml(response.url, xml)
        else:
            print(f'Invalid dataset file found: "{filetype}", skipping...')
            return

        return txc_df

    def _extract_zip(self, response):
        """Download a ZIP file and extract the relevant contents
        of each xml file into a dataframe.
        """
        output = []

        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            for zipinfo in thezip.infolist():
                extension = zipinfo.filename.split('.')[-1]
                if extension != 'xml':
                    print(f'Found "{extension}" file in zip folder, passing...')
                    continue

                with thezip.open(zipinfo) as thefile:
                    try:
                        xml_output = self._extract_xml(response.url, thefile)
                        output.append(xml_output)
                    except:  # TODO really should be catching specific errors
                        TimetableExtractor.error_list.append(response.url)

        return pd.concat(output)

    def _extract_xml(self, url, xml):
        xml_output = [url]
        xml_data_extractor = xmlDataExtractor(xml)
        xml_output.extend(xml_data_extractor.extract_service_level_info())

        # if stop level data is requested, then need the additional columns that contain jsons of the stop level info
        if self.stop_level:
            # =============================================================================
            #               also read in xml as a text string
            #               this is required for extracting sections of the xml for further stop level extraction, not just elements or attribs
            # =============================================================================
            xml.seek(0)
            xml_text = xml.read()
            xml_json = xmltodict.parse(
                xml_text,
                process_namespaces=False,
                attr_prefix='_',
                force_list=(
                    'JourneyPatternSection',
                    'JourneyPatternTimingLink',
                    'VehicleJourney',
                    'VehicleJourneyTimingLink',
                    'JourneyPattern'))

            journey_pattern_json = xml_json['TransXChange']['JourneyPatternSections']  # ['JourneyPatternSection']
            xml_output.append(journey_pattern_json)

            vehicle_journey_json = xml_json['TransXChange']['VehicleJourneys']  # ['VehicleJourney']
            xml_output.append(vehicle_journey_json)

            services_json = xml_json['TransXChange']['Services']['Service']
            xml_output.append(services_json)

            stops_json = xml_json['TransXChange']['StopPoints']
            xml_output.append(stops_json)

        output_df = pd.DataFrame(xml_output).T

        if self.stop_level:
            output_df.columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName',
                                 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'OperatingDays', 'Origin',
                                 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion',
                                 'RevisionNumber', 'la_code', 'journey_pattern_json', 'vehicle_journey_json',
                                 'services_json', 'stops_json']
        else:
            output_df.columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName',
                                 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'OperatingDays', 'Origin',
                                 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion',
                                 'RevisionNumber', 'la_code']

        return output_df

    def fetch_xml_filenames(self):

        metadata_table = TimetableExtractor.pull_timetable_data(self)

        xml_filenames = TimetableExtractor.open_urls(self, metadata_table)

        full_table = metadata_table.merge(xml_filenames, on='url', how='left')

        return full_table

    def analytical_timetable_data(self):
        """Uses a collection of extraction functions to extract data from within xml files.
        Some of these xml files are within zip files, and so these are processed differently.
        This extracted data is combined with the metadata of each file, and columns renamed to
        yield analytical ready timetable data.
        """
        orig_cols = [
            "url",
            "id",
            "operator_name",
            "description",
            "comment",
            "status",
            "dq_score",
            "dq_rag",
            "bods_compliance",
            "filetype",
        ]
        txc_cols = [
            "URL",
            "DatasetID",
            "OperatorName",
            "Description",
            "Comment",
            "Status",
            "dq_score",
            "dq_rag",
            "bods_compliance",
            "FileType",
        ]
        rename_mapper = {orig: txc for orig, txc in zip(orig_cols, txc_cols)}

        if self.threaded:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self.download_extract_txc, self.metadata["url"].to_list())
                xml_table = pd.concat([df for df in results])
        else:
            extracted_xmls = []
            for dataset_url in self.metadata["url"]:
                extracted_xmls.append(self.download_extract_txc(dataset_url))
            xml_table = pd.concat(extracted_xmls)

        self.service_line_extract_with_stop_level_json = (
            self.metadata.filter(orig_cols, axis=1)
            .rename(columns=rename_mapper)
            .merge(xml_table, how="outer", on="URL")
        )

        # explode rows that are always just 1 value to get attributes out of lists
        self.service_line_extract_with_stop_level_json = self.xplode(
            self.service_line_extract_with_stop_level_json,
            [
                "NOC",
                "TradingName",
                "LicenceNumber",
                "OperatorShortName",
                "OperatorCode",
                "ServiceCode",
                "PublicUse",
                "OperatingDays",
                "Origin",
                "Destination",
                "OperatingPeriodStartDate",
                "OperatingPeriodEndDate",
            ],
        )
        self.service_line_extract_with_stop_level_json = self.xplode(
            self.service_line_extract_with_stop_level_json, ['LineName']
        )
        self.service_line_extract_with_stop_level_json = self.xplode(
            self.service_line_extract_with_stop_level_json, ['la_code']
        )

        self.service_line_extract_with_stop_level_json["dq_score"] = (
            self.service_line_extract_with_stop_level_json["dq_score"]
            .str.rstrip("%")
            .astype("float")
        )

        print(f'The following URLs failed: {TimetableExtractor.error_list}')

        # =============================================================================
        #         dataset level filtering by atco codes has already been handled if requested
        #         however, within a dataset some service lines will not have stops within all atco codes
        #         that the dataset as a whole has. Therefore now we filter again to return just specifc service
        #         lines for requested atcos
        # =============================================================================
        if self.atco_code is not None:
            self.service_line_extract_with_stop_level_json = self.service_line_extract_with_stop_level_json[
                self.service_line_extract_with_stop_level_json['la_code'].isin(self.atco_code)]

    def analytical_timetable_data_analysis(self):
        """Returns a copy of the service line level data suitable for analysis. Omits the columns with jsons
        of the final stop level data required for further processing and stop level analysis, for
        performance and storage sake. Also omits la_code column, as if user is not interested in
        local authorities of services then this adds unnecessary duplication (one service line can be in
        multiple las.)
        """
        self.service_line_extract = self.service_line_extract_with_stop_level_json.drop(
            ["la_code"], axis=1
        )

        if self.stop_level:
            self.service_line_extract = self.service_line_extract_with_stop_level_json.drop(
                ["journey_pattern_json", "vehicle_journey_json", "services_json", "stops_json"], axis=1
            )

        self.service_line_extract = self.service_line_extract.drop_duplicates()
        self.check_for_expired_services()

    def check_for_expired_services(self):
        """Adds service expiry status (True or False) to service level extract,
        based on "OperatingPeriodEndDate" and today's date, where applicable.
        If no end date provided then "No End Date" entered.
        """
        today = datetime.datetime.now()
        self.service_line_extract["expired service"] = (
                pd.to_datetime(self.service_line_extract["OperatingPeriodEndDate"]) < today
        )
        self.service_line_extract.loc[
            self.service_line_extract["OperatingPeriodEndDate"].isna(), "expired service"
        ] = 'No End Date'

    def xplode(self, df, cols_to_explode):
        """Explode out lists in dataframes.
        Taken from https://stackoverflow.com/a/61390677"""
        rest = {*df} - {*cols_to_explode}
        zipped = zip(zip(*map(df.get, rest)), zip(*map(df.get, cols_to_explode)))
        tups = [tup + exploded for tup, pre in zipped for exploded in zip_longest(*pre)]

        return pd.DataFrame(tups, columns=[*rest, *cols_to_explode])

    # =============================================================================
    #       FUNCTIONS FOR EXTRACTING STOP LEVEL DATA
    # =============================================================================

    def fetch_naptan_data(self):

        '''
        Access NAPTAN API to fetch lat and long coordinates for all relevant stops.
        Edited to fetch of all stops to avoid bugs. This could be improved in future.
        '''

        # get list of relevant admin area codes, to target api call
        # atcos = list(self.service_line_extract_with_stop_level_json['la_code'].unique())
        # atcos = ",".join(atcos)

        # call naptan api
        # url = f"https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes={atcos}&dataFormat=csv"
        url = "https://naptan.api.dft.gov.uk/v1/access-nodes?&dataFormat=csv"

        # filter results within call to those needed (just lat and long)
        r = requests.get(url).content
        naptan = pd.read_csv(io.StringIO(r.decode('utf-8')),
                             usecols=['ATCOCode', 'CommonName', 'Longitude', 'Latitude'])

        return naptan

    def get_user_downloads_folder(self):
        if platform == "win32":
            downloads_folder = str(Path.home() / "Downloads")

        elif platform == "darwin" or "linux":
            # for macOS or linux
            downloads_folder = str(os.path.join(Path.home(), "Downloads"))

        else:
            print("Unrecognised OS, cannot locate downloads folder")
            downloads_folder = ""

        return downloads_folder

    def create_today_folder(self):
        '''
        Create a folder, named with the days data, so that timetables can be saved locally
        '''
        today = str(date.today())
        downloads_folder = TimetableExtractor.get_user_downloads_folder(self)

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

    def create_timetable_folder(self):

        today_folder = TimetableExtractor.create_today_folder(self)
        timetable_folder = 'timetable_output'

        timetable_destination = f'{today_folder}/{timetable_folder}'

        files = os.listdir(today_folder)

        if timetable_folder not in files:
            os.mkdir(timetable_destination)

        else:
            print('timetable folder with todays date already exists')

        return timetable_destination

    def filter_timetable_dict(self, service_code):

        '''
        Filter the timetable dictionary for a specific service code.
        This can also be used to filter for a specific licence number, or anything else
        in the composite key (DatasetID_ServiceCode_LineName_RevisionNumber), using free
        text arguement.
        '''

        filtered_dict = {k: v for k, v in self.timetable_dict.items() if service_code in k}
        return filtered_dict

    def save_metadata_to_csv(self):
        """
        Save metadata table to csv file
        """

        # ensure no cell value exceeds 32,767 characters (this is excel limit)
        metadata = self.metadata
        metadata['localities'] = metadata['localities'].apply(lambda x: x[0:400])

        destination = TimetableExtractor.create_today_folder(self)
        metadata.to_csv(f'{destination}/metadata.csv', index=False)

    def save_service_line_extract_to_csv(self):
        """
        Save service line table to csv file
        """

        destination = TimetableExtractor.create_today_folder(self)
        self.service_line_extract.to_csv(f'{destination}/service_line_extract.csv', index=False)

    def save_all_timetables_to_csv(self):

        '''
        Save all timetables from the timetable_dict attribute as local csv files.
        '''

        # create folder to save timetables int and get name of new folder
        destination = TimetableExtractor.create_today_folder(self)

        for k, v in self.my_bus_data_object.stop_level_extract.items():
            print(f'writing {k} to csv...')
            k = str(k)
            k = k.replace(':', '_')
            v.to_csv(f'{destination}/{k}_timetable.csv', index=False)

    def save_dataframe_to_csv(self,dataframe, column_name, folder_path):
        '''
        Create the name of the timetable file and save the timetable to the project folder in the specified folders "outbound_timetable_folder" and "inbound_timetable_folder"
        '''

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        for index, row in dataframe.iterrows():
            #Create a dataframe from the specified column name, either for inbound or outbound timetables
            df = row[column_name]
            #Extract information for each timetable
            ServiceCode=str(dataframe.loc[index, "ServiceCode"])
            LineName=str(dataframe.loc[index, "LineName"])
            OperatingDays = str(dataframe.loc[index, "OperatingDays"])
            RevisionNumber = str(dataframe.loc[index, "RevisionNumber"])
            Filename=str(dataframe.loc[index, "FileName"])

            #Clean the data gathered above so that it can be saved
            Filename=(Filename.split("\\")[-1]).replace(".xml","")
            Filename = Filename.replace(":", ";")
            ServiceCode = ServiceCode.replace(":", ";")

            #Shorten the filename if it exceeds 80 characters
            if len(Filename)>80:
                Filename=Filename[:77]
            else:
                pass

            #Create the name of the timetable file
            timetable_file= ServiceCode+"-"+LineName+"_"+OperatingDays+"_"+"RN-"+RevisionNumber+"-"+Filename

            #If the dataframe is empty, continue
            if df.empty:
                continue
            else:
                #If the service doesn't exist, create a folder for it, otherwise save the new timetable inside of it
                if not os.path.exists(folder_path + "/" + ServiceCode):
                    os.makedirs(folder_path + "/" + ServiceCode)
                csv_file_path = os.path.join(folder_path, ServiceCode, f"{''}{timetable_file}.csv")

                df.to_csv(csv_file_path, index=False)
                print(f"Saved {timetable_file}.csv to {folder_path}")

    def save_timetables(self):
        '''
        Save the timetable dataframe to a csv file, seperated into inbound and outbound journeys
        '''

        df = self.stop_level_extract
        self.save_dataframe_to_csv(df, 'collated_timetable_outbound', 'outbound_timetable_folder')
        self.save_dataframe_to_csv(df, 'collated_timetable_inbound', 'inbound_timetable_folder')


    def save_filtered_timetables_to_csv(self, service_code):

        '''
        Save a filtered subset of timetables from the timetable_dict attribute as local csv files.
        The timetable dictionary can be filtered for a specific service code.
        This can also be used to filter for a specific licence number, or anything else
        in the composite key (DatasetID_ServiceCode_LineName_RevisionNumber), using free
        text argument.
        '''

        # create folder to save timetables int and get name of new folder
        destination = TimetableExtractor.create_today_folder(self)

        filtered_dict = TimetableExtractor.filter_timetable_dict(self, service_code)

        for k, v in filtered_dict.items():
            print(f'writing {k} to csv...')
            k = str(k)
            k = k.replace(':', '_')
            v.to_csv(f'{destination}/{k}_timetable.csv', index=False)

    def visualise_service_line(self, service_code):
        '''
       Visualise the route and timings of vehicle journeys from a specified
       service code.
        '''

        # filter dictionary of dataframes to just service code of interest and access df
        filtered_dict = TimetableExtractor.filter_timetable_dict(self, service_code)
        for k, v in filtered_dict.items():
            df = v

        # df must be processed from wide to long for visualisation
        df_melt = pd.melt(df, id_vars=['DatasetID', 'ServiceCode_LineName_RevisionNumber', 'ServiceCode', 'LineName',
                                       'RevisionNumber', 'sequence_number', 'stop_from', 'CommonName', 'Longitude',
                                       'Latitude'])
        # remove nulls that represent where a bus doesnt stop at that stop
        df_melt = df_melt.dropna(subset=['value'])

        # get names of service, line, revision number needed for the title of the viz
        service_code = df_melt['ServiceCode'].iloc[0]
        line_name = df_melt['LineName'].iloc[0]
        revision_number = df_melt['RevisionNumber'].iloc[0]

        # create geo df
        geometry = [Point(xy) for xy in zip(df_melt['Longitude'], df_melt['Latitude'])]
        gdf = GeoDataFrame(df_melt, geometry=geometry)

        # create viz
        pio.renderers.default = 'browser'
        fig = px.line_mapbox(
            lat=gdf.geometry.y,
            lon=gdf.geometry.x,
            color=gdf.variable
            , hover_data={'Stop name': gdf.CommonName, 'Stop number': gdf.sequence_number, 'time at stop': gdf.value, }
            ,
            title=f'Vehicle Journeys (VJ) for: Service code - {service_code}, Line - {line_name}, File revision number - {revision_number}'
        ).update_traces(mode="lines+markers").update_layout(
            mapbox={
                "style": "carto-positron",
                "zoom": 12,
            },
            margin={"l": 25, "r": 25, "t": 50},
        )

        print('\nTimetable visualised in browser!')
        return fig.show()

    # =============================================================================
    #       REPORTING FUNCTIONS
    # =============================================================================

    def red_dq_scores(self):
        ''' returns number of operators in a table with red dq scores as well as which ones'''

        red_score = self.metadata.query('dq_rag == "red"')

        red_count = red_score['operator_name'].unique()

        datasets = red_score['url']

        dataset_ids = red_score['id']

        print(f'\nNumber of operators with red dq score: {len(red_count)}')
        print(*red_count, sep=', ')
        print(f'\nNumber of datasets with red dq score: {len(red_score)}')
        print(*datasets, sep=', ')

        return red_score

    def dq_less_than_x(self, score):

        ''' returns number of operators in a table with dq scores less than input amount'''

        self.metadata['dq_score'] = self.metadata['dq_score'].astype(str)
        self.metadata['dq_score'] = self.metadata['dq_score'].str.rstrip('%').astype('float')

        score_filter = self.metadata.query(f'dq_score < {score}')

        output = score_filter['operator_name'].unique()

        datasets = score_filter['url']

        print(f'\nNumber of operators with dq score less than {score}: {len(output)}')
        print(*output, sep=', ')

        print(f'\nNumber of datasets with dq score less than {score}: {len(score_filter)}')
        print(*datasets, sep=', ')

        return score_filter

    def no_licence_no(self):
        '''how many files have no licence number'''

        no_licence_number_total=self.service_line_extract_with_stop_level_json['LicenceNumber'].isna().sum()

        if no_licence_number_total<=0:
            # analytical_data = TimetableExtractor.analytical_timetable_data(self)
            grouped = self.service_line_extract_with_stop_level_json[
                self.service_line_extract_with_stop_level_json['LicenceNumber'].map(len) == 0]
            datasets = grouped['URL'].unique()
            print(f'\nNumber of datasets with files containing no licence number: {len(datasets)}')
            print(*datasets, sep=', ')
            return grouped

        elif no_licence_number_total>0:
            print(f'\nNumber of datasets with files containing no licence number: {no_licence_number_total}')
            return no_licence_number_total


    # =============================================================================
    #     ## DFT Reporting ###
    # =============================================================================

    def count_operators(self):
        '''
        returns count of distinct operators (measured by operator_name) in a chosen dataset
        '''
        num_ops = len(self.metadata['operator_name'].unique())
        print(f'\nNumber of distinct operator names in chosen dataset: {num_ops}\n')
        return num_ops

    def count_service_codes(self):
        '''
        returns count of unique service codes chosen dataset
        '''
        non_null_service_codes = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['ServiceCode'] != None]
        unique_service_codes = len(non_null_service_codes['ServiceCode'].unique())
        print(f'\nNumber of unique service codes in chosen dataset: {unique_service_codes}\n')
        return unique_service_codes

    def valid_service_codes(self):
        '''
        returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes
        and a dataframe with all the invalid service codes.
        '''
        # left 2 are characters
        correct_service_code = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['ServiceCode'].str[:2].str.isalpha() == True]
        # 10th is colon
        correct_service_code = correct_service_code[correct_service_code['ServiceCode'].str[9:10] == ':']
        # > than 10 characters
        correct_service_code = correct_service_code[correct_service_code['ServiceCode'].str.len() > 10]
        # unreg must be handled differently
        correct_service_code_unreg = correct_service_code[correct_service_code['ServiceCode'].str[:2] == 'UZ']
        correct_service_code_reg = correct_service_code[correct_service_code['ServiceCode'].str[:2] != 'UZ']
        # next 7 are int unless first two of whole thing are UZ
        correct_service_code_reg = correct_service_code_reg[
            correct_service_code_reg['ServiceCode'].str[2:9].str.isnumeric() == True]
        # right after colon are int, unless first two of whole thing are UZ
        correct_service_code_reg = correct_service_code_reg[
            correct_service_code_reg['ServiceCode'].str[10:].str.isnumeric() == True]
        correct_service_code_final = pd.concat([correct_service_code_reg, correct_service_code_unreg])

        # return the invalid service codes
        valid_serv = correct_service_code_final[['ServiceCode']]
        all_serv = self.service_line_extract_with_stop_level_json[['ServiceCode']]
        invalid_serv = all_serv[~all_serv.apply(tuple, 1).isin(valid_serv.apply(tuple, 1))]
        invalid_service_codes = invalid_serv.merge(self.service_line_extract_with_stop_level_json, how='left',
                                                   on='ServiceCode')

        unique_valid_service_codes = len(correct_service_code_final['ServiceCode'].unique())
        print(f'\nNumber of unique valid service codes in chosen dataset: {unique_valid_service_codes}\n')
        return correct_service_code_final, invalid_service_codes

    def services_published_in_TXC_2_4(self):
        '''
        returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records
        that are not published in this schema
        '''
        count_published_in_2_4_schema = len(self.service_line_extract_with_stop_level_json[
                                                self.service_line_extract_with_stop_level_json[
                                                    'SchemaVersion'] == '2.4'])
        TXC_2_4_schema = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['SchemaVersion'] == '2.4']
        not_TXC_2_4_schema = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['SchemaVersion'] != '2.4']
        perc_published_in_2_4_schema = (count_published_in_2_4_schema / len(
            self.service_line_extract_with_stop_level_json)) * 100
        print(f'\nPercentage of services published in TXC 2.4 schema: {perc_published_in_2_4_schema}\n')
        return perc_published_in_2_4_schema, TXC_2_4_schema, not_TXC_2_4_schema

    def datasets_published_in_TXC_2_4(self):
        '''
        returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records
        that are not published in this schema
        '''
        datasets_schema_pre = self.service_line_extract_with_stop_level_json.copy()
        datasets_schema_pre['SchemaVersion'] = datasets_schema_pre['SchemaVersion'].astype('float')
        datasets_schema = datasets_schema_pre.groupby('DatasetID').agg({'SchemaVersion': 'min'})
        count_datasets_published_in_2_4_schema = len(datasets_schema[datasets_schema['SchemaVersion'] == 2.4])
        perc_datasets_published_in_2_4_schema = (count_datasets_published_in_2_4_schema / len(datasets_schema)) * 100
        TXC_2_4_schema = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['SchemaVersion'] == '2.4']
        not_TXC_2_4_schema = self.service_line_extract_with_stop_level_json[
            self.service_line_extract_with_stop_level_json['SchemaVersion'] != '2.4']
        print(f'\nPercentage of datasets published in TXC 2.4 schema: {perc_datasets_published_in_2_4_schema}\n')
        return perc_datasets_published_in_2_4_schema, TXC_2_4_schema, not_TXC_2_4_schema

    def timetables_publishing_mi(self):
        '''
        returns high level MI for reporting into DFT on progress
        of publishing of timetables data
        '''
        TimetableExtractor.count_operators(self)
        TimetableExtractor.count_service_codes(self)
        TimetableExtractor.valid_service_codes(self)
        TimetableExtractor.services_published_in_TXC_2_4(self)
        TimetableExtractor.datasets_published_in_TXC_2_4(self)

    # =============================================================================
    #     ## OTC reporting - to only be run on all published datasets ###
    # =============================================================================

    def licence_from_sc(self):
        """Returns the licence number from the service code column of the OTC Database
        file input if the service code is properly formatted as a published service code"""

        registered_licence_from_sc = self.otc_db['service_code'].str.split(':', expand=True)
        registered_licence_from_sc = pd.Series(registered_licence_from_sc[0])

        return registered_licence_from_sc

    def registered_published_services_all(self):
        """This function returns a dataframe with two columns: Licence number, taken from the service code column of OTC Database (input) and the number of registered services associated to that licence number in the published data (input) """

        # unique licences from service code column in otc_db
        registered_licence_from_sc = self.otc_db['service_code'].str.split(':', expand=True)
        unique_registered_licence_from_sc = pd.Series(registered_licence_from_sc[0])

        # a list of licence codes from the published data, derived from the service codes
        published_licence_from_sc = self.service_line_extract['ServiceCode'].str.split(':', expand=True)
        published_licence_from_sc = published_licence_from_sc[0]

        # count of how many published records of each service codes exist in the published data
        count_of_published_services = dict(Counter(published_licence_from_sc))

        # new dictionary to count occurances of otcdb licences in the published data
        number_registered_services_published = {}

        for x in unique_registered_licence_from_sc:
            if x in count_of_published_services.keys():
                number_registered_services_published[x] = count_of_published_services[x]
            else:
                number_registered_services_published[x] = 0

        all_services = pd.DataFrame(number_registered_services_published.items(),
                                    columns=['licence', 'published_services'])
        # published_services = all_services.query('published_services >0')
        # not_published_services = all_services.query('published_services == 0')

        return all_services

    def count_registered_published_services(self):
        """dataframe of counts of services registered for each otcdb licence"""
        all_services = TimetableExtractor.registered_published_services_all(self)
        published_services = all_services.query('published_services >0')

        return published_services

    def count_registered_not_published_services(self):
        all_services = TimetableExtractor.registered_published_services_all(self)
        not_published_services = all_services.query('published_services == 0')

        return not_published_services

    def percent_published_licences(self):
        '''percentage of registered licences with at least one published service
        Note - only to be run on all published datasets'''

        all_services = TimetableExtractor.registered_published_services_all(self)
        published = TimetableExtractor.count_registered_published_services(self)

        percentage = round(float((len(published) / len(all_services)) * 100), 2)

        print(
            f'\nPercentage of registered licences (on OTC) with at least one published service on BODS: {percentage}%')

        return f'{percentage}%'

    def registered_not_published_services(self):
        '''returns a dataframe of services found in the otc database (input) which are not found in the published data from the api (input)
        Note - only to be run on all published datasets'''

        service_line_lite = self.service_line_extract[
            ['DatasetID', 'OperatorName', 'bods_compliance', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName',
             'OperatorCode', 'ServiceCode']]

        full_merge = self.otc_db[['service_code', 'op_name', 'lic_no', 'auth_description']].merge(service_line_lite,
                                                                                                  how='outer',
                                                                                                  right_on='ServiceCode',
                                                                                                  left_on='service_code')
        not_published_full = full_merge[full_merge['ServiceCode'].isnull()]
        not_published = not_published_full[['service_code', 'op_name', 'lic_no', 'auth_description']]

        not_published = not_published.drop_duplicates()

        print(f'\nNumber of service codes in OTC database that are not published on BODS: {len(not_published)}')

        return not_published

    def published_not_registered_services(self):
        '''returns a dataframe of services found in the published data from the api (input) which are not found in the otc database (input)
        Note - only to be run on all published datasets'''

        service_line_lite = self.service_line_extract[
            ['DatasetID', 'OperatorName', 'bods_compliance', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName',
             'OperatorCode', 'ServiceCode', ]]

        full_merge = self.otc_db[['service_code', 'op_name', 'lic_no', 'auth_description']].merge(service_line_lite,
                                                                                                  how='outer',
                                                                                                  right_on='ServiceCode',
                                                                                                  left_on='service_code')

        not_registered_full = full_merge[full_merge['service_code'].isnull()]
        not_registered = not_registered_full[
            ['DatasetID', 'OperatorName', 'bods_compliance', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName',
             'OperatorCode', 'ServiceCode']]

        not_registered = not_registered.drop_duplicates()

        print(f'\nNumber of service codes published on BODS not in OTC database: {len(not_registered)}')

        return not_registered

    def services_on_bods_or_otc_by_area(self):
        '''
        Generates a dataframe of all service codes published on BODS and/or
        in the OTC database, indicates whether they are published on both or one
        of BODS and OTC, and provides the admin area the services has stops within
        Note - only to be run on all published datasets
        '''

        # read in lookup file from repo that links admin areas to ATCO codes

        # try except ensures that this reads in lookup file whether pip installing the library, or cloning the repo from GitHub
        try:
            # import the csv file as a text string from the BODSDataExtractor package
            atco_lookup_file = importlib.resources.read_text('BODSDataExtractor',
                                                             'BODSDataExtractor/ATCO_code_to_LA_lookup.csv')
            # wrap lookup_file string into a stringIO object so it can be read by pandas
            atco_lookup_string = io.StringIO(atco_lookup_file)

            la_lookup = pd.read_csv(atco_lookup_string, dtype={'ATCO Code': str})
        except:
            la_lookup = pd.read_csv('ATCO_code_to_LA_lookup.csv', dtype={'ATCO Code': str})

        # fetch latest version of OTC database
        otc = self.otc_db.drop_duplicates()

        # enrich OTC data with ATCO code
        otc_la_merge = otc[['service_code', 'service_number', 'op_name', 'auth_description']].merge(
            la_lookup[['Auth_Description', 'ATCO Code']], how='left', right_on='Auth_Description',
            left_on='auth_description').drop_duplicates()

        # call full BODS timetables data extract and enrich with admin area name
        bods_la_merge = self.service_line_extract_with_stop_level_json[
            ['ServiceCode', 'LineName', 'la_code', 'OperatorName']].merge(
            la_lookup[['Admin Area Name associated with ATCO Code', 'ATCO Code']], how='left', right_on='ATCO Code',
            left_on='la_code').drop_duplicates()

        # add cols to distinguish if in otc and if in bods
        otc_la_merge['in'] = 1
        bods_la_merge['in'] = 1

        # ensure linename col is consistent across bods and otc
        otc_la_merge.rename(columns={'service_number': 'LineName'}, inplace=True)

        # merge OTC service level data with BODS service level data
        full_service_code_with_atco = otc_la_merge[
            ['service_code', 'LineName', 'op_name', 'ATCO Code', 'in', 'auth_description']].add_suffix('_otc').merge(
            bods_la_merge.add_suffix('_bods'), how='outer', right_on=['ServiceCode_bods', 'ATCO Code_bods'],
            left_on=['service_code_otc', 'ATCO Code_otc']).drop_duplicates()

        # coalesce service code and atco code cols
        full_service_code_with_atco['service_code'] = full_service_code_with_atco['service_code_otc'].combine_first(
            full_service_code_with_atco['ServiceCode_bods'])
        full_service_code_with_atco['atco_code'] = full_service_code_with_atco['ATCO Code_otc'].combine_first(
            full_service_code_with_atco['ATCO Code_bods'])

        # keep only necessary cols
        full_service_code_with_atco = full_service_code_with_atco[
            ['service_code', 'LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc',
             'in_bods', 'auth_description_otc']]

        # add admin area name
        full_service_code_with_atco = full_service_code_with_atco.merge(
            la_lookup[['Admin Area Name associated with ATCO Code', 'ATCO Code']], how='left', left_on='atco_code',
            right_on='ATCO Code').drop_duplicates()

        # remove dupicate atco code col
        del full_service_code_with_atco['ATCO Code']

        # replace nans with 0s (necessary for mi reporting calculations)
        full_service_code_with_atco['in_otc'] = full_service_code_with_atco['in_otc'].fillna(0)
        full_service_code_with_atco['in_bods'] = full_service_code_with_atco['in_bods'].fillna(0)

        # format line nos into desired list format, so as to not explode rest of dataframe
        # replace nulls with string so groupby doesnt omit them
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].fillna('xxxxx')
        # groupby to concat the bods line nos together
        full_service_code_with_atco = full_service_code_with_atco.groupby(
            ['service_code', 'LineName_otc', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc', 'in_bods',
             'auth_description_otc', 'Admin Area Name associated with ATCO Code'], as_index=False, dropna=False).agg(
            {'LineName_bods': lambda x: ','.join(x)})
        # regenerate the nulls
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].replace('xxxxx',
                                                                                                            None)
        # reorder cols
        full_service_code_with_atco = full_service_code_with_atco[
            ['service_code', 'LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc',
             'in_bods', 'auth_description_otc', 'Admin Area Name associated with ATCO Code']]
        return full_service_code_with_atco

    def services_on_bods_or_otc_by_area_mi(self):
        '''
        Generates MI from dataframe that lists all service codes from BODS and/or
        OTC database, by admin area. Specifically notes the number of services from
        these sources, and the fraction of all within BODS and OTC respectively.
        Note - only to be run on all published datasets
        '''

        full_service_code_with_atco = TimetableExtractor.services_on_bods_or_otc_by_area(self)
        service_atco_mi = full_service_code_with_atco.groupby('atco_code').agg({'service_code': 'count'
                                                                                   ,
                                                                                'Admin Area Name associated with ATCO Code': 'first'
                                                                                   , 'in_otc': 'mean'
                                                                                   , 'in_bods': 'mean'})

        service_atco_mi.rename(columns={'service_code': 'count_services'}, inplace=True)

        # round fractions to 2 decimals
        service_atco_mi = service_atco_mi.round(2)

        return service_atco_mi

    def services_on_bods_or_otc_by_area_just_otc(self):
        '''
        Generates a dataframe of all service codes published
        in the OTC database, indicates whether they are published on
        BODS as well, and provides the admin area the services has stops within
        Note - only to be run on all published datasets
        '''

        # read in lookup file from repo that links admin areas to ATCO codes

        # try except ensures that this reads in lookup file whether pip installing the library, or cloning the repo from GitHub
        try:
            # import the csv file as a text string from the BODSDataExtractor package
            atco_lookup_file = importlib.resources.read_text('BODSDataExtractor',
                                                             'BODSDataExtractor/ATCO_code_to_LA_lookup.csv')
            # wrap lookup_file string into a stringIO object so it can be read by pandas
            atco_lookup_string = io.StringIO(atco_lookup_file)

            la_lookup = pd.read_csv(atco_lookup_string, dtype={'ATCO Code': str})



        except:
            la_lookup = pd.read_csv('ATCO_code_to_LA_lookup.csv', dtype={'ATCO Code': str})

        # fetch latest version of OTC database
        otc = self.otc_db.drop_duplicates()

        # enrich OTC data with ATCO code
        otc_la_merge = otc[['service_code', 'service_number', 'op_name', 'auth_description']].merge(
            la_lookup[['Auth_Description', 'ATCO Code']], how='left', right_on='Auth_Description',
            left_on='auth_description').drop_duplicates()

        # call full BODS timetables data extract and enrich with admin area name
        bods_la_merge = self.service_line_extract_with_stop_level_json[
            ['ServiceCode', 'LineName', 'la_code', 'OperatorName']].merge(
            la_lookup[['Admin Area Name associated with ATCO Code', 'ATCO Code']], how='left', right_on='ATCO Code',
            left_on='la_code').drop_duplicates()

        # add cols to distinguish if in otc and if in bods
        otc_la_merge['in'] = 1
        bods_la_merge['in'] = 1

        # ensure linename col is consistent across bods and otc
        otc_la_merge.rename(columns={'service_number': 'LineName'}, inplace=True)

        # merge OTC service level data with BODS service level data
        full_service_code_with_atco = otc_la_merge[
            ['service_code', 'LineName', 'op_name', 'ATCO Code', 'in', 'auth_description']].add_suffix('_otc').merge(
            bods_la_merge.add_suffix('_bods'), how='outer', right_on=['ServiceCode_bods', 'ATCO Code_bods'],
            left_on=['service_code_otc', 'ATCO Code_otc']).drop_duplicates()

        # coalesce service code and atco code cols
        full_service_code_with_atco['service_code'] = full_service_code_with_atco['service_code_otc'].combine_first(
            full_service_code_with_atco['ServiceCode_bods'])
        full_service_code_with_atco['atco_code'] = full_service_code_with_atco['ATCO Code_otc'].combine_first(
            full_service_code_with_atco['ATCO Code_bods'])

        # keep only necessary cols
        full_service_code_with_atco = full_service_code_with_atco[
            ['service_code', 'LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc',
             'in_bods', 'auth_description_otc']]

        # add admin area name
        full_service_code_with_atco = full_service_code_with_atco.merge(
            la_lookup[['Admin Area Name associated with ATCO Code', 'ATCO Code']], how='left', left_on='atco_code',
            right_on='ATCO Code').drop_duplicates()

        # remove dupicate atco code col
        del full_service_code_with_atco['ATCO Code']

        # replace nans with 0s (necessary for mi reporting calculations)
        full_service_code_with_atco['in_otc'] = full_service_code_with_atco['in_otc'].fillna(0)
        full_service_code_with_atco['in_bods'] = full_service_code_with_atco['in_bods'].fillna(0)

        # remove services not in otc
        full_service_code_with_atco = full_service_code_with_atco[full_service_code_with_atco['in_otc'] == 1]

        # format line nos into desired list format, so as to not explode rest of dataframe
        # replace nulls with string so groupby doesnt omit them
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].fillna('xxxxx')
        # groupby to concat the bods line nos together
        full_service_code_with_atco = full_service_code_with_atco.groupby(
            ['service_code', 'LineName_otc', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc', 'in_bods',
             'auth_description_otc', 'Admin Area Name associated with ATCO Code'], as_index=False, dropna=False).agg(
            {'LineName_bods': lambda x: ','.join(x)})
        # regenerate the nulls
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].replace('xxxxx',
                                                                                                            None)
        # reorder cols
        full_service_code_with_atco = full_service_code_with_atco[
            ['service_code', 'LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods', 'atco_code', 'in_otc',
             'in_bods', 'auth_description_otc', 'Admin Area Name associated with ATCO Code']]
        return full_service_code_with_atco

    def services_on_bods_or_otc_by_area_mi_just_otc(self):
        '''
        Generates MI of all service codes published
        in the OTC database, indicates whether they are published on
        BODS as well, and provides the admin area the services has stops within
        Note - only to be run on all published datasets
        '''

        full_service_code_with_atco = TimetableExtractor.services_on_bods_or_otc_by_area_just_otc(self)
        service_atco_mi = full_service_code_with_atco.groupby('atco_code').agg({'service_code': 'count'
                                                                                   ,
                                                                                'Admin Area Name associated with ATCO Code': 'first'
                                                                                   , 'in_otc': 'mean'
                                                                                   , 'in_bods': 'mean'})

        service_atco_mi.rename(columns={'service_code': 'count_services'}, inplace=True)

        # round fractions to 2 decimals
        service_atco_mi = service_atco_mi.round(2)

        return service_atco_mi

    def extract_timetable_operating_days(self, days):
        ''' Ensuring the operating days are ordered appropriately '''

        if days is not None:
            operating_day_list = list(days)
            if any(day in {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"} for day in operating_day_list):
                pass
            else:
                operating_days=', '.join(operating_day_list)
                return operating_days

        else:
            operating_days= None
            return operating_days



        # adding dictionary variables and values to "day" dictionary

        day = {}
        day['Monday'] = 1
        day['Tuesday'] = 2
        day['Wednesday'] = 3
        day['Thursday'] = 4
        day['Friday'] = 5
        day['Saturday'] = 6
        day['Sunday'] = 7

        brand_new = {}

        # checking if the day of the week is in the above dictionary so we can sort the days
        for i in operating_day_list:
            if i in day:
                brand_new.update({i: day[i]})

            # sorting the days of operation
        sortit = sorted(brand_new.items(), key=lambda x: x[1])

        length = len(sortit)

        operating_days = ""

        consecutive = True

        # checking to see if the days in the list are not consective

        for i in range(length - 1):
            if sortit[i + 1][1] - sortit[i][1] != 1:
                consecutive = False
                break

        # if there are no days of operation entered
        if length == 0:
            operating_days = "None"

        # if there is only one day of operation
        elif length == 1:
            operating_days = sortit[0][0]

            # if the operating days are not consecutive, they're seperated by commas
        elif consecutive:
            for i in range(length):
                operating_days = operating_days + sortit[i][0] + ","

            # if consecutive, operating days are given as a range
        else:
            # print(sortit)
            operating_days = sortit[0][0] + "-" + sortit[-1][0]

        return operating_days

    def extract_runtimes(self, vj, journey_pattern_timing_link, vjtl_index):

        """Extract the runtimes from timing links as a string. If JPTL run time is 0, VJTL will be checked"""

        # extract run times from jptl
        runtime = journey_pattern_timing_link.RunTime

        # if jptl runtime is 0, find equivalent vehicle journey pattern timing link run time
        if pd.Timedelta(runtime).value == 0:

            if vj.VehicleJourneyTimingLink is not None:
                runtime = vj.VehicleJourneyTimingLink[vjtl_index[journey_pattern_timing_link.id]].RunTime

                return runtime

        return runtime

    def extract_common_name(self, stop_object, StopPointRef, stop_point_index):
        """Extract information about the name of the stop including longitude and latitude"""

        stop_common_name = stop_object.AnnotatedStopPointRef[stop_point_index[StopPointRef]].CommonName
        stop_location = stop_object.AnnotatedStopPointRef[stop_point_index[StopPointRef]].Location

        if not stop_location:
            stop_lat = "-"
            stop_long = "-"

        else:
            stop_lat = stop_object.AnnotatedStopPointRef[stop_point_index[StopPointRef]].Location.Latitude
            stop_long = stop_object.AnnotatedStopPointRef[stop_point_index[StopPointRef]].Location.Longitude

        return stop_common_name, stop_lat, stop_long

    def next_jptl_in_sequence(self, jptl, vj_departure_time, vj, vjtl_index, stop_object, stop_point_index,
                              first_jptl=False):
        """Returns the sequence number, stop point ref and time for a JPTL to be added to a outboud or inbound df"""

        runtime = self.extract_runtimes(vj, jptl, vjtl_index)
        common_name, latitude, longitude = self.extract_common_name(stop_object, str(jptl.To.StopPointRef),
                                                                    stop_point_index)

        to_sequence = [int(jptl.To.sequence_number),
                       str(jptl.To.StopPointRef),
                       latitude,
                       longitude,
                       str(common_name),
                       pd.Timedelta(runtime)]

        if first_jptl:
            common_name, latitude, longitude = self.extract_common_name(stop_object, jptl.From.StopPointRef,
                                                                        stop_point_index)

            from_sequence = [int(jptl.From.sequence_number),
                             str(jptl.From.StopPointRef),
                             latitude,
                             longitude,
                             str(common_name),
                             vj_departure_time]

            return from_sequence, to_sequence

        if not first_jptl:
            return to_sequence

    def collate_vjs(self, direction_df, collated_timetable):
        """Combines all vehicle journeys together for inbound or outbound"""

        if direction_df.empty:
            pass
        elif collated_timetable.empty:
            # match stop point ref + sequence number with the initial timetable's stop point ref+sequence number
            collated_timetable = collated_timetable.merge(direction_df, how='outer', left_index=True, right_index=True)

        else:

            if direction_df["Sequence Number"].equals(collated_timetable["Sequence Number"]) and direction_df['Stop Point Ref'].equals(collated_timetable['Stop Point Ref']) and direction_df["Common Name"].equals(collated_timetable["Common Name"]):
                last_column = direction_df.iloc[:, -1]
                collated_timetable=pd.concat([collated_timetable, last_column], axis=1)

            else:
                # match stop point ref + sequence number with the initial timetable's stop point ref+sequence number
                collated_timetable = pd.merge(collated_timetable, direction_df, on=["Sequence Number",
                                                                                'Stop Point Ref',
                                                                                "Latitude",
                                                                                "Longitude",
                                                                                "Common Name"],
                                              validate='1:m', how='outer').fillna("-")

        return collated_timetable

    def reformat_times(self, timetable, vj, base_time):
        '''Turns the time deltas into time of day, final column is formatted as string'''

        timetable[f"{vj.VehicleJourneyCode}"] = timetable[f"{vj.VehicleJourneyCode}"].cumsum()
        timetable[f"{vj.VehicleJourneyCode}"] = timetable[f"{vj.VehicleJourneyCode}"].map(lambda x: x + base_time)
        timetable[f"{vj.VehicleJourneyCode}"] = timetable[f"{vj.VehicleJourneyCode}"].map(lambda x: x.strftime('%H:%M'))

        return timetable[f"{vj.VehicleJourneyCode}"]

    def add_dataframe_headers(self, direction, operating_days, JourneyPattern_id, RouteRef, lineref, JourneyCode):
        """Populate headers with information associated to each individual VJ"""

        direction.loc[-1] = ["Operating Days ", "->", "->", "->", "->", operating_days]
        direction.loc[-2] = ["Journey Pattern ", "->", "->", "->", "->", JourneyPattern_id]
        direction.loc[-3] = ["Journey Code", "->", "->", "->", "->", JourneyCode]
        direction.loc[-4] = ["RouteID", "->", "->", "->", "->", RouteRef]
        direction.loc[-5] = ["Line", "->", "->", "->", "->", lineref]
        direction.index = direction.index + 1  # shifting index
        direction.sort_index(inplace=True)

        return direction

    def create_journey_pattern_section_object(self, journey_pattern_json):

        if isinstance(journey_pattern_json['JourneyPatternSection'], dict):
            journey_pattern_json['JourneyPatternSection'] = [journey_pattern_json['JourneyPatternSection']]

        journey_pattern_section_object = from_dict(data_class=JourneyPatternSections, data=journey_pattern_json)

        return journey_pattern_section_object

    def create_vehicle_journey_object(self, vehicle_journey_json):

        if isinstance(vehicle_journey_json['VehicleJourney'], dict):
            vehicle_journey_json['VehicleJourney'] = [vehicle_journey_json['VehicleJourney']]

        vehicle_journey = from_dict(data_class=VehicleJourneys, data=vehicle_journey_json)

        return vehicle_journey

    def create_service_object(self, services_json):

        if isinstance(services_json['Lines']['Line'], dict):
            services_json['Lines']['Line'] = [services_json['Lines']['Line']]

        if isinstance(services_json['StandardService']['JourneyPattern'], dict):
            services_json['StandardService']['JourneyPattern'] = [services_json['StandardService']['JourneyPattern']]

        service_object = from_dict(data_class=Service, data=services_json)

        return service_object

    def create_stop_object(self, stops_json):

        stop_object = from_dict(data_class=StopPoints, data=stops_json)

        return stop_object

    def map_indicies(self, service_object, stop_object, journey_pattern_section_object):
        """Initialise values to be used when generating timetables"""

        # List of journey patterns in service object
        journey_pattern_list = service_object.StandardService.JourneyPattern

        # Iterate once through JPs and JPS to find the indices in the list of each id
        journey_pattern_index = {key.id: value for value, key in
                                 enumerate(service_object.StandardService.JourneyPattern)}
        journey_pattern_section_index = {key.id: value for value, key in
                                         enumerate(journey_pattern_section_object.JourneyPatternSection)}

        # Map stop point refs
        stop_point_index = {key.StopPointRef: value for value, key in enumerate(stop_object.AnnotatedStopPointRef)}

        return journey_pattern_section_index, journey_pattern_index, journey_pattern_list, stop_point_index

    def create_txc_objects(self):

        self.stop_level_extract = self.service_line_extract_with_stop_level_json.copy(deep=True)

        self.stop_level_extract['vj_objects'] = self.stop_level_extract[
            'vehicle_journey_json'].apply(self.create_vehicle_journey_object)

        self.stop_level_extract['jps_objects'] = self.stop_level_extract[
            'journey_pattern_json'].apply(self.create_journey_pattern_section_object)

        self.stop_level_extract['service_object'] = self.stop_level_extract[
            'services_json'].apply(self.create_service_object)

        self.stop_level_extract['stop_objects'] = self.stop_level_extract[
            'stops_json'].apply(self.create_stop_object)

    def iterate_vjs(self, service_object, stop_object, vehicle_journey, journey_pattern_section_object,
                    collated_timetable_outbound, collated_timetable_inbound, base_time, journey_pattern_section_index,
                    journey_pattern_index, journey_pattern_list, stop_point_index):

        for vj in vehicle_journey.VehicleJourney:

            # take vehicle journey departure time
            departure_time = pd.Timedelta(vj.DepartureTime)

            vj_columns = ["Sequence Number", "Stop Point Ref", "Latitude", "Longitude", "Common Name",
                          f"{vj.VehicleJourneyCode}"]

            # init empty timetable for outbound Vehicle journey
            outbound = pd.DataFrame(columns=vj_columns)

            # init empty timetable for inbound Vehicle journey
            inbound = pd.DataFrame(columns=vj_columns)

            if vj.LineRef[-1] == ":":
                lineref = vj.LineRef.split(':')[-2]
            else:
                lineref = vj.LineRef.split(':')[-1]

            vjtl_index = {}

            if vj.VehicleJourneyTimingLink is not None:
                vjtl_index = {key.JourneyPatternTimingLinkRef: value for value, key in
                              enumerate(vj.VehicleJourneyTimingLink)}

            # Create vars for relevant indices of this vehicle journey
            vehicle_journey_jp_index = journey_pattern_index[vj.JourneyPatternRef]

            direction = service_object.StandardService.JourneyPattern[vehicle_journey_jp_index].Direction
            RouteRef = service_object.StandardService.JourneyPattern[vehicle_journey_jp_index].RouteRef
            JourneyPattern_id = service_object.StandardService.JourneyPattern[vehicle_journey_jp_index].id
            if vj.Operational is None or vj.Operational.TicketMachine is None:
                JourneyCode=None
            else:
                JourneyCode=str(vj.Operational.TicketMachine.JourneyCode)

            # Get the journey pattern sections for this vehicle journey, can be a single string or a list of strings
            vehicle_journey_jps_list = service_object.StandardService.JourneyPattern[
                journey_pattern_index[vj.JourneyPatternRef]].JourneyPatternSectionRefs

            # If there are multiple JPS, put them into a single list for later
            if isinstance(vehicle_journey_jps_list, list):
                vehicle_journey_jps_index = [journey_pattern_section_index[jpsr] for jpsr in
                                             journey_pattern_list[vehicle_journey_jp_index].JourneyPatternSectionRefs]
                all_jptl = [
                    [jptl for jptl in journey_pattern_section_object.JourneyPatternSection[x].JourneyPatternTimingLink]
                    for x in vehicle_journey_jps_index]
                flat_jptl = [jptl for sublist in all_jptl for jptl in sublist]

            # if just one JPS, find the journey pattern section directly for later
            else:
                vehicle_journey_jps_index = journey_pattern_section_index[vehicle_journey_jps_list]
                flat_jptl = journey_pattern_section_object.JourneyPatternSection[
                    vehicle_journey_jps_index].JourneyPatternTimingLink

            # Mark the first JPTL
            first = True

            # Loop through relevant timing links

            for JourneyPatternTimingLink in flat_jptl:

                # first JPTL should use 'From' AND 'To' stop data
                if first:

                    # remaining JPTLs are not the first one
                    first = False

                    timetable_sequence = self.next_jptl_in_sequence(JourneyPatternTimingLink,
                                                                    departure_time,
                                                                    vj,
                                                                    vjtl_index,
                                                                    stop_object,
                                                                    stop_point_index,
                                                                    first_jptl=True)

                    # Add first sequence, stop ref and departure time
                    first_timetable_row = pd.DataFrame([timetable_sequence[0]], columns=outbound.columns)

                    # Add To sequence number and stop point ref to the initial timetable

                    if direction == 'outbound':
                        outbound = pd.concat([outbound, first_timetable_row], ignore_index=True)
                        outbound.loc[len(outbound)] = timetable_sequence[1]

                    elif direction == 'inbound':
                        inbound = pd.concat([inbound, first_timetable_row], ignore_index=True)
                        inbound.loc[len(inbound)] = timetable_sequence[1]
                    else:
                        print(f'Unknown Direction in vehicle journey:{vj}: {direction}')

                # if not first JPTL use 'to' sequence only
                else:
                    timetable_sequence = self.next_jptl_in_sequence(JourneyPatternTimingLink, departure_time, vj,
                                                                    vjtl_index,
                                                                    stop_object, stop_point_index)

                    if direction == 'outbound':
                        outbound.loc[len(outbound)] = timetable_sequence
                    elif direction == 'inbound':
                        inbound.loc[len(inbound)] = timetable_sequence
                    else:
                        print(f'Unknown Direction in vehicle journey:{vj}: {direction}')

            # Fetch operating profile from either service object or vehicle journey
           # if vj.OperatingProfile is None and service_object.OperatingProfile is not None:
           #     days = service_object.OperatingProfile.RegularDayType.DaysOfWeek
           # elif vj.OperatingProfile is not None:
           #     days = vj.OperatingProfile.RegularDayType.DaysOfWeek

            if vj.OperatingProfile is not None:
                if vj.OperatingProfile.RegularDayType.DaysOfWeek is not None:
                    days = vj.OperatingProfile.RegularDayType.DaysOfWeek
                elif vj.OperatingProfile.BankHolidayOperation is not None:
                    days = vj.OperatingProfile.BankHolidayOperation.DaysOfOperation

                else:
                    days = "Days Not Found"

            elif service_object.OperatingProfile is not None:
                if service_object.OperatingProfile.RegularDayType.DaysOfWeek is not None:
                    days = service_object.OperatingProfile.RegularDayType.DaysOfWeek
                elif service_object.OperatingProfile.BankHolidayOperation is not None:
                    days = service_object.OperatingProfile.BankHolidayOperation.DaysOfOperation
                else:
                    days = "Days Not Found"
            else:
                days = "Days Not Found"

            if days is None and (service_object.OperatingProfile is None):
                operating_days = "Error: Check File"
            else:
                operating_days = self.extract_timetable_operating_days(days)

            if not outbound.empty:
                outbound[f"{vj.VehicleJourneyCode}"] = self.reformat_times(outbound, vj, base_time)
                outbound = self.add_dataframe_headers(outbound, operating_days, JourneyPattern_id, RouteRef, lineref,JourneyCode)

            if not inbound.empty:
                inbound[f"{vj.VehicleJourneyCode}"] = self.reformat_times(inbound, vj, base_time)
                inbound = self.add_dataframe_headers(inbound, operating_days, JourneyPattern_id, RouteRef, lineref,JourneyCode)

            # collect vj information together for outbound
            collated_timetable_outbound = self.collate_vjs(outbound, collated_timetable_outbound)

            # collect vj information together for inbound
            collated_timetable_inbound = self.collate_vjs(inbound, collated_timetable_inbound)

        collated_timetable_outbound, collated_timetable_inbound = self.organise_timetables(service_object,
                                                                                           collated_timetable_outbound,
                                                                                           collated_timetable_inbound)

        return collated_timetable_outbound, collated_timetable_inbound

    def generate_timetable(self):

        """Extracts timetable information for a VJ individually and
        adds to a collated dataframe of vjs, split by outbound and inbound"""
        print('Generating Timetables...')
        # Define a base time to add run times to
        base_time = datetime.datetime(2000, 1, 1, 0, 0, 0)

        # Dataframe to store all inbound vjs together
        self.collated_timetable_inbound = pd.DataFrame()

        # Dataframe to store all outbound vjs together
        self.collated_timetable_outbound = pd.DataFrame()

        self.create_txc_objects()

        print('Mapping service indexes...')
        # Map Indicies based on objects present on dataframe row
        self.stop_level_extract[
            ['jps_index', 'jpindex', 'jplist', 'stop_index']] = self.stop_level_extract.apply(
            lambda x: TimetableExtractor.map_indicies(self, x.service_object, x.stop_objects,
                                                      x.jps_objects), axis=1).apply(pd.Series)
        print('Calculating vehicle journeys...')
        # Create Inbound and Outbound Timetables based on objects and indices
        self.stop_level_extract[
            ['collated_timetable_outbound', 'collated_timetable_inbound']] = self.stop_level_extract.apply(
            lambda x: TimetableExtractor.iterate_vjs(self, x.service_object,
                                                     x.stop_objects,
                                                     x.vj_objects,
                                                     x.jps_objects,
                                                     self.collated_timetable_outbound,
                                                     self.collated_timetable_inbound,
                                                     base_time,
                                                     x.jps_index,
                                                     x.jpindex,
                                                     x.jplist,
                                                     x.stop_index), axis=1).apply(pd.Series)
        # Reduce size of stop level extract
        self.stop_level_extract = self.stop_level_extract.drop(
            columns=['dq_score', 'OperatorShortName', 'Status', 'services_json', 'NOC', 'PublicUse', 'TradingName',
                     'SchemaVersion', 'OperatorName', 'LicenceNumber', 'Origin', 'vehicle_journey_json', 'OperatorCode',
                     'Destination', 'dq_rag', 'Description', 'Comment', 'FileType', 'bods_compliance',
                     'journey_pattern_json', 'stops_json', 'OperatingPeriodEndDate', 'la_code', 'vj_objects',
                     'jps_objects', 'service_object', 'stop_objects', 'jps_index', 'jpindex', 'jplist', 'stop_index'])
        print('Timetables Generated!')

        #self.stop_level_extract = self.stop_level_extract.to_dict()

        return self.stop_level_extract

    def organise_timetables(self, service_object, collated_timetable_outbound, collated_timetable_inbound):
        """Ordering the timetables correctly"""

        service_code = str(service_object.ServiceCode)

        if not collated_timetable_outbound.empty:

            # ensuring the sequence numbers are sorted in ascending order
            collated_timetable_outbound.iloc[5:] = collated_timetable_outbound.iloc[5:].sort_values(by="Sequence Number"
                                                                                                    , ascending=True)

        if not collated_timetable_inbound.empty:

            collated_timetable_inbound.iloc[5:] = collated_timetable_inbound.iloc[5:].sort_values(by="Sequence Number"
                                                                                                  , ascending=True)

        return collated_timetable_outbound, collated_timetable_inbound


class xmlDataExtractor:

    def __init__(self, filepath):
        self.root = ET.parse(filepath).getroot()
        self.namespace = self.root.nsmap

    def extract_service_level_info(self):
        filename = self.extract_filename()
        noc = self.extract_noc()
        trading_name = self.extract_trading_name()
        licence_number = self.extract_licence_number()
        operator_short_name = self.extract_operator_short_name()
        operator_code = self.extract_operator_code()
        service_code = self.extract_service_code()
        line_name = self.extract_line_name()
        public_use = self.extract_public_use()
        operating_days = self.extract_operating_days()
        service_origin = self.extract_service_origin()
        service_destination = self.extract_service_destination()
        operating_period_start_date = self.extract_operating_period_start_date()
        operating_period_end_date = self.extract_operating_period_end_date()
        schema_version = self.extract_schema_version()
        revision_number = self.extract_revision_number()
        la_code = self.extract_la_code()

        return [
            filename,
            noc,
            trading_name,
            licence_number,
            operator_short_name,
            operator_code,
            service_code,
            line_name,
            public_use,
            operating_days,
            service_origin,
            service_destination,
            operating_period_start_date,
            operating_period_end_date,
            schema_version,
            revision_number,
            la_code,
        ]

    def extract_filename(self):

        ''''
        Return the filename of a file extracted from the BODS platform.
        '''

        try:

            return self.root.attrib['FileName']

        except:
            return 'Not Found'

    def extract_noc(self):

        ''''
        Extracts the National Operator Code from an xml file in a given location with a known namespace
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//NationalOperatorCode', self.namespace)

        # iterate through the data element and add the text to a list of NOCs
        noc = [i.text for i in data]

        return noc

    def extract_trading_name(self):

        '''
        Extracts the Trading Name from an xml file in a given location with a known namespace
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//TradingName', self.namespace)

        trading_name = [i.text for i in data]

        return trading_name

    def extract_licence_number(self):

        '''
        Extracts the Licence Number from an xml file in a given location with a known namespace
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//LicenceNumber', self.namespace)

        licence_number = [i.text for i in data]

        return licence_number

    def extract_operator_short_name(self):

        '''
        Extracts the Operator Short Name from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//OperatorShortName', self.namespace)

        operator_short_name = [i.text for i in data]

        return operator_short_name

    def extract_operator_code(self):

        '''
        Extracts the Operator Code from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//OperatorCode', self.namespace)

        operator_code = [i.text for i in data]

        return operator_code

    def extract_service_code(self):

        '''
        Extracts the Service Code from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/ServiceCode', self.namespace)

        service_code = [i.text for i in data]

        return service_code

    def extract_line_name(self):

        '''
        Extracts the Line Name from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/Lines/Line/LineName', self.namespace)

        line_name = [i.text for i in data]

        return line_name

    def extract_public_use(self):

        '''
        Extracts the public use boolean from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/PublicUse', self.namespace)

        public_use = [i.text for i in data]

        return public_use

    def extract_operating_days(self):

        '''
        Extracts the regular operating days from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''
        # find all text in the given xpath, return as a element object
        # Check service line level first
        data = self.root.findall("Services//Service/OperatingProfile/RegularDayType/DaysOfWeek/", self.namespace)

        # if empty we proceed to service line level
        if data == []:
            data = self.root.findall("VehicleJourneys//VehicleJourney/OperatingProfile/RegularDayType/DaysOfWeek/",
                                     self.namespace)
        else:
            pass

        daysoperating = []

        for count, value in enumerate(data):
            # convert each element into a string
            day = str(data[count])

            # only keep the element data associated with the day of the week
            changedday = day[42:52]

            # split up the weekday string
            before, sep, after = changedday.partition(' ')

            # keep the string data before the partition mentioned above
            changedday = before

            daysoperating.append(changedday)

        # remove duplicates from the operating days extracted
        setoperatingdays = set(daysoperating)

        # change the operating days into a list format so they can be ordered
        operating_day_list = list(setoperatingdays)

        # adding dictionary variables and values to "day" dictionary

        day = {}
        day['Monday'] = 1
        day['Tuesday'] = 2
        day['Wednesday'] = 3
        day['Thursday'] = 4
        day['Friday'] = 5
        day['Saturday'] = 6
        day['Sunday'] = 7

        brand_new = {}

        # checking if the day of the week is in the above dictionary so we can sort the days
        for i in operating_day_list:
            if i in day:
                brand_new.update({i: day[i]})

        # sorting the days of operation
        sortit = sorted(brand_new.items(), key=lambda x: x[1])

        length = len(sortit)

        operating_days = ""

        consecutive = True

        # checking to see if the days in the list are not consective

        for i in range(length - 1):
            if sortit[i + 1][1] - sortit[i][1] != 1:
                consecutive = False
                break

        # if there are no days of operation entered
        if length == 0:
            operating_days = "Other"

        # if there is only one day of operation
        elif length == 1:
            operating_days = sortit[0][0]

        # if the operating days are not consecutive, they're seperated by commas
        elif consecutive == False:
            for i in range(length):
                operating_days = operating_days + sortit[i][0] + ","

        # if consecutive, operating days are given as a range
        else:
            # print(sortit)
            operating_days = sortit[0][0] + "-" + sortit[-1][0]

        operating_days = [operating_days]

        return operating_days

    def extract_service_origin(self):

        '''
        Extracts the service origin from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/StandardService/Origin', self.namespace)

        service_origin = [i.text for i in data]

        return service_origin

    def extract_service_destination(self):

        '''
        Extracts the service destination from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/StandardService/Destination', self.namespace)

        service_destination = [i.text for i in data]

        return service_destination

    def extract_operating_period_start_date(self):

        '''
        Extracts the service start date from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/OperatingPeriod/StartDate', self.namespace)

        operating_period_start_date = [i.text for i in data]

        return operating_period_start_date

    def extract_operating_period_end_date(self):

        '''
        Extracts the service end date from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/OperatingPeriod/EndDate', self.namespace)

        operating_period_end_date = [i.text if len(i.text) > 0 else 'No Data' for i in data]

        return operating_period_end_date

    def extract_schema_version(self):

        '''
        Extracts the schema version from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        try:

            return self.root.attrib['SchemaVersion']

        except:
            return 'Not Found'

    def extract_revision_number(self):

        '''
        Extracts the schema version from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        try:
            return self.root.attrib['RevisionNumber']
        except:
            return 'Not Found'

    def extract_la_code(self):

        '''
        Extracts the first 3 characters of the ATCO code (unique stop ref) from an xml file in a
        given location with a known namespace. First 3 characters correspond to the asspciate Admin
        Area. Namespace can be found in constants.py and depends on if data is timetable or fares data

        '''

        # find all text in the given xpath, return as a element object
        # atco_first_3_letters = []

        data = self.root.findall(f'StopPoints//StopPointRef', self.namespace)

        atco_first_3_letters = [i.text[0:3] for i in data]

        unique_atco_first_3_letters = list(set(atco_first_3_letters))

        return unique_atco_first_3_letters
