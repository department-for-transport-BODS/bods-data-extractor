import pandas as pd
import json
import requests
import zipfile
import io
import os
from bods_client.client import BODSClient
from bods_client.models import timetables
import lxml.etree as ET
import xmltodict
import itertools
from itertools import zip_longest, product
import numpy as np
from pathlib import Path
from sys import platform
# try except ensures that this reads in lookup file whether pip installing the library, or cloning the repo from GitHub
try:
    import BODSDataExtractor.otc_db_download as otc_db_download
except:
    import otc_db_download
from datetime import date
import datetime
from collections import Counter
import importlib.resources

from shapely.geometry import Point
from geopandas import GeoDataFrame
import plotly.express as px
import plotly.io as pio


class TimetableExtractor:


    error_list = []

    def __init__(self, api_key, limit=10000, nocs=None, status='published', search=None, bods_compliant=True, atco_code=None, service_line_level=False, stop_level=False):
        self.api_key = api_key
        self.limit = limit
        self.nocs = nocs
        self.status = status
        self.search = search
        self.bods_compliant = bods_compliant
        self.atco_code = atco_code
        self.service_line_level = service_line_level
        self.stop_level = stop_level
        self.pull_timetable_data()
        self.otc_db = otc_db_download.fetch_otc_db()

        
        if service_line_level == True and stop_level == True:
            self.analytical_timetable_data()
            self.analytical_timetable_data_analysis()
            self.generate_timetable()
        else:
            pass
            

        if service_line_level == True and stop_level == False:
            self.analytical_timetable_data()
            self.analytical_timetable_data_analysis()
        else:
            pass

        if stop_level == True and  service_line_level == False:
            self.analytical_timetable_data()
            self.analytical_timetable_data_analysis()
            self.generate_timetable()

        # self.service_line_extract = service_line_extract


    def create_zip_level_timetable_df(self, response):

        """This function takes the json api response file 
        and returns it as a pandas dataframe"""

        j = response.json()
        j1 = json.loads(j)
        df = pd.json_normalize(j1['results'])
        return df

    def determine_file_type(self ,url):
        '''downloads a file from a url and returns the extension'''

        response = requests.head(url)
        try:
            filename = response.headers["Content-Disposition"].split('"')[1]
            extension = filename.split('.')[-1]

            return extension
        except:
            return 'error in filename'

    def extract_dataset_level_atco_codes(self):

        #initiate list for atco codes
        atco_codes = []
        #extract atco code from admin_area list of dicts on each row
        for r in self.metadata['admin_areas']:
            atco_codes.append([d['atco_code'] for d in r])

        #flatten list of lists to list, and ensure only unique values
        atco_codes = list(set((itertools.chain(*atco_codes))))

        return atco_codes

    def pull_timetable_data(self):

        '''Combines a number of functions to call the BODS API, 
        set the limit for number of records to return 
        and returns the json output as a dataframe
        '''

        #instantiate a BODSClient object
        bods = BODSClient(api_key = self.api_key)

        params = timetables.TimetableParams(limit = self.limit
                                            , nocs = self.nocs
                                            , status = self.status
                                            , search = self.search
                                            )

        #set params of get_timetable_datasets method
        print(f"Fetching timetable metadata for up to {self.limit} datasets...\n")
        data = bods.get_timetable_datasets(params = params)

        #convert the json output into a dataframe

        self.metadata = TimetableExtractor.create_zip_level_timetable_df(self, data)
        print(f"metadata downloaded for {len(self.metadata['url'])} records, converting to dataframe...\n")

        print('appending filetypes...\n')
        self.metadata['filetype'] = [TimetableExtractor.determine_file_type(self.metadata,x) for x in self.metadata['url']]

        #limit to just bods compliant files if requested
        if self.bods_compliant == True:
            self.metadata = self.metadata[self.metadata['bods_compliance']==True]
        else:
            pass

        #limit results to specific atco codes if requested
        if self.atco_code is not None:
            #atco codes are stored within a list of dicts in the api response - need to extract these
            #because of this, must process in a separate dataframe to the output metadata df
            exploded_metadata = self.metadata.copy()
            exploded_metadata['admin_areas'] =  exploded_metadata['admin_areas'].apply(lambda x: [d['atco_code'] for d in x])
            #atco codes extracted into list; need to explode these out so one atco code per row
            exploded_metadata = TimetableExtractor.xplode(exploded_metadata,['admin_areas'])
            #use exploded out atco codes to filter for only the requested ones
            exploded_metadata = exploded_metadata[exploded_metadata['admin_areas'].isin(self.atco_code)]

            #filter the output metadata dataframe by the atco codes
            self.metadata = self.metadata[self.metadata['id'].isin(exploded_metadata['id'])]
        else:
            pass

        return self.metadata



    def xml_metadata(self, url, error_list):

        '''' This function assumes the file at the end of the URL is an xml file.
             If so, it returns the filename, size and url link as a tuple. If
             file type is invalid it will print an error and skip.
             
             Arguments: URL and a list in which to pass urls which could not be treated as xmls
        '''

        try:
            resp = requests.get(url)

            #create a temporary file in the local directory to get the file size
            with open(r'temp.xml', 'w', encoding="utf-8") as file:
                file.write(resp.text)
                size = os.path.getsize(r'temp.xml')

                #dig into the headers of the file to pull the file name
                meta = resp.headers
                filename = str(meta["Content-Disposition"].split('"')[1])

            print("xml file found and appended to list\n")
            return(filename, size, url)


        except:
            #if we reach this then the filetype may not be xml or zip and needs investigating
            print(f"*****Error in dataset. Please check filetype: {url}*****\n")
            TimetableExtractor.error_list.append(url)
            pass

    def download_extract_zip(self, url):

        """
        Download a ZIP file and extract the relevant contents
        of each xml file within into a dataframe

        """

        output = []

        print(f"Fetching zip file from {url} in metadata table...\n")
        response = requests.get(url)

        #unizp the zipfile
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:

            #loop through files in the zip
            for zipinfo in thezip.infolist():

                extension = zipinfo.filename.split('.')[-1]

                #if the filename has an 'xml' extension
                if extension == 'xml':

                    xml_output = []

                    #open each file (assumed to be XML file)
                    with thezip.open(zipinfo) as thefile:

                        try:

                            #note the url
                            xml_output.append(url)

                            #Creating xml data object
                            xml_data = xmlDataExtractor(thefile)

                            #extract data from xml 
                            filename = xmlDataExtractor.extract_filename(xml_data)
                            xml_output.append(filename)

                            noc = xmlDataExtractor.extract_noc(xml_data)
                            xml_output.append(noc)


                            trading_name = xmlDataExtractor.extract_trading_name(xml_data)
                            xml_output.append(trading_name)

                            licence_number = xmlDataExtractor.extract_licence_number(xml_data)
                            xml_output.append(licence_number)


                            operator_short_name = xmlDataExtractor.extract_operator_short_name(xml_data)
                            xml_output.append(operator_short_name)


                            operator_code = xmlDataExtractor.extract_operator_code(xml_data)
                            xml_output.append(operator_code)


                            service_code = xmlDataExtractor.extract_service_code(xml_data)
                            xml_output.append(service_code)


                            line_name = xmlDataExtractor.extract_line_name(xml_data)
                            xml_output.append(line_name)


                            public_use = xmlDataExtractor.extract_public_use(xml_data)
                            xml_output.append(public_use)
                            
                            
                            operating_days = xmlDataExtractor.extract_operating_days(xml_data)
                            xml_output.append(operating_days)


                            service_origin = xmlDataExtractor.extract_service_origin(xml_data)
                            xml_output.append(service_origin)


                            service_destination = xmlDataExtractor.extract_service_destination(xml_data)
                            xml_output.append(service_destination)


                            operating_period_start_date = xmlDataExtractor.extract_operating_period_start_date(xml_data)
                            xml_output.append(operating_period_start_date)


                            operating_period_end_date = xmlDataExtractor.extract_operating_period_end_date(xml_data)
                            xml_output.append(operating_period_end_date)


                            schema_version = xmlDataExtractor.extract_schema_version(xml_data)
                            xml_output.append(schema_version)


                            revision_number = xmlDataExtractor.extract_revision_number(xml_data)
                            xml_output.append(revision_number)

                            la_code = xmlDataExtractor.extract_la_code(xml_data)
                            xml_output.append(la_code)

                            #reset read cursor
                            thefile.seek(0)

                            #if stop level data is requested, then need the additional columns that contain jsons of the stop level info        
                            if self.stop_level == True:
# =============================================================================
#                             also read in xml as a text string
#                             this is required for extracting sections of the xml for further stop level extraction, not just elements or attribs
# =============================================================================
                                #reset read cursor
                                thefile.seek(0)
                                xml_text = thefile.read()
                                xml_json = xmltodict.parse(xml_text, process_namespaces=False, force_list=('JourneyPatternSection','JourneyPatternTimingLink','VehicleJourney','VehicleJourneyTimingLink'))

                                journey_pattern_json = xml_json['TransXChange']['JourneyPatternSections']['JourneyPatternSection']
                                xml_output.append(journey_pattern_json)

                                vehicle_journey_json = xml_json['TransXChange']['VehicleJourneys']['VehicleJourney']
                                xml_output.append(vehicle_journey_json)

                                services_json = xml_json['TransXChange']['Services']['Service']
                                xml_output.append(services_json)

                            else:
                                pass

                        except:
                            TimetableExtractor.error_list.append(url)

                        output.append(xml_output)

                else:
                    print(f'file extension in zip folder is {extension}, passing...\n')
                    pass


        #if stop level data is requested, then need the additional columns that contain jsons of the stop level info        
        if self.stop_level == True:
            output_df = pd.DataFrame(output
                                 , columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'OperatingDays', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','la_code','journey_pattern_json', 'vehicle_journey_json','services_json']
                                 )
        else:
            output_df = pd.DataFrame(output
                                     , columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'OperatingDays', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','la_code']
                                     )
        return output_df

    def download_extract_xml(self, url):

        """
        Download an xml file and extract its relevant contents into a dataframe
        """

        xml_output = []
        print(f"Fetching xml file from {url} in metadata table...\n")
        resp = requests.get(url)
        resp.encoding = 'utf-8-sig'

        #save the filea as an xml then reopen it to parse, this can and should be optimised 
        with open('temp.xml', 'w', encoding = 'utf-8') as file:
            file.write(resp.text)
            #size = os.path.getsize(r'temp.xml')

        with open(r'temp.xml', 'r', encoding = 'utf-8') as xml:

            #note the url
            xml_output.append(url)

            #create xml data object
            xml_data = xmlDataExtractor(xml)

            #extract data from xml 
            filename = xmlDataExtractor.extract_filename(xml_data)
            xml_output.append(filename)

            noc = xmlDataExtractor.extract_noc(xml_data)
            xml_output.append(noc)


            trading_name = xmlDataExtractor.extract_trading_name(xml_data)
            xml_output.append(trading_name)

            licence_number = xmlDataExtractor.extract_licence_number(xml_data)
            xml_output.append(licence_number)


            operator_short_name = xmlDataExtractor.extract_operator_short_name(xml_data)
            xml_output.append(operator_short_name)


            operator_code = xmlDataExtractor.extract_operator_code(xml_data)
            xml_output.append(operator_code)


            service_code = xmlDataExtractor.extract_service_code(xml_data)
            xml_output.append(service_code)


            line_name = xmlDataExtractor.extract_line_name(xml_data)
            xml_output.append(line_name)


            public_use = xmlDataExtractor.extract_public_use(xml_data)
            xml_output.append(public_use)
            
            
            operating_days = xmlDataExtractor.extract_operating_days(xml_data)
            xml_output.append(operating_days)
            

            service_origin = xmlDataExtractor.extract_service_origin(xml_data)
            xml_output.append(service_origin)


            service_destination = xmlDataExtractor.extract_service_destination(xml_data)
            xml_output.append(service_destination)


            operating_period_start_date = xmlDataExtractor.extract_operating_period_start_date(xml_data)
            xml_output.append(operating_period_start_date)


            operating_period_end_date = xmlDataExtractor.extract_operating_period_end_date(xml_data)
            xml_output.append(operating_period_end_date)
            

            schema_version = xmlDataExtractor.extract_schema_version(xml_data)
            xml_output.append(schema_version)


            revision_number = xmlDataExtractor.extract_revision_number(xml_data)
            xml_output.append(revision_number)

            la_code = xmlDataExtractor.extract_la_code(xml_data)
            xml_output.append(la_code)

            #if stop level data is requested, then need the additional columns that contain jsons of the stop level info        
            if self.stop_level == True:

# =============================================================================
#               also read in xml as a text string
#               this is required for extracting sections of the xml for further stop level extraction, not just elements or attribs
# =============================================================================
                xml.seek(0)
                xml_text = xml.read()
                xml_json = xmltodict.parse(xml_text, process_namespaces=False,  force_list=('JourneyPatternSection','JourneyPatternTimingLink','VehicleJourney','VehicleJourneyTimingLink'))

                journey_pattern_json = xml_json['TransXChange']['JourneyPatternSections']['JourneyPatternSection']
                xml_output.append(journey_pattern_json)

                vehicle_journey_json = xml_json['TransXChange']['VehicleJourneys']['VehicleJourney']
                xml_output.append(vehicle_journey_json)

                services_json = xml_json['TransXChange']['Services']['Service']
                xml_output.append(services_json)

            else:
                pass

        output_df = pd.DataFrame(xml_output).T

        if self.stop_level == True:
            output_df.columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse','OperatingDays', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','la_code','journey_pattern_json', 'vehicle_journey_json','services_json']
        else:
            output_df.columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse','OperatingDays', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','la_code']

        return output_df



    def fetch_xml_filenames(self):

        metadata_table = TimetableExtractor.pull_timetable_data(self)

        xml_filenames = TimetableExtractor.open_urls(self, metadata_table)

        full_table = metadata_table.merge(xml_filenames, on = 'url', how = 'left')

        return full_table

    def analytical_timetable_data(self):

        ''''
        Uses a collection of extraction functions to extract data from within xml files. 
        Some of these xml files are within zip files, and so these are processed differently.
        This extracted data is combined with the metadata of each file, and columns renamed to
        yield analytical ready timetable data
        '''

        #make the 3 tables
        tXC_columns = ['URL', 'DatasetID', 'OperatorName','Description', 'Comment', 'Status', 'dq_score', 'dq_rag', 'bods_compliance', 'FileType']
        master_table = pd.DataFrame(columns = tXC_columns)
        zip_table = pd.DataFrame(columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','journey_pattern_json'] )
        xml_table = pd.DataFrame(columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','journey_pattern_json'] )


        metadata_table = self.metadata

        master_table[['URL', 'DatasetID', 'OperatorName' ,'Description', 'Comment', 'Status', 'dq_score', 'dq_rag', 'bods_compliance', 'FileType' ]] = metadata_table[['url', 'id', 'operator_name' ,'description', 'comment', 'status', 'dq_score', 'dq_rag', 'bods_compliance', 'filetype']]

        #handle xmls and zips differently - in case there are no xmls/zips to interogate pass
        xml_table = master_table.query('FileType == "xml"')
        xml_table = [TimetableExtractor.download_extract_xml(self, x) for x in xml_table['URL']]
        try:
            xml_table = pd.concat(xml_table)
        except:
            #empty dataframe required even if no xmls so concating dfs below does not break 
            xml_table = pd.DataFrame()


        zip_table = master_table.query('FileType == "zip"')
        zip_table = [TimetableExtractor.download_extract_zip(self, x) for x in zip_table['URL']]
        try:
            zip_table = pd.concat(zip_table)
        except:
            #empty dataframe required even if no zips so concating dfs below does not break 
            zip_table = pd.DataFrame()


        zip_xml_table = pd.concat([xml_table, zip_table])

        self.service_line_extract_with_stop_level_json = master_table.merge(zip_xml_table, how = 'outer', on = 'URL')

        #explode rows that are always just 1 value to get attributes out of lists
        self.service_line_extract_with_stop_level_json = TimetableExtractor.xplode(self.service_line_extract_with_stop_level_json,['NOC'
                                                                 ,'TradingName'
                                                                 ,'LicenceNumber'
                                                                 ,'OperatorShortName'
                                                                 ,'OperatorCode'
                                                                 ,'ServiceCode'
                                                                 ,'PublicUse'
                                                                 ,'OperatingDays'
                                                                 ,'Origin'
                                                                 ,'Destination'
                                                                 ,'OperatingPeriodStartDate'
                                                                 ,'OperatingPeriodEndDate'
                                                                 ])
        #explode rows that might be mulitple values
        self.service_line_extract_with_stop_level_json = TimetableExtractor.xplode(self.service_line_extract_with_stop_level_json,['LineName'])

        #explode rows that might be mulitple values
        self.service_line_extract_with_stop_level_json = TimetableExtractor.xplode(self.service_line_extract_with_stop_level_json,['la_code'])

        self.service_line_extract_with_stop_level_json.to_csv('output.csv')

        self.service_line_extract_with_stop_level_json['dq_score'] = self.service_line_extract_with_stop_level_json['dq_score'].str.rstrip('%').astype('float')

        print(f'The following URLs failed: {TimetableExtractor.error_list}')

# =============================================================================
#         dataset level filtering by atco codes has already been handled if requested
#         however, within a dataset some service lines will not have stops within all atco codes
#         that the dataset as a whole has. Therefore now we filter again to return just specifc service
#         lines for requested atcos
# =============================================================================
        if self.atco_code is not None:
            self.service_line_extract_with_stop_level_json = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['la_code'].isin(self.atco_code)]
        else:
            pass

        return self.service_line_extract_with_stop_level_json


    def analytical_timetable_data_analysis(self):

        '''
        Returns a copy of the service line level data suitable for analysis. Omits the columns with jsons
        of the final stop level data required for further processing and stop level analysis, for 
        performance and storage sake. Also omits la_code column, as if user is not interested in 
        local authorities of services then this adds unnecessary duplication (one service line can be in
        multiple las.)

        '''

        #conditional logic required, as json cols dont exist if stop_level parameter != True

        
        if self.stop_level == True:
            self.service_line_extract = self.service_line_extract_with_stop_level_json.drop(['journey_pattern_json','vehicle_journey_json','services_json','la_code'],axis=1).drop_duplicates()
        else:
            self.service_line_extract = self.service_line_extract_with_stop_level_json.drop(['la_code'],axis=1).drop_duplicates()
        
        return self.service_line_extract


    def zip_or_xml(self, extension, url):

        """
        Dictates which extraction function to use based on whether the downloaded
        file from the BODS platform is an xml, or a zip file.
        """

        if extension == 'zip':
            TimetableExtractor.download_extract_zip(self, url)
        else:
            TimetableExtractor.download_extract_xml(self, url)

    def xplode(df, explode, zipped=True):

        """
        Explode out lists in dataframes
        """

        method = zip_longest if zipped else product

        rest = {*df} - {*explode}

        zipped = zip(zip(*map(df.get, rest)), zip(*map(df.get, explode)))
        tups = [tup + exploded
         for tup, pre in zipped
         for exploded in method(*pre)]

        return pd.DataFrame(tups, columns=[*rest, *explode])[[*df]]


# =============================================================================
#       FUNCTIONS FOR EXTRACTING STOP LEVEL DATA
# =============================================================================
    def unwrap_journey_pattern_json(self, json):

        """
        For each record in the service line level table, this function extracts journey pattern 
        (route pattern and time taken to travel from each stop) stop level info
        by unwrapping the json like structure in the line level table.
        This is used by the produce_stop_level_df_journey function to create a dataframe 
        of journey stop level info
        """

        #initiate empty lists for results to be appended to
        js_id = []
        jptl_id = []
        runtime = []
        stop_from = []
        stop_to = []
        sequence_number = []
        timingstatus = []

        #loop through the JourneyPatternSection elements within the JourneyPatternSections frame
        for js in range(0,len(json)):

            #loops through the JourneyPatternTimingLink elements within the JourneyPatternSection element to get relevant info
            for jptl in range(0,len(json[js]['JourneyPatternTimingLink'])):
                js_id.append(json[js]['@id'])
                jptl_id.append(json[js]['JourneyPatternTimingLink'][jptl]['@id'])
                runtime.append(json[js]['JourneyPatternTimingLink'][jptl]['RunTime'])
                stop_from.append(json[js]['JourneyPatternTimingLink'][jptl]['From']['StopPointRef'])
                stop_to.append(json[js]['JourneyPatternTimingLink'][jptl]['To']['StopPointRef'])
                # sequence no and timing status not mandated by schema so must use try except
                try:
                    sequence_number.append(json[js]['JourneyPatternTimingLink'][jptl]['From']['@SequenceNumber'])
                except:
                    sequence_number.append(np.NaN)
                try:
                    timingstatus.append(json[js]['JourneyPatternTimingLink'][jptl]['From']['TimingStatus'])
                except:
                    timingstatus.append(np.NaN)


        #zip these lists into a dict            
        journey_pattern_dict = {
            "JourneyPatternSectionID": js_id
            ,"journey_pattern_timing_link": jptl_id
            ,"stop_from": stop_from
            ,"stop_to": stop_to
            ,"sequence_number": sequence_number
            ,"timingstatus": timingstatus
            ,"runtime": runtime
            }

        return journey_pattern_dict


    def unwrap_vehicle_journey_json(self, json):

        """
        For each record in the service line level table, this function extracts vehicle journey 
        (specific vehicle depature times) 
        stop level info by unwrapping the json like structure in the line level table.
        This is used by the produce_stop_level_df_vehicle function to create a dataframe 
        of vehicle stop level info.
        """

        #initiate empty lists for results to be appended to
        VehicleJourneyCode = []
        JourneyPatternRef = []
        DepartureTime = []
        LineRef = []

        #loop through the VehicleJourney elements within the VehicleJourneys frame
        for v in range(0,len(json)):
            LineRef.append(json[v]['LineRef'])
            VehicleJourneyCode.append(json[v]['VehicleJourneyCode'])
            JourneyPatternRef.append(json[v]['JourneyPatternRef']  )
            DepartureTime.append(json[v]['DepartureTime'])

        #zip these lists into a dict                               
        vehicle_journey_dict = {
            "VehicleJourneyCode": VehicleJourneyCode
            ,"JourneyPatternRef": JourneyPatternRef
            ,"DepartureTime": DepartureTime
            ,"LineRef": LineRef
            }

        return vehicle_journey_dict


    #test to get run times where necessary
    def unwrap_vehicle_journey_json_for_runtime(self, json):

        """
        For each record in the service line level table, this function extracts vehicle journey 
        (specific vehicle depature times and cases time taken to travel from each stop) 
        stop level info by unwrapping the json like structure in the line level table.
        This is used by the produce_stop_level_df_vehicle function to create a dataframe 
        of vehicle stop level info.
        This differs from the the unwrap_vehicle_journey_json in that it handles xml files 
        where they contain runtime info in the vehicle frame, and not the journey pattern frame.
        """

        #initiate empty lists for results to be appended to
        JourneyPatternRef = []
        LineRef = []
        jptl_id = []
        runtime = []

        for v in range(0,len(json)):

            #loops through the VehicleJourneyTimingLink elements within the VehicleJourney element to get relevant info
             for vjtl in range(0,len(json[v]['VehicleJourneyTimingLink'])):
                LineRef.append(json[v]['LineRef'])
                JourneyPatternRef.append(json[v]['JourneyPatternRef']  )
                jptl_id.append(json[v]['VehicleJourneyTimingLink'][vjtl]['JourneyPatternTimingLinkRef'])
                runtime.append(json[v]['VehicleJourneyTimingLink'][vjtl]['RunTime'])


        #zip these lists into a dict            
        vehicle_journey_runtime_dict = {
            "JourneyPatternRef": JourneyPatternRef
            ,"LineRef": LineRef
            ,"journey_pattern_timing_link": jptl_id
            ,"runtime": runtime
            }

        return vehicle_journey_runtime_dict

    def unwrap_service_json(self, json):

        """
        For each record in the service line level table, this function extracts service
        info by unwrapping the json like structure in the line level table. This service info
        is used to join the vehicle journey and journey pattern data together.
        This is used by the produce_stop_level_df_service function to create a dataframe 
        of stop level info.
        """

        #initiate empty lists for results to be appended to
        jp_id = []
        jpsf_id = []

        #if there is just one journeypattern in a service frame, must treat as a dict, not a list
        if type(json['StandardService']['JourneyPattern']) is list:
            #list handling
            for jp in range(0,len(json['StandardService']['JourneyPattern'])):
                jp_id.append(json['StandardService']['JourneyPattern'][jp]['@id'])
                jpsf_id.append(json['StandardService']['JourneyPattern'][jp]['JourneyPatternSectionRefs'])
        else:
            #dict handling
            jp_id.append(json['StandardService']['JourneyPattern']['@id'])
            jpsf_id.append(json['StandardService']['JourneyPattern']['JourneyPatternSectionRefs'])

        #zip these lists into a dict
        service_pattern_dict = {
            #"ServiceCode":service_list
            "JourneyPattern_id": jp_id
            ,"JourneyPatternSectionRef": jpsf_id

            }

        return service_pattern_dict


    def produce_stop_level_df_journey(self):

        """
        Using the unwrap_journey_pattern_json function, this produces a dataframe 
        that describes the journey patterns detailed in each xml file
        """


        #select just relevant columns
        full_data_extract_no_la_code = self.service_line_extract_with_stop_level_json.drop('la_code',axis=1)
        full_data_extract_no_la_code = full_data_extract_no_la_code.drop_duplicates(subset=['URL','DatasetID','FileName','NOC','ServiceCode','ServiceCode','LineName','RevisionNumber','Origin','Destination']).reset_index()

        #initiate list for results to be added to
        stop_level_list_journey = []

        #loop through each record (xml file) in the service line level extract, and unwrap the journey pattern json
        for i in range(0,len(full_data_extract_no_la_code)):
            try:
                stop_level_list_journey_pre = TimetableExtractor.unwrap_journey_pattern_json(self, full_data_extract_no_la_code['journey_pattern_json'][i])
            except:
                print(full_data_extract_no_la_code['DatasetID'][i], full_data_extract_no_la_code['FileName'][i], 'journey extraction error')
                pass

            #add relevant meta data from that file to the unwrapped journey pattern details
            stop_level_list_journey_pre['DatasetID'] = full_data_extract_no_la_code['DatasetID'][i]
            stop_level_list_journey_pre['FileName'] = full_data_extract_no_la_code['FileName'][i]
            stop_level_list_journey_pre['ServiceCode'] = full_data_extract_no_la_code['ServiceCode'][i]
            stop_level_list_journey_pre['LineName'] = full_data_extract_no_la_code['LineName'][i]
            stop_level_list_journey_pre['RevisionNumber'] = full_data_extract_no_la_code['RevisionNumber'][i]

            #append results to list
            stop_level_list_journey.append(stop_level_list_journey_pre)

        #convert list to dataframe
        stop_level_df_journey = pd.DataFrame(stop_level_list_journey)

        #explode out lists in dataframe
        stop_level_df_journey = TimetableExtractor.xplode(stop_level_df_journey,['JourneyPatternSectionID'
                                          ,"journey_pattern_timing_link"
                                          ,"stop_from"
                                          ,"stop_to"
                                          ,"sequence_number"
                                          ,"timingstatus"
                                          ,"runtime"
                                             ])

        return stop_level_df_journey



    def produce_stop_level_df_vehicle(self):

        """
        Using the unwrap_journey_pattern_vehicle function, this produces a dataframe 
        that describes the vehicle journeys detailed in each xml file
        """

        #select just relevant columns
        full_data_extract_no_la_code = self.service_line_extract_with_stop_level_json.drop('la_code',axis=1)
        full_data_extract_no_la_code = full_data_extract_no_la_code.drop_duplicates(subset=['URL','DatasetID','FileName','NOC','ServiceCode','ServiceCode','LineName','RevisionNumber','Origin','Destination']).reset_index()

        #initiate list for results to be added to
        stop_level_list_vehicle = []

        #loop through each record (xml file) in the service line level extract, and unwrap the vehicle journey json
        for i in range(0,len(full_data_extract_no_la_code)):
            try:
                stop_level_list_vehicle_pre = TimetableExtractor.unwrap_vehicle_journey_json(self, full_data_extract_no_la_code['vehicle_journey_json'][i])
            except:
                print(full_data_extract_no_la_code['DatasetID'][i], full_data_extract_no_la_code['FileName'][i], 'vehicle extraction error')
                pass

            #add relevant meta data from that file to the unwrapped vehicle journey details
            stop_level_list_vehicle_pre['DatasetID'] = full_data_extract_no_la_code['DatasetID'][i]
            stop_level_list_vehicle_pre['FileName'] = full_data_extract_no_la_code['FileName'][i]
            stop_level_list_vehicle_pre['ServiceCode'] = full_data_extract_no_la_code['ServiceCode'][i]
            stop_level_list_vehicle_pre['LineName'] = full_data_extract_no_la_code['LineName'][i]
            stop_level_list_vehicle_pre['RevisionNumber'] = full_data_extract_no_la_code['RevisionNumber'][i]

            #append results to list
            stop_level_list_vehicle.append(stop_level_list_vehicle_pre)

        #convert list to dataframe
        stop_level_df_vehicle = pd.DataFrame(stop_level_list_vehicle)

        #explode out lists in dataframe
        stop_level_df_vehicle = TimetableExtractor.xplode(stop_level_df_vehicle,[
                                            "VehicleJourneyCode"
                                            ,"JourneyPatternRef"
                                            ,"DepartureTime"
                                            ,"LineRef"
                                              ])

        return stop_level_df_vehicle

    def produce_stop_level_df_vehicle_for_runtime(self):

        """
        Using the unwrap_journey_pattern_vehicle function, this produces a dataframe 
        that describes the vehicle journeys detailed in each xml file. 
        This differes from the produce_stop_level_df_vehicle in that it handles xml files 
        where they contain runtime info in the vehicle frame, and not the journey pattern frame.
        """
            
        #select just relevant columns
        full_data_extract_no_la_code = self.service_line_extract_with_stop_level_json.drop('la_code',axis=1)
        full_data_extract_no_la_code = full_data_extract_no_la_code.drop_duplicates(subset=['URL','DatasetID','FileName','NOC','ServiceCode','ServiceCode','LineName','RevisionNumber','Origin','Destination']).reset_index()

        #initiate list for results to be added to
        stop_level_list_vehicle = []

        #loop through each record (xml file) in the service line level extract, and unwrap the vehicle journey json
        for i in range(0,len(full_data_extract_no_la_code)):
            try:
                stop_level_list_vehicle_pre = TimetableExtractor.unwrap_vehicle_journey_json_for_runtime(self, full_data_extract_no_la_code['vehicle_journey_json'][i])
            except:
                # print(full_data_extract_no_la_code['DatasetID'][i], full_data_extract_no_la_code['FileName'][i], 'vehicle for runtime extraction error')
                stop_level_list_vehicle_pre = {'JourneyPatternRef':'N/A','LineRef':'N/A','journey_pattern_timing_link':'N/A','runtime':'N/A'}

            #add relevant meta data from that file to the unwrapped vehicle journey details
            stop_level_list_vehicle_pre['DatasetID'] = full_data_extract_no_la_code['DatasetID'][i]
            stop_level_list_vehicle_pre['FileName'] = full_data_extract_no_la_code['FileName'][i]
            stop_level_list_vehicle_pre['ServiceCode'] = full_data_extract_no_la_code['ServiceCode'][i]
            stop_level_list_vehicle_pre['LineName'] = full_data_extract_no_la_code['LineName'][i]
            stop_level_list_vehicle_pre['RevisionNumber'] = full_data_extract_no_la_code['RevisionNumber'][i]

            #append results to list
            stop_level_list_vehicle.append(stop_level_list_vehicle_pre)

        #convert list to dataframe
        stop_level_df_vehicle = pd.DataFrame(stop_level_list_vehicle)

        #explode out lists in dataframe
        stop_level_df_vehicle = TimetableExtractor.xplode(stop_level_df_vehicle,[
                                                # "VehicleJourneyCode"
                                                "JourneyPatternRef"
                                                ,"LineRef"
                                                ,'journey_pattern_timing_link'
                                                ,'runtime'
                                                ])

        stop_level_df_vehicle = stop_level_df_vehicle.drop_duplicates()

        return stop_level_df_vehicle
    

    def produce_stop_level_df_service(self):

        """
        Using the unwrap_journey_pattern_service function, this produces a dataframe 
        that provides key info from the service frames in each xml file, that will
        allow vehicle journey and journey pattern info to be joined together.
        """

        #select just relevant columns
        full_data_extract_no_la_code = self.service_line_extract_with_stop_level_json.drop('la_code',axis=1)
        full_data_extract_no_la_code = full_data_extract_no_la_code.drop_duplicates(subset=['URL','DatasetID','FileName','NOC','ServiceCode','ServiceCode','LineName','RevisionNumber','Origin','Destination']).reset_index()

        #initiate list for results to be added to
        stop_level_list_service = []

        #loop through each record (xml file) in the service line level extract, and unwrap the service json
        for i in range(0,len(full_data_extract_no_la_code)):

            try:
                stop_level_list_service_pre = TimetableExtractor.unwrap_service_json(self, full_data_extract_no_la_code['services_json'][i])
            except:
                print(full_data_extract_no_la_code['DatasetID'][i], full_data_extract_no_la_code['FileName'][i],'service frame extraction error')
                pass

            #add relevant meta data from that file to the unwrapped service details
            stop_level_list_service_pre['DatasetID'] = full_data_extract_no_la_code['DatasetID'][i]
            stop_level_list_service_pre['FileName'] = full_data_extract_no_la_code['FileName'][i]
            stop_level_list_service_pre['ServiceCode'] = full_data_extract_no_la_code['ServiceCode'][i]
            stop_level_list_service_pre['LineName'] = full_data_extract_no_la_code['LineName'][i]
            stop_level_list_service_pre['RevisionNumber'] = full_data_extract_no_la_code['RevisionNumber'][i]

            #append results to list
            stop_level_list_service.append(stop_level_list_service_pre)

            #convert list to dataframe
        stop_level_df_service = pd.DataFrame(stop_level_list_service)

        #explode out lists in dataframe
        stop_level_df_service = TimetableExtractor.xplode(stop_level_df_service,['JourneyPattern_id', 'JourneyPatternSectionRef'])
        stop_level_df_service = stop_level_df_service.explode(['JourneyPatternSectionRef']).reset_index()
        del stop_level_df_service['index']

        return stop_level_df_service



    def fetch_naptan_data(self):

        '''
        Access NAPTAN API to fetch lat and long coordinates for all relevant stops.
        Edited to fetch of all stops to avoid bugs. This could be improved in future.
        '''

        #get list of relevant admin area codes, to target api call
        # atcos = list(self.service_line_extract_with_stop_level_json['la_code'].unique())
        # atcos = ",".join(atcos)

        #call naptan api
        # url = f"https://naptan.api.dft.gov.uk/v1/access-nodes?atcoAreaCodes={atcos}&dataFormat=csv"
        url = "https://naptan.api.dft.gov.uk/v1/access-nodes?&dataFormat=csv"

        r = requests.get(url).content
        naptan = pd.read_csv(io.StringIO(r.decode('utf-8')))

        #filter results to those needed (just lat and long)
        naptan = naptan[['ATCOCode','CommonName','Longitude','Latitude']]

        return naptan

    def join_stop_level_data(self):

        '''
        Stitch together the journey, vehicle and service stop level data, which
        are extracted and processed in other functions in this class.
        '''

        print('Extracting stop level data... \n')

        #call functions to extract stop level data
        vehicle_stop_level = TimetableExtractor.produce_stop_level_df_vehicle(self)
        vehicle_with_runtime_stop_level = TimetableExtractor.produce_stop_level_df_vehicle_for_runtime(self)
        journey_stop_level = TimetableExtractor.produce_stop_level_df_journey(self)
        service_stop_level = TimetableExtractor.produce_stop_level_df_service(self)


        #join vehicle stop level data to services (this will allow subsequent join to journey data)
        stop_level_joined = vehicle_stop_level.merge(service_stop_level[['JourneyPattern_id','JourneyPatternSectionRef','ServiceCode','LineName','RevisionNumber']]
                                                            ,how='left'
                                                            ,left_on=['JourneyPatternRef','ServiceCode','LineName','RevisionNumber']
                                                            ,right_on=['JourneyPattern_id','ServiceCode','LineName','RevisionNumber']).drop_duplicates()
        #remove extra col added in join
        del stop_level_joined['JourneyPattern_id']

        #join on each stop from journey frame
        stop_level_joined = stop_level_joined.merge(journey_stop_level[['JourneyPatternSectionID','ServiceCode','journey_pattern_timing_link','stop_from','stop_to','sequence_number','timingstatus','runtime','LineName','RevisionNumber' ]]#,'Longitude','Latitude']]
                                                    ,how='left'
                                                    ,left_on=['JourneyPatternSectionRef','ServiceCode','LineName','RevisionNumber']
                                                    ,right_on=['JourneyPatternSectionID','ServiceCode','LineName','RevisionNumber']).drop_duplicates()

        #remove extra col added in join
        del stop_level_joined['JourneyPatternSectionID']

        #remove null sequence numbers - in 1.1 release fix will ensure null seq numbers can be handled
        stop_level_joined = stop_level_joined[~stop_level_joined['sequence_number'].isnull()]

        #convert data type of seq number to int
        stop_level_joined['sequence_number'] = stop_level_joined['sequence_number'].astype('int')

        #join on vehicle for runtime frame to get runtimes where not in journey frame
        stop_level_joined = stop_level_joined.merge(vehicle_with_runtime_stop_level[['ServiceCode','JourneyPatternRef','journey_pattern_timing_link','runtime','LineName','RevisionNumber']]
                                                                                             ,how='left'
                                                                                             ,left_on=['ServiceCode','JourneyPatternRef','journey_pattern_timing_link','LineName','RevisionNumber']
                                                                                             ,right_on=['ServiceCode','JourneyPatternRef','journey_pattern_timing_link','LineName','RevisionNumber']
                                                                                             ).drop_duplicates()

        
        return stop_level_joined


    def clean_stop_level_data(self):

        '''
        Process and clean the joined stop level data, to return stop level data
        that has correct depature times for the start of each vehicle journey,
        and run time in minutes for subsequent stops.
        '''

        #call function to return joined stop level data
        stop_level_joined_clean = TimetableExtractor.join_stop_level_data(self)

        print('Cleaning stop level data... \n')

        #extract minute from runtime col (regex find numeric characters)
        stop_level_joined_clean['runtime_x'] = stop_level_joined_clean['runtime_x'].astype('str').str.extract(r'(\d+)')
        stop_level_joined_clean['runtime_y'] = stop_level_joined_clean['runtime_y'].astype('str').str.extract(r'(\d+)')

        #coalesce these cols - PTI logic dictates that if there is no runtime in the vehicle frame, then use the journey frame
        stop_level_joined_clean['runtime'] = stop_level_joined_clean['runtime_y'].combine_first(stop_level_joined_clean['runtime_x'])

        #remove unnecessary cols
        del stop_level_joined_clean['runtime_y']
        del stop_level_joined_clean['runtime_x']

        #convert to time delta
        stop_level_joined_clean['runtime'] = stop_level_joined_clean['runtime'].apply(lambda x: pd.Timedelta(minutes=int(x)))

        #get depature times for the first stop
        stop_level_joined_clean.loc[stop_level_joined_clean["sequence_number"] == 1, "runtime"] = stop_level_joined_clean['DepartureTime']

        return stop_level_joined_clean


    def pivot_clean_stop_level_data(self):

        '''
        Pivot the cleaned stop level data, to return stop level data in a more
        usable timetable format; with each unique vehicle journey as a column,
        and the service code, stop code and sequence number as row headers
        '''

        #call function to return joined and cleaned stop level data
        stop_level_clean_pivoted = TimetableExtractor.clean_stop_level_data(self)

        print('Pivoting stop level data... \n')

        #create composite key field within each dataset
        stop_level_clean_pivoted['ServiceCode_LineName_RevisionNumber'] = stop_level_clean_pivoted['ServiceCode'] +'_'+ stop_level_clean_pivoted['LineName']  +'_'+ stop_level_clean_pivoted['RevisionNumber']

        #select just relevant cols
        stop_level_clean_pivoted = stop_level_clean_pivoted[['DatasetID','ServiceCode','LineName','RevisionNumber','ServiceCode_LineName_RevisionNumber', 'VehicleJourneyCode','sequence_number','stop_from','runtime']].drop_duplicates()
                                                             #,'Longitude','Latitude',]].drop_duplicates()

        #handle entries where exact same service line revision number combo appears in multiple files - take just the first file's runtime in these cases
        stop_level_clean_pivoted = stop_level_clean_pivoted.groupby(['DatasetID','ServiceCode_LineName_RevisionNumber','ServiceCode','LineName','RevisionNumber','sequence_number','stop_from','VehicleJourneyCode']).first().reset_index()

        #pivot by vehicle code
        stop_level_clean_pivoted = stop_level_clean_pivoted.pivot(index=['DatasetID','ServiceCode_LineName_RevisionNumber','ServiceCode','LineName','RevisionNumber','sequence_number','stop_from'],columns='VehicleJourneyCode', values = 'runtime')

        return stop_level_clean_pivoted

    def generate_timetable(self):

        '''
        Return a dictionary of timetable like dataframes, with each unique vehicle journey as a column,
        and the service code, stop code and sequence number as row headers.
        The dictionary key is composed of DatasetID, ServiceCode, LineName, RevisionNumber all separated 
        by '_'. The values are the timetable dataframes.
        
        '''

        #call function to return  pivoted stop level data
        stop_level_clean_pivoted = TimetableExtractor.pivot_clean_stop_level_data(self)

        #call naptan api so that lat lon can be added later in for loop below
        #enrich with lat long data from Naptan API
        print('Calling Naptan API to get lat/lon for each stop... \n')

        naptan = TimetableExtractor.fetch_naptan_data(self)
        print('Generating timetable... \n')

        #initialise lists to add each service code and its timetable df too
        list_of_datasets = []
        list_of_dfs = []
        list_of_service_codes = []

        #split for each dataset, so can assign dataset id to each service code
        for dataset in set(stop_level_clean_pivoted.reset_index()['DatasetID']):
            timetable_df_pre_pre = stop_level_clean_pivoted.loc[[dataset]].dropna(axis=1, how='all')

            #split for each service code, so that final timetable output is specific to each service code
            for service in set(timetable_df_pre_pre.reset_index()['ServiceCode_LineName_RevisionNumber']):
                timetable_df_pre = timetable_df_pre_pre.loc[(slice(None),[service]),:].dropna(axis=1, how='all')
                timetable_df = pd.DataFrame(index=timetable_df_pre.index)

                #for each col (vehicle journey), add each time delta to the depature time to find the time of each stop
                #to do this nas (representing the bus not stopping at a certain stop, or not at the sequence number) must be removed
                #these must later be added to ensure the timetable is correct across multiple journeys
                for c in timetable_df_pre.columns:
                    timetable_df_temp = timetable_df_pre[c]
                    timetable_df_temp.dropna(inplace=True)

                    #add times together
                    timetable_df_temp = timetable_df_temp.cumsum()
                    #join back to main df
                    timetable_df = timetable_df.merge(timetable_df_temp,how='left',left_index=True,right_index=True)

                #set base datetime for deltas to be added too
                base_time = datetime.datetime(1900,1,1,0,0,0)
                timetable_df = timetable_df.applymap(lambda x: x + base_time)

                #convert back to just times
                timetable_df.fillna('null',inplace=True)
                timetable_df=timetable_df.applymap(lambda x: x.strftime('%H:%M') if x != 'null' else np.nan).reset_index()

                #merge to get lon and lat of stop_from 
                # timetable_df = timetable_df.merge(naptan, how='left',left_on='stop_from',right_on='ATCOCode')
                timetable_df = naptan.merge(timetable_df, how='right',right_on='stop_from',left_on='ATCOCode')

                del timetable_df['ATCOCode']
                
                #get columns in right order
                timetable_df = timetable_df.set_index(['DatasetID','ServiceCode_LineName_RevisionNumber','ServiceCode','LineName','RevisionNumber','sequence_number','stop_from'])
                timetable_df = timetable_df.reset_index()
                
                #append result for 1 service code to lists
                list_of_datasets.append(dataset)
                list_of_dfs.append(timetable_df)
                list_of_service_codes.append(service)

            #concat lists together
            self.timetable_dict = {f'{i}_{j}': k for i, j, k in zip(list_of_datasets, list_of_service_codes, list_of_dfs)}

        print('Timetable generated!')
        return self.timetable_dict


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

        filtered_dict = {k:v for k,v in self.timetable_dict.items() if service_code in k}
        return filtered_dict


    def save_metadata_to_csv(self):
        """
        Save metadata table to csv file
        """
        
        
        #ensure no cell value exceeds 32,767 characters (this is excel limit)
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

        #create folder to save timetables int and get name of new folder
        destination = TimetableExtractor.create_today_folder(self)

        for k,v in self.timetable_dict.items():
            print (f'writing {k} to csv...')
            k = str(k)
            k = k.replace(':','_')
            v.to_csv(f'{destination}/{k}_timetable.csv', index=False)


    def save_filtered_timetables_to_csv(self, service_code):

        '''
        Save a filtered subset of timetables from the timetable_dict attribute as local csv files.
        The timetable dictionary can be filtered for a specific service code.
        This can also be used to filter for a specific licence number, or anything else
        in the composite key (DatasetID_ServiceCode_LineName_RevisionNumber), using free
        text argument.
        '''

        #create folder to save timetables int and get name of new folder
        destination = TimetableExtractor.create_today_folder(self)


        filtered_dict = TimetableExtractor.filter_timetable_dict(self, service_code)

        for k, v in filtered_dict.items():
            print (f'writing {k} to csv...')
            k = str(k)
            k = k.replace(':','_')
            v.to_csv(f'{destination}/{k}_timetable.csv', index=False)


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
        print(*red_count, sep = ', ')
        print(f'\nNumber of datasets with red dq score: {len(red_score)}')
        print(*datasets, sep = ', ')

        return red_score
    

    def dq_less_than_x(self, score):

        ''' returns number of operators in a table with dq scores less than input amount'''

        self.metadata['dq_score'] = self.metadata['dq_score'].astype(str)
        self.metadata['dq_score'] = self.metadata['dq_score'].str.rstrip('%').astype('float')

        score_filter = self.metadata.query(f'dq_score < {score}')

        output = score_filter['operator_name'].unique()
        
        datasets = score_filter['url']


        print(f'\nNumber of operators with dq score less than {score}: {len(output)}')
        print(*output, sep = ', ')
        
        print(f'\nNumber of datasets with dq score less than {score}: {len(score_filter)}')
        print(*datasets, sep = ', ')


        return score_filter

    def no_licence_no(self):
        '''how many files have no licence number'''

        #analytical_data = TimetableExtractor.analytical_timetable_data(self)
        grouped = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['LicenceNumber'].map(len) == 0]

        datasets = grouped['URL'].unique()

        print(f'\nNumber of datasets with files containing no licence number: {len(datasets)}')
        print(*datasets, sep = ', ')

        return grouped


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
        non_null_service_codes = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['ServiceCode'] != None]
        unique_service_codes = len(non_null_service_codes['ServiceCode'].unique())
        print(f'\nNumber of unique service codes in chosen dataset: {unique_service_codes}\n')
        return unique_service_codes

    def valid_service_codes(self):
        ''' 
        returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes
        and a dataframe with all the invalid service codes.
        '''
        #left 2 are characters
        correct_service_code = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['ServiceCode'].str[:2].str.isalpha()==True]
        #10th is colon
        correct_service_code = correct_service_code[correct_service_code['ServiceCode'].str[9:10]==':']
        #> than 10 characters
        correct_service_code = correct_service_code[correct_service_code['ServiceCode'].str.len() > 10]
        #unreg must be handled differently
        correct_service_code_unreg = correct_service_code[correct_service_code['ServiceCode'].str[:2] == 'UZ']
        correct_service_code_reg = correct_service_code[correct_service_code['ServiceCode'].str[:2] != 'UZ']
        #next 7 are int unless first two of whole thing are UZ
        correct_service_code_reg = correct_service_code_reg[correct_service_code_reg['ServiceCode'].str[2:9].str.isnumeric()==True]
        #right after colon are int, unless first two of whole thing are UZ
        correct_service_code_reg = correct_service_code_reg[correct_service_code_reg['ServiceCode'].str[10:].str.isnumeric()==True]
        correct_service_code_final = pd.concat([correct_service_code_reg,correct_service_code_unreg])

        #return the invalid service codes
        valid_serv = correct_service_code_final[['ServiceCode']]
        all_serv = self.service_line_extract_with_stop_level_json[['ServiceCode']]
        invalid_serv = all_serv[~all_serv.apply(tuple,1).isin(valid_serv.apply(tuple,1))]
        invalid_service_codes = invalid_serv.merge(self.service_line_extract_with_stop_level_json,how='left',on='ServiceCode')

        unique_valid_service_codes = len(correct_service_code_final['ServiceCode'].unique())
        print(f'\nNumber of unique valid service codes in chosen dataset: {unique_valid_service_codes}\n')
        return correct_service_code_final, invalid_service_codes

    def services_published_in_TXC_2_4(self):
        ''' 
        returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records 
        that are not published in this schema
        '''
        count_published_in_2_4_schema = len(self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['SchemaVersion']=='2.4'])
        TXC_2_4_schema = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['SchemaVersion']=='2.4']
        not_TXC_2_4_schema = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['SchemaVersion']!='2.4']
        perc_published_in_2_4_schema = (count_published_in_2_4_schema / len(self.service_line_extract_with_stop_level_json)) * 100
        print(f'\nPercentage of services published in TXC 2.4 schema: {perc_published_in_2_4_schema}\n')
        return perc_published_in_2_4_schema, TXC_2_4_schema, not_TXC_2_4_schema

    def datasets_published_in_TXC_2_4(self):
        ''' 
        returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records 
        that are not published in this schema
        '''
        datasets_schema_pre = self.service_line_extract_with_stop_level_json.copy()
        datasets_schema_pre['SchemaVersion'] = datasets_schema_pre['SchemaVersion'].astype('float')
        datasets_schema = datasets_schema_pre.groupby('DatasetID').agg({'SchemaVersion':'min'})
        count_datasets_published_in_2_4_schema = len(datasets_schema[datasets_schema['SchemaVersion']==2.4])
        perc_datasets_published_in_2_4_schema = (count_datasets_published_in_2_4_schema / len(datasets_schema)) * 100
        TXC_2_4_schema = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['SchemaVersion']=='2.4']
        not_TXC_2_4_schema = self.service_line_extract_with_stop_level_json[self.service_line_extract_with_stop_level_json['SchemaVersion']!='2.4']
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
        
        print(f'\nPercentage of registered licences (on OTC) with at least one published service on BODS: {percentage}%')

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

        #try except ensures that this reads in lookup file whether pip installing the library, or cloning the repo from GitHub
        try:
            #import the csv file as a text string from the BODSDataExtractor package
            atco_lookup_file = importlib.resources.read_text('BODSDataExtractor','ATCO_code_to_LA_lookup.csv')
            
            #wrap lookup_file string into a stringIO object so it can be read by pandas
            atco_lookup_string = io.StringIO(atco_lookup_file)

            la_lookup = pd.read_csv(atco_lookup_string ,dtype={'ATCO Code':str})
            
        except:
            la_lookup = pd.read_csv('ATCO_code_to_LA_lookup.csv',dtype={'ATCO Code':str})
    


        #fetch latest version of OTC database
        otc = self.otc_db.drop_duplicates()

        #enrich OTC data with ATCO code
        otc_la_merge = otc[['service_code','service_number','op_name','auth_description']].merge(la_lookup[['Auth_Description','ATCO Code']], how='left', right_on= 'Auth_Description', left_on= 'auth_description').drop_duplicates()

        #call full BODS timetables data extract and enrich with admin area name
        bods_la_merge = self.service_line_extract_with_stop_level_json[['ServiceCode','LineName','la_code','OperatorName']].merge(la_lookup[['Admin Area Name associated with ATCO Code','ATCO Code']], how='left', right_on= 'ATCO Code', left_on= 'la_code').drop_duplicates()

        #add cols to distinguish if in otc and if in bods
        otc_la_merge['in'] = 1
        bods_la_merge['in'] = 1

        #ensure linename col is consistent across bods and otc
        otc_la_merge.rename(columns = {'service_number':'LineName'}, inplace = True)
        
        #merge OTC service level data with BODS service level data
        full_service_code_with_atco = otc_la_merge[['service_code','LineName','op_name','ATCO Code','in']].add_suffix('_otc').merge(bods_la_merge.add_suffix('_bods'),how='outer',right_on=['ServiceCode_bods','ATCO Code_bods'],left_on=['service_code_otc', 'ATCO Code_otc']).drop_duplicates()

        #coalesce service code and atco code cols
        full_service_code_with_atco['service_code'] = full_service_code_with_atco['service_code_otc'].combine_first(full_service_code_with_atco['ServiceCode_bods'])
        full_service_code_with_atco['atco_code'] = full_service_code_with_atco['ATCO Code_otc'].combine_first(full_service_code_with_atco['ATCO Code_bods'])

        #keep only necessary cols
        full_service_code_with_atco = full_service_code_with_atco[['service_code','LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods']]

        #add admin area name
        full_service_code_with_atco = full_service_code_with_atco.merge(la_lookup[['Admin Area Name associated with ATCO Code','ATCO Code']],how='left',left_on='atco_code',right_on='ATCO Code').drop_duplicates()

        #remove dupicate atco code col
        del full_service_code_with_atco['ATCO Code']

        #replace nans with 0s (necessary for mi reporting calculations)
        full_service_code_with_atco['in_otc'] = full_service_code_with_atco['in_otc'].fillna(0)
        full_service_code_with_atco['in_bods'] = full_service_code_with_atco['in_bods'].fillna(0)
        
        #format line nos into desired list format, so as to not explode rest of dataframe
        #replace nulls with string so groupby doesnt omit them
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].fillna('xxxxx')
        #groupby to concat the bods line nos together
        full_service_code_with_atco = full_service_code_with_atco.groupby(['service_code','LineName_otc', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods','Admin Area Name associated with ATCO Code'], as_index=False, dropna=False).agg({'LineName_bods' : lambda x:','.join(x)})
        #regenerate the nulls
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].replace('xxxxx',None)
        #reorder cols
        full_service_code_with_atco = full_service_code_with_atco[['service_code','LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods','Admin Area Name associated with ATCO Code']]

        return full_service_code_with_atco

    def services_on_bods_or_otc_by_area_mi(self):
        '''
        Generates MI from dataframe that lists all service codes from BODS and/or
        OTC database, by admin area. Specifically notes the number of services from
        these sources, and the fraction of all within BODS and OTC respectively.
        Note - only to be run on all published datasets
        '''

        full_service_code_with_atco = TimetableExtractor.services_on_bods_or_otc_by_area(self)
        service_atco_mi = full_service_code_with_atco.groupby('atco_code').agg({'service_code':'count'
                                                                                     ,'Admin Area Name associated with ATCO Code':'first'
                                                                                     ,'in_otc':'mean'
                                                                                     , 'in_bods':'mean'})

        service_atco_mi.rename(columns={'service_code':'count_services'},inplace=True)

        #round fractions to 2 decimals 
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

        #try except ensures that this reads in lookup file whether pip installing the library, or cloning the repo from GitHub
        try:
            #import the csv file as a text string from the BODSDataExtractor package
            atco_lookup_file = importlib.resources.read_text('BODSDataExtractor','ATCO_code_to_LA_lookup.csv')
            
            #wrap lookup_file string into a stringIO object so it can be read by pandas
            atco_lookup_string = io.StringIO(atco_lookup_file)

            la_lookup = pd.read_csv(atco_lookup_string ,dtype={'ATCO Code':str})
            
        except:
            la_lookup = pd.read_csv('ATCO_code_to_LA_lookup.csv',dtype={'ATCO Code':str})
    


        #fetch latest version of OTC database
        otc = self.otc_db.drop_duplicates()

        #enrich OTC data with ATCO code
        otc_la_merge = otc[['service_code','service_number','op_name','auth_description']].merge(la_lookup[['Auth_Description','ATCO Code']], how='left', right_on= 'Auth_Description', left_on= 'auth_description').drop_duplicates()

        #call full BODS timetables data extract and enrich with admin area name
        bods_la_merge = self.service_line_extract_with_stop_level_json[['ServiceCode','LineName','la_code','OperatorName']].merge(la_lookup[['Admin Area Name associated with ATCO Code','ATCO Code']], how='left', right_on= 'ATCO Code', left_on= 'la_code').drop_duplicates()

        #add cols to distinguish if in otc and if in bods
        otc_la_merge['in'] = 1
        bods_la_merge['in'] = 1

        #ensure linename col is consistent across bods and otc
        otc_la_merge.rename(columns = {'service_number':'LineName'}, inplace = True)
        
        #merge OTC service level data with BODS service level data
        full_service_code_with_atco = otc_la_merge[['service_code','LineName','op_name','ATCO Code','in']].add_suffix('_otc').merge(bods_la_merge.add_suffix('_bods'),how='outer',right_on=['ServiceCode_bods','ATCO Code_bods'],left_on=['service_code_otc', 'ATCO Code_otc']).drop_duplicates()

        #coalesce service code and atco code cols
        full_service_code_with_atco['service_code'] = full_service_code_with_atco['service_code_otc'].combine_first(full_service_code_with_atco['ServiceCode_bods'])
        full_service_code_with_atco['atco_code'] = full_service_code_with_atco['ATCO Code_otc'].combine_first(full_service_code_with_atco['ATCO Code_bods'])

        #keep only necessary cols
        full_service_code_with_atco = full_service_code_with_atco[['service_code','LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods']]

        #add admin area name
        full_service_code_with_atco = full_service_code_with_atco.merge(la_lookup[['Admin Area Name associated with ATCO Code','ATCO Code']],how='left',left_on='atco_code',right_on='ATCO Code').drop_duplicates()

        #remove dupicate atco code col
        del full_service_code_with_atco['ATCO Code']

        #replace nans with 0s (necessary for mi reporting calculations)
        full_service_code_with_atco['in_otc'] = full_service_code_with_atco['in_otc'].fillna(0)
        full_service_code_with_atco['in_bods'] = full_service_code_with_atco['in_bods'].fillna(0)
        
        #remove services not in otc
        full_service_code_with_atco = full_service_code_with_atco[full_service_code_with_atco['in_otc']==1]

        #format line nos into desired list format, so as to not explode rest of dataframe
        #replace nulls with string so groupby doesnt omit them
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].fillna('xxxxx')
        #groupby to concat the bods line nos together
        full_service_code_with_atco = full_service_code_with_atco.groupby(['service_code','LineName_otc', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods','Admin Area Name associated with ATCO Code'], as_index=False, dropna=False).agg({'LineName_bods' : lambda x:','.join(x)})
        #regenerate the nulls
        full_service_code_with_atco['LineName_bods'] = full_service_code_with_atco['LineName_bods'].replace('xxxxx',None)
        #reorder cols
        full_service_code_with_atco = full_service_code_with_atco[['service_code','LineName_otc', 'LineName_bods', 'op_name_otc', 'OperatorName_bods','atco_code','in_otc','in_bods','Admin Area Name associated with ATCO Code']]

        return full_service_code_with_atco

    def services_on_bods_or_otc_by_area_mi_just_otc(self):
        '''
        Generates MI of all service codes published
        in the OTC database, indicates whether they are published on 
        BODS as well, and provides the admin area the services has stops within
        Note - only to be run on all published datasets
        '''

        full_service_code_with_atco = TimetableExtractor.services_on_bods_or_otc_by_area_just_otc(self)
        service_atco_mi = full_service_code_with_atco.groupby('atco_code').agg({'service_code':'count'
                                                                                     ,'Admin Area Name associated with ATCO Code':'first'
                                                                                     ,'in_otc':'mean'
                                                                                     , 'in_bods':'mean'})

        service_atco_mi.rename(columns={'service_code':'count_services'},inplace=True)

        #round fractions to 2 decimals 
        service_atco_mi = service_atco_mi.round(2)

        return service_atco_mi
    
class xmlDataExtractor:
    
    
    
    def __init__(self, filepath):
        self.root = ET.parse(filepath).getroot()
        self.namespace = self.root.nsmap

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

        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//NationalOperatorCode', self.namespace)
        
        #iterate through the data element and add the text to a list of NOCs
        noc = [i.text for i in data]
        
        return noc
        
    def extract_trading_name(self):
        
        '''
        Extracts the Trading Name from an xml file in a given location with a known namespace
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        '''

        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//TradingName', self.namespace)
        
        trading_name = [i.text for i in data]
        
        return trading_name

   
    def extract_licence_number(self):
        
        '''
        Extracts the Licence Number from an xml file in a given location with a known namespace
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        '''
   
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//LicenceNumber', self.namespace)
        
        licence_number = [i.text for i in data] 
        
        return licence_number
    


    def extract_operator_short_name(self):
        
        '''
        Extracts the Operator Short Name from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//OperatorShortName', self.namespace)
        
        operator_short_name = [i.text for i in data]
        
        return operator_short_name
    
    def extract_operator_code(self):
        
        '''
        Extracts the Operator Code from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Operators/.//OperatorCode', self.namespace)
        
        operator_code = [i.text for i in data]
        
        return operator_code

    def extract_service_code(self):
        
        '''
        Extracts the Service Code from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/ServiceCode', self.namespace)
        
        service_code = [i.text for i in data]
        
        return service_code


    def extract_line_name(self):
        
        '''
        Extracts the Line Name from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/Lines/Line/LineName', self.namespace)
        
        line_name = [i.text for i in data]
        
        return line_name 
    
    def extract_public_use(self):
        
        '''
        Extracts the public use boolean from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/PublicUse', self.namespace)
        
        public_use = [i.text for i in data]
        
        return public_use
    
    def extract_operating_days(self):
        
        '''
        Extracts the regular operating days from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data

        
        '''
        #find all text in the given xpath, return as a element object
        data = self.root.findall("VehicleJourneys//VehicleJourney/OperatingProfile/RegularDayType/DaysOfWeek/", self.namespace)

        daysoperating=[]
        
        for count, value in enumerate(data):
            
            #convert each element into a string
            day=str(data[count])
            
            #only keep the element data associated with the day of the week
            changedday=day[42:52]
            
            #split up the weekday string
            before, sep, after = changedday.partition(' ')
            
            #keep the string data before the partition mentioned above
            changedday = before
        
            daysoperating.append(changedday)
            
        #remove duplicates from the operating days extracted
        setoperatingdays=set(daysoperating)
        
        #change the operating days into a list format so they can be ordered
        operating_day_list=list(setoperatingdays)
        
        #adding dictionary variables and values to "day" dictionary
        
        day={}
        day['Monday']=1
        day['Tuesday']=2
        day['Wednesday']=3
        day['Thursday']=4
        day['Friday']=5
        day['Saturday']=6
        day['Sunday']=7
        
        brand_new={}

        
        #checking if the day of the week is in the above dictionary so we can sort the days
        for i in operating_day_list:
            if i in day:
                brand_new.update({i:day[i]})
        
        #sorting the days of operation
        sortit=sorted(brand_new.items(), key=lambda x:x[1])
        
        length=len(sortit)

        operating_days=""
        
        consecutive=True
        
        
        #checking to see if the days in the list are not consective
        
        for i in range(length-1):
            if sortit[i+1][1]-sortit[i][1]!=1:
                consecutive=False
                break

                     
        # if there are no days of operation entered
        if length==0:
            operating_days="None"
        
        #if there is only one day of operation
        elif length==1:
            operating_days=sortit[0][0]
        
        #if the operating days are not consecutive, they're seperated by commas
        elif consecutive==False:
            for i in range(length):
                operating_days= operating_days + sortit[i][0] + ","
                
        #if consecutive, operating days are given as a range           
        else:
           # print(sortit)
            operating_days=sortit[0][0] + "-" + sortit[-1][0]
                

        operating_days=[operating_days]
        

        return operating_days

    def extract_service_origin(self):
        
        '''
        Extracts the service origin from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/StandardService/Origin', self.namespace)
        
        service_origin = [i.text for i in data]
        
        return service_origin    
    
    def extract_service_destination(self):
        
        '''
        Extracts the service destination from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/StandardService/Destination', self.namespace)
        
        service_destination = [i.text for i in data]
        
        return service_destination     
    
    def extract_operating_period_start_date(self):
        
        '''
        Extracts the service start date from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/OperatingPeriod/StartDate', self.namespace)
        
        operating_period_start_date = [i.text for i in data]
        
        return operating_period_start_date    
    
    
    def extract_operating_period_end_date(self):
        
        '''
        Extracts the service end date from an xml file in a given location with a known namespace.
        Namespace can be found in constants.py and depends on if data is timetable or fares data
        
        '''
        
        #find all text in the given xpath, return as a element object
        data = self.root.findall(f'Services//Service/OperatingPeriod/EndDate', self.namespace)
        
        operating_period_end_date = [i.text if len(i.text) >0 else 'No Data' for i in data]
        
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
        
        #find all text in the given xpath, return as a element object
        #atco_first_3_letters = []
        
        data = self.root.findall(f'StopPoints//StopPointRef', self.namespace)
                
        atco_first_3_letters = [i.text[0:3] for i in data]
        
        unique_atco_first_3_letters = list(set(atco_first_3_letters))
        
        return unique_atco_first_3_letters


api=os.environ.get("API")    

 

my_bus_data_object = TimetableExtractor(api_key=api # Your API Key Here
                                  ,limit=1 # How many datasets to view
                                  ,status = 'published' # Only view published datasets
                                  ,service_line_level=True # True if you require Service line data 
                                  ,stop_level=False # True if you require stop level data

 

                                )

 


#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

 

#save the extracted service line level data to dataset_level variable
filtered_service_line_level = my_bus_data_object.service_line_extract

 

#export to csv if you wish to save this data
filtered_service_line_level.to_csv('all_sevice_line.csv')