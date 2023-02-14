
<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/github_username/repo_name">
    <img src="https://user-images.githubusercontent.com/105863784/190128076-b6630a01-5809-4018-ae11-b7da83ce131c.png" alt="Logo" width="427" height="66">
    
  </a>

<h3 align="center">BODS Data Extractor</h3>

  <p align="center">
    A python client for downloading and extracting data from the UK Bus Open Data Service 
    <br />
    <a href="https://github.com/department-for-transport-BODS/bods-data-extractor"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/department-for-transport-BODS/bods-data-extractor/blob/main/src/BODSDataExtractor/example.py">View Demo</a>
    ·
    <a href="https://github.com/department-for-transport-BODS/bods-data-extractor/issues">Report Bug</a>
    ·
    <a href="https://github.com/department-for-transport-BODS/bods-data-extractor/issues">Request Feature</a>
    ·
    <a href="https://github.com/department-for-transport-BODS/bods-data-extractor/fork">Contribute</a>
  </p>
</div>





# Table of contents

- [About The Project](#about-the-project)
  - [Built With](#built-with)
  - [Useful Links](#useful-links)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Quick Reference](#quick-reference)
  - [Example of timetables output](#example-of-timetables-output)
  - [Fundamentals of extracting data using this package](#fundamentals-of-extracting-data-using-this-package)
  - [How to extract Dataset Level data](#how-to-extract-dataset-level-data)
  - [How to extract Service Line Level data](#how-to-extract-service-line-level-data)
  - [How to extract Stop Level data](#how-to-extract-stop-level-data)
  - [Expected run times and performance](#expected-run-times-and-performance)
  - [How to fine tune your results using additional parameters](#how-to-fine-tune-your-results-using-additional-parameters)
  - [ATCO Code to Local Authority Lookup Table](#atco-code-to-local-authority-lookup-table)
  - [Using Previously Downloaded Data](#using-previously-downloaded-data)
  - [OTC Database](#otc-database)
  - [Reporting and Analytics](#reporting-and-analytics)
- [Roadmap & Limitations](#roadmap--limitations)
    - [Handling non standard files](#handling-non-standard-files)
    - [Handling vehicle journeys that cross midnight](#handling-vehicle-journeys-that-cross-midnight)
    - [Providing additional detail about services](#providing-additional-detail-about-services)
    - [Incorporating AVL and Fares data](#incorporating-avl-and-fares-data)
    - [Optimising performance](#optimising-performance)
    - [And more!](#and-more)
- [Contact / Contribute](#contact--contribute)
  - [Contact the Bus Open Data Service](#contact-the-bus-open-data-service)
- [License](#license)

<!-- ABOUT THE PROJECT -->
## About The Project


![image](https://user-images.githubusercontent.com/105863784/190125659-b5dc1d1f-820c-405b-aa9b-5b3b6738dddc.png)




This project was created to lower the barrier to entry for analysis of UK Bus Open Data. It facilitates the fetching and extraction of data, currently focussed on Timetables, into tables to be used for analysis or your own projects.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With


[![Python][python.com]][python-url]


<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Useful Links

There are a number of different documents and data standards that are used as part of the Bus Open Data Service. Please find these links below: 

- [Bus Services Act 2017](https://www.legislation.gov.uk/ukpga/2017/21/introduction/enacted)
- [National Bus Strategy](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/980227/DfT-Bus-Back-Better-national-bus-strategy-for-England.pdf)
- [Timetables Data Standard](https://pti.org.uk/system/files/files/TransXChange_UK_PTI_Profile_v1.1.A.pdf)
- [Versioning in Timetables](https://pti.org.uk/system/files/files/TransXChange%20UK%20PTI%20Profile%20-%20Versioning%20Application%20Note%20v1.0.pdf)
- [Location Data Standard](https://www.gov.uk/government/publications/technical-guidance-publishing-location-data-using-the-bus-open-data-service-siri-vm/technical-guidance-siri-vm#the-siri-vm-standard)
- [Guide to Matching Location and Timetables Data](https://pti.org.uk/system/files/files/SIRI_VM_PTI_Data_Matching_v1-0.pdf)
- [Fares Data Standard](http://netex.uk/farexchange/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Glossary of Terms

To help you when using the data, we've put together a list of useful terms and their meanings. 

| Term    | Definition |
|:-------|:-----------|
|ATCO Code| ATCO (Association of Transport Co-ordinating Officers) codes refer to the unique identifiers used for individual UK bus stops. The first 3 numbers denote the authority responsible for the stop. The fourth character is a 0 (zero). The remaining characters to a maximum of 8 are alpha-numeric determined locally. BODS Data Extractor allows you to filter results on the ATCO admin authority (the first 3 numbers). Please see fine-tuning section [below](#how-to-fine-tune-your-results-using-additional-parameters) for more detail on how to do this.|
|Data Field Definitions | For definitions of each field within the data returned, please see [BODS Data Catalogue](https://data.bus-data.dft.gov.uk/guidance/requirements/?section=datacatalogue).
| Data Set/Feed | Operators must provide a complete set of data regarding local bus services. Please see [BODS Quick Start Guidance](https://data.bus-data.dft.gov.uk/guidance/requirements/?section=quickstart) for more information. |
|NeTEx | NeTEx is a CEN standard that can be used to represent many aspects of a multi-modal transport network. The UK profile includes elements related to fares for buses. Please see [above](#Useful-Links) and [BODS Data Formats](https://data.bus-data.dft.gov.uk/guidance/requirements/?section=dataformats) for more information.|
| NOC | NOC stands for National Operator Code. Each bus operator has at least one identifying code, consisting of four letters, but can have multiple. To find the NOCs for the operators you are interested in, you can browse or download the NOC Database from [Traveline](https://www.travelinedata.org.uk/traveline-open-data/transport-operations/about-2/). BODS Data Extractor also allows you to filter results by NOC, please see fine-tuning section [below](#how-to-fine-tune-your-results-using-additional-parameters) for more detail. |
|OTC Database| OTC stands for Office of the Traffic Commissioner. The OTC database contains information for every registered bus service in the UK. Please see [below](#otc-database) for information on how the database is used within the package.|
|Siri-VM | Siri-VM is an XML standard for exchanging real time bus location information. Please see [above](#Useful-Links) and [BODS Data Formats](https://data.bus-data.dft.gov.uk/guidance/requirements/?section=dataformats) for more information.|
| TransXChange | TransXChange is the UK nationwide standard for exchanging bus schedules and related data. Please see [above](#Useful-Links) and [BODS Data Formats](https://data.bus-data.dft.gov.uk/guidance/requirements/?section=dataformats) for more information.|

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

Follow these simple steps to get started with the project. It is recommended that you use a Python IDE rather than a console.

### Prerequisites

Download and install Python 

(If you are unfamiliar with Python, do not worry. This package is designed to be very easy to use, and once you have python installed you can copy and paste example code from below and through only making small tweaks to input parameters get the data or reporting metrics you require.

For those unsure how to install Python, we recommend downloading Anaconda, an open-source distribution for Python, using the following link: 

https://www.anaconda.com/distribution/

Once this is installed, open Anaconda Navigator GUI and ensure 'Spyder' is installed, before launching it. Here you will be able to run code from the examples below, in order to make use of this package's functionalities. 

Please check this is inline with your organisation's policy before installing.

### Installation

1. Get a free API Key by creating an account at: [https://data.bus-data.dft.gov.uk/](https://data.bus-data.dft.gov.uk/). Your API key can be found under 'Account Settings'

Make sure to do step 2 in the terminal or command line (if using Anaconda Navigator open Anaconda Prompt and install using the above code here). All other code is to be run in the python IDE of your choice (we recommend Spyder if using Anaconda)
  
2. Install BODS Data Extractor package 
    ```commandline
      pip install BODSDataExtractor
      ```
3. Open up a .py file and save your API to a variable
   ```python
   api = 'ENTER YOUR API KEY'
   ```
4. Follow the steps below in the Usage section. The code examples are ready to copy and paste into your .py file.
   

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->


## Usage

### Quick Reference

The below code acts as a quick reference guide to calling the attributes of a TimetableExtractor object at each level as explained below in this document.

```python

#Dataset Level
TimetableExtractor.metadata

#Service Line Level
TimetableExtractor.service_line_extract

#Stop Level
TimetableExtractor.timetable_dict
```

### Example of timetables output

Timetable data can be extracted at 3 levels (note that these examples are restricted to 1 dataset, of ID 322):

* Dataset level - High level overview of key dataset information (each row represents 1 dataset on BODS platform)

<img width="1408" alt="image" src="https://user-images.githubusercontent.com/81578708/189095249-79a51be0-0145-4712-b538-65d80b76e99a.png">

* Service Line Level - Detailed information about each service and line extracted from individual files within datasets (each row represents 1 timetables xml file)

<img width="1400" alt="image" src="https://user-images.githubusercontent.com/81578708/189094457-ee19dc5b-97b8-409c-a75d-04ca4adb5016.png">

* Stop level - Stop level data in the form of a traditional timetable (1 timetable from each xml file)

<img width="1362" alt="image" src="https://user-images.githubusercontent.com/81578708/189094683-ce047d25-051c-470e-be8e-70e6b5d47eab.png">

### Fundamentals of extracting data using this package

This package is written using object orientated python principles. Put simply, one must initiate an object based upon the subset of BODS timetables data they wish to extract data on, or analyse. Upon initiating an instance of this object, parameters can be specified to select the desired subset. Once this object instance has been initiated, there is no immediate output in the python console. One can now access attributes of the instance by calling them. 

Note that for the data extraction, all of the processing is done in the initiation of the object instance, and not in the accessing of the attributes. This means it make take some time to initiate the object instance, but should take almost no time to access your desired data once the object instance has been initiated.

The examples below should bring this to life.

### How to extract Dataset Level data

```python
#intiate an object instance called my_bus_data_object with desired parameters
from BODSDataExtractor.extractor import TimetableExtractor

my_bus_data_object = TimetableExtractor(api_key=api # Your API Key Here
                                 ,limit=1 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=False # True if you require Service line data 
                                 ,stop_level=False # True if you require stop level data
                                 )

#save the extracted dataset level data to dataset_level variable
dataset_level = my_bus_data_object.metadata

#save this data to a csv file in your downloads directory
#note that entries in the 'localities' field may be truncated to comply with excel cell limits
my_bus_data_object.save_metadata_to_csv()
```
### How to extract Service Line Level data
```python
#intiate an object instance called my_bus_data_object with desired parameters
from BODSDataExtractor.extractor import TimetableExtractor

my_bus_data_object = TimetableExtractor(api_key=api # Your API Key Here
                                 ,limit=1 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=True # True if you require Service line data 
                                 ,stop_level=False # True if you require stop level data
                                 )

#save the extracted service line level data to service_line_level variable
service_line_level = my_bus_data_object.service_line_extract

#note that in downloading the service line level data, the dataset level will also be downloaded. Can access this as below:
dataset_level = my_bus_data_object.metadata

#save these data to a csv file in your downloads directory
my_bus_data_object.save_metadata_to_csv()
my_bus_data_object.save_service_line_extract_to_csv()
```
### How to extract Stop Level data
```python
#intiate an object instance called my_bus_data_object with desired parameters
from BODSDataExtractor.extractor import TimetableExtractor

my_bus_data_object = TimetableExtractor(api_key=api # Your API Key Here
                                 ,limit=1 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=True # True if you require Service line data 
                                 ,stop_level=True # True if you require stop level data
                                 )

#save the extracted stop level data to stop_level variable
stop_level = my_bus_data_object.timetable_dict

#note that in downloading stop level the  data, the dataset and service line level will also be downloaded. Can access this as below:
dataset_level = my_bus_data_object.metadata
service_line_level = my_bus_data_object.service_line_extract

#save meta data and service line level data to csv file in your downloads directory
my_bus_data_object.save_metadata_to_csv()
my_bus_data_object.save_service_line_extract_to_csv()

#stop_level variable is a dictionary of dataframes, which can be saved to csv as follows (saves in downloads directory)
my_bus_data_object.save_all_timetables_to_csv()

#or can filter to filter timetable results to a specfic licence number of service code (saves in downloads directory)
my_bus_data_object.save_filtered_timetables_to_csv('PC0001838:41')

#visualise a particular service line on an interactive map
my_bus_data_object.visualise_service_line('PC0001838:41')

```
### Expected run times and performance

The volume of timetables data available on the BODS platform is very significant, and while this package simplifies the extraction of this data, and processes it into a analytical ready form, the sheer amount of data dictates that it can take a non trivial amount of time to initiate the above object instances. 

One way of getting around this problem is to narrow your requested data request using additional parameters. Another is to run the download once, and save the output to disk as a csv, to allow re loading of this at a later data for reporting analysis. Both of these approaches are outlined in more detail in below sections.

Directly below are some sample expected run times for extracting data using this package. This should still give you a very approximate idea of how long to expect your code to execute, depending on how much data you are trying to extract.

It is important to note that this can vary depending on your local processing power, internet connection and on the nature of the datasets you are extracting (a dataset may contain one xml file, or several hundred).

| Granularity of data extraction    | 1 dataset timing  | 20 dataset timing | 200 datasets timing      |
| --------------------------------- | ----------------- |-------------------|--------------------------|
| Dataset                           | > 0 hrs 1 min     | > 0 hrs 1 min     | 0 hrs 2 min              |
| Service line                      | > 0 hrs 1 min     | > 0 hrs 1 min     | 0 hrs 6 min              |
| Stop                              | 0 hrs 3 min       | > 0 hrs 6 min     | Memory issues @ 16Gb RAM |


### How to fine tune your results using additional parameters

As well as specifying the granularity of data to extract (dataset, service line or stop level), limiting the number of datasets, and restricting to just published datasets, there are a number of additional parameters that the object instance can be initiated with. These are as follows:

- nocs - _accepts list input of valid National Operator Codes e.g. ['HIPK', 'YCST']_
- search -  _accepts string input of key words to filter for the data set name, data set description, organisation name, or admin name e.g. 'Arriva'_
- bods_compliant - _accepts boolean input (True or False), where True filters for only BODS Compliant datasets. Default value is True_
- atco_code - _accepts list input of the first three characters of ATCO codes (ATCO codes are unique identifiers of UK bus stops, where first three characters signify the admin area they are within). This filters datasets and/or service lines that have stops within the specified admin areas. e.g. ['320','450']_

Example of this:
```python
#intiate an object instance called my_bus_data_object with desired parameters
from BODSDataExtractor.extractor import TimetableExtractor

my_bus_data_object = TimetableExtractor(api_key=api # Your API Key Here
                                 #,limit=1 # commented out limit so will return all results within other parameters
                                 ,status = 'published' 
                                 ,service_line_level=True 
                                 ,stop_level=False 
                                 ,nocs=['FSCE','FGLA','FCYM'] #values must be entered in this list format - each noc within quotes, separated by comma, all within []
                                 ,search='First Bus' # this is actually redundant as nocs are specific to this operator, but included for demo purpose
                                 ,bods_compliant=True
                                 ,atco_code=['320', '450'] # filter to stops within just north and west yorkshire. Values must be entered in this list format - each code within quotes, separated by comma, all within []
                                 )

#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

#save the extracted service line level data to dataset_level variable
filtered_service_line_level = my_bus_data_object.service_line_extract

#export to csv if you wish to save this data
filtered_service_line_level.to_csv('filtered_service_line_level_export.csv')
```
### ATCO Code to Local Authority Lookup Table

The package contains an unofficial lookup table used to map bus stop ATCO codes to Local Authorities. This may be useful as a rough guide to filtering services around a certain area of England. 

Please note that this is not an official definition of ATCO / LA mappings and has been manually created.

The table can be accessed using the code below.

```python
#import the csv file as a text string from the BODSDataExtractor package
atco_lookup_file = importlib.resources.read_text('BODSDataExtractor','ATCO_code_to_LA_lookup.csv')

#wrap lookup_file string into a stringIO object so it can be read by pandas
atco_lookup_string = io.StringIO(atco_lookup_file)

#load into a DataFrame
la_lookup = pd.read_csv(atco_lookup_string ,dtype={'ATCO Code':str})
```

### Using Previously Downloaded Data

The previous examples are based on using the same TimetableMetadata object instance created in the first example. This is good for most cases but there may be times when the user wishes to use previously saved exports, without having to extract all the datasets again. In this case, the user can initiate an object, and manually set the `metadata`, `service_line_extract` and `stop_level` attributes. This will then allow the user to run all relevant reporting functions.

```python
from BODSDataExtractor.extractor import TimetableExtractor

service_data_path = "path to your service line level data here"
service_data = pd.read_csv(bods_data_path)

my_bus_data_object = TimetableExtractor(api_key=api  # Your API Key Here
                                     , limit=1)  # set the limit to 1 to avoid waiting for many datasets to be downloaded                                    

my_bus_data_object.full_data_extract = service_data #set the 'full_data_extract' attribute to the service_data variable loaded from the saved csv file

all_sc = my_bus_data_object.count_service_codes() #this function counts all the service codes in a given service line level dataset
```
### OTC Database

The package is also able to pull the latest copy of the OTC Database (Currently England only) from the [gov.uk](https://www.data.gov.uk/dataset/9ea90ed8-de54-4274-92c6-272edd518bfb/traffic-commissioners-local-bus-service-registration) website. The code below demonstrates how this can be done in a single line of code.
There are two options to downloading the latest copy of the OTC Database using the `otc_db_download` file. The `save_otc_db` function creates a local folder named as the current date and saves a copy of the OTC DB there.
It also returns a dataframe containing the OTC DB. The second option is to use the `fetch_otc_db` function which only saves the OTC DB to a variable, and does not save it to a folder.

```python
from BODSDataExtractor import otc_db_download

local_otc = otc_db_download.save_otc_db() # download and save a copy of the otc database, as well as assigning it to the 'otc' variable

otc = otc_db_download.fetch_otc_db() #assign a copy of the otc database to the 'otc' variable
```

This code is executed automatically when calling a reporting function that requires the OTC database.

### Reporting and Analytics

As well as delivering analytical ready data extracts from the BODS platform, this package also contains a variety of functions for analysing both the specific timetables data you have requested, as well as all of the timetables data on the platform.

#### Custom reporting on your specified data

Each of these functions (below) run on a variable containing the data you have extracted, as outlined earlier in this READme. This allows you to generate custom report metrics on the particular slice of BODS timetables data you are interested in, rather than on all of the data in the platform. The reporting functions, and how to run them, are detailed below. Of course, any of these can be run on the whole dataset too if that is the slice you specify for when initiating your object instance.

```python

count_of_operators = my_bus_data_object.count_operators() #returns count of distinct operators (measured by operator_name) in a chosen dataset

count_of_service_codes = my_bus_data_object.count_service_codes()# returns count of unique service codes chosen dataset

valid_service_codes = my_bus_data_object.valid_service_codes()# returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes and a dataframe with all the invalid service codes.

services_published_in_TXC_2_4 = my_bus_data_object.services_published_in_TXC_2_4()#returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

datasets_published_in_TXC_2_4 = my_bus_data_object.datasets_published_in_TXC_2_4()# returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

red_dq = my_bus_data_object.red_dq_scores() #returns the number of operators in a table with red dq scores

less_than_10 = my_bus_data_object.dq_less_than_x(90) # takes an integer as input (in this case 10) and returns a list of operators with a data quality score less than that integer

no_lic_no = my_bus_data_object.no_licence_no() # returns a report listing which datasets contain files which do not have a licence number

```

#### Reporting on all of the published timetables data in BODS

The below functions are to be run on all of the published timetables data in the BODS platform. The general purpose of these is to cross check the timetables data published in BODS with the services registered on the OTC database. 

```python
services_by_area = my_bus_data_object.services_on_bods_or_otc_by_area() #Generates a dataframe of all service codes published on BODS and/or in the OTC database, indicates whether they are published on both or one of BODS and OTC, and provides the admin area the services has stops within

services_by_area_MI = my_bus_data_object.services_on_bods_or_otc_by_area_mi() #Generates MI from dataframe that lists all service codes from BODS and/or OTC database, by admin area. Specifically notes the number of services from these sources, and the fraction of all within BODS and OTC respectively.

percent_published_licences = my_bus_data_object.percent_published_licences() #percentage of registered licences with at least one published service

unpublished_services = my_bus_data_object.registered_not_published_services() #feed in a copy of the BODS timetable data and otc database to return a list of unpublished service codes

published_not_registered_services = my_bus_data_object.published_not_registered_services()#returns a dataframe of services found in the published data from the api which are not found in the otc database
```

Reporting and analytics can be performed with the builtin functions as shown above. This code first fetches the latest copy of the otc database and compares it to a copy of the service line level data extracted using `service_line_extract`. The code returns a dataframe listing information about service codes registered with OTC but not published in the BODS data.


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP & Limitations -->
## Roadmap & Limitations

As the first release of this project, the focus was on getting analytical ready timetable data in the hands of consumers. As a result, there are a number of limitations to bear in mind when using this package. These aim to be addressed in subsequent releases. 

#### Handling non standard files
Whilst all BODS compliant files meet a certain set of standards, there is still variation in exactly how timetables xml files can be populated. This pacakge currently handles files that meet the most common structure, and a number of notable exceptions, however there are some files that stop level timetables will not be generated for. It will, however, generate dataset level and service line level data for all published files. Future releases will aim to close this gap so timetables can be generated for most, if not all files.

#### Handling vehicle journeys that cross midnight
As the PTI standards dictate, sequence number logic is different for services that have stop times which cross midnight. This currently results in the stop level timetable output having vehicle journeys that do not start at sequence number 1. Future releases will include logic that better handles this to provide a more uniform output for such vehicle journeys.

#### Providing additional detail about services
Future releases will extract additional data from timetables xml files in order to provide further detail on services; for example details regarding the days of operation and exceptions around bank holidays, as well as more detailed route information including distance between stops, in addition to time.

#### Incorporating AVL and Fares data
The BODS platform also provides live vehicle location data (AVL), as well as Fares data. Future releases will aim to incorporate functionality for downloading and analysis AVL data initially, and subsequently Fares data once this has been validated. 

#### Optimising performance
Generating stop level timetables requires downloading and parsing a significant volume of data. It therefore takes quite some time to run the code. Future releases will aim to reduce execution time through optimisations.

#### And more!
This project has consumers of BODS data at its heart, and so if any other features would be valuable, or any bugs are noticed please get in touch. Details of how to do this can be found in the 'Contributing' section below.

See the [open issues](https://github.com/KPMG-UK/bods_pseudo_test/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contribute / Contact

### Contribute to this project

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you want to suggest an new or improved feature, open an issue with the tag "enhancement". 

If you want to develop a new or improved feature yourself, fork the repo, develop within a new branch, then create a pull request back to this repo. We will then review and potentially merge into main if appropriate. Click the link below for more detailed instructions on how to contribute to a repo.

https://docs.github.com/en/get-started/quickstart/contributing-to-projects

Don't forget to give the project a star! Thanks again!

### Contact the Bus Open Data Service


If you require support or are experiencing issues, please contact the Bus Open Data Service Help Desk.

The Help Desk is available Monday to Friday, 9am to 5pm (excluding Bank Holidays in England and Wales, and the 24th December).

The Help Desk can be contacted by telephone or email as follows.

Telephone: +44 (0) 800 028 0930

Email: bodshelpdesk@kpmg.co.uk

There is a discord server for consumers of BODS Data:

https://discord.gg/4mMg5VXm5A




<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>






<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/KPMG-UK/bods_pseudo_test/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/github_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/github_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/github_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/github_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/github_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/github_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt

[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/linkedin_username
[python.com]: https://img.shields.io/badge/Python-0769AD?style=for-the-badge&logo=python&logoColor=white
[python-url]: https://www.python.org
