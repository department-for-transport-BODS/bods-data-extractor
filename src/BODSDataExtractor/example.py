# =============================================================================
# This examples file is designed to show an example use case of this library,
# and be executed in one go.
# In this example, we are interested in 2 particular noc codes (BPTR and RBTS),
# that have been flagged as in need of further investigation from previous manual analysis.
# 
# All that is needed is to enter your api key in the variable 'api', and run the code!
# =============================================================================

try:
  from BODSDataExtractor.extractor import TimetableExtractor
except:
  from extractor import TimetableExtractor
  
import os

#retrieve api key from environment variables
api = os.environ.get('BODS_API_KEY')

#-------------------------------------------
#            FINE TUNED RESULTS
#-------------------------------------------
#intiate an object instance called my_bus_data_object with desired parameters 

my_bus_data_object = TimetableExtractor(api_key=api
                                 ,limit=0
                                 ,offset=0
                                 ,status = 'published' 
                                 ,service_line_level=True 
                                 ,stop_level=True
                                 ,nocs=['BPTR','RBTS']
                                 ,bods_compliant=True
                                 )

#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

#save the extracted dataset level data to lcoal csv file
my_bus_data_object.save_metadata_to_csv()

#save the extracted service line level data to dataset_level variable
filtered_service_line_level = my_bus_data_object.service_line_extract

#save the extracted service line level data to lcoal csv file
my_bus_data_object.save_service_line_extract_to_csv()

#stop_level_extract is a dataframe, which contains a collumn of timetables (inbound/outbound) to be saved to csv as follows (saves in project folder)
my_bus_data_object.save_timetables()

#visualise a particular service line on an interactive map
#my_bus_data_object.visualise_service_line('PB0001746:3')


#-------------------------------------------
#       REPORTING / ANALYTICS
#-------------------------------------------
count_of_operators = my_bus_data_object.count_operators() #returns count of distinct operators (measured by operator_name) in a chosen dataset

count_of_service_codes = my_bus_data_object.count_service_codes()# returns count of unique service codes chosen dataset

valid_service_codes = my_bus_data_object.valid_service_codes()# returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes and a dataframe with all the invalid service codes.

services_published_in_TXC_2_4 = my_bus_data_object.services_published_in_TXC_2_4()#returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

datasets_published_in_TXC_2_4 = my_bus_data_object.datasets_published_in_TXC_2_4()# returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

red_dq = my_bus_data_object.red_dq_scores() #returns the number of operators in a table with red dq scores

less_than_10 = my_bus_data_object.dq_less_than_x(90) # takes an integer as input (in this case 10) and returns a list of operators with a data quality score less than that integer

no_lic_no = my_bus_data_object.no_licence_no() # returns a report listing which datasets contain files which do not have a licence number


