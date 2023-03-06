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
  
  
from dotenv import load_dotenv
import os

#load environment variables
load_dotenv()

#retrieve api key from environment variables
api = os.getenv('api_key')

#-------------------------------------------
#            FINE TUNED RESULTS
#-------------------------------------------
#intiate an object instance called my_bus_data_object with desired parameters 

my_bus_data_object = TimetableExtractor(api_key=api
                                 ,limit=100000      
                                 ,status = 'published' 
                                 ,service_line_level=True 
                                 ,stop_level=False
                                 ,bods_compliant=True
                                 )

#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

#save the extracted dataset level data to lcoal csv file
#my_bus_data_object.save_metadata_to_csv()

#save the extracted service line level data to dataset_level variable
filtered_service_line_level = my_bus_data_object.service_line_extract

#save the extracted service line level data to lcoal csv file
#my_bus_data_object.save_service_line_extract_to_csv()
