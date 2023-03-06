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
                                 ,status = 'published' 
                                 ,service_line_level=False 
                                 ,stop_level=True 
                                 ,nocs=['BPTR','RBTS']
                                 ,bods_compliant=True
                                 )

#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

#save the extracted dataset level data to lcoal csv file
#my_bus_data_object.save_metadata_to_csv()

#save the extracted stop level data to stop_level variable
stop_level = my_bus_data_object.timetable_dict

#stop_level variable is a dictionary of dataframes, which can be saved to csv as follows (saves in downloads folder)
#my_bus_data_object.save_all_timetables_to_csv()
