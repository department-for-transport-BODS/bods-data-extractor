from BODSDataExtractor.extractor import TimetableExtractor
import csv

import os
from dotenv import load_dotenv
load_dotenv()
#retrieve api key from environment variables
api = os.environ.get('api_key')

# -------------------------------------------
#            FINE TUNED RESULTS
# -------------------------------------------
# intiate an object instance called my_bus_data_object with desired parameters 

my_bus_data_object = TimetableExtractor(api_key=api
                                 ,status = 'published' 
                                 ,service_line_level=True 
                                 ,stop_level=True 
                                 ,nocs=['RBTS']
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

#save the extracted stop level data to stop_level variable
stop_level = my_bus_data_object.timetable_dict

#stop_level variable is a dictionary of dataframes, which can be saved to csv as follows (saves in downloads folder)
my_bus_data_object.save_all_timetables_to_csv()

today_folder_path = my_bus_data_object.create_today_folder() # use this to find the files to iterate through

# Function to count the number of services in the download that have a timetable that begins before 8am

def count_services_start_time_before_eight_am(today_folder_path):
    csv_files = [file for file in os.listdir(today_folder_path) if file.endswith('.csv')]

    service_counter = 0
   
    for file in csv_files:
        file_path = os.path.join(today_folder_path,file)
        break_out_loop = False 

        with open(file_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader,None) # to skip the header row in the csv files

            #Process data in each csv file to check whether times between 4am and 8am are present
            for row in reader:
                if break_out_loop == True:
                    break
                for col in row[10::]:                
                    try:
                        start_time = col
                        start_hour = int(start_time[:2])
                        if start_hour >= 4 and start_hour < 8:
                            service_counter+=1
                            break_out_loop = True
                            break
                    except:
                        pass
                
    return service_counter

counting_files = count_services_start_time_before_eight_am(today_folder_path)
print("The number of services with start times before 8am is: " + str(counting_files))