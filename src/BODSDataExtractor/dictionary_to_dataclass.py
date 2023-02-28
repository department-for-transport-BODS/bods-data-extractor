
import inspect
from dataclasses import dataclass,field,is_dataclass
from typing import List,Dict,Optional
from dacite import from_dict


def dict_to_object(dictionary,class_to_convert):
    return class_to_convert(**dictionary)




@dataclass
class JourneyPatternTimingLink:
    id: str
    FromStop: dict
    To: dict
    RouteLinkRef: str
    RunTime: str

@dataclass
class JourneyPatternSection:
    id: str
    JourneyPatternTimingLink: List[JourneyPatternTimingLink]

@dataclass
class Route:
    id: str
    description: str
    route_section_ref: str

@dataclass
class RouteSection:
    id: str
    route_link_id: str
    from_stop_point_ref: str
    to_stop_point_ref: str

@dataclass
class Location:
    Longitude: str
    Latitude: str

@dataclass
class AnnotatedStopPointRef:
    StopPointRef: str
    CommonName: str
    Location: Location

@dataclass
class StopPoints:
    AnnotatedStopPointRef: AnnotatedStopPointRef

@dataclass
class TicketMachine:
    JourneyCode: str

@dataclass
class Operational:
    TicketMachine: TicketMachine

@dataclass
class VehicleJourney:
    OperatorRef: str
    Operational: Operational
    VehicleJourneyCode: str
    ServiceRef: str
    LineRef: str
    JourneyPatternRef: str
    DepartureTime: str
    OperatingProfile: opt

@dataclass
class OutboundDescription:
    Description: str
    
@dataclass
class InboundDescription:
    Description: str

@dataclass
class Lines:
    LineName: str
    OutboundDescription: OutboundDescription
    InboundDescription: InboundDescription
    _id: Optional[str] 


@dataclass
class OperatingPeriod:
    StartDate: str

@dataclass
class DaysOfWeek:
    day : str
    
@dataclass
class DaysOfNonOperation:
    day : str

@dataclass
class RegularDayType:
    DaysOfWeek: List[DaysOfWeek]
    
@dataclass
class BankHolidayOperation:
    DaysOfNonOperation: List[DaysOfNonOperation]

@dataclass
class OperatingProfile:
    RegularDayType: Dict[str,RegularDayType]
    DaysOfNonOperation: DaysOfNonOperation
    BankHolidayOperation:

@dataclass
class JourneyPattern:
    DestinationDisplay: str
    OperatorRef: str
    Direction: str
    RouteRef: str
    JourneyPatternSectionRefs: str
    _id:str
    
@dataclass
class StandardService:
    Origin: str
    Destination: str
    UseAllPoints: bool
    JourneyPattern	: List[JourneyPattern]

@dataclass
#define default values here

class Service:
    ServiceCode: str
    Lines: Dict[str,Lines]
    OperatingPeriod: OperatingPeriod
    OperatingProfile: OperatingProfile
    TicketMachineServiceCode: Optional[str]
    RegisteredOperatorRef: str
    PublicUse: bool
    StandardService: StandardService
    _CreationDateTime: Optional[str]
    _ModificationDateTime: Optional[str]
    _Modification:Optional[str]
    _RevisionNumber: Optional[int]



import xmltodict

with open(r'ADER.xml', 'r', encoding='utf-8') as file:
    xml_text = file.read()
    xml_json = xmltodict.parse(xml_text, process_namespaces=False)
    xml_root = xml_json['TransXChange']
    services_json = xml_root['Services']['Service']
    
    
    # #Checking attributes in class with elements taken out of JSON
    # for attribute_name in Service.__annotations__:
    #     print(attribute_name)
    #     if attribute_name in services_json:
    #         print("Found")
    #     else:
    #         print("Not Found")
            
    
    
    
    serviceobject = from_dict(data_class=Service, data=services_json)
    
    
    
    
    
    
    serviceobject=dict_to_object(services_json, Service)
    
    #change attribute name to class item
    
    print(serviceobject.__dict__.items())
    
    
    for attribute_name, attribute_value in serviceobject.__dict__.items():
        
        print(attribute_name, ":", type(attribute_value))
        
        
        
        if str(type(attribute_value))=="<class 'dict'>":
            print("populate further")
            
            new_object=attribute_name+"object"
            
            
           # for _, cls in inspect.getmembers(inspect.getmodule()):
                
              #  if is_dataclass(cls) and cls.name == attribute_name:
               #     print("is dataclass")
                #    print (cls.name)
            
            
            new_object=dict_to_object(attribute_value, attribute_name)
    
    
    
    
    

#     subclass_count=0
        
#     subclassdict={}
    
#     #assigning default values
#     #use null instead of empty
#     ServiceObject1=Service(None, None, None, None, None, None, None, None, None, None, None, None)
    
    
    
#     print(Service.__annotations__.items())
    
#     for key,value in Service.__annotations__.items():

                        
            
#         if "<class '__main__." in str(value):
#             keytoadd=str(key)
            
#             valuetoadd=str(value)
            
#             print("keep looking")
#             subclass_count=subclass_count+1
#             subclassdict[keytoadd]=valuetoadd
                
#                 #ReachRoot()
                
#         else:
#             if key in services_json:
#                 attribute=str(key)
                
#                 print(attribute)
            
#                 setattr(ServiceObject1,attribute,services_json[key])
                
#                 print(ServiceObject1)
#                 print("assign attribute here")    
#                 print(key)
#                 print(value)
                    
#             else:
#                 print("This attribute doesn't exist in this file")
        
        
        
#         print(subclassdict)
        


# #Python dictionary to object, should already know before for loop what attributes refer to other classes
    
    
#     print(ServiceObject1.ServiceCode)    
        
        
        
        
        
#         # if hasattr(Service,"ServiceCode"):
#         #     print("PRESENT")
#         # else:
#         #     print(hasattr(Service,i))
            
    
#     services_json= Service
    
    
    
#     vehicle_journey_json = xml_root['VehicleJourneys']['VehicleJourney']
#     journey_pattern_json = xml_root['JourneyPatternSections']['JourneyPatternSection']