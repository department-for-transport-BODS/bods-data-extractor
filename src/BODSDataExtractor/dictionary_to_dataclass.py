
import inspect
from dataclasses import dataclass, field, is_dataclass
from typing import List, Dict, Optional
from dacite import from_dict
import xmltodict


def dict_to_object(dictionary,class_to_convert):
    return class_to_convert(**dictionary)


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
class OutboundDescription:
    Description: str


@dataclass
class InboundDescription:
    Description: str


@dataclass
class Lines:
    LineName: str
    OutboundDescription: OutboundDescription
    InboundDescription: Optional[InboundDescription]
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
    DaysOfWeek: Optional[List[DaysOfWeek]]


@dataclass
class BankHolidayOperation:
    DaysOfNonOperation: Optional[List[DaysOfNonOperation]]


@dataclass
class OperatingProfile:
    RegularDayType: Dict[str,RegularDayType]
    #DaysOfNonOperation: Optional[Dict[str,DaysOfNonOperation]]
    BankHolidayOperation:Dict[str,BankHolidayOperation]


@dataclass
class VehicleJourney:
    OperatorRef: str
    Operational: Operational
    VehicleJourneyCode: str
    ServiceRef: str
    LineRef: str
    JourneyPatternRef: str
    DepartureTime: str
    OperatingProfile: Optional[OperatingProfile]


@dataclass
class VehicleJourneys:
    VehicleJourney: List[VehicleJourney]


@dataclass
class JourneyPattern:
    DestinationDisplay: str
    OperatorRef: str
    Direction: str
    RouteRef: str
    JourneyPatternSectionRefs: str
    _id:Optional[str]


@dataclass
class StandardService:
    Origin: str
    Destination: str
    UseAllPoints: Optional[str]
    JourneyPattern	: List[JourneyPattern]


@dataclass
class Service:
    ServiceCode: str
    Lines: Dict[str,Lines]
    OperatingPeriod: OperatingPeriod
    OperatingProfile: Optional[OperatingProfile]
    TicketMachineServiceCode: Optional[str] = field(init=False)
    RegisteredOperatorRef: str
    PublicUse: str
    StandardService: StandardService
    _CreationDateTime: Optional[str]  = field(init=False)
    _ModificationDateTime: Optional[str]  = field(init=False)
    _Modification:Optional[str]  = field(init=False)
    _RevisionNumber: Optional[int]  = field(init=False)


@dataclass
class From:
    Activity: str
    StopPointRef: str
    TimingStatus: str


@dataclass
class To:
    StopPointRef: str
    TimingStatus: str


@dataclass
class JourneyPatternTimingLink:
    id: str
    From: From
    To: To
    RouteLinkRef: str
    RunTime: str


@dataclass
class JourneyPatternSection:
    # running into an issue here because the 'id' in a JPS is an xml attribute and is parsed as '@id' this doesnt/
    # match the field name in the dataclass. How can we handle xml attributes in python dataclasses?
    id: str
    JourneyPatternTimingLink: List[JourneyPatternTimingLink]


@dataclass
class JourneyPatternSections:
    JourneyPatternSection: List[JourneyPatternSection]


with open(r'ADER.xml', 'r', encoding='utf-8') as file:
    xml_text = file.read()
    xml_json = xmltodict.parse(xml_text, process_namespaces=False)
    xml_root = xml_json['TransXChange']
    services_json = xml_root['Services']['Service']
    vehicle_journey_json = xml_root['VehicleJourneys']
    journey_pattern_json = xml_root['JourneyPatternSections']
    
    # #Checking attributes in class with elements taken out of JSON
    # for attribute_name in Service.__annotations__:
    #     print(attribute_name)
    #     if attribute_name in services_json:
    #         print("Found")
    #     else:
    #         print("Not Found")

    service_object = from_dict(data_class=Service, data=services_json)
    
    vehicle_journey = from_dict(data_class=VehicleJourneys, data=vehicle_journey_json)
    
    journey_pattern_object = from_dict(data_class=JourneyPatternTimingLink, data=journey_pattern_json)


    

    
    

    
