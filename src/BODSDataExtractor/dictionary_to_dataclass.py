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
class OperatingPeriod:
    StartDate: str


@dataclass
class RegularDayType:
    DaysOfWeek: Dict


@dataclass
class WorkingDays:
    ServicedOrganisationRef: Optional[str]

@dataclass
class ServicedOrganisationDayType:
    DaysOfOperation: WorkingDays


@dataclass
class BankHolidayOperation:
    DaysOfNonOperation: Dict
    DaysOfOperation: Optional[Dict]


@dataclass
class OperatingProfile:
    RegularDayType: RegularDayType
    BankHolidayOperation: BankHolidayOperation
    PublicUse: Optional[str]
    DaysOfNonOperation: Optional[Dict]
    RegisteredOperatorRef: Optional[str]
    ServicedOrganisationDayType: Optional[ServicedOrganisationDayType]


@dataclass
class VehicleJourney:
    OperatorRef: str
    Operational: Optional[Operational]
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
    _id: Optional[str]


@dataclass
class StandardService:
    Origin: str
    Destination: str
    UseAllPoints: Optional[str]
    JourneyPattern: List[JourneyPattern]


@dataclass
class Line:
    _id: Optional[str]
    LineName: str
    OutboundDescription: OutboundDescription
    InboundDescription: Optional[InboundDescription]


@dataclass
class Lines:
    Line: Line


@dataclass
class Service:
    ServiceCode: str
    Lines: Lines
    OperatingPeriod: OperatingPeriod
    OperatingProfile: Optional[OperatingProfile]
    TicketMachineServiceCode: Optional[str]# = field(init=False)
    RegisteredOperatorRef: str
    PublicUse: str
    StandardService: StandardService
    _CreationDateTime: Optional[str]#  = field(init=False)
    _ModificationDateTime: Optional[str]#  = field(init=False)
    _Modification:Optional[str]#  = field(init=False)
    _RevisionNumber: Optional[str]#  = field(init=False)


@dataclass
class From:
    Activity: Optional[str]
    StopPointRef: str
    TimingStatus: str


@dataclass
class To:
    StopPointRef: str
    TimingStatus: str


@dataclass
class JourneyPatternTimingLink:
    _id: str
    From: From
    To: To
    RouteLinkRef: str
    RunTime: str


@dataclass
class JourneyPatternSection:
    _id: str
    JourneyPatternTimingLink: List[JourneyPatternTimingLink]


@dataclass
class JourneyPatternSections:
    JourneyPatternSection: List[JourneyPatternSection]


with open(r'CHAM.xml', 'r', encoding='utf-8') as file:
    xml_text = file.read()
    xml_json = xmltodict.parse(xml_text, process_namespaces=False, attr_prefix='_')
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
    
    journey_pattern_object = from_dict(data_class=JourneyPatternSections, data=journey_pattern_json)

