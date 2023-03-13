# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 14:36:32 2023

@author: aakram7
"""
import pandas as pd
from dataclasses import dataclass, field, is_dataclass
from typing import List, Dict, Optional
from dacite import from_dict
import xmltodict
import datetime
import re
import pandas as pd

# set default value to null in optional values


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
    Origin: Optional[str]
    Destination: Optional[str]


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
class From:
    Activity: Optional[str]
    StopPointRef: Optional[str]
    TimingStatus: Optional[str]
    _SequenceNumber: Optional[str]


@dataclass
class To:
    StopPointRef: Optional[str]
    TimingStatus: Optional[str]
    _SequenceNumber: Optional[str]

    @property
    def SequenceNumber(self):
        return self._SequenceNumber


@dataclass
class OperatingProfile:
    RegularDayType: RegularDayType
    BankHolidayOperation: BankHolidayOperation
    PublicUse: Optional[str]
    DaysOfNonOperation: Optional[Dict]
    RegisteredOperatorRef: Optional[str]
    ServicedOrganisationDayType: Optional[ServicedOrganisationDayType]


@dataclass
class VehicleJourneyTimingLink:
    DutyCrewCode:Optional[str]
    JourneyPatternTimingLinkRef:Optional[str]
    RunTime:Optional[str]
    From: From
    To: To




@dataclass
class VehicleJourneyTimingLinks:

    VehicleJourneyTimingLink: list[VehicleJourneyTimingLink]


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
    DepartureDayShift: Optional[str]
    VehicleJourneyTimingLink: Optional[list[VehicleJourneyTimingLink]]




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


def extract_runtimes(journey_pattern_timing_link):

    """Extract the runtimes from timing links as a string. If JPTL run time is 0, VJTL will be cecked"""

    # extract run times from jptl
    runtime = journey_pattern_timing_link.RunTime

    # if jptl runtime is 0, find equivalent vehicle journey pattern timing link run time
    if runtime == 'PT0M0S':

        if vj.VehicleJourneyTimingLink is not None:

            for vjtl in vj.VehicleJourneyTimingLink:


                if journey_pattern_timing_link._id == vjtl.JourneyPatternTimingLinkRef:
                    runtime = vjtl.RunTime

                    return runtime

                else:
                    pass

        else:
            print("VJ timing link Not Present")

    else:
        return runtime


def next_jptl_in_sequence(jptl, vj_departure_time, first_jptl=False):
    """Returns the sequence number, stop point ref and time for a JPTL to be added to a timetable df"""

    runtime = extract_runtimes(jptl)

    to_sequence = [jptl.To._SequenceNumber,
                   jptl.To.StopPointRef,
                   pd.Timedelta(runtime)]

    if first_jptl:

        from_sequence = [jptl.From._SequenceNumber,
                         jptl.From.StopPointRef,
                         vj_departure_time]

        return from_sequence, to_sequence

    if not first_jptl:
        return to_sequence


with open(r'SCHN.xml', 'r', encoding='utf-8') as file:
    xml_text = file.read()
    xml_json = xmltodict.parse(xml_text, process_namespaces=False, attr_prefix='_')
    xml_root = xml_json['TransXChange']
    services_json = xml_root['Services']['Service']
    vehicle_journey_json = xml_root['VehicleJourneys']
    journey_pattern_json = xml_root['JourneyPatternSections']

    service_object = from_dict(data_class=Service, data=services_json)
    vehicle_journey = from_dict(data_class=VehicleJourneys, data=vehicle_journey_json)
    journey_pattern_section_object = from_dict(data_class=JourneyPatternSections, data=journey_pattern_json)

# Init empty timetable for a single Vehicle journey
timetable = pd.DataFrame(columns=["Sequence Number", "Stop Point Ref", "VJ"])

# List of journey patterns in service object
journey_pattern_list = service_object.StandardService.JourneyPattern

# Take an example vehicle journey to start with, later we will iterate through multiples ones
vj = vehicle_journey.VehicleJourney[0]
departure_time = pd.Timedelta(vj.DepartureTime)

# Define a base time to add run times to
base_time = datetime.datetime(2000, 1, 1, 0, 0, 0)


# Iterate once through JPs and JPS to find the indices in the list of each _id
journey_pattern_index = {key._id: value for value, key in enumerate(service_object.StandardService.JourneyPattern)}
journey_pattern_section_index = {key._id: value for value, key in enumerate(journey_pattern_section_object.JourneyPatternSection)}

# Create vars for relevant indices of this vehicle journey
vehicle_journey_jp_index = journey_pattern_index[vj.JourneyPatternRef]
vehicle_journey_jps_index = journey_pattern_section_index[journey_pattern_list[vehicle_journey_jp_index].JourneyPatternSectionRefs]
vehicle_journey_jpsr = journey_pattern_section_object.JourneyPatternSection[vehicle_journey_jp_index]._id

# Mark the first JPTL
first = True

# Loop through relevant timing links
for JourneyPatternTimingLink in journey_pattern_section_object.JourneyPatternSection[vehicle_journey_jps_index].JourneyPatternTimingLink:


    # first JPTL should use 'From' AND 'To' stop data
    if first:

        # remaining JPTLs are not the first one
        first = False

        timetable_sequence = next_jptl_in_sequence(JourneyPatternTimingLink,
                                                   departure_time,
                                                   first_jptl=True)
        # Add first sequence, stop ref and departure time
        timetable.loc[0] = timetable_sequence[0]

        # Add second sequence etc
        timetable.loc[len(timetable)] = timetable_sequence[1]

    else:
        timetable_sequence = next_jptl_in_sequence(JourneyPatternTimingLink, departure_time)
        timetable.loc[len(timetable)] = timetable_sequence


# Turns the time deltas into time of day, final column is formatted as string for now
timetable['VJ'] = timetable['VJ'].cumsum()
timetable['VJ'] = timetable['VJ'].map(lambda x: x + base_time)
timetable['VJ'] = timetable['VJ'].map(lambda x: x.strftime('%H:%M'))
