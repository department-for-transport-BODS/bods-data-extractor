# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 14:36:32 2023

@author: aakram7
"""
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Optional
from dacite import from_dict
import xmltodict
import datetime

# set default value to null in optional values


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

    @property
    def sequence_number(self):
        return self._SequenceNumber



@dataclass
class To:
    StopPointRef: Optional[str]
    TimingStatus: Optional[str]
    _SequenceNumber: Optional[str]

    @property
    def sequence_number(self):
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
    DutyCrewCode: Optional[str]
    JourneyPatternTimingLinkRef: Optional[str]
    RunTime: Optional[str]
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

    @property
    def id(self):
        return self._id


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

    @property
    def id(self):
        return self._id



@dataclass
class Lines:
    Line: Line


@dataclass
class Service:
    ServiceCode: str
    Lines: Lines
    OperatingPeriod: OperatingPeriod
    OperatingProfile: Optional[OperatingProfile]
    TicketMachineServiceCode: Optional[str]
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

    @property
    def id(self):
        return self._id


@dataclass
class JourneyPatternSection:
    _id: str
    JourneyPatternTimingLink: List[JourneyPatternTimingLink]

    @property
    def id(self):
        return self._id


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

                if journey_pattern_timing_link.id == vjtl.JourneyPatternTimingLinkRef:
                    runtime = vjtl.RunTime

                    return runtime

                else:
                    pass

        else:
            print(f"VJ timing link Not Present: {journey_pattern_timing_link}")

    else:
        return runtime


def next_jptl_in_sequence(jptl, vj_departure_time, first_jptl=False):
    """Returns the sequence number, stop point ref and time for a JPTL to be added to a timetable df"""

    runtime = extract_runtimes(jptl)

    to_sequence = [int(jptl.To.sequence_number),
                   str(jptl.To.StopPointRef),
                   pd.Timedelta(runtime)]

    if first_jptl:

        from_sequence = [jptl.From.sequence_number,
                         jptl.From.StopPointRef,
                         vj_departure_time]

        return from_sequence, to_sequence

    if not first_jptl:
        return to_sequence

def collate_vjs(direction, collated_timetable):
    '''Combines all vehicle journeys together for inbound or outbound'''

    if direction.empty:
        pass
    elif collated_timetable.empty:
        # match stop point ref + sequence number with the initial timetable's stop point ref+sequence number
        collated_timetable=collated_timetable.merge(direction, how='outer', left_index=True, right_index=True)
    else:
        # match stop point ref + sequence number with the initial timetable's stop point ref+sequence number
        collated_timetable = pd.merge(direction, collated_timetable, on=['Stop Point Ref', "Sequence Number"], how='outer').fillna("-")

    return collated_timetable

def reformat_times(direction):
    '''Turns the time deltas into time of day, final column is formatted as string for now outbound'''
    direction[f"{vj.VehicleJourneyCode}"] = direction[f"{vj.VehicleJourneyCode}"].cumsum()
    direction[f"{vj.VehicleJourneyCode}"] = direction[f"{vj.VehicleJourneyCode}"].map(lambda x: x + base_time)
    direction[f"{vj.VehicleJourneyCode}"] = direction[f"{vj.VehicleJourneyCode}"].map(lambda x: x.strftime('%H:%M'))


    return direction[f"{vj.VehicleJourneyCode}"]




with open(r'ANEA_MONFRI.xml', 'r', encoding='utf-8') as file:
    xml_text = file.read()
    xml_json = xmltodict.parse(xml_text, process_namespaces=False, attr_prefix='_')
    xml_root = xml_json['TransXChange']
    services_json = xml_root['Services']['Service']
    vehicle_journey_json = xml_root['VehicleJourneys']
    journey_pattern_json = xml_root['JourneyPatternSections']

    service_object = from_dict(data_class=Service, data=services_json)
    vehicle_journey = from_dict(data_class=VehicleJourneys, data=vehicle_journey_json)
    journey_pattern_section_object = from_dict(data_class=JourneyPatternSections, data=journey_pattern_json)


# Define a base time to add run times to
base_time = datetime.datetime(2000, 1, 1, 0, 0, 0)

# List of journey patterns in service object
journey_pattern_list = service_object.StandardService.JourneyPattern

# Iterate once through JPs and JPS to find the indices in the list of each id
journey_pattern_index = {key.id: value for value, key in enumerate(service_object.StandardService.JourneyPattern)}
journey_pattern_section_index = {key.id: value for value, key in enumerate(journey_pattern_section_object.JourneyPatternSection)}


collated_timetable_inbound= pd.DataFrame()

collated_timetable_outbound= pd.DataFrame()


# Take an example vehicle journey to start with, later we will iterate through multiples ones

for vj in vehicle_journey.VehicleJourney:


    departure_time = pd.Timedelta(vj.DepartureTime)

    # Init empty timetable for a single Vehicle journey
    timetable = pd.DataFrame(columns=["Sequence Number", "Stop Point Ref", f"{vj.VehicleJourneyCode}"])

    #init empty timetable for outbound Vehicle journey
    outbound = pd.DataFrame(columns=["Sequence Number", "Stop Point Ref", f"{vj.VehicleJourneyCode}"])

    # init empty timetable for inbound Vehicle journey
    inbound = pd.DataFrame(columns=["Sequence Number", "Stop Point Ref", f"{vj.VehicleJourneyCode}"])



    # Create vars for relevant indices of this vehicle journey
    vehicle_journey_jp_index = journey_pattern_index[vj.JourneyPatternRef]
    vehicle_journey_jps_index = journey_pattern_section_index[journey_pattern_list[vehicle_journey_jp_index].JourneyPatternSectionRefs]
    #vehicle_journey_jpsr = journey_pattern_section_object.JourneyPatternSection[vehicle_journey_jp_index].id

    # Mark the first JPTL
    first = True

    #intitalise_data_for_timetable()

    # Loop through relevant timing links

    for JourneyPatternTimingLink in journey_pattern_section_object.JourneyPatternSection[vehicle_journey_jps_index].JourneyPatternTimingLink:


        direction = service_object.StandardService.JourneyPattern[vehicle_journey_jp_index].Direction

        RouteRef = service_object.StandardService.JourneyPattern[vehicle_journey_jp_index].RouteRef

        JourneyPattern_id=  service_object.StandardService.JourneyPattern[vehicle_journey_jp_index]._id

        # first JPTL should use 'From' AND 'To' stop data
        if first:

            # remaining JPTLs are not the first one
            first = False

            timetable_sequence = next_jptl_in_sequence(JourneyPatternTimingLink,
                                                   departure_time,
                                                   first_jptl=True)


            # Add first sequence, stop ref and departure time
            first_timetable_row = pd.DataFrame([timetable_sequence[0]], columns=timetable.columns)


            #add to sequence number and stop point ref to the initial timetable

            if direction == 'outbound':
                outbound = pd.concat([outbound, first_timetable_row], ignore_index=True)
                outbound.loc[len(outbound)] = timetable_sequence[1]

            elif direction == 'inbound':
                inbound = pd.concat([inbound, first_timetable_row], ignore_index=True)
                inbound.loc[len(inbound)] = timetable_sequence[1]
            else:
                print("Unknown Direction")

        else:
            timetable_sequence = next_jptl_in_sequence(JourneyPatternTimingLink, departure_time)

            if direction == 'outbound':
                outbound.loc[len(outbound)] = timetable_sequence
            elif direction == 'inbound':
                inbound.loc[len(inbound)] = timetable_sequence
            else:
                print("Unknown Direction")


    outbound[f"{vj.VehicleJourneyCode}"]= reformat_times(outbound)

    #mention with previous outbound/inbound checks
    outbound.loc[0.5] = ["RouteID", "->", RouteRef]
    outbound.loc[0.6] = ["Journey Pattern ", "->", JourneyPattern_id]

    inbound[f"{vj.VehicleJourneyCode}"] = reformat_times(inbound)

    #mention with previous outbound/inbound checks
    #inbound.loc[-0.5] = ["RouteID", "->", RouteRef]
    #inbound.loc[-0.6] = ["Journey Pattern ", "->", JourneyPattern_id]

    #collect vj information together for outbound
    collated_timetable_outbound=collate_vjs(outbound, collated_timetable_outbound)

    # collect vj information together for inbound
    collated_timetable_inbound = collate_vjs(inbound, collated_timetable_inbound)
