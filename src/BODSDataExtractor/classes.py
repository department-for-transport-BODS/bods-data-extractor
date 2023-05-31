from dataclasses import dataclass
from typing import List, Dict, Optional, Union

# set default value to null in optional values - dacite does this automatically

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
    Location: Optional[Location]


@dataclass
class StopPoints:
    AnnotatedStopPointRef: List[AnnotatedStopPointRef]


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
    DaysOfWeek: Optional[Dict]
    HolidaysOnly: Optional[Dict]


@dataclass
class WorkingDays:
    ServicedOrganisationRef: Optional[str]


@dataclass
class ServicedOrganisationDayType:
    DaysOfOperation: Optional[WorkingDays]


@dataclass
class BankHolidayOperation:
    DaysOfNonOperation: Optional[Dict]
    DaysOfOperation: Optional[Dict]


@dataclass
class From:
    Activity: Optional[str]
    StopPointRef: Optional[str]
    TimingStatus: Optional[str]
    _SequenceNumber: Optional[str] = 0

    @property
    def sequence_number(self):
        return self._SequenceNumber



@dataclass
class To:
    StopPointRef: Optional[str]
    TimingStatus: Optional[str]
    _SequenceNumber: Optional[str] = 1

    @property
    def sequence_number(self):
        return self._SequenceNumber


@dataclass
class OperatingProfile:
    RegularDayType: RegularDayType
    BankHolidayOperation: Optional[BankHolidayOperation]
    PublicUse: Optional[str]
    DaysOfNonOperation: Optional[Dict]
    RegisteredOperatorRef: Optional[str]
    ServicedOrganisationDayType: Optional[ServicedOrganisationDayType]


@dataclass
class VehicleJourneyTimingLink:
    DutyCrewCode: Optional[str]
    JourneyPatternTimingLinkRef: Optional[str]
    RunTime: Optional[str]
    From: Optional[From]
    To: Optional[To]


@dataclass
class VehicleJourneyTimingLinks:

    VehicleJourneyTimingLink: list[VehicleJourneyTimingLink]


@dataclass
class VehicleJourney:
    OperatorRef: Optional[str]
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
    OperatorRef: Optional[str]
    Direction: str
    RouteRef: Optional[str]
    JourneyPatternSectionRefs: Union[str, list]
    _id: Optional[str]

    @property
    def id(self):
        return self._id


@dataclass
class StandardService:
    Origin: str
    Destination: str
    UseAllPoints: Optional[str]
    #JourneyPattern: Union[JourneyPattern, List[JourneyPattern]]
    JourneyPattern: List[JourneyPattern]


@dataclass
class Line:
    _id: Optional[str]
    LineName: str
    OutboundDescription: Optional[OutboundDescription]
    InboundDescription: Optional[InboundDescription]

    @property
    def id(self):
        return self._id



@dataclass
class Lines:
    Line: list[Line]

@dataclass
class Service:
    ServiceCode: str
    Lines: Lines
    OperatingPeriod: OperatingPeriod
    OperatingProfile: Optional[OperatingProfile]
    TicketMachineServiceCode: Optional[str]
    RegisteredOperatorRef: str
    PublicUse: Optional[str]
    StandardService: StandardService



@dataclass
class JourneyPatternTimingLink:
    _id: str
    From: From
    To: To
    RouteLinkRef: Optional[str]
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