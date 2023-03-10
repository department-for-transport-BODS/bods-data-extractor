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
    VehicleJourneyTimingLink: Optional[list[VehicleJourneyTimingLink]]
    
    _CreationDateTime: Optional[str]#  = field(init=False)
    _ModificationDateTime: Optional[str]#  = field(init=False)
    _Modification:Optional[str]#  = field(init=False)
    _RevisionNumber: Optional[str]#  = field(init=False)



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



    


def extract_runtimes(TimingLink):
    min_runtime=int(re.findall(r'(\d+)', TimingLink)[0])
    sec_runtime=int(re.findall(r'(\d+)', TimingLink)[1])
    
    return min_runtime,sec_runtime


#Checks if vj_timing_link is populated so we can take runtime from here
def check_vj_timing_link(min_runtime,sec_runtime):
    if min_runtime==0 and sec_runtime==0:
        if vj.VehicleJourneyTimingLink is not None:
            for VJTL in vj.VehicleJourneyTimingLink :
                
                if JourneyPatternTimingLink._id==VJTL.JourneyPatternTimingLinkRef:
                    print("We have a match")
                    min_runtime=int(re.findall(r'(\d+)', VJTL.RunTime)[0])
                    sec_runtime=int(re.findall(r'(\d+)', VJTL.RunTime)[1])
                else:
                    pass
        else:
            print("VJ TIMIMG LINK Not Present")
    
    print(min_runtime,sec_runtime)
    
    return min_runtime,sec_runtime







with open(r'SCHN.xml', 'r', encoding='utf-8') as file:
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
    
    journey_pattern_section_object = from_dict(data_class=JourneyPatternSections, data=journey_pattern_json)
    
    #route_object= from_dict(data_class=Route, data=route)


timetable= pd.DataFrame(columns=["Sequence Number", "Stop Point Ref", "VJ"])



vj=vehicle_journey.VehicleJourney[0]


#for vj in vehicle_journey.VehicleJourney:
    #create a dataframe here 

JourneyPatternRef_var=vj.JourneyPatternRef
DepartureTime_var=vj.DepartureTime

#convert to datetime object
DepartureTime_var=datetime.datetime.strptime(DepartureTime_var, "%H:%M:%S")


print(JourneyPatternRef_var)
print(DepartureTime_var)


def add_to_dataframe(where):
    print("Sorry the solution isn't ready yet")
    #attempt started here making a generalised row addition, deciding to and from in parameter passed
    
    
    
    # print(JourneyPatternTimingLink.where._SequenceNumber)
    # row_to_add.append(JourneyPatternTimingLink.From._SequenceNumber)

    # #StopPointRef
    # print(JourneyPatternTimingLink.where.StopPointRef)
    # row_to_add.append(JourneyPatternTimingLink.From.StopPointRef)
    
    # #initital departure time
    # row_to_add.append(DepartureTime_var.time())
    
    # #show link id
    # print(JourneyPatternTimingLink._id)

    # #extract run times from runtime string
    # min_runtime,sec_runtime=extract_runtimes(JourneyPatternTimingLink.RunTime)

    # #check if these run times are in vj timing link and reassign accordingly
    # min_runtime,sec_runtime=check_vj_timing_link(min_runtime,sec_runtime)
    
    # #add onto departure time
    # NewDepartureTime=(DepartureTime_var + datetime.timedelta(seconds=sec_runtime) + datetime.timedelta(minutes=min_runtime))      
    
    # DepartureTime_var=NewDepartureTime
    
    # timetable.loc[len(timetable)]=row_to_add





for jp in service_object.StandardService.JourneyPattern:

    
    
    if JourneyPatternRef_var==jp._id:
        
        print(JourneyPatternRef_var)
        print(jp._id)
        print("match")
        
        
        JourneyPatternSectionRef_var=jp.JourneyPatternSectionRefs
        print(JourneyPatternSectionRef_var)
        
        for jpSecRef in journey_pattern_section_object.JourneyPatternSection:

            if jpSecRef._id==JourneyPatternSectionRef_var:
                print(JourneyPatternSectionRef_var)
                print(jpSecRef._id)
                print("match")
                
                #JourneyPatternTiming Link Index
                first=True
                

                for JourneyPatternTimingLink in jpSecRef.JourneyPatternTimingLink:
                    

                    
                    row_to_add=[]

                    
                    
                    
                    if first:
                        first=False
                        print(JourneyPatternTimingLink.From._SequenceNumber)
                        row_to_add.append(JourneyPatternTimingLink.From._SequenceNumber)
            
                        #StopPointRef
                        print(JourneyPatternTimingLink.From.StopPointRef)
                        row_to_add.append(JourneyPatternTimingLink.From.StopPointRef)
                        
                        #initital departure time
                        row_to_add.append(DepartureTime_var.time())
                        
                        #show link id
                        print(JourneyPatternTimingLink._id)

                        #extract run times from runtime string
                        min_runtime,sec_runtime=extract_runtimes(JourneyPatternTimingLink.RunTime)

                        #check if these run times are in vj timing link and reassign accordingly
                        min_runtime,sec_runtime=check_vj_timing_link(min_runtime,sec_runtime)
                        
                        #add onto departure time
                        NewDepartureTime=(DepartureTime_var + datetime.timedelta(seconds=sec_runtime) + datetime.timedelta(minutes=min_runtime))      
                        
                        DepartureTime_var=NewDepartureTime
                        
                        timetable.loc[len(timetable)]=row_to_add
                        
                        
                        row_to_add=[]
                        
                        

  
                        #Sequence Number
                        print(JourneyPatternTimingLink.To._SequenceNumber)
                        row_to_add.append(JourneyPatternTimingLink.To._SequenceNumber)
                        
                        
                        #StopPointRef
                        print(JourneyPatternTimingLink.To.StopPointRef)
                        row_to_add.append(JourneyPatternTimingLink.To.StopPointRef)
                        
                        
                        
                        NewDepartureTime=(DepartureTime_var + datetime.timedelta(seconds=sec_runtime) + datetime.timedelta(minutes=min_runtime))  
                        DepartureTime_var=NewDepartureTime
                        
                        row_to_add.append(DepartureTime_var.time())
                        
                        
                    else:
                        
                    #Keep track of JourneyPatternTiming Link Index

                    
                    #JourneyPatternTimingLink
                        print(JourneyPatternTimingLink._id)
                        
                        
                        
        
                        #Sequence Number
                        print(JourneyPatternTimingLink.To._SequenceNumber)
                        row_to_add.append(JourneyPatternTimingLink.To._SequenceNumber)
                        
                        
                        #StopPointRef
                        print(JourneyPatternTimingLink.To.StopPointRef)
                        row_to_add.append(JourneyPatternTimingLink.To.StopPointRef)
    
                        
                        row_to_add.append(DepartureTime_var.time())
                        
                    
                    
                    min_runtime,sec_runtime=extract_runtimes(JourneyPatternTimingLink.RunTime)
                    
                    
                    min_runtime,sec_runtime=extract_runtimes(JourneyPatternTimingLink.RunTime)
                    print(min_runtime,sec_runtime)
                    
                    
                    min_runtime,sec_runtime=check_vj_timing_link(min_runtime,sec_runtime)

        
                    #change x to arrival time
                    NewDepartureTime=(DepartureTime_var + datetime.timedelta(seconds=sec_runtime) + datetime.timedelta(minutes=min_runtime))      
                    
                    print(NewDepartureTime.time())

                    DepartureTime_var=NewDepartureTime
                    
                    
                    print(DepartureTime_var.time())
                        
                    #row_to_add.append(leaving_time)
                    
                    
                    
                    
                    
                    timetable.loc[len(timetable)]=row_to_add 
                    
                    
                    print(len(jpSecRef.JourneyPatternTimingLink))
                    




                                
