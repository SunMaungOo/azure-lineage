from dataclasses import dataclass,field
from enum import Enum
from typing import List,Optional,Union,Any,Dict
from graph import Edge
from datetime import datetime

class DatasetType(Enum):
    Oracle = 1
    AzureSQL = 2
    Synapse = 3
    Blob = 4
    OnPrimeMSSQL = 5
    Unsupported = 6

class LinkedServiceType(Enum):
    Oracle = 1
    AzureSQL = 2
    Synapse = 3 
    Blob = 4
    OnPrimeMSSQL = 5
    Unsupported = 6

class ParameterType(Enum):
    Expression=1
    Static=2

@dataclass
class Parameter:
    value:str
    parameter_type:ParameterType

@dataclass
class QueryDataset:
    name:str
    type:DatasetType

@dataclass
class SingleTableDataset:
    name:str
    type:DatasetType
    schema:Optional[Parameter]
    table:Optional[Parameter]

@dataclass
class LocationDataset:
    name:str
    type:DatasetType
    location:str

ProcessDataset = Union[QueryDataset,SingleTableDataset,LocationDataset]

@dataclass
class Dataset:
    #data set name
    name:str
    #data set type 
    type:DatasetType
    linked_service_name:str
    info:Optional[ProcessDataset]

@dataclass
class DatabaseLinkedService:
    host:Parameter
    database:Parameter

@dataclass
class BlobLinkedService:
    url:Parameter

ProcessLinkedService = Union[DatabaseLinkedService,BlobLinkedService]

@dataclass
class LinkedService:
    name:str
    type:LinkedServiceType
    info:ProcessLinkedService

@dataclass
class PipelineLineage:
    pipeline_name:str
    lineage:List[Edge]


@dataclass
class APIDatasetResource:
    dataset_name:str
    linked_service_name:str
    azure_data_type:str
    #dataset properties
    properties:Dict[str,Any]

@dataclass
class APILinkedServiceResource:
    linked_service_name:str
    azure_data_type:str
    #linked service properties
    properties:Dict[str,Any]


@dataclass
class APITriggerResource:
    trigger_name:str
    trigger_type:str
    runtime_state:str
    pipeline_names:List[str]


@dataclass
class APIPipelineResource:
    name:str
    #list of activities object
    activities:List[Any]

@dataclass
class APIPipelineRun:
    pipeline_name:str
    run_id:str
    run_start:datetime
    run_end:datetime
    is_latest:bool
    parameters:Any


@dataclass
class APIActivityRun:
    activity_name:str
    activity_type:str
    input:Any


class ActivityType(Enum):
    Copy = 1
    Procedure = 2
    Execute = 3
    If = 4 
    ForEach = 5
    While = 6
    Unsupported = 7


@dataclass
class Activity:
    activity_name:str
    activity_type:ActivityType
    depends_on:List[str] = field(default_factory=list)
    true_children:List["Activity"] = field(default_factory=list)
    false_children:List["Activity"] = field(default_factory=list)
    # ForEach/while which only have 1 single body
    body_children:List["Activity"] = field(default_factory=list)


@dataclass(frozen=True)
class Resolved:
    value:str

@dataclass(frozen=True)
class Unresolved:
    expression:str
    reason:str

ParameterValue = Resolved | Unresolved

@dataclass
class PipelineRuntimeContext:
    pipeline_name:str
    run_id:str
    run_start:datetime
    run_end:datetime
    pipeline_parameters:Dict[str,str]
    # key : activity_name
    # value : source dict from the activity run input
    activity_source_inputs: Dict[str, Dict[str, Any]]

@dataclass
class GenericActivity:
    name:str
    activity_type:ActivityType
    input_dataset:Optional[Dataset]
    output_dataset:Optional[Dataset]
    input_dataset_parameters:List[str]
    output_dataset_parameters:List[str]
    is_input_supported:bool
    is_output_supported:bool

@dataclass
class StaticPipeline:
    pipeline_name:str
    virtual_graph:List[Edge]
    # key = activity name
    activities:Dict[str,GenericActivity]