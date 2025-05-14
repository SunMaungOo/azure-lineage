from dataclasses import dataclass
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
class CopyActivity:
    #activity name
    name:str
    input:Optional[ProcessDataset]
    output:Optional[ProcessDataset]
    input_parameter_names:List[str]
    output_parameter_names:List[str]
    is_input_supported:bool
    is_output_supported:bool

@dataclass
class Pipeline:
    pipeline_name:str
    copy_activities:List[CopyActivity]

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


