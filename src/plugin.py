from dataclasses import dataclass
from typing import Dict,Optional,List,Tuple,Set,TypedDict
from abc import ABC,abstractmethod
from datetime import datetime

class LinkedServiceProperties(TypedDict,total=False):
    host:str
    database:str
    url:str

@dataclass
class LinkedServiceConnection:
    name:str
    type:str
    properties:LinkedServiceProperties 

@dataclass
class StoreProcedurePluginContext:
    activity_name:str
    linked_service_name:str
    procedure_name:str
    procedure_parameters:Dict[str, str]
    pipeline_parameters:Dict[str, str]

@dataclass
class ScriptPluginContext:
    activity_name:str
    linked_service_name:str
    script:str
    pipeline_parameters: Dict[str, str]

@dataclass
class LineageEdge:
    node_name:str
    parent_nodes:List[str]

@dataclass
class PipelineLineageContext:
    """
    Lineage for pipeline
    """
    pipeline_name:str
    pipeline_run_id:str
    pipeline_run_status:str
    pipeline_run_start:datetime
    pipeline_run_end:datetime
    lineage:List[LineageEdge]

@dataclass
class ActivityLineageContext:
    """
    Lineage for individual activity of the pipeline
    """
    pipeline_name:str
    pipeline_run_id:str
    pipeline_run_status:str
    pipeline_run_start:datetime
    pipeline_run_end:datetime
    activity_name:str
    activity_type:str
    lineage:List[LineageEdge]

@dataclass
class ActivityLineageInfo:
    """
    Activity which have lineage is extracted
    """
    pipeline_name:str
    # whether the pipeline contain the activity that lineage can be extracted from
    is_pipeline_supported:bool
    activity_name:Optional[str]
    activity_type:Optional[str]
    # whether the lineage is extract from activity
    is_lineage_extracted:bool


PluginContext = StoreProcedurePluginContext | ScriptPluginContext

LineageContext = List[PipelineLineageContext] | List[ActivityLineageContext] | List[ActivityLineageInfo]

PluginLineage = List[Tuple[Set[str],str]]

class LineagePlugin(ABC):

    @abstractmethod
    def init(self)->bool:
        """
        init the plugion
        """
        pass

    @abstractmethod
    def is_can_handle(self,\
                   context:PluginContext)->bool:
        """
        check whether the plugin can handle this kind of context
        """
        pass

    @abstractmethod
    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        """
        Return the lineage: List of (Set[source],target)
        Return the None if we cannot generate the lineage.
        """
        pass

class LineageWriterPlugin(ABC):
    @abstractmethod
    def init(self)->bool:
        """
        init the plugion
        """
        pass

    @abstractmethod
    def is_can_handle(self,\
                      context:LineageContext)->bool:
        """
        check whether the plugin can handle this kind of context
        """
        pass

    @abstractmethod
    def write(self,\
                context:LineageContext)->bool:
        """
        Write the lineage.
        """
        pass

