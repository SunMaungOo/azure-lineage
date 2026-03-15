from dataclasses import dataclass
from typing import Dict,Optional,List,Tuple,Set
from abc import ABC,abstractmethod

@dataclass
class LinkedServiceConnection:
    name:str
    type:str
    properties:Dict[str, str] 

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
    sql_script:str
    pipeline_parameters: Dict[str, str]

PluginContext = StoreProcedurePluginContext | ScriptPluginContext

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
