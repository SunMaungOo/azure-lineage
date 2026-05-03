from plugin import (
    LineagePlugin,
    PluginContext,
    LinkedServiceConnection,
    PluginLineage,
    LineageWriterPlugin,
    LineageContext,
    LinkedServiceProperties,
    ScriptPluginContext,
    StoreProcedurePluginContext
)
from typing import (
    Optional,
    List,
    Dict,
    Any,
    Tuple
)
from logging import Logger
from pathlib import Path
from importlib.util import (
    spec_from_file_location,
    module_from_spec
)
from inspect import (
    getmembers,
    isabstract,
    isclass
)
from model import (
    LinkedService,
    DatabaseLinkedService,
    BlobLinkedService,
    PLUGIN_TYPES,
    LinkedServiceType,
    ParameterType,
    PipelineRuntimeContext
)
from abc import ABC
import sys
from types import ModuleType
from core import resolve_parameter
from util import (
    has_field,
    create_parameter
)

class BasePluginWrapper(ABC):

    def __init__(self,\
                logger:Optional[Logger],\
                plugin:PLUGIN_TYPES):
        
        self.logger = logger
        self.plugin = plugin
        self.is_healthy = False
        self.name = type(plugin).__name__

    def init(self)->bool:
        
        try:

            self.plugin.init()
            self.is_healthy = True

            if self.logger is not None:
                self.logger.info(f"Plugin {self.name} : registered successfully")

            return True     
        except ImportError as e:

            if self.logger is not None:
                self.logger.error(f"Plugin {self.name} : missing dependency {e.name}")

            return False
        except Exception as e:

            if self.logger is not None:
                self.logger.info(f"Plugin {self.name} : init failed - {e}")

            return False


class LineagePluginWrapper(BasePluginWrapper):

    def __init__(self,\
                logger:Optional[Logger],\
                plugin:LineagePlugin):
        
        super().__init__(logger=logger,\
                         plugin=plugin)

    def is_can_handle(self,\
                   context:PluginContext)->bool:
        
        if not self.is_healthy:
            return False
        
        try:
            return self.plugin.is_can_handle(context=context)
        except Exception as e:

            if self.logger is not None:
                self.logger.error(f"Plugin {self.name} : is_can_handle failed - {e}")
        
        return False

    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        
        try:
            return self.plugin.execute(context=context,\
                                            connection=connection)
        except Exception as e:

            if self.logger is not None:
                self.logger.error(f"Plugin {self.name} : execute failed - {e}")

        return None
    
class LineageWriterPluginWrapper(BasePluginWrapper):

    def __init__(self,\
                logger:Optional[Logger],\
                plugin:LineageWriterPlugin):
        
        super().__init__(logger=logger,\
                         plugin=plugin)

    def is_can_handle(self,\
                   context:LineageContext)->bool:
        
        if not self.is_healthy:
            return False
        
        try:
            return self.plugin.is_can_handle(context=context)
        except Exception as e:

            if self.logger is not None:
                self.logger.error(f"Plugin {self.name} : is_can_handle failed - {e}")
        
        return False

    def write(self,\
              context:LineageContext)->bool:
        
        try:
            return self.plugin.write(context=context)
        except Exception as e:

            if self.logger is not None:
                self.logger.error(f"Plugin {self.name} : write failed - {e}")

        return False

def get_database_connection(linked_service:LinkedService,\
                             pipeline_parameters:Dict[str,str],\
                             linked_service_parameters:Dict[str,str])->LinkedServiceConnection:
    
    properties:LinkedServiceProperties = {}

    if isinstance(linked_service.info,DatabaseLinkedService):

        if linked_service.info.host is not None:

            host_value = resolve_parameter(parameter=linked_service.info.host,\
                                           dataset_parameters={},
                                           pipeline_parameters=pipeline_parameters,\
                                           linked_service_parameters=linked_service_parameters)
            
            if host_value is not None:
                properties["host"] = linked_service.info.host.value
        
        if linked_service.info.database is not None:

            database_value = resolve_parameter(parameter=linked_service.info.database,\
                                               dataset_parameters={},\
                                               pipeline_parameters=pipeline_parameters,\
                                               linked_service_parameters=linked_service_parameters)
            
            if database_value is not None:                        
                properties["database"] = database_value
    
    elif isinstance(linked_service.info,BlobLinkedService):

        if linked_service.info.url is not None:

            url_value = resolve_parameter(parameter=linked_service.info.url,\
                                         dataset_parameters={},\
                                         pipeline_parameters=pipeline_parameters,\
                                         linked_service_parameters=linked_service_parameters)
            
            if url_value is not None:
                properties["url"] = url_value


    return LinkedServiceConnection(
        name=linked_service.name,
        type=linked_service.type.name,
        properties=properties
    )

def get_sql_pool_database_connection(linked_service_name:str,\
                                     synapse_workspace_name:str)->LinkedServiceConnection:
    
    properties:LinkedServiceProperties = {}

    # deciated synapse sql pool host always have this format

    properties["host"] = f"{synapse_workspace_name}.sql.azuresynapse.net"
    properties["database"] = linked_service_name

    return LinkedServiceConnection(
        name=linked_service_name,
        type=LinkedServiceType.Synapse.name,
        properties=properties
    )




def get_activity_plugins(plugins:List[BasePluginWrapper])->List[LineagePluginWrapper]:
    return [x for x in plugins if isinstance(x,LineagePluginWrapper)]

def get_writer_plugins(plugins:List[BasePluginWrapper])->List[LineageWriterPluginWrapper]:
    return [x for x in plugins if isinstance(x,LineageWriterPluginWrapper)]

def resolve_activity_plugins(plugins:List[LineagePluginWrapper],\
                    context:PluginContext,\
                    connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
    
    for plugin in plugins:

        # resolve the first plugin which can handle the context

        if plugin.is_can_handle(context=context):
            return plugin.execute(context=context,\
                           connection=connection)
        
    return None

def resolve_writer_plugins(plugins:List[LineageWriterPluginWrapper],\
                           context:LineageContext)->bool:

    is_writer_failed = False

    for plugin in plugins:

        if not plugin.is_can_handle(context=context):
            continue

        if not plugin.write(context=context):
            is_writer_failed = True

    return not is_writer_failed


def register_plugins(logger:Logger,\
                     plugins:List[PLUGIN_TYPES])->List[BasePluginWrapper]:
    """
    init the plugins
    """

    activity_plugins = [LineagePluginWrapper(logger=logger,\
                                             plugin=plugin) 
                        for plugin in plugins 
                        if isinstance(plugin,LineagePlugin)]

    writer_plugins = [LineageWriterPluginWrapper(logger=logger,\
                                             plugin=plugin) 
                        for plugin in plugins 
                        if isinstance(plugin,LineageWriterPlugin)]

    wrappers = list()
    wrappers.extend(activity_plugins)
    wrappers.extend(writer_plugins)
    
    return [plugin for plugin in wrappers if plugin.init()]
        
def load_plugins(logger:Logger,\
                 folder_path:str)->List[PLUGIN_TYPES]:
    
    plugins:List[PLUGIN_TYPES] = list()

    plugin_dir = Path(folder_path)  

    if not plugin_dir.exists():
        logger.warning(f"Plugin folder '{folder_path}' does not exist — skip loading the plugin")
        return list()
    
    # module in the plugin folder

    plugin_model_name:List[str] = [
        py_file.stem
        for py_file in plugin_dir.glob("*.py")
        if not py_file.name.startswith("_")\
        and py_file.name!="plugin.py"
    ]

    # backup the module which have the same name as name as plugin module

    backup_conflict_module:Dict[str,ModuleType] = {
        name:sys.modules[name]
        for name in plugin_model_name
        if name in sys.modules
    }


    for py_file in plugin_dir.glob("*.py"):
        #skip __init__.py like file

        if py_file.name.startswith("_"):
            continue

        module_name = f"plugins.{py_file.stem}"

        
        try:

            # delete module which have the same module name as plugin folder
            for name in backup_conflict_module:
                del sys.modules[name]

            sys.path.insert(0,str(plugin_dir))

            spec = spec_from_file_location(name=module_name,\
                                               location=py_file)
                
            module = module_from_spec(spec=spec)

            spec.loader.exec_module(module=module)

        except Exception as e:
            logger.error(f"Plugin file '{py_file.name}': failed to import — {e}")
            continue
        finally:

            # restore the original path import

            if str(plugin_dir) in sys.path:
                sys.path.remove(str(plugin_dir))
            
            # restore the original module

            for name,mod in backup_conflict_module.items():
                sys.modules[name] = mod
        
        # get all the class object

        for _,obj in getmembers(module,isclass):
            
             # find all the plugin
            
            is_lineage_plugin = issubclass(obj,LineagePlugin) and\
                obj is not LineagePlugin and\
                not isabstract(obj)
             
            is_lineage_writer_plugin = issubclass(obj,LineageWriterPlugin) and\
                obj is not LineageWriterPlugin and\
                not isabstract(obj)

            if not(is_lineage_plugin or is_lineage_writer_plugin):
                continue
            
            logger.info(f"Plugin file '{py_file.name}': found '{obj.__name__}'") 

            plugins.append(obj())  
     
    return plugins


def get_script_context(script_activity:Any,\
                        runtime_context:PipelineRuntimeContext,\
                        logger:Optional[Logger])->Optional[ScriptPluginContext]:
    
    try:
        
        linked_service_name = None

        linked_service_parameters:Dict[str,str] = dict()

        linked_service_raw_parameters:Dict[str,Any] = dict()

        script = ""

        if has_field(script_activity,"linked_service_name") and\
            script_activity.linked_service_name is not None:
    
            linked_service_name = script_activity.linked_service_name.reference_name

            if has_field(script_activity.linked_service_name,"parameters"):
                linked_service_raw_parameters = script_activity.linked_service_name.parameters

        if has_field(script_activity,"scripts") and\
            script_activity.scripts is not None:
            
            script_parameter = create_parameter(parameter_value=script_activity.scripts[0].text)

            script = script_parameter.value

            if script_parameter.parameter_type==ParameterType.Expression:
                
                resolved_parameter = resolve_parameter(script_parameter,\
                                dataset_parameters={},\
                                pipeline_parameters=runtime_context.pipeline_parameters,\
                                linked_service_parameters={})
                
                if resolved_parameter is not None:
                    script = resolved_parameter
        
        if linked_service_raw_parameters is not None:
            for parameter_name in linked_service_raw_parameters:

                parameter = create_parameter(parameter_value=linked_service_raw_parameters[parameter_name])

                # only support static parameter type at the moment

                if parameter.parameter_type!=ParameterType.Static:
                    continue

                linked_service_parameters[parameter_name] = parameter.value

        return ScriptPluginContext(
            activity_name=script_activity.name,\
            linked_service_name=linked_service_name,\
            script=script,\
            pipeline_parameters=runtime_context.pipeline_parameters,\
            linked_service_parameters=linked_service_parameters
        )

    except Exception:
        
        if logger is not None:

            logger.warning(f"get script context failed",extra={
                "event":"get_script_context_failed",
                "activity": script_activity.name
            })

    return None

def get_procedure_context(procedure_activity:Any,\
                          runtime_context:PipelineRuntimeContext,\
                          logger:Optional[Logger])->Optional[Tuple[StoreProcedurePluginContext,bool]]:
    
    is_sql_pool = False
    
    try:

        procedure_name = create_parameter(parameter_value=procedure_activity.stored_procedure_name).value

        procedure_parameters:Dict[str,str] = dict()

        linked_service_parameters:Dict[str,str] = dict()

        linked_service_raw_parameters:Dict[str,Any] = dict()

        if has_field(procedure_activity,"stored_procedure_parameters"):
            raw_parameters:Optional[Dict[str,Any]] = procedure_activity.stored_procedure_parameters

            if raw_parameters is not None:

                procedure_parameters = {
                    key:str(value if has_field(value,"value") else value)
                    for key,value in raw_parameters
                }

        linked_service_name = None

        # for Azure Store Procedure

        if has_field(procedure_activity,"linked_service_name") and\
        procedure_activity.linked_service_name is not None:    
            
            linked_service_name = procedure_activity.linked_service_name.reference_name

            if has_field(procedure_activity.linked_service_name,"parameters"):
                linked_service_raw_parameters = procedure_activity.linked_service_name.parameters
        
        # for sql pool

        elif has_field(procedure_activity,"sql_pool") and\
            procedure_activity.sql_pool is not None:
            linked_service_name = procedure_activity.sql_pool.reference_name

            is_sql_pool = True

        if linked_service_raw_parameters is not None:
            for parameter_name in linked_service_raw_parameters:

                parameter = create_parameter(parameter_value=linked_service_raw_parameters[parameter_name])

                # only support static parameter type at the moment

                if parameter.parameter_type!=ParameterType.Static:
                    continue

                linked_service_parameters[parameter_name] = parameter.value
        
        

        return StoreProcedurePluginContext(
            activity_name=procedure_activity.name,\
            linked_service_name=linked_service_name,\
            procedure_name=procedure_name,\
            procedure_parameters=procedure_parameters,\
            pipeline_parameters=runtime_context.pipeline_parameters,\
            linked_service_parameters=linked_service_parameters
        ),is_sql_pool
        
    except Exception:

        if logger is not None:

            logger.warning(f"get store procedure context failed",extra={
                "event":"get_store_procedure_context_failed",
                "activity": procedure_activity.name
            })

    return None,False