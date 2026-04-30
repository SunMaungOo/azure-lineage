from plugin import LineagePlugin,PluginContext,LinkedServiceConnection,PluginLineage,LineageWriterPlugin,LineageContext,LinkedServiceProperties
from typing import Optional,List,Dict
from logging import Logger
from pathlib import Path
from importlib.util import spec_from_file_location,module_from_spec
from inspect import getmembers,isabstract,isclass
from model import LinkedService,DatabaseLinkedService,BlobLinkedService,PLUGIN_TYPES,LinkedServiceType
from abc import ABC
import sys
from types import ModuleType

class BasePluginWrapper(ABC):

    def __init__(self,\
                logger:Logger,\
                plugin:PLUGIN_TYPES):
        
        self.logger = logger
        self.plugin = plugin
        self.is_healthy = False
        self.name = type(plugin).__name__

    def init(self)->bool:
        
        try:

            self.plugin.init()
            self.is_healthy = True
            self.logger.info(f"Plugin {self.name} : registered successfully")
            return True     
        except ImportError as e:
            self.logger.error(f"Plugin {self.name} : missing dependency {e.name}")
            return False
        except Exception as e:
            self.logger.info(f"Plugin {self.name} : init failed - {e}")
            return False


class LineagePluginWrapper(BasePluginWrapper):

    def __init__(self,\
                logger:Logger,\
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
            self.logger.error(f"Plugin {self.name} : is_can_handle failed - {e}")
        
        return False

    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        
        try:
            return self.plugin.execute(context=context,\
                                            connection=connection)
        except Exception as e:
            self.logger.error(f"Plugin {self.name} : execute failed - {e}")

        return None
    
class LineageWriterPluginWrapper(BasePluginWrapper):

    def __init__(self,\
                logger:Logger,\
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
            self.logger.error(f"Plugin {self.name} : is_can_handle failed - {e}")
        
        return False

    def write(self,\
              context:LineageContext)->bool:
        
        try:
            return self.plugin.write(context=context)
        except Exception as e:
            self.logger.error(f"Plugin {self.name} : write failed - {e}")

def get_database_connections(linked_service:LinkedService)->LinkedServiceConnection:
    
    properties:LinkedServiceProperties = {}

    if isinstance(linked_service.info,DatabaseLinkedService):

        if linked_service.info.host is not None:
            properties["host"] = linked_service.info.host.value
        
        if linked_service.info.database is not None:
            properties["database"] = linked_service.info.database.value
    
    elif isinstance(linked_service.info,BlobLinkedService):

        if linked_service.info.url is not None:
            properties["url"] = linked_service.info.url.value

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
