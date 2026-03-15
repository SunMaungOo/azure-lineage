from plugin import LineagePlugin,PluginContext,LinkedServiceConnection,PluginLineage
from typing import Optional,List,Dict
from logging import Logger
from pathlib import Path
from importlib.util import spec_from_file_location,module_from_spec
from inspect import getmembers,isabstract,isclass
from model import LinkedService,DatabaseLinkedService,BlobLinkedService

class PluginWrapper(LineagePlugin):

    def __init__(self,\
                logger:Logger,\
                plugin:LineagePlugin):
        
        self.__logger = logger
        self.__plugin = plugin
        self.__is_healthy = False
        self.__name = type(plugin).__name__

    def init(self)->bool:
        
        try:

            self.__plugin.init()
            self.__is_healthy = True
            self.__logger.info(f"Plugin {self.__name} : registered successfully")
            return True     
        except ImportError as e:
            self.__logger.error(f"Plugin {self.__name} : missing dependency {e.name}")
            return False
        except Exception as e:
            self.__logger.info(f"Plugin {self.__name} : init failed - {e}")
            return False


    def is_can_handle(self,\
                   context:PluginContext)->bool:
        
        if not self.__is_healthy:
            return False
        
        try:
            return self.__plugin.is_can_handle(context=context)
        except Exception as e:
            self.__logger.error(f"Plugin {self.__name} : is_can_handle failed - {e}")
        
        return False

    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        try:
            return self.__plugin.execute(context=context,\
                                         connection=connection)
        except Exception as e:
            self.__logger.error(f"Plugin {self.__name} : execute failed - {e}")

        return None

def get_database_connections(linked_service:LinkedService)->LinkedServiceConnection:
    
    properties:Dict[str,str] = dict()

    if isinstance(linked_service.info,DatabaseLinkedService):
        properties["host"] = linked_service.info.host
        properties["database"] = linked_service.info.database.value
    
    elif isinstance(linked_service.info,BlobLinkedService):
        properties["url"] = linked_service.info.url

    return LinkedServiceConnection(
        name=linked_service.name,
        type=linked_service.type.name,
        properties=properties
    )

def resolve_plugins(plugins:List[PluginWrapper],\
                    context:PluginContext,\
                    connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
    
    for plugin in plugins:

        # resolve the first plugin which can handle the context

        if plugin.is_can_handle(context=context):
            return plugin.execute(context=context,\
                           connection=connection)
        
    return None


def register_plugins(logger:Logger,\
                     plugins:List[LineagePlugin])->List[PluginWrapper]:
    """
    init the plugins
    """

    wrappers = [PluginWrapper(logger=logger,\
                              plugin=plugin) 
                for plugin in plugins]
    
    return [plugin for plugin in wrappers if plugin.init()]
        
def load_plugins(logger:Logger,\
                 folder_path:str)->List[LineagePlugin]:
    
    plugins:List[LineagePlugin] = list()

    plugin_dir = Path(folder_path)  

    if not plugin_dir.exists():
        logger.warning(f"Plugin folder '{folder_path}' does not exist — skip loading the plugin")
        return list()

    for py_file in plugin_dir.glob("*.py"):
        #skip __init__.py like file

        if py_file.name.startswith("_"):
            continue

        module_name = f"plugins.{py_file.stem}"

        try:

            spec = spec_from_file_location(name=module_name,\
                                               location=py_file)
                
            module = module_from_spec(spec=spec)

            spec.loader.exec_module(module=module)

        except Exception as e:
            logger.error(f"Plugin file '{py_file.name}': failed to import — {e}")
            continue
        
        # get all the class object

        for _,obj in getmembers(module,isclass):
            
             # find all the obj which used the LineagePlugin

            if not(issubclass(obj,LineagePlugin) and\
                obj is not LineagePlugin and\
                not isabstract(obj)):
                
                continue
            
            logger.info(f"Plugin file '{py_file.name}': found '{obj.__name__}'") 

            plugins.append(obj())  
 

    return plugins
