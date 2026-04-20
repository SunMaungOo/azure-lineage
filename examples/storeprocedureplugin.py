from plugin import LineagePlugin,PluginContext,LinkedServiceConnection,PluginLineage,StoreProcedurePluginContext
from typing import Optional

class StoreProcedurePlugin(LineagePlugin):
    
    def init(self)->bool:
        """
        init the plugin
        """
        print("StoreProcedurePlugin::init()")

    def is_can_handle(self,\
                   context:PluginContext)->bool:
        """
        check whether the plugin can handle store procedure
        """
        return isinstance(context,StoreProcedurePluginContext)

    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        """
        Return the lineage: List of (Set[source],target)
        Return the None if we cannot generate the lineage.
        """

        print(f"Store Procedure Activity Name:{context.activity_name}")

        if connection is not None:
            print("fields are:")
            print(f"name:{connection.name}")
            print(f"type:{connection.type}")
            print("properties are:")
            print(connection.properties)

        sources = set()
        sources.add("Example Source 1")
        sources.add("Example Source 2")

        target = "Example Target"

        return [(sources,target)]