from plugin import LineagePlugin,PluginContext,LinkedServiceConnection,PluginLineage,ScriptPluginContext
from typing import Optional

class ScriptPlugin(LineagePlugin):
    
    def init(self)->bool:
        print("ScriptPlugin::init()")

    def is_can_handle(self,\
                   context:PluginContext)->bool:
        """
        check whether the plugin can handle script activity
        """
        return isinstance(context,ScriptPluginContext)

    def execute(self,\
                context:PluginContext,\
                connection:Optional[LinkedServiceConnection])->Optional[PluginLineage]:
        """
        Return the lineage: List of (Set[source],target)
        Return the None if we cannot generate the lineage.
        """

        print(f"Script Activity Name:{context.activity_name}")

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