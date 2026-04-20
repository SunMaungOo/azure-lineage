from plugin import LineageWriterPlugin,LineageContext,PipelineLineageContext

class PipelineLineagePlugin(LineageWriterPlugin):

    def init(self)->bool:
        print("PipelineLineagePlugin::init()")

    def is_can_handle(self,\
                      context:LineageContext)->bool:
        """
        check whether the plugin can handle this kind of context
        """
        if not isinstance(context,list):
            return False
        
        if len(context)==0:
            return False
        
        return isinstance(context[0],PipelineLineageContext)

    def write(self,\
                context:LineageContext)->bool:
        """
        Write the lineage
        """
        for plugin_lineage in context:
            print(plugin_lineage)