from typing import List,Optional
from model import Dataset,LinkedService,APIPipelineRun

def find_dataset(datasets:List[Dataset],search_dataset_name:str)->Optional[Dataset]:

    search_value = [x for x in datasets if x.name==search_dataset_name]

    if len(search_value)>0:
        return search_value[0]
    
    return None

def find_linked_service(linked_services:List[LinkedService],search_linked_service_name:str)->Optional[LinkedService]:

    search_value = [x for x in linked_services if x.name==search_linked_service_name]

    if len(search_value)>0:
        return search_value[0]
    
    return None

def find_latest_pipeline_info(pipeline_runs:List[APIPipelineRun])->Optional[APIPipelineRun]:
    """
    Get only the latest pipeline run to get the last run of the pipeline
    """
    pipeline_run =  [x for x in pipeline_runs if x.is_latest]

    if len(pipeline_run)>0:
        return max(pipeline_run,key=lambda x:x.run_start)
        
    return None