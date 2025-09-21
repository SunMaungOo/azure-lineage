from typing import Dict,List,Set,Optional,Any,Tuple
from copy import deepcopy
from lineage import get_sql_lineage
from graph import Edge,merge_edge
from model import (
    DatasetType,
    Dataset,
    Parameter,
    QueryDataset,
    SingleTableDataset,
    LocationDataset,
    ParameterType,
    Parameter,
    CopyActivity,
    Pipeline,
    ProcessDataset,
    PipelineLineage,
    APIPipelineResource,
    APIPipelineRun,
    APIActivityRun,
    LinkedService,
    LinkedServiceType,
    DatabaseLinkedService,
    BlobLinkedService
)
from connector import get_sql_script,get_dataset_type,get_dataset_info,get_linked_service_info,get_linked_service_type
from client import AzureClient
from config import get_api_client,DAYS_SEARCH,OPENLINEAGE_NAMESPACE
from config import OPENLINEAGE_OUTPUT_FILE_PATH,OPENLINEAGE_PRODUCER,IS_USE_FQN,LINEAGE_OUTPUT_FILE_PATH
import uuid
from datetime import datetime
import json
from pathlib import Path
import logging
import sys
from dataclasses import asdict

logger = logging.getLogger("azure-lineage")

logger.setLevel(logging.DEBUG)

logger.propagate = False

formatter = logging.Formatter(
    fmt='%(asctime)s | %(levelname)-8s | %(name)-15s | %(lineno)-3d | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%fZ'  # ISO 8601 with microseconds → Z for UTC
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)


def get_datasets(client:AzureClient)->Optional[List[Dataset]]:

    datasets:List[Dataset] = list()

    try:

        #DatasetResource 

        for dataset_resource in client.get_datasets():
            
            dataset_name = dataset_resource.dataset_name

            dataset_type = get_dataset_type(azure_dataset_type=dataset_resource.azure_data_type)

            linked_service_name = dataset_resource.linked_service_name

            info = get_dataset_info(dataset_resource=dataset_resource,\
                                    dataset_name=dataset_name,\
                                    dataset_type=dataset_type)
            
            datasets.append(Dataset(name=dataset_name,\
                                    type=dataset_type,\
                                    linked_service_name=linked_service_name,\
                                    info = info))

        return datasets
    
    except Exception:
        return None

def get_linked_service(client:AzureClient)->Optional[List[LinkedService]]:

    linked_service:List[LinkedService] = list()

    try:
        
        #LinkedServiceResource

        for linked_service_resource in client.get_linked_service():
            info = get_linked_service_info(linked_service_resource=linked_service_resource)

            linked_service.append(
                LinkedService(
                    name=linked_service_resource.linked_service_name,\
                    type= get_linked_service_type(azure_linked_service_type=linked_service_resource.azure_data_type),
                    info=info
                )
            )
        
        return linked_service
        
    except Exception:
        return None


def get_started_pipeline_name(client:AzureClient)->Optional[Set[str]]:

    unique_started_pipeline_name:Set[str] = set()

    try:

        #TriggerResource

        for trigger in client.get_triggers():
            
            if trigger.runtime_state=="Started":
                for pipeline_name in trigger.pipeline_names:
                    unique_started_pipeline_name.add(pipeline_name)
                    
        return unique_started_pipeline_name
    
    except Exception:
        return None


def get_latest_pipeline_info(pipeline_runs:List[APIPipelineRun])->List[APIPipelineRun]:
    """
    Get only the latest pipeline run to get the last run of the pipeline
    """
    return [x for x in pipeline_runs if x.is_latest]

    
def get_copy_activities_run(activities:List[APIActivityRun])->List[APIActivityRun]:
    """
    Get copy activity 
    """
    return [actv for actv in activities if actv.activity_type=="Copy"]


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


def get_copy_activity(datasets:List[Dataset],\
                      input_dataset_name:str,\
                      output_dataset_name:str,\
                      activity_name:str,\
                      input_parameters:List[Parameter],
                      output_parameters:List[Parameter]):
    
    input = find_dataset(datasets=datasets,\
                         search_dataset_name=input_dataset_name)

    output = find_dataset(datasets=datasets,\
                          search_dataset_name=output_dataset_name)
    
    is_input_supported = not(input is None or input.type==DatasetType.Unsupported)

    is_output_supported = not(output is None or output.type==DatasetType.Unsupported)

    if not is_input_supported:
        input = None

    if not is_output_supported:
        output = None

    
    return CopyActivity(name=activity_name,\
                        input=input,\
                        output=output,\
                        input_parameter_names=input_parameters,\
                        output_parameter_names=output_parameters,\
                        is_input_supported=is_input_supported,\
                        is_output_supported=is_output_supported)

def get_copy_pipelines(pipeline_resources:List[APIPipelineResource],\
                  datasets:List[Dataset])->List[Pipeline]:
    """
    Get pipeline with all the copy activities of it
    """
    
    pipelines:List[Pipeline] = list()

    for pipeline in pipeline_resources:

        pipeline_name = pipeline.name

        copy_activities:List[CopyActivity] = list()

        for activity in pipeline.activities:

            copy_activities.extend(from_activity_get_copy_activity(activity=activity,\
                                                                   datasets=datasets))
        
        pipelines.append(Pipeline(pipeline_name=pipeline_name,\
                                 copy_activities=copy_activities))
    
    return pipelines


def from_activity_get_copy_activity(activity:Dict[str,Any],\
                                    datasets:List[Dataset])->List[CopyActivity]:
    """
    Recursively get the copy activity
    """
    
    copy_activities:List[CopyActivity] = list()

    if activity.type=="Copy":
        
        input_parameters = [x for x in activity.inputs[0].parameters]

        output_parameters = [x for x in activity.outputs[0].parameters]

        copy_activities.append(get_copy_activity(datasets=datasets,\
                                    input_dataset_name=activity.inputs[0].reference_name,\
                                    output_dataset_name=activity.outputs[0].reference_name,\
                                    activity_name=activity.name,
                                    input_parameters=input_parameters,\
                                    output_parameters=output_parameters))
        
    elif activity.type=="IfCondition":
        if activity.if_true_activities is not None:
            for inner_activity in activity.if_true_activities:
                copy_activities.extend(from_activity_get_copy_activity(activity=inner_activity,\
                                                                       datasets=datasets))
                
        if activity.if_false_activities is not None:
            for inner_activity in activity.if_false_activities:
                copy_activities.extend(from_activity_get_copy_activity(activity=inner_activity,\
                                                                       datasets=datasets))

    elif activity.type=="ForEach":
        for inner_activity in activity.activities:
            copy_activities.extend(from_activity_get_copy_activity(activity=inner_activity,\
                                                                       datasets=datasets))




    return copy_activities


def pass_parameters_single_table_dataset(dataset:SingleTableDataset,\
                                         parameter_names:List[str],\
                                         pipeline_parameter:Dict[str,str])->SingleTableDataset:
    
    process_dataset = deepcopy(dataset)
    
    pass_parameter_value:Dict[str,str] = dict()

    for parameter in pipeline_parameter:
        if parameter in parameter_names:
            pass_parameter_value["@dataset()."+parameter] = pipeline_parameter[parameter]

    if process_dataset.schema.parameter_type==ParameterType.Expression:
        for x in pass_parameter_value:
            process_dataset.schema.parameter_type = ParameterType.Static
            process_dataset.schema.value =  process_dataset.schema.value.replace(x,pass_parameter_value[x])

    if process_dataset.table.parameter_type==ParameterType.Expression:
        for x in pass_parameter_value:
            process_dataset.table.parameter_type = ParameterType.Static
            process_dataset.table.value = process_dataset.table.value.replace(x,pass_parameter_value[x])

    return process_dataset


def get_source_tables(input_dataset:ProcessDataset,\
                      input_parameter_names:List[str],\
                      pipeline_parameters:Dict[str,str],\
                      input_source_obj:Dict[str,Any])->Set[str]:
    
    source_tables:Set[str] = set()

    if isinstance(input_dataset,SingleTableDataset):
        
        process_input_dataset = pass_parameters_single_table_dataset(dataset=input_dataset,\
                                                             parameter_names=input_parameter_names,\
                                                            pipeline_parameter=pipeline_parameters)
                
        source_tables.add(f"{process_input_dataset.schema.value}.{process_input_dataset.table.value}")

    elif isinstance(input_dataset,QueryDataset):

        sql = get_sql_script(input_source_obj=input_source_obj,\
                                     dataset_type=input_dataset.type)
         
        #ignore when we cannot parse the sql 
        try:
            source_tables = get_sql_lineage(sql=sql)
        except Exception:
            return set()

    elif isinstance(input_dataset,LocationDataset):
        source_tables.add(input_dataset.location)

    return source_tables

def get_target_table(output_dataset:ProcessDataset,\
                     output_parameter_names:List[str],\
                     pipeline_parameters:Dict[str,str])->Optional[str]:
    
    target_table = None
    
    if isinstance(output_dataset,SingleTableDataset):
        process_output_dataset = pass_parameters_single_table_dataset(dataset=output_dataset,\
                                                             parameter_names=output_parameter_names,\
                                                            pipeline_parameter=pipeline_parameters)
                
        target_table = f"{process_output_dataset.schema.value}.{process_output_dataset.table.value}"

    elif isinstance(output_dataset,LocationDataset):
        target_table = output_dataset.location

    return target_table

def is_valid_lineage_linked_service(linked_service:LinkedService)->bool:
    
    if linked_service.type==LinkedServiceType.Unsupported:
        return False
    
    elif isinstance(linked_service.info,DatabaseLinkedService):
        return linked_service.info.host.parameter_type!=ParameterType.Expression and\
        linked_service.info.database.parameter_type!=ParameterType.Expression
    
    elif isinstance(linked_service.info,BlobLinkedService):
        return linked_service.info.url.parameter_type!=ParameterType.Expression

    return False

def get_linked_service_host_prefix(linked_service:LinkedService)->Optional[str]:

    if not is_valid_lineage_linked_service(linked_service=linked_service):
        return None
    
    if isinstance(linked_service.info,DatabaseLinkedService):
        return f"{linked_service.info.host.value}.{linked_service.info.database.value}"
    
    elif isinstance(linked_service.info,BlobLinkedService):
        return linked_service.info.url.value
    
    return None

def add_source_host_prefix(table_value:str,host_prefix:str)->str:
    """
    Append the host information to the schema name and table name
    table_value: string in schema.table format
    """

    transform_table_value = table_value

    blocks = table_value.split(".")

    block_lengths = len(blocks)

    # get only the schema name and table name

    if block_lengths>2:
        transform_table_value = blocks[block_lengths-2]+"."+blocks[block_lengths-1] 

    transform_table_value = f"{host_prefix}.{transform_table_value}"

    return transform_table_value





def get_pipeline_table_lineage(client:AzureClient,
                               pipeline_run:APIPipelineRun,\
                               pipeline_info:Pipeline,
                               linked_services:List[LinkedService],\
                               is_use_fqn:bool)->List[Edge]:
    """
    pipeline_run : pipeline run activity
    pipeline_info : pipeline name which have the same as pipeline info
    """

    lineage:List[Edge] = list()
    
    # the pipeline parameter which is actually passed

    pipeline_parameters = pipeline_run.parameters

    activities = client.get_activities_run(pipeline_run=pipeline_run)

    # get the copy activity from activity run log

    for activity_run in get_copy_activities_run(activities=activities):
                
        activity_name = activity_run.activity_name

        # get the same copy activity information that we got from static analysis
    
        static_copy_activity = [x for x in pipeline_info.copy_activities if x.name==activity_name][0]

        # will only support lineage if we support both input and output

        if not (static_copy_activity.is_input_supported and static_copy_activity.is_output_supported):
            continue

         # by common sense , we should only support single table dataset and location dataset as sink

        if not(isinstance(static_copy_activity.output.info,SingleTableDataset) or\
            isinstance(static_copy_activity.output.info,LocationDataset)):
            continue

        source_table = get_source_tables(input_dataset=static_copy_activity.input.info,\
                                         input_parameter_names=static_copy_activity.input_parameter_names,\
                                        pipeline_parameters=pipeline_parameters,\
                                        input_source_obj=activity_run.input["source"])
        

        if is_use_fqn:

            source_linked_service = find_linked_service(linked_services=linked_services,\
                                search_linked_service_name=static_copy_activity.input.linked_service_name)
                        
            source_host_prefix = None
            
            if source_linked_service is not None:
                source_host_prefix = get_linked_service_host_prefix(linked_service=source_linked_service)

                if source_host_prefix is not None:
                    source_table = {add_source_host_prefix(table_value=table_value,\
                                                        host_prefix=source_host_prefix) for table_value in source_table}
                    
        target_table = get_target_table(output_dataset=static_copy_activity.output.info,\
                                        output_parameter_names=static_copy_activity.output_parameter_names,\
                                        pipeline_parameters=pipeline_parameters)
        
        if is_use_fqn:

            target_linked_service = find_linked_service(linked_services=linked_services,\
                                search_linked_service_name=static_copy_activity.output.linked_service_name)
            
            target_host_prefix = None
            
            if target_linked_service is not None:
                target_host_prefix = get_linked_service_host_prefix(linked_service=target_linked_service)

                if target_host_prefix is not None:
                    target_table = add_source_host_prefix(table_value=target_table,\
                                                        host_prefix=target_host_prefix)
            

        # we cannot add lineage when we cannot parse the sink 

        if target_table is None:
            continue

        edge = Edge(
            node_name=target_table,
            parent_nodes=list(source_table)
        )

        if len(lineage)==0:
            lineage.append(edge)

        else:
            #combine lineage based on multiple copy activity

            lineage = merge_edge(left_edges=lineage,right_edges=[edge])
        
    
    return lineage

def to_open_lineage(namespace:str,producer:str,pipeline_lineage:PipelineLineage)->Tuple[Dict[str,Any],Dict[str,Any]]:
    
    start_time = datetime.now().isoformat()

    end_time = datetime.now().isoformat()

    run_id = str(uuid.uuid4())

    start_job_event = {
        "eventType":"START",
        "eventTime":start_time,
        "producer":producer,
        "run": {
            "runId": run_id
        },
        "job":{
            "namespace": namespace,
            "name": pipeline_lineage.pipeline_name,
        },
        "inputs": [],
        "outputs": []
    }

    complete_job_event = {
        "eventType":"COMPLETE",
        "eventTime":end_time,
        "producer":producer,
        "run": {
            "runId": run_id
        },
        "job":{
            "namespace": namespace,
            "name": pipeline_lineage.pipeline_name,
        },
        "inputs": [],
        "outputs": []
 
    }

    for edge in pipeline_lineage.lineage:
        # when there is no node
        if len(edge.parent_nodes)==0:

            output_dataset = {
                "namespace": namespace,
                "name": edge.node_name,
                "facets": {
                    "dataSource": {
                         "_producer": producer,
                         "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json",
                         "name":pipeline_lineage.pipeline_name,
                         "uri": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"
                    },
                    "schema": {
                         "_producer": producer,
                         "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"
                    },
                    "storageDatasetFacet": {
                        "_producer": producer,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json",
                        "isSource": True  # Mark as source dataset
                    }
                }
            }  

            start_job_event["outputs"].append(output_dataset)
            complete_job_event["outputs"].append(output_dataset)


        else:

            for x in edge.parent_nodes:
                input_dataset = {
                    "namespace": namespace,
                    "name": x
                }

                start_job_event["inputs"].append(input_dataset)
                complete_job_event["inputs"].append(input_dataset)


            output_dataset = {
                "namespace": namespace,
                "name": edge.node_name
            }

            start_job_event["outputs"].append(output_dataset)
            complete_job_event["outputs"].append(output_dataset)

    return (start_job_event,complete_job_event)

def main()->int:

    client = get_api_client()

    logger.info("Extracting datasets:")

    datasets = get_datasets(client=client)

    if datasets is None:
        logger.info("Extracting datasets:fail")
        return 1 
    else:
        logger.info("Extracting datasets:success")

    logger.info("Extracting linked service:")

    linked_services = get_linked_service(client=client)

    if linked_services is None:
        logger.info("Extracting linked service:fail")
        return 1
    else:
        logger.info("Extracting linked service:success")

    logger.info("Extracting pipeline:")

    raw_pipeline = client.get_pipelines()

    if raw_pipeline is None:
        logger.info("Extracting pipeline:fail")
        return 1
    else:
        logger.info("Extracting pipeline:success")



    raw_pipeline_names = [x.name for x in raw_pipeline]
        
    pipelines = get_copy_pipelines(pipeline_resources=raw_pipeline,\
        datasets=datasets)
    
    processed_pipeline:Set[str] = set()

    pipeline_lineage:List[PipelineLineage] = list()

    logger.info("Extracting lineage:")

    for x in raw_pipeline_names:

        for pipeline_run in get_latest_pipeline_info(pipeline_runs= client.get_pipeline_runs(pipeline_name=x,\
                                                                                    days=DAYS_SEARCH)):
            
            # this is hacked. There should be only 1 single pipeline run but where it have multiple pipeline for the same pipeline name
            if pipeline_run.pipeline_name in processed_pipeline:
                continue
            
            pipeline_info = [x for x in pipelines if x.pipeline_name==pipeline_run.pipeline_name][0]

            lineage = get_pipeline_table_lineage(client=client,\
                                         pipeline_run=pipeline_run,\
                                        pipeline_info=pipeline_info,\
                                        linked_services=linked_services,\
                                        is_use_fqn=IS_USE_FQN)
        
            pipeline_lineage.append(
                PipelineLineage(
                    pipeline_name=pipeline_info.pipeline_name,\
                    lineage=lineage
                )
            )

            processed_pipeline.add(pipeline_run.pipeline_name)
    
    logger.info("Extracting lineage:success")

    logger.info(f"Lineage found:{len(pipeline_lineage)}")

    openlineage:List[Dict[str,Any]] = list()

    try:
        for x in pipeline_lineage:

            (start_event,complete_event) = to_open_lineage(namespace=OPENLINEAGE_NAMESPACE,producer=OPENLINEAGE_PRODUCER,pipeline_lineage=x)

            openlineage.append(start_event)
            openlineage.append(complete_event)

        output_file_path = Path(OPENLINEAGE_OUTPUT_FILE_PATH)
        output_file_path.parent.mkdir(parents=True,exist_ok=True)


        with output_file_path.open("w") as file:
            json.dump(openlineage,file,indent=4)

        logger.info(f"Saving lineage (openlineage) to {OPENLINEAGE_OUTPUT_FILE_PATH}:success")

    except:
        logger.info(f"Saving lineage (openlineage) to {OPENLINEAGE_OUTPUT_FILE_PATH}:fail")
        return 1
    
    try:

        Path(LINEAGE_OUTPUT_FILE_PATH).parent.mkdir(parents=True,exist_ok=True)

        with open(LINEAGE_OUTPUT_FILE_PATH,"w") as file:
            json.dump([asdict(edge) for edge in lineage],file,indent=4)

        logger.info(f"Saving lineage to {LINEAGE_OUTPUT_FILE_PATH}:success")
    
    except:
        logger.info(f"Saving lineage to {LINEAGE_OUTPUT_FILE_PATH}:fail")

        return 1

    return 0

if __name__=="__main__":
    sys.exit(main())