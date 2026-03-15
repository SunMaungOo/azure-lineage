from typing import Dict,List,Set,Optional,Any,Tuple
from lineage import get_sql_lineage,clean_sql
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
    ProcessDataset,
    PipelineLineage,
    APIPipelineResource,
    APIPipelineRun,
    LinkedService,
    LinkedServiceType,
    DatabaseLinkedService,
    BlobLinkedService,
    PipelineRuntimeContext,
    GenericActivity,
    StaticPipeline,
    ActivityType
)
from connector import get_sql_script,get_dataset_type,get_dataset_info,get_linked_service_info,get_linked_service_type
from client import AzureClient
from config import get_api_client,DAYS_SEARCH,OPENLINEAGE_NAMESPACE,PLUGIN_FOLDER_PATH
from config import OPENLINEAGE_OUTPUT_FILE_PATH,OPENLINEAGE_PRODUCER,IS_USE_FQN,LINEAGE_OUTPUT_FILE_PATH
import uuid
from datetime import datetime,timezone
import json
from pathlib import Path
import logging
import sys
from dataclasses import asdict
from core import (
    to_activities,
    get_virtual_graph,
    get_activity_type,
    resolve_table_expression,
    expand_activities,
    resolve_blob_expression,
    normalize_blob_path,
    resolve_dataset_parameter
)
from util import has_field,create_parameter
from formatter import LogFormatter
from plugin import StoreProcedurePluginContext
from pluginhelper import PluginWrapper,resolve_plugins,get_database_connections,load_plugins,register_plugins

logger = logging.getLogger("azure-lineage")

logger.setLevel(logging.DEBUG)
logger.propagate = False

formatter = LogFormatter(
    fmt='%(asctime)s | %(levelname)-8s | %(name)-15s | %(lineno)-3d | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'  # ISO 8601 with microseconds → Z for UTC
)


console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

logging.getLogger("azure.mgmt.datafactory").setLevel(logging.ERROR)
logging.getLogger("azure.synapse.artifacts").setLevel(logging.ERROR)

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

def get_latest_pipeline_info(pipeline_runs:List[APIPipelineRun])->Optional[APIPipelineRun]:
    """
    Get only the latest pipeline run to get the last run of the pipeline
    """
    pipeline_run =  [x for x in pipeline_runs if x.is_latest]

    if len(pipeline_run)>0:
        return max(pipeline_run,key=lambda x:x.run_start)
        
    return None

    
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


def get_placeholder_activity(activity_name:str)->GenericActivity:
    """
    Return generic activity for activity we will wanted to support in the future
    """
    return GenericActivity(
        name=activity_name,
        activity_type=ActivityType.Unsupported,\
        input_dataset=None,
        output_dataset=None,\
        input_dataset_parameters=list(),\
        output_dataset_parameters=list(),\
        is_input_supported=False,\
        is_output_supported=False,\
        raw_activity=None
    )

def get_generic_activity(raw_activity:Any,\
                         datasets:List[Dataset])->GenericActivity:

    activity_type = get_activity_type(raw_activity.type)

    if activity_type==ActivityType.Procedure:

        return GenericActivity(
            name=raw_activity.name,
            activity_type=ActivityType.Procedure,
            input_dataset=None,
            output_dataset=None,
            input_dataset_parameters=list(),
            output_dataset_parameters=list(),
            is_input_supported=True,\
            is_output_supported=False,\
            raw_activity=raw_activity
        )


    # something went wrong when there is no inputs and output

    if not has_field(raw_activity,"inputs") and not has_field(raw_activity,"outputs"):
        return get_placeholder_activity(activity_name=raw_activity.name)
    
    # for more activity support , we need to modify this function

    input_dataset_name = raw_activity.inputs[0].reference_name

    output_dataset_name = raw_activity.outputs[0].reference_name

    input_dataset = find_dataset(datasets=datasets,\
                                 search_dataset_name=input_dataset_name)
    
    output_dataset = find_dataset(datasets=datasets,\
                                 search_dataset_name=output_dataset_name)
    
    is_input_supported = not(input_dataset is None or input_dataset.type==DatasetType.Unsupported)

    is_output_supported = not(output_dataset is None or output_dataset.type==DatasetType.Unsupported)

    input_dataset_parameters:Dict[str,Parameter] = dict()

    if raw_activity.inputs[0].parameters is not None:

        for parameter_name in raw_activity.inputs[0].parameters:
            input_dataset_parameters[parameter_name] = create_parameter(raw_activity.inputs[0].parameters[parameter_name])

    output_dataset_parameters:Dict[str,Parameter] = dict()

    if raw_activity.outputs[0].parameters is not None:

        for parameter_name in raw_activity.outputs[0].parameters:
            output_dataset_parameters[parameter_name] = create_parameter(raw_activity.outputs[0].parameters[parameter_name])


    if not is_input_supported:
        input_dataset = None

    if not is_output_supported:
        output_dataset = None

    return GenericActivity(
        name=raw_activity.name,\
        activity_type=activity_type,\
        input_dataset=input_dataset,\
        output_dataset=output_dataset,\
        input_dataset_parameters=input_dataset_parameters,\
        output_dataset_parameters=output_dataset_parameters,\
        is_input_supported=is_input_supported,\
        is_output_supported=is_output_supported,\
        raw_activity=raw_activity
    )


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

def resolve_source_table(activity:GenericActivity,\
                         runtime:PipelineRuntimeContext,\
                         linked_services:List[LinkedService],\
                         is_use_fqn:bool)->Set[str]:
    
    input_dataset = activity.input_dataset

    input_dataset_info = input_dataset.info
            
    source_tables:Set[str] = set()

    log_context = {
        "pipeline": runtime.pipeline_name,
        "activity": activity.name,
        "dataset": input_dataset.name,
    }

    dataset_parameters = resolve_dataset_parameter(dataset_parameters=activity.input_dataset_parameters,\
                            pipeline_parameter=runtime.pipeline_parameters)

    if isinstance(input_dataset_info,SingleTableDataset):

        table_reference = resolve_table_expression(schema_expression=input_dataset_info.schema.value,\
                                                   table_expression=input_dataset_info.table.value,\
                                                   dataset_parameters=dataset_parameters,
                                                   pipeline_parameters=runtime.pipeline_parameters)

        if table_reference is None:

            logger.warning(
                "source table resolution failed",
                extra={
                    "event": "source_table_resolution_failed",
                    **log_context
                }
            )
            

        else:
            source_tables.add(table_reference)

    elif isinstance(input_dataset_info,QueryDataset):

        input_source_obj = runtime.activity_source_inputs.get(activity.name)

        # if it is null , it mean the activity is not run so we cannot get any information about it (activity have failed,skipped).
        # essential we cannot get the runtime information for it
        if input_source_obj is None:

            logger.warning(
                "sql script not available",
                 extra={
                    "event": "sql_script_missing",
                    **log_context
                }
            )
            return set()

        sql = get_sql_script(input_source_obj=input_source_obj,\
                                     dataset_type=input_dataset_info.type)
         
        #ignore when we cannot parse the sql 
        try:
            source_tables = get_sql_lineage(sql=clean_sql(sql=sql))
        except Exception:

            logger.warning(
                "sql lineage parsing failed",
                extra={
                    "event": "sql_parse_failed",
                    **log_context
                }
            )

            return set()

    elif isinstance(input_dataset_info,LocationDataset):


        blob_location = resolve_blob_expression(container=input_dataset_info.container,\
                                                folder_path=input_dataset_info.folder_path,\
                                                file_name=input_dataset_info.file_name,\
                                                dataset_parameters=dataset_parameters,\
                                                pipeline_parameters=runtime.pipeline_parameters)
        
        if blob_location is None:
            logger.warning("Source blob resolution failed",
                           extra={
                                "event":"source_blob_resolution_failed",
                                **log_context
                            })
        else:
            source_tables.add(normalize_blob_path(raw_blob_path=blob_location))

    if is_use_fqn and len(source_tables)>0:

        linked_service = find_linked_service(linked_services=linked_services,\
                                             search_linked_service_name=input_dataset.linked_service_name)
        
        if linked_services is not None:

            source_host_prefix = get_linked_service_host_prefix(linked_service=linked_service)

            if source_host_prefix is not None:
                source_tables = {
                    add_source_host_prefix(table_value=table,\
                                           host_prefix=source_host_prefix) 
                    for table in source_tables
                }

    
    return source_tables

def resolve_target_table(activity:GenericActivity,\
                         runtime:PipelineRuntimeContext,\
                         linked_services:List[LinkedService],\
                         is_use_fqn:bool)->Optional[str]:

    
    output_dataset = activity.output_dataset

    output_dataset_info = output_dataset.info

    target_table = None


    dataset_parameters = resolve_dataset_parameter(dataset_parameters=activity.output_dataset_parameters,\
                              pipeline_parameter=runtime.pipeline_parameters)

    log_context = {
        "pipeline": runtime.pipeline_name,
        "activity": activity.name,
        "dataset": output_dataset.name,
    }

    if isinstance(output_dataset_info,SingleTableDataset):

        target_table = resolve_table_expression(schema_expression=output_dataset_info.schema.value,\
                                                   table_expression=output_dataset_info.table.value,\
                                                   dataset_parameters=dataset_parameters,
                                                   pipeline_parameters=runtime.pipeline_parameters)
                
        if target_table is None:
             
             logger.warning("Target table resolution failed",
                            extra={
                                "event":"target_table_resolution_failed",
                                **log_context
                            })

    elif isinstance(output_dataset_info,LocationDataset):

        blob_location = resolve_blob_expression(container=output_dataset_info.container,\
                                                folder_path=output_dataset_info.folder_path,\
                                                file_name=output_dataset_info.file_name,\
                                                dataset_parameters=dataset_parameters,\
                                                pipeline_parameters=runtime.pipeline_parameters)
          
        if blob_location is None:
            logger.warning("Target blob resolution failed",
                           extra={
                                "event":"target_blob_resolution_failed",
                                **log_context
                            })
            
        else:
            target_table = normalize_blob_path(raw_blob_path=blob_location)

    if is_use_fqn and target_table is not None:

        linked_service = find_linked_service(linked_services=linked_services,\
                                             search_linked_service_name=output_dataset.linked_service_name)
        
        if linked_services is not None:

            target_host_prefix = get_linked_service_host_prefix(linked_service=linked_service)

            if target_host_prefix is not None:
                target_table = add_source_host_prefix(table_value=target_table,\
                                                      host_prefix=target_host_prefix)
                            
    return target_table

def add_lineage(initial_lineage:List[Edge],\
                sources:Set[str],\
                target:str)->List[Edge]:

    new_edge = Edge(
        node_name=target,
        parent_nodes=list(sources)
    )

    if len(initial_lineage)==0:
        initial_lineage.append(new_edge)

    else:
        #combine lineage based on multiple activity
        initial_lineage = merge_edge(left_edges=initial_lineage,\
                                     right_edges=[new_edge])


    return initial_lineage

def get_pipeline_table_lineage(static_pipeline:StaticPipeline,\
                                runtime_context:PipelineRuntimeContext,\
                                linked_services:List[LinkedService],\
                                is_use_fqn:bool,\
                                plugins:List[PluginWrapper])->List[Edge]:
    
    lineage:List[Edge] = list()


    for edge in static_pipeline.virtual_graph:
        
        generic_activity = static_pipeline.activities[edge.node_name]

        if generic_activity.activity_type==ActivityType.Procedure:

            procedure_context = get_procedure_context(procedure_activity=generic_activity.raw_activity,\
                                            runtime_context=runtime_context)

            if procedure_context is None:
                continue
            
            if len(plugins)>0:
                
                linked_service_name = procedure_context.linked_service_name

                linked_service = None

                database_conection = None

                if linked_service_name is not None:
                    linked_service = find_linked_service(linked_services=linked_services,
                                        search_linked_service_name=linked_service_name)
                
                if linked_service is not None:  
                    database_conection = get_database_connections(linked_service=linked_service)


                for plugin_lineage in resolve_plugins(plugins=plugins,\
                                context=procedure_context,\
                                connection=database_conection):
                    
                    source_tables = plugin_lineage[0]

                    target_table = plugin_lineage[1]

                    lineage = add_lineage(initial_lineage=lineage,\
                                          sources=source_tables,\
                                          target=target_table)

        else:

            # will only support lineage if we support both input and output

            if not (generic_activity.is_input_supported and generic_activity.is_output_supported):
                continue

            # by common sense , we should only support single table dataset and location dataset as sink

            if not isinstance(generic_activity.output_dataset.info,(SingleTableDataset,LocationDataset)):
                continue
            
            source_tables = resolve_source_table(activity=generic_activity,\
                                                runtime=runtime_context,\
                                                linked_services=linked_services,\
                                                is_use_fqn=is_use_fqn)
            
            target_table = resolve_target_table(activity=generic_activity,\
                                                runtime=runtime_context,\
                                                linked_services=linked_services,\
                                                is_use_fqn=is_use_fqn)
        
            # we cannot add lineage when we cannot parse the sink 

            if target_table is None:
                continue
            
            lineage = add_lineage(initial_lineage=lineage,\
                                  sources=source_tables,\
                                  target=target_table)
            
    return lineage

def to_open_lineage(namespace:str,producer:str,pipeline_lineage:PipelineLineage)->Tuple[Dict[str,Any],Dict[str,Any]]:
    
    start_time = datetime.now(timezone.utc).isoformat()

    end_time = datetime.now(timezone.utc).isoformat()

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

def get_runtime_context(client:AzureClient,\
                        pipeline_run:APIPipelineRun)->PipelineRuntimeContext:
    
    activity_source_inputs: Dict[str, Dict[str, Any]] = dict()

    activities_run = client.get_activities_run(pipeline_run=pipeline_run)

    pipeline_parameters:Dict[str,str] = dict()

    if pipeline_run.parameters is not None:
        pipeline_parameters = pipeline_run.parameters

    for activity_run in activities_run:

        if get_activity_type(activity_run.activity_type) == ActivityType.Copy and\
            activity_run.input:

            source = activity_run.input.get("source", {})   

            # if our SqlPoolSource has no mapping , it fall back to CopySource based type , source field is in additional_properties

            if not source and has_field(activity_run.input,"additional_properties"):
                source = activity_run.input.additional_properties.get("source",{})

            if source:
                activity_source_inputs[activity_run.activity_name] = source
                

    return PipelineRuntimeContext(
        pipeline_name=pipeline_run.pipeline_name,\
        run_id=pipeline_run.run_id,\
        run_start=pipeline_run.run_start,\
        run_end=pipeline_run.run_end,
        pipeline_parameters=pipeline_parameters,\
        activity_source_inputs=activity_source_inputs
    )

def get_static_pipeline(pipeline:APIPipelineResource,\
                        datasets:List[Dataset])->StaticPipeline:

    virtual_graph = get_virtual_graph(activities=to_activities(raw_activities=pipeline.activities))

    generic_activities:Dict[str,GenericActivity] = dict()

    expanded_activities = expand_activities(raw_activities=pipeline.activities)

    for edge in virtual_graph:

        activity = expanded_activities.get(edge.node_name)   

        if activity is None:
            continue

        generic_activity = get_generic_activity(raw_activity=activity,\
                                                    datasets=datasets)
            
        generic_activities[activity.name] = generic_activity
    
    return StaticPipeline(
        pipeline_name=pipeline.name,\
        virtual_graph=virtual_graph,\
        activities=generic_activities
    )

def get_procedure_context(procedure_activity:Any,
                          runtime_context:PipelineRuntimeContext)->Optional[StoreProcedurePluginContext]:
    
    try:

        procedure_name = procedure_activity.stored_procedure_name

        procedure_parameters:Dict[str,str] = dict()

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
        
        # for sql pool

        elif has_field(procedure_activity,"sql_pool") and\
            procedure_activity.sql_pool is not None:
            linked_service_name = procedure_activity.sql_pool.reference_name
        

        return StoreProcedurePluginContext(
            activity_name=procedure_activity.name,\
            linked_service_name=linked_service_name,\
            procedure_name=procedure_name,\
            procedure_parameters=procedure_parameters,\
            pipeline_parameters=runtime_context.pipeline_parameters
        )
        
    except Exception:

        logger.warning(f"get store procedure context failed",extra={
            "event":"get_store_procedure_context_failed",
            "activity": procedure_activity.name
        })

        return None
    
def main()->int:

    logger.info("Loading plugins:")

    raw_plugins = load_plugins(logger=logger,\
                               folder_path=PLUGIN_FOLDER_PATH)
    
    plugins = register_plugins(logger=logger,\
                               plugins=raw_plugins)
    
    logger.info("Loading plugins:complete")

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

    raw_pipelines = client.get_pipelines()

    if raw_pipelines is None:
        logger.info("Extracting pipeline:fail")
        return 1
    else:
        logger.info("Extracting pipeline:success")

    logger.info("Extracting lineage:")

    static_pipelines:Dict[str,StaticPipeline] = dict()

    for pipeline in raw_pipelines:

        static_pipeline = get_static_pipeline(pipeline=pipeline,\
                                              datasets=datasets)    
        
        if len(static_pipeline.activities)==0:
            continue

        static_pipelines[pipeline.name] = static_pipeline


    runtime_contexts:Dict[str,PipelineRuntimeContext] = dict()

    for pipeline_name in static_pipelines:

        latest_pipeline_info = get_latest_pipeline_info(pipeline_runs=client.get_pipeline_runs(pipeline_name=pipeline_name,\
                                                                        days=DAYS_SEARCH))
        
        if latest_pipeline_info is None:
            continue

        runtime_context = get_runtime_context(client=client,\
                                              pipeline_run=latest_pipeline_info)
        
        runtime_contexts[runtime_context.pipeline_name] = runtime_context

    pipeline_lineage:List[PipelineLineage] = list()

    for pipeline_name in runtime_contexts:
                
        lineage = get_pipeline_table_lineage(static_pipeline=static_pipelines[pipeline_name],\
                                              runtime_context=runtime_contexts[pipeline_name],\
                                              linked_services=linked_services,\
                                              is_use_fqn=IS_USE_FQN,\
                                              plugins=plugins)

        pipeline_lineage.append(PipelineLineage(
                            pipeline_name=pipeline_name,\
                            lineage=lineage)
                            )
    
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
            json.dump([asdict(lineage) for lineage in pipeline_lineage],file,indent=4)

        logger.info(f"Saving lineage to {LINEAGE_OUTPUT_FILE_PATH}:success")
    
    except:
        logger.info(f"Saving lineage to {LINEAGE_OUTPUT_FILE_PATH}:fail")

        return 1
    
    return 0

if __name__=="__main__":
    sys.exit(main())