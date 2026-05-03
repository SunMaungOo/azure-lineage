from typing import (
    Dict,
    List,
    Set,
    Any
)
from lineage import get_pipeline_table_lineage
from graph import (
    Edge,
    merge_edges
)
from model import (
    PipelineLineage,
    PipelineRuntimeContext,
    StaticPipeline,
    LineageActivityInfo
)
from client import (
    get_datasets,
    get_linked_service,
    get_runtime_context
)
from config import (
    get_api_client,
    DAYS_SEARCH,
    OPENLINEAGE_NAMESPACE,
    PLUGIN_FOLDER_PATH,
    OPENLINEAGE_OUTPUT_FILE_PATH,
    OPENLINEAGE_PRODUCER,
    IS_USE_FQN,
    LINEAGE_OUTPUT_FILE_PATH,
    IS_DEBUG,
    DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME,
    IS_AZURE_DATA_FACTORY
)
import json
from pathlib import Path
import logging
import sys
from dataclasses import asdict
from core import get_static_pipeline
from util import (
    to_pipeline_lineage_context,
    to_open_lineage,
    get_activity_lineage_infos
)
from formatter import LogFormatter
from plugin import (
    PipelineLineageContext,
    ActivityLineageContext
)
from pluginhelper import (
    load_plugins,
    register_plugins,
    get_activity_plugins,
    get_writer_plugins,
    resolve_writer_plugins,
)
from search import find_latest_pipeline_info

logging.getLogger("azure.mgmt.datafactory").setLevel(logging.ERROR)
logging.getLogger("azure.synapse.artifacts").setLevel(logging.ERROR)

def get_logger()->logging.Logger:

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

    return logger

def main()->int:

    logger = get_logger()

    logger.info("Loading plugins:")

    raw_plugins = load_plugins(logger=logger,\
                               folder_path=PLUGIN_FOLDER_PATH)
    
    plugins = register_plugins(logger=logger,\
                               plugins=raw_plugins)
    
    activity_plugins = get_activity_plugins(plugins=plugins)

    writer_plugins = get_writer_plugins(plugins=plugins)

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

        latest_pipeline_info = find_latest_pipeline_info(pipeline_runs=client.get_pipeline_runs(pipeline_name=pipeline_name,\
                                                                        days=DAYS_SEARCH))
        
        if latest_pipeline_info is None:
            continue

        runtime_context = get_runtime_context(client=client,\
                                              pipeline_run=latest_pipeline_info)           
        runtime_contexts[runtime_context.pipeline_name] = runtime_context
    
    pipeline_lineage:List[PipelineLineage] = list()

    pipeline_lineage_contexts:List[PipelineLineageContext] = list()

    activity_lineage_contexts:List[ActivityLineageContext] = list()

    lineage_activity_infos:Set[LineageActivityInfo] = set()

    synapse_workspace_name = None

    if not IS_AZURE_DATA_FACTORY:
        synapse_workspace_name = DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME

    for pipeline_name in runtime_contexts:
        
        activity_lineage,lineage_activities = get_pipeline_table_lineage(static_pipeline=static_pipelines[pipeline_name],\
                                              runtime_context=runtime_contexts[pipeline_name],\
                                              linked_services=linked_services,\
                                              is_use_fqn=IS_USE_FQN,\
                                              plugins=activity_plugins,\
                                              synapse_workspace_name=synapse_workspace_name,\
                                              logger=logger)
        
        lineage_activity_infos = lineage_activity_infos.union(lineage_activities)
        
        edges:List[List[Edge]] = [x.lineage for x in activity_lineage]
        
        pipeline_lineage.append(PipelineLineage(
                            pipeline_name=pipeline_name,\
                            lineage=merge_edges(edges))
                            )
        
        pipeline_lineage_context = to_pipeline_lineage_context(activity_lineage_context=activity_lineage)

        if pipeline_lineage_context is None:

            current_pipeline_context = runtime_contexts[pipeline_name]

            pipeline_lineage_context = PipelineLineageContext(
                pipeline_name=pipeline_name,\
                pipeline_run_id=current_pipeline_context.run_id,\
                pipeline_run_status=current_pipeline_context.pipeline_run_status,\
                pipeline_run_start=current_pipeline_context.run_start,\
                pipeline_run_end=current_pipeline_context.run_end,\
                lineage=list()
            )
                
        pipeline_lineage_contexts.append(pipeline_lineage_context)

        activity_lineage_contexts.extend(activity_lineage)

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
    
    if len(writer_plugins)>0:

        if not resolve_writer_plugins(plugins=writer_plugins,\
                               context=pipeline_lineage_contexts):
            logger.warning("Some plugins fail to write lineage")

        if not resolve_writer_plugins(plugins=writer_plugins,\
                               context=activity_lineage_contexts):
            logger.warning("Some plugins fail to write lineage") 

    if IS_DEBUG:

        raw_pipeline_names = {x.name for x in raw_pipelines}

        static_pipeline_names = {key for key in runtime_contexts}

        activity_lineage_infos = get_activity_lineage_infos(raw_pipeline_names=raw_pipeline_names,\
                        static_pipeline_names=static_pipeline_names,\
                        lineage_activity_infos=lineage_activity_infos)
        
        if not resolve_writer_plugins(plugins=writer_plugins,\
                                      context=activity_lineage_infos):
                                      
            logger.warning("Some plugins fail to write activity lineage info") 

    return 0

if __name__=="__main__":
    sys.exit(main())