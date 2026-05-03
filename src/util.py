from model import (
    Parameter,
    ParameterType,
    AZURE_PARAMETER_TYPES,
    AZURE_PARAMETER_TYPES_TUPLE,
    PipelineLineage,
    LineageActivityInfo
)
from typing import (
    List,
    Set,
    Union,
    Dict,
    Optional,
    Any,
    Tuple
)
from graph import (
    Edge,
    merge_edge
)
from plugin import (
    LineageEdge,
    ActivityLineageContext,
    PipelineLineageContext,
    ActivityLineageInfo
)
import uuid
from datetime import (
    datetime,
    timezone
)

def create_parameter(parameter_value:Union[AZURE_PARAMETER_TYPES,Dict[str,str]])->Parameter:
    
    if isinstance(parameter_value,AZURE_PARAMETER_TYPES_TUPLE):
        return Parameter(value=str(parameter_value),parameter_type=ParameterType.Static)

    return Parameter(value=parameter_value["value"],parameter_type=ParameterType.Expression)

def get_connection_properties(connection_str:str)->Dict[str,str]:
    connection_propertity:Dict[str,str] = dict()

    for block in connection_str.split(";"):
        property_block = block.split("=")

        if len(property_block)>=2:
            connection_propertity[property_block[0].replace(" ","").lower()] = property_block[1]

    return connection_propertity

def has_field(obj:object,field_name:str)->bool:
    """
    Check whether the obj have either key or attribute of field_name
    """

    if isinstance(obj,dict):
        return field_name in obj

    return hasattr(obj,field_name)


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

def to_lineage_edge(edges:List[Edge])->List[LineageEdge]:
    return [LineageEdge(node_name=edge.node_name,\
                        parent_nodes=edge.parent_nodes) for edge in edges]


def to_pipeline_lineage_context(activity_lineage_context:List[ActivityLineageContext])->Optional[PipelineLineageContext]:
    """
    activities : activity context of same pipeline
    """

    if len(activity_lineage_context)==0:
        return None
    
    pipeline_names:Set[str] = {
        x.pipeline_name
        for x in activity_lineage_context
    }

    # the activity context in the parameter are of different pipeline

    if len(pipeline_names)>1:
        return None
    
    pipeline_lineage:List[Edge] = list()

    first_activity = activity_lineage_context[0]

    for activity_context in activity_lineage_context:

        for lineage in activity_context.lineage:

            pipeline_lineage = add_lineage(initial_lineage=pipeline_lineage,\
                                           sources=set(lineage.parent_nodes),\
                                           target=lineage.node_name)
            
    return PipelineLineageContext(
        pipeline_name=first_activity.pipeline_name,\
        pipeline_run_id=first_activity.pipeline_run_id,\
        pipeline_run_status=first_activity.pipeline_run_status,\
        pipeline_run_start=first_activity.pipeline_run_start,\
        pipeline_run_end=first_activity.pipeline_run_end,\
        lineage=to_lineage_edge(edges=pipeline_lineage)
    )

def get_activity_lineage_infos(raw_pipeline_names:Set[str],\
                    static_pipeline_names:Set[str],\
                    lineage_activity_infos:Set[LineageActivityInfo])->List[ActivityLineageInfo]:

    activities_lineage_infos:List[ActivityLineageInfo] = list()

    for raw_pipeline_name in raw_pipeline_names:
        
        if raw_pipeline_name not in static_pipeline_names:

            activities_lineage_infos.append(ActivityLineageInfo(
                pipeline_name=raw_pipeline_name,\
                is_pipeline_supported=False,\
                activity_name=None,\
                activity_type=None,\
                is_lineage_extracted=False
            ))
            
            continue

        activities = [x for x in lineage_activity_infos if x.pipeline_name==raw_pipeline_name]

        for activity in activities:

            activities_lineage_infos.append(ActivityLineageInfo(
                pipeline_name=raw_pipeline_name,\
                is_pipeline_supported=True,\
                activity_name=activity.activity_name,\
                activity_type=str(activity.activity_type),\
                is_lineage_extracted=not(activity.is_skipped)
            ))
                  
    return activities_lineage_infos


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