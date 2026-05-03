from model import (
    Activity,
    ActivityType,
    ParameterValue,
    Resolved,
    Unresolved,
    Parameter,
    ParameterType,
    GenericActivity,
    Dataset,
    DatasetType,
    StaticPipeline,
    APIPipelineResource
)
from typing import (
    List,
    Dict,
    Optional,
    Any,
    Set
)
from graph import (
    Edge,
    merge_edge,
    replace_node_with_edge,
    remove_node
)
from util import (
    has_field,
    create_parameter
)
from search import find_dataset

import re

WHOLE_DATASET_PATTERN = re.compile(r"^@dataset\(\)\.(\w+)$")

WHOLE_PIPELINE_PATTERN = re.compile(r"^@pipeline\(\)\.parameters\.(\w+)$")

WHOLE_LINKED_SERVICE_PATTERN = re.compile(r"^@linkedService\(\)\.(\w+)$")

# find @{...} interolated pattern

INTERPOLATED_PATTERN = re.compile(r"@\{([^}]+)\}")

INTERPOLATED_DATASET_PATTERN = re.compile(r"^dataset\(\)\.(\w+)$")

INTERPOLATED_PIPELINE_PATTERN = re.compile(r"^pipeline\(\)\.parameters\.(\w+)$")

INTERPOLATED_LINKED_SERVICE_PATTERN =  re.compile(r"^linkedService\(\)\.(\w+)$")

ACTIVITY_TYPE_MAP:Dict[str,ActivityType] = {
    "Copy":ActivityType.Copy,
    "ExecutePipeline":ActivityType.Execute,
    "ForEach":ActivityType.ForEach,
    "IfCondition":ActivityType.If,
    "Until":ActivityType.While,
    "SqlPoolStoredProcedure":ActivityType.Procedure,
    "SqlServerStoredProcedure":ActivityType.Procedure,
    "Script":ActivityType.Script
}

BLOB_PARTITION_PATTERN = re.compile(
   # handle hive-based style pattern like year=yyyy/month=mm 
   r"(/year=\d{4}.*|" 
   # handle date path patern yyyy/mm/dd 
   r"/\d{4}/\d{2}/\d{2}.*)"
)

BLOB_TRAILING_FILE_PATTERN = re.compile(r"/[^/]+\.[^/]+$")

# for removing _yyyy_mm_dd date suffix from a file name

BLOB_DATE_SUFFIX_PATTERN = re.compile(r"_\d{4}_\d{2}_\d{2}")

# for removing file extension of last path segment 

BLOB_FILE_EXTENSION_PATTERN = re.compile(r"\.[^./]+$")

def get_activity_type(raw_activity_type:str)->ActivityType:
    
    if raw_activity_type in ACTIVITY_TYPE_MAP:
        return ACTIVITY_TYPE_MAP[raw_activity_type]

    return ActivityType.Unsupported

def to_activities(raw_activities:List[Any])->List[Activity]:

    transform_activities:List[Activity] = list()

    for activity in raw_activities:

        activity_type = get_activity_type(activity.type)

        true_children:List[Activity] = list()

        false_children:List[Activity] = list()

        body_children:List[Activity] = list()

        depends_on:List[str] = list()
        
        if activity.depends_on is not None:
            depends_on = [x.activity for x in activity.depends_on]

        if activity_type == ActivityType.If:

            if activity.if_true_activities is not None:
                true_children = to_activities(activity.if_true_activities)

            if activity.if_false_activities is not None:
                false_children = to_activities(activity.if_false_activities)

        elif activity_type in [ActivityType.ForEach,ActivityType.While]:

            if activity.activities is not None:
                body_children = to_activities(activity.activities)

        transform_activities.append(Activity(
            activity_name=activity.name,
            activity_type=activity_type,
            depends_on=depends_on,
            true_children=true_children,
            false_children=false_children,
            body_children=body_children
        ))

    return transform_activities


def get_activities_graph(activities:List[Activity])->List[Edge]:
    """
    Get the activities as edge (excluded nested activity)
    """
    return [Edge(node_name=actv.activity_name,\
                 parent_nodes=list(actv.depends_on))for actv in activities]

def branch_to_edges(activities:List[Activity])->List[Edge]:
    """
    Get the activities as edge (include nested activity)
    """
    
    edges:List[Edge] = list()

    for activity in activities:
        
        edges.append(Edge(node_name=activity.activity_name,\
                          parent_nodes=list(activity.depends_on)))
        
        if activity.activity_type==ActivityType.If:

            edges.extend(branch_to_edges(activities=activity.true_children))
            edges.extend(branch_to_edges(activities=activity.false_children))

        elif activity.activity_type in [ActivityType.ForEach,ActivityType.While]:

            edges.extend(branch_to_edges(activities=activity.body_children))

    return edges

def expand_activities(raw_activities:List[Any],\
                      expanded:Dict[str,Any]=None)->Dict[str,Any]:
    
    if expanded is None:
        expanded = dict()

    for activity in raw_activities:

        expanded[activity.name] = activity

        activity_type = get_activity_type(raw_activity_type=activity.type)

        if activity_type==ActivityType.If:

             if activity.if_true_activities is not None:
                 expand_activities(raw_activities=activity.if_true_activities,\
                                   expanded=expanded)
                 
             if activity.if_false_activities is not None:
                 expand_activities(raw_activities=activity.if_false_activities,\
                                   expanded=expanded)
           

        elif activity_type in [ActivityType.ForEach,ActivityType.While]:
            if activity.activities is not None:
                expand_activities(raw_activities=activity.activities,\
                                   expanded=expanded)


    return expanded
    


def get_flatten_branches(activities:List[Activity],\
                         edges:List[Edge])->List[Edge]:
    """
    Expand the activity with have the inner activity
    """

    for activity in activities:

        if activity.activity_type == ActivityType.If:

            true_edges = branch_to_edges(activities=activity.true_children)

            false_edges = branch_to_edges(activities=activity.false_children)

            merged_edge = merge_edge(left_edges=true_edges,\
                                     right_edges=false_edges)
            
            if len(merged_edge)>0:
                edges = replace_node_with_edge(node_name=activity.activity_name,\
                                               replace_edges=merged_edge,\
                                               edges=edges)
            else:
                edges = remove_node(node_name=activity.activity_name,\
                                    edges=edges)
            
            # handle nested activites

            edges = get_flatten_branches(activities=activity.true_children,\
                                         edges=edges)
            
            edges = get_flatten_branches(activities=activity.false_children,\
                                               edges=edges)
            

        elif activity.activity_type in [ActivityType.ForEach,ActivityType.While]:

            body_edges = branch_to_edges(activities=activity.body_children)

            if len(body_edges)>0:
                edges = replace_node_with_edge(node_name=activity.activity_name,\
                                               replace_edges=body_edges,\
                                               edges=edges)
                
            else:
                edges = remove_node(node_name=activity.activity_name,\
                                    edges=edges)
            
            # handle nested activites

            edges = get_flatten_branches(activities=activity.body_children,\
                                         edges=edges)
            
    return edges
            
def get_activities_type(activities:List[Activity],\
                      activities_type:Dict[str,ActivityType]=None)->Dict[str,ActivityType]:
    """
    Recursively get the activity type (including inner activity)
    """
    
    if activities_type is None:
        activities_type = dict()

    for activity in activities:
        
        activities_type[activity.activity_name] = activity.activity_type

        activities_type = get_activities_type(activities=activity.true_children,\
                                            activities_type=activities_type)
        
        activities_type = get_activities_type(activities=activity.false_children,\
                                            activities_type=activities_type)
        
        activities_type = get_activities_type(activities=activity.body_children,\
                                            activities_type=activities_type)

    
    return activities_type

def get_simplify_graph(activities:List[Activity],\
                       edges:List[Edge])->List[Edge]:
    """
    Remove the unsupported activity type from the graph
    """
    
    activities_type = get_activities_type(activities=activities,\
                                        activities_type=dict())
    
    required_activities_type:List[ActivityType] = [ActivityType.Copy,\
                                                   ActivityType.Procedure,\
                                                   ActivityType.Execute,\
                                                   ActivityType.Script]
    
    to_remove_node:Set[str] = set()
    
    for edge in edges:

        if activities_type[edge.node_name] not in required_activities_type:
            to_remove_node.add(edge.node_name)
            
    for node in to_remove_node:
        
        result = remove_node(node_name=node,\
                                edges=edges)
        
        if result is not None:
            edges = result
            
    return edges

def get_virtual_graph(activities:List[Activity])->List[Edge]:

    edges = branch_to_edges(activities=activities)

    edges = get_flatten_branches(activities=activities,\
                                 edges=edges)

    edges = get_simplify_graph(activities=activities,\
                               edges=edges)
    
    return edges

def resolve_expression(expression:str,\
                       dataset_parameters:Dict[str, str],\
                       pipeline_parameters:Dict[str, str],\
                       linked_service_parameters:Dict[str,str])->ParameterValue:
    """
    Resolve the expression on dataset_parameters and pipeline_parameters. 
    It could not resolve the adf functions and return Unresolved

    expression : to resolve. It could be 
     static value :  foo 
     dataset parameter : @dataset().foo ,
     pipeline paramter : @pipeline().parameters.foo 
     linked_service_parameters : @linkedService().foo
     interpolated expression : @{dataset().foo} , @{pipeline().parameters.foo}
    dataset_parameters (parameter_name,value) : value to replace it with
    pipeline_parameters (parameter_name,value) : value to replace it with
    """

    if expression is None:

        return Unresolved(expression="None",\
                          reason="null value")

    match = WHOLE_LINKED_SERVICE_PATTERN.match(expression)
    
    if match:

        name = match.group(1)

        if name in linked_service_parameters:
            return Resolved(linked_service_parameters[name])

    if WHOLE_LINKED_SERVICE_PATTERN.search(expression):

        return Unresolved(expression=expression,
                          reason="linkedService() cannot be resolved statically")

    match = WHOLE_DATASET_PATTERN.match(expression)

    if match:

        name = match.group(1)

        if name in dataset_parameters:
            return Resolved(dataset_parameters[name])
        
        return Unresolved(expression=expression,\
                          reason=f"dataset parameter '{name}' not in context")

    match = WHOLE_PIPELINE_PATTERN.match(expression)

    if match:

        name = match.group(1)

        if name in pipeline_parameters:
            return Resolved(pipeline_parameters[name])
        
        return Unresolved(expression=expression,\
                          reason=f"pipeline parameter '{name}' not in context")
    
    # handle interpolated @{...} anywhere in the expression
    
    tokens = INTERPOLATED_PATTERN.findall(expression)

    if tokens:

        result = expression

        for interpolated_expression in tokens:

            resolved = resolve_interpolated_expression(interpolated_expression=interpolated_expression,\
                                                       dataset_parameters=dataset_parameters,\
                                                       pipeline_parameters=pipeline_parameters,\
                                                       linked_service_parameters=linked_service_parameters)
            if isinstance(resolved, Unresolved):

                return Unresolved(expression=expression,\
                                  reason=f"could not resolve @{{{interpolated_expression}}}: {resolved.reason}")
            
            result = result.replace(f"@{{{interpolated_expression}}}",\
                                    resolved.value,\
                                    1)
        return Resolved(result)

    # if it is a constant value , just return it

    if not expression.startswith("@"):
        return Resolved(expression)            

    return Unresolved(expression=expression,\
                      reason="unrecognised expression")

def resolve_interpolated_expression(interpolated_expression:str,\
                                    dataset_parameters:Dict[str, str],\
                                    pipeline_parameters:Dict[str, str],\
                                    linked_service_parameters:Dict[str,str])->ParameterValue:
    
    """Resolve the expression with @{...}."""

    match = INTERPOLATED_LINKED_SERVICE_PATTERN.search(interpolated_expression)

    if match:

        name = match.group(1)

        if name in linked_service_parameters:
            return Resolved(linked_service_parameters[name])
        
        return Unresolved(expression=interpolated_expression,\
                                reason=f"linked service parameter '{name}' not in context")

    match = INTERPOLATED_DATASET_PATTERN.match(interpolated_expression)

    if match:

        name = match.group(1)

        if name in dataset_parameters:
            return Resolved(dataset_parameters[name])
        
        return Unresolved(expression=interpolated_expression,\
                                reason=f"dataset parameter '{name}' not in context")

    match = INTERPOLATED_PIPELINE_PATTERN.match(interpolated_expression)

    if match:

        name = match.group(1) 

        if name in pipeline_parameters:
            return Resolved(pipeline_parameters[name])
        
        return Unresolved(expression=interpolated_expression,\
                                reason=f"pipeline parameter '{name}' not in context")


    return Unresolved(expression=interpolated_expression,\
                       reason="unrecognised expression")

def resolve_table_expression(schema_expression:Optional[str],
                            table_expression:str,
                            dataset_parameters:Dict[str, str],
                            pipeline_parameters:Dict[str, str])->Optional[str]:
    """
    Resolve schema expression and table expression to schema.table format or table format 
    if the schema is None because there can be optional schema like mongodb
    """

    if schema_expression is not None:
        schema = resolve_expression(expression=schema_expression,\
                                    dataset_parameters=dataset_parameters,\
                                    pipeline_parameters=pipeline_parameters,\
                                    linked_service_parameters=dict())
    
    table = resolve_expression(expression=table_expression,\
                                dataset_parameters=dataset_parameters,\
                                pipeline_parameters=pipeline_parameters,\
                                linked_service_parameters=dict())
    
    if schema_expression is None and\
    not isinstance(table,Unresolved):
        return table.value

    if isinstance(schema, Unresolved) or isinstance(table, Unresolved):
        return None

    return f"{schema.value}.{table.value}"

def resolve_parameter(parameter:Optional[Parameter],\
                      dataset_parameters:Dict[str, str],\
                      pipeline_parameters:Dict[str, str],\
                      linked_service_parameters:Dict[str,str])->Optional[str]:
    
    if parameter is None:
        return None
    
    result = resolve_expression(expression=parameter.value,\
                       dataset_parameters=dataset_parameters,\
                       pipeline_parameters=pipeline_parameters,\
                       linked_service_parameters=linked_service_parameters)
    
    if isinstance(result,Resolved):
        return result.value
    
    return None

def resolve_blob_expression(container:Optional[Parameter],\
                      folder_path:Optional[Parameter],\
                      file_name:Optional[Parameter],\
                      dataset_parameters:Dict[str, str],\
                      pipeline_parameters:Dict[str, str])->Optional[str]:
    
    resolved_container = resolve_parameter(parameter=container,\
                                           dataset_parameters=dataset_parameters,\
                                           pipeline_parameters=pipeline_parameters,\
                                           linked_service_parameters=dict())

    resolved_folder_path = resolve_parameter(parameter=folder_path,\
                                            dataset_parameters=dataset_parameters,\
                                            pipeline_parameters=pipeline_parameters,\
                                            linked_service_parameters=dict())

    resolved_file_name = resolve_parameter(parameter=file_name,\
                                           dataset_parameters=dataset_parameters,\
                                           pipeline_parameters=pipeline_parameters,\
                                           linked_service_parameters=dict())

    parts = [x for x in [resolved_container,resolved_folder_path,resolved_file_name] if x is not None]

    if len(parts)>0:
        return "/".join(parts)
    
    return None

def normalize_blob_path(raw_blob_path:str)->str:
    """
    Return the blob path which is normalize to logical folder like

    container/data/year=YYYY/month=MM/day=MM/data.ext = container/data

    container/data/YYYY/MM/DD/data.ext = container/data

    for non date pattern like container/folder/data/data.ext = container/folder/data

    container/data.ext = container/data

    container/data_part_YYYY_MM_DD.ext = container/data_part

    data.ext = data.ext
    """
    blob_path = BLOB_PARTITION_PATTERN.sub("",raw_blob_path).rstrip("/")

    if blob_path.count("/")>1:
        blob_path = BLOB_TRAILING_FILE_PATTERN.sub("",blob_path).rstrip("/")

    blob_path = BLOB_DATE_SUFFIX_PATTERN.sub("",blob_path)

    blob_path = BLOB_FILE_EXTENSION_PATTERN.sub("",blob_path)

    return blob_path.rstrip("/")

def resolve_dataset_parameter(dataset_parameters:Dict[str,Parameter],\
                          pipeline_parameter:Dict[str,str])->Dict[str,str]:
    """
    Resolve the dataset_parameters with pipeline_parameter
    """
    
    # for the static dataset parameter , we just use it dataset parameter without modifying anythong

    static_dataset_parameters = {
        parameter_name:dataset_parameters[parameter_name].value
        for parameter_name in dataset_parameters
        if dataset_parameters[parameter_name].parameter_type==ParameterType.Static
    }

    # for the expression dataset parameter , if there is the same parameter name in pipeline_parameter , we use pipeline_parameter name

    expression_dataset_parameters = {
        parameter_name:pipeline_parameter[parameter_name]
        for parameter_name in dataset_parameters
        if parameter_name in pipeline_parameter\
        and dataset_parameters[parameter_name].parameter_type==ParameterType.Expression
    }

    unresolved_dataset_parameters = {
        parameter_name:dataset_parameters[parameter_name]
        for parameter_name in dataset_parameters
        if dataset_parameters[parameter_name].parameter_type==ParameterType.Expression and\
        parameter_name not in pipeline_parameter
    }

    # resolve the dataset parameter using the pipeline parameter

    resolved_dataset_parameter_result = { 
        parameter_name : resolve_expression(expression=unresolved_dataset_parameters[parameter_name].value,\
                                            dataset_parameters=dict(),\
                                            pipeline_parameters=pipeline_parameter,\
                                            linked_service_parameters=dict())

        for parameter_name in unresolved_dataset_parameters 
    }

    # get the resolved result
    
    resolved_dataset_parameter = {
        parameter_name:resolved_dataset_parameter_result[parameter_name].value 
        for parameter_name in resolved_dataset_parameter_result 
        if isinstance(resolved_dataset_parameter_result[parameter_name],Resolved)
    }

    return {**static_dataset_parameters,\
            **expression_dataset_parameters,\
            **resolved_dataset_parameter}


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
    
    if activity_type==ActivityType.Script:

        return GenericActivity(
            name=raw_activity.name,
            activity_type=ActivityType.Script,
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