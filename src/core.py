from model import Activity,ActivityType,ParameterValue,Resolved,Unresolved
from typing import List,Dict,Optional,Any,Set
from graph import Edge,merge_edge,replace_node_with_edge,remove_node,get_node_names
import re

WHOLE_DATASET_PATTERN = re.compile(r"^@dataset\(\)\.(\w+)$")

WHOLE_PIPELINE_PATTERN = re.compile(r"^@pipeline\(\)\.parameters\.(\w+)$")

WHOLE_LINKED_SERVICE_PATTERN = re.compile(r"^@linkedService\(")

# find @{...} interolated pattern

INTERPOLATED_PATTERN = re.compile(r"@\{([^}]+)\}")

INTERPOLATED_DATASET_PATTERN = re.compile(r"^dataset\(\)\.(\w+)$")

INTERPOLATED_PIPELINE_PATTERN = re.compile(r"^pipeline\(\)\.parameters\.(\w+)$")

INTERPOLATED_LINKED_SERVICE_PATTERN = re.compile(r"linkedService\(")

ACTIVITY_TYPE_MAP:Dict[str,ActivityType] = {
    "Copy":ActivityType.Copy,
    "ExecutePipeline":ActivityType.Execute,
    "ForEach":ActivityType.ForEach,
    "IfCondition":ActivityType.If,
    "Until":ActivityType.While
}

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
                                                   ActivityType.Execute]
    
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
                       pipeline_parameters:Dict[str, str])->ParameterValue:

    if expression is None:

        return Unresolved(expression="None",\
                          reason="null value")

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
                                                       pipeline_parameters=pipeline_parameters)
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
                                    pipeline_parameters:Dict[str, str])->ParameterValue:
    
    """Resolve the expression with @{...}."""

    if INTERPOLATED_LINKED_SERVICE_PATTERN.search(interpolated_expression):

        return Unresolved(expression=interpolated_expression,\
                          reason="linkedService() cannot be resolved statically")

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

def resolve_table_expression(schema_expression:str,
                            table_expression:str,
                            dataset_parameters:Dict[str, str],
                            pipeline_parameters:Dict[str, str])->Optional[str]:
    """
    Resolve schema expression and table expression to schema.table format
    """
    
    schema = resolve_expression(expression=schema_expression,\
                                dataset_parameters=dataset_parameters,\
                                pipeline_parameters=pipeline_parameters)
    
    table = resolve_expression(expression=table_expression,\
                                dataset_parameters=dataset_parameters,\
                                pipeline_parameters=pipeline_parameters)

    if isinstance(schema, Unresolved) or isinstance(table, Unresolved):
        return None

    return f"{schema.value}.{table.value}"

