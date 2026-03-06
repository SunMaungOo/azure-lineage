from model import Activity,ActivityType
from typing import List,Dict
from graph import Edge,merge_edge,replace_node_with_edge,remove_node

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
            
def get_activity_type(activities:List[Activity],\
                      activities_type:Dict[str,ActivityType]=None)->Dict[str,ActivityType]:
    """
    Recursively get the activity type (including inner activity)
    """
    
    if activities_type is None:
        activities_type = dict()

    for activity in activities:
        
        activities_type[activity.activity_name] = activity.activity_type

        activities_type = get_activity_type(activities=activity.true_children,\
                                            activities_type=activities_type)
        
        activities_type = get_activity_type(activities=activity.false_children,\
                                            activities_type=activities_type)
        
        activities_type = get_activity_type(activities=activity.body_children,\
                                            activities_type=activities_type)

    
    return activities_type

def get_simplify_graph(activities:List[Activity],\
                       edges:List[Edge])->List[Edge]:
    """
    Remove the unsupported activity type from the graph
    """
    
    activities_type = get_activity_type(activities=activities,\
                                        activities_type=dict())
    
    required_activities_type:List[ActivityType] = [ActivityType.Copy,\
                                                   ActivityType.Procedure,\
                                                   ActivityType.Execute]
    
    for edge in list(edges):
        if activities_type[edge.node_name] not in required_activities_type:
            edges = remove_node(node_name=edge.node_name,\
                        edges=edges)
            
    return edges

def get_virtual_graph(activities:List[Activity])->List[Edge]:

    edges = branch_to_edges(activities=activities)

    edges = get_flatten_branches(activities=activities,\
                                 edges=edges)

    edges = get_simplify_graph(activities=activities,\
                               edges=edges)
    
    return edges
