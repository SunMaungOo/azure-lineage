from model import Activity, ActivityType
from graph import get_node_names,get_parent_nodes
from core import (
    branch_to_edges,
    get_flatten_branches,
    get_simplify_graph,
    get_activity_type,
    get_virtual_graph,
)
from typing import List


def copy_activity(name:str,\
         depends_on:List[Activity] = list())->Activity:

    return Activity(activity_name=name,
                    activity_type=ActivityType.Copy,
                    depends_on=depends_on)

def procedure_activity(name:str,\
                        depends_on:List[Activity] = list())->Activity:
    
    return Activity(activity_name=name,
                    activity_type=ActivityType.Procedure,
                    depends_on=depends_on)

def unsupported_activity(name:str,\
                        depends_on:List[Activity] = list())->Activity:
    
    return Activity(activity_name=name,
                    activity_type=ActivityType.Unsupported,
                    depends_on=depends_on)

def if_activity(name:str,\
                true_branch:List[Activity] = list(),\
                false_branch:List[Activity] = list(),\
                depends_on:List[Activity] = list())-> Activity:
    
    return Activity(activity_name=name,
                    activity_type=ActivityType.If,
                    depends_on=depends_on,
                    true_children=true_branch,
                    false_children=false_branch)


def for_each_activity(name:str,\
                     body_children:List[Activity] = list(),
                     depends_on:List[Activity]= list())->Activity:
    
    return Activity(activity_name=name,
                    activity_type=ActivityType.ForEach,
                    depends_on=depends_on,
                    body_children=body_children)

def while_activity(name:str,\
                  body_children:List[Activity] = list(),\
                  depends_on:List[Activity] = list())->Activity:
    
    return Activity(activity_name=name,
                    activity_type=ActivityType.While,
                    depends_on=depends_on,
                    body_children=body_children)



def test_branch_to_edges_flat():

    activities = [copy_activity(name="A"),\
                  copy_activity(name="B", depends_on=["A"])]
    
    edges = branch_to_edges(activities)

    nodes_name = get_node_names(edges=edges)

    parent_nodes = get_parent_nodes(node_name="B",\
                                    edges=edges)

    assert "A" in nodes_name
    assert "B" in nodes_name
    assert "A" in parent_nodes
    assert len(parent_nodes)==1

def test_branch_to_edges_includes_if_children():

    activities = [if_activity(name="A",\
                            true_branch=[copy_activity(name="B")],\
                            false_branch=[copy_activity(name="C")])]

    edges = branch_to_edges(activities)

    assert "A" in get_node_names(edges)
    assert "B" in get_node_names(edges)
    assert "C" in get_node_names(edges)

def test_branch_to_edges_includes_foreach_body():

    activities = [for_each_activity(name="A",\
                                    body_children=[copy_activity(name="B")])]

    edges = branch_to_edges(activities)

    assert "A" in get_node_names(edges)
    assert "B" in get_node_names(edges)


def test_branch_to_edges_includes_while_body():

    activities = [while_activity(name="A",\
                        body_children=[copy_activity(name="B")])]

    edges = branch_to_edges(activities)

    assert "A" in get_node_names(edges)
    assert "B" in get_node_names(edges)


def test_get_activity_type_flat():

    activities = [copy_activity(name="A"),\
                  procedure_activity(name="B")]

    result = get_activity_type(activities)

    assert result["A"] == ActivityType.Copy
    assert result["B"] == ActivityType.Procedure

def test_get_activity_type_includes_nested():

    activities = [if_activity(name="A",\
                              true_branch=[copy_activity(name="B")],\
                                false_branch=[copy_activity(name="C")])]
    
    result = get_activity_type(activities)

    assert result["A"] == ActivityType.If
    assert result["B"] == ActivityType.Copy
    assert result["C"] == ActivityType.Copy

def test_get_activity_type_no_mutation_across_calls():

    # Mutable default argument bug — second call must not see first call's data

    get_activity_type([copy_activity(name="A")])

    result = get_activity_type([copy_activity(name="B")])

    assert "A" not in result



def test_flatten_foreach_removes_loop_node():

    activities = [for_each_activity(name="A",\
                                    body_children=[copy_activity("B")])]

    edges = branch_to_edges(activities)

    edges = get_flatten_branches(activities, edges)

    nodes_name = get_node_names(edges=edges)


    assert "A" not in nodes_name
    assert "B" in nodes_name

def test_flatten_foreach_connect_prev_to_body():

    activities = [
        copy_activity(name="A"),
        for_each_activity(name="B",\
                        body_children=[copy_activity(name="Body")],\
                        depends_on=["A"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "A" in get_parent_nodes("Body", edges)

def test_flatten_foreach_connect_body_to_next():

    activities = [
        for_each_activity(name="A",\
                          body_children=[copy_activity(name="Body")]),
        copy_activity(name="Next",\
                    depends_on=["A"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "Body" in get_parent_nodes("Next", edges)
    assert "A" not in get_parent_nodes("Next", edges)

def test_flatten_while_same_as_foreach():

    activities = [
        copy_activity(name="A"),
        while_activity(name="B",\
                       body_children=[copy_activity(name="Body")],\
                       depends_on=["A"]),
        copy_activity("Next",\
                      depends_on=["B"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "B" not in get_node_names(edges)
    assert "A" in get_parent_nodes("Body", edges)
    assert "Body" in get_parent_nodes("Next", edges)

def test_flatten_empty_foreach_removed():
    activities = [
        copy_activity(name="A"),
        for_each_activity(name="C",\
                          body_children=[],\
                          depends_on=["A"]),
        copy_activity(name="B",\
                      depends_on=["C"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "C" not in get_node_names(edges)
    assert "A" in get_parent_nodes("B", edges)

def test_flatten_multi_node_foreach_body():

    activities = [

        for_each_activity(name="A",\
                          body_children=[
                              copy_activity(name="Body1"),
                              copy_activity(name="Body2",\
                                            depends_on=["Body1"]),
        ]),
        copy_activity("Next", depends_on=["A"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    # node after the for each should have the last inner node of for each activity as parent

    assert "Body2" in get_parent_nodes("Next", edges)
    assert "Body1" not in get_parent_nodes("Next", edges)

def test_flatten_if_removes_if_node():

    activities = [if_activity(name="IF",\
                            true_branch=[copy_activity(name="TrueActivity")],\
                            false_branch=[copy_activity(name="FalseActivity")])]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "IF" not in get_node_names(edges)
    assert "TrueActivity" in get_node_names(edges)
    assert "FalseActivity" in get_node_names(edges)

def test_flatten_if_connect_prev_to_both_branches():

    activities = [
        copy_activity(name="A"),
        if_activity(name="IF",\
                    true_branch=[copy_activity("TrueActivity")],\
                    false_branch=[copy_activity("FalseActivity")],\
                    depends_on=["A"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "A" in get_parent_nodes("TrueActivity", edges)
    assert "A" in get_parent_nodes("FalseActivity", edges)

def test_flatten_if_both_branches_wire_to_next():

    activities = [
        if_activity(name="IF",\
                    true_branch=[copy_activity("TrueActivity")],\
                    false_branch=[copy_activity("FalseActivity")]),
        copy_activity(name="Next",\
                     depends_on=["IF"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "TrueActivity" in get_parent_nodes("Next", edges)
    assert "FalseActivity" in get_parent_nodes("Next", edges)



def test_flatten_if_multiple_node():

    # CopyA  -> If(true: CopyB, false: CopyC) -> CopyD
    activities = [
        copy_activity(name="CopyA"),
        if_activity(name="IF",
               true_branch=[copy_activity("CopyB")],
               false_branch=[copy_activity("CopyC")],
               depends_on=["CopyA"]),
        copy_activity("CopyD",\
                    depends_on=["IF"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "IF" not in get_node_names(edges)
    assert "CopyA" in get_parent_nodes("CopyB", edges)
    assert "CopyA" in get_parent_nodes("CopyC", edges)
    assert "CopyB" in get_parent_nodes("CopyD", edges)
    assert "CopyC" in get_parent_nodes("CopyD", edges)

def test_flatten_if_empty_false_branch():

    activities = [if_activity(name="IF",\
                              true_branch=[copy_activity(name="TrueActivity")],\
                              false_branch=[])]

    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "IF" not in get_node_names(edges)
    assert "TrueActivity"   in     get_node_names(edges)

def test_flatten_if_nested_inside_foreach():

    inner_activities = if_activity(name="InnerIF",\
                                   true_branch=[copy_activity(name="InnerTrue")],\
                                   false_branch=[copy_activity("InnerFalse")])

    activities  = [for_each_activity(name="FE",\
                                     body_children=[inner_activities])]
    
    edges = branch_to_edges(activities)
    edges = get_flatten_branches(activities, edges)

    assert "FE" not in get_node_names(edges)
    assert "InnerIF" not in get_node_names(edges)
    assert "InnerTrue" in get_node_names(edges)
    assert "InnerFalse" in get_node_names(edges)


def test_simplify_removes_unsupported():

    activities = [
        copy_activity(name="A"),
        unsupported_activity(name="Lookup", depends_on=["A"]),
        copy_activity(name="B", depends_on=["Lookup"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_simplify_graph(activities, edges)

    assert "Lookup" not in get_node_names(edges)

def test_simplify_connect_through_removed_node():

    activities = [
        copy_activity(name="A"),
        unsupported_activity(name="Lookup", depends_on=["A"]),
        copy_activity(name="B", depends_on=["Lookup"]),
    ]

    edges = branch_to_edges(activities)
    edges = get_simplify_graph(activities, edges)

    assert "A" in get_parent_nodes("B", edges)


def test_virtual_graph_linear():

    activities = [
        copy_activity(name="A"),
        unsupported_activity(name="Lookup", depends_on=["A"]),
        copy_activity("B", depends_on=["Lookup"]),
    ]

    edges = get_virtual_graph(activities)

    assert "Lookup" not in get_node_names(edges)
    assert "A" in get_parent_nodes("B", edges)

def test_virtual_graph_parallel():

    activities = [
        copy_activity(name="CopyA"),
        if_activity(name="IF",
               true_branch=[copy_activity(name="CopyB")],
               false_branch=[copy_activity(name="CopyC")],
               depends_on=["CopyA"]),
        copy_activity(name="CopyD", depends_on=["IF"]),
    ]

    edges = get_virtual_graph(activities)

    assert "IF" not in get_node_names(edges)
    assert "CopyA" in get_parent_nodes("CopyB", edges)
    assert "CopyA" in get_parent_nodes("CopyC", edges)
    assert "CopyB" in get_parent_nodes("CopyD", edges)
    assert "CopyC" in  get_parent_nodes("CopyD", edges)

def test_virtual_graph_all_unsupported_returns_empty():

    activities = [
        unsupported_activity("A"),
        unsupported_activity("B", depends_on=["A"])
    ]

    edges = get_virtual_graph(activities)
    assert len(edges) == 0

def test_virtual_graph_foreach_with_unsupported_body():

    activities = [
        copy_activity(name="A"),
        for_each_activity(name="FE",\
                          body_children=[
                              unsupported_activity(name="Lookup"),
                              copy_activity(name="Body", depends_on=["Lookup"]),
                              ], depends_on=["A"]),
        copy_activity(name="Next",\
                      depends_on=["FE"]),
    ]
    edges = get_virtual_graph(activities)
    assert "FE" not in get_node_names(edges)
    assert "Lookup" not in get_node_names(edges)
    assert "A" in get_parent_nodes("Body", edges)
    assert "Body" in get_parent_nodes("Next", edges)

def run_all_test():

    tests = [func for func_name,func in list(globals().items()) if func_name.startswith("test_")]
    
    passed = 0 

    failed = 0

    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
            passed += 1
        except Exception:
            print(f"  FAIL  {test.__name__}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")



if __name__ == "__main__":
    run_all_test()