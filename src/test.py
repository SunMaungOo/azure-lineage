from model import Activity, ActivityType,Resolved,Unresolved
from graph import get_node_names,get_parent_nodes
from core import (
    branch_to_edges,
    get_flatten_branches,
    get_simplify_graph,
    get_activity_type,
    get_virtual_graph,
    resolve_expression,
    resolve_table_expression
)
from typing import List

# virtual-dom test

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

# expression resolver test


def test_constant_value_resolves():
    assert resolve_expression("hello", {}, {}) == Resolved("hello")

def test_dataset_whole_field_resolved():

    assert resolve_expression("@dataset().schemaName",
                   dataset_parameters={"schemaName": "dbo"},
                   pipeline_parameters={}) == Resolved("dbo")
    
def test_dataset_whole_field_missing_param():

    result = resolve_expression("@dataset().schemaName", {}, {})

    assert isinstance(result, Unresolved)
    assert "schemaName" in result.reason

def test_dataset_whole_field_not_matched_by_pipeline():

    result = resolve_expression("@dataset().tableName",
                     dataset_parameters={"tableName": "foo"},
                     pipeline_parameters={"tableName": "bar"})
    
    assert result == Resolved("foo")



def test_pipeline_whole_field_resolved():

    assert resolve_expression("@pipeline().parameters.env",
                   dataset_parameters={},
                   pipeline_parameters={"env": "prod"}) == Resolved("prod")
    

def test_pipeline_whole_field_missing_param():

    result = resolve_expression("@pipeline().parameters.env", {}, {})
    assert isinstance(result, Unresolved)

def test_pipeline_whole_field_not_matched_by_dataset():

    result = resolve_expression("@pipeline().parameters.schema",
                     dataset_parameters={"schema": "wrong"},
                     pipeline_parameters={"schema": "correct"})
    
    assert result == Resolved("correct")


# test interpolated expression

def test_dataset_interpolated_resolved():

    assert resolve_expression("@{dataset().schemaName}",
                   dataset_parameters={"schemaName": "foo"},
                   pipeline_parameters={}) == Resolved("foo")
    

def test_dataset_interpolated_missing_param():

    result = resolve_expression("@{dataset().schemaName}", {}, {})

    assert isinstance(result, Unresolved)
    assert "schemaName" in result.reason

def test_dataset_interpolated_with_prefix_suffix():

    result = resolve_expression("prefix_@{dataset().tableName}_suffix",
                     dataset_parameters={"tableName": "middle"},
                     pipeline_parameters={})
    
    assert result == Resolved("prefix_middle_suffix")


def test_pipeline_interpolated_resolved():

    assert resolve_expression("@{pipeline().parameters.env}",
                   dataset_parameters={},
                   pipeline_parameters={"env": "prod"}) == Resolved("prod")
    

def test_pipeline_interpolated_with_surrounding_text():

    result = resolve_expression(
        "First Name: @{pipeline().parameters.firstName} Last Name: @{pipeline().parameters.lastName}",
        dataset_parameters={},
        pipeline_parameters={"firstName": "Mr", "lastName": "Smith"}
    )

    assert result == Resolved("First Name: Mr Last Name: Smith")

def test_pipeline_interpolated_missing_param():

    result = resolve_expression("@{pipeline().parameters.env}", {}, {})
    assert isinstance(result, Unresolved)
    assert "env" in result.reason

def test_two_dataset_tokens_in_one_string():
    result = resolve_expression(
        "@{dataset().schema}.@{dataset().table}",
        dataset_parameters={"schema": "dbo", "table": "foo"},
        pipeline_parameters={}
    )
    assert result == Resolved("dbo.foo")

def test_first_token_unresolved():

    result = resolve_expression(
        "@{dataset().schema}.@{dataset().table}",
        dataset_parameters={"table": "orders"}, 
        pipeline_parameters={}
    )

    assert isinstance(result, Unresolved)



def test_linked_service_whole_field_unresolved():

    result = resolve_expression("@linkedService().connectionString", {}, {})

    assert isinstance(result, Unresolved)


def test_linked_service_interpolated_unresolved():

    result = resolve_expression("@{linkedService().connectionString}", {}, {})

    assert isinstance(result, Unresolved)

def test_linked_service_named_argument_unresolved():

    result = resolve_expression("@{linkedService('MyAzureSql').connectionString}", {}, {})

    assert isinstance(result, Unresolved)

def test_multiple_linked_service_tokens_unresolved():

    result = resolve_expression(
        "@{linkedService().param}@{linkedService().connectionString}",
        {}, {}
    )

    assert isinstance(result, Unresolved)

def test_linked_service_mixed_with_static_unresolved():

    result = resolve_expression("Server=@{linkedService('Sql').host};Database=mydb", {}, {})

    assert isinstance(result, Unresolved)

def test_linked_service_params_present_still_unresolved():

    result = resolve_expression("@{linkedService().host}",
                     dataset_parameters={"host": "myserver"},
                     pipeline_parameters={"host": "myserver"})
    
    assert isinstance(result, Unresolved)


def test_table_expression_both_static():

    assert resolve_table_expression("dbo", "orders", {}, {}) == "dbo.orders"

def test_table_expression_schema_from_dataset_whole():

    assert resolve_table_expression(
        "@dataset().schema", "orders",
        dataset_parameters={"schema": "sales"}, pipeline_parameters={}
    ) == "sales.orders"


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