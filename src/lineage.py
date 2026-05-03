from sqlglot.expressions import Expression
from sqlglot import (
    parse_one,
    exp
)
from typing import (
    Set,
    List,
    Optional,
    Dict,
    Tuple
)
from core import (
    resolve_dataset_parameter,
    resolve_table_expression,
    resolve_blob_expression,
    normalize_blob_path
)
from search import find_linked_service
from logging import Logger
from model import (
    GenericActivity,
    PipelineRuntimeContext,
    LinkedService,
    SingleTableDataset,
    QueryDataset,
    LocationDataset,
    LineageActivityInfo,
    ActivityType,
    StaticPipeline
)
from connector import(
    get_linked_service_host_prefix,
    get_sql_script,
    get_sql_pool_host_prefix
)
from graph import Edge
from util import (
    has_field,
    add_lineage
)
from pluginhelper import (
    LineagePluginWrapper,
    get_procedure_context,
    get_script_context,
    get_sql_pool_database_connection,
    resolve_activity_plugins,
    get_database_connection
)
from plugin import ActivityLineageContext

def clean_sql(sql:str)->str:
    return sql.replace("[","").replace("]","")

def get_sql_lineage(sql:str,dialect:str=None)->Set[str]:
    """
    Get the set of base table (excluding CTE)
    """

    ast = None

    if dialect is None:
        ast = parse_one(sql=sql)
    else:
        ast = parse_one(sql=sql,dialect=dialect)
    

    return find_base_tables(ast)

def find_base_tables(ast:Expression)->Set[str]:
    
    ctes:Set[str] = set()

    tables:Set[str] = set()

    for cte in ast.find_all(exp.CTE):
        ctes.add(cte.alias_or_name)

    for table in ast.find_all(exp.Table):
        #ignore if the table is the cte
        if table.name not in ctes:
            tables.add(internal_get_table_name(table))

    return tables

def internal_add_surfix(text:str,surfix:str,false_value:str)->str:
    if text is None:
        return false_value
    
    return text+surfix

def internal_get_table_name(table_expression:Expression)->str:
    """
    From the table_expression , try to get the table name in 
    database.schema.table format
    """

    database_name = None

    schema_name = None

    table_name = table_expression.name

    if "db" in table_expression.args.keys() and table_expression.args["db"] is not None:
        schema_name = table_expression.args["db"].name

    if "catalog" in table_expression.args.keys() and table_expression.args["catalog"] is not None:
        database_name = table_expression.args["catalog"].name

    return internal_add_surfix(database_name,".","") + internal_add_surfix(schema_name,".","") + table_name

def resolve_source_table(activity:GenericActivity,\
                         runtime:PipelineRuntimeContext,\
                         linked_services:List[LinkedService],\
                         is_use_fqn:bool,\
                         synapse_workspace_name:Optional[str],\
                         logger:Optional[Logger])->Set[str]:
    
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

        schema_value = None
        
        if input_dataset_info.schema is not None:
            schema_value = input_dataset_info.schema.value


        table_reference = resolve_table_expression(schema_expression=schema_value,\
                                                   table_expression=input_dataset_info.table.value,\
                                                   dataset_parameters=dataset_parameters,
                                                   pipeline_parameters=runtime.pipeline_parameters)

        if table_reference is None:

            if logger is not None:

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

            if logger is not None:

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

            if logger is not None:

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

            if logger is not None:

                logger.warning("Source blob resolution failed",
                            extra={
                                    "event":"source_blob_resolution_failed",
                                    **log_context
                                })
        else:
            source_tables.add(normalize_blob_path(raw_blob_path=blob_location))

    if is_use_fqn and len(source_tables)>0:

        is_sql_pool = input_dataset.linked_service_name is None\
        and isinstance(input_dataset_info,QueryDataset)\
        and synapse_workspace_name is not None

        source_host_prefix = None

        if is_sql_pool:

            sql_pool_name = input_dataset_info.reference_name

            source_host_prefix = get_sql_pool_host_prefix(synapse_workspace_name=synapse_workspace_name,\
                                     sql_pool_name=sql_pool_name)
            
        else:

            if input_dataset.linked_service_name is not None:

                linked_service = find_linked_service(linked_services=linked_services,\
                                                    search_linked_service_name=input_dataset.linked_service_name)

                if linked_service is not None:
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
                         is_use_fqn:bool,\
                         synapse_workspace_name:Optional[str],\
                         logger:Optional[Logger])->Optional[str]:

    
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
             
             if logger is not None:
                 
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

            if logger is not None:

                logger.warning("Target blob resolution failed",
                            extra={
                                    "event":"target_blob_resolution_failed",
                                    **log_context
                                })
            
        else:
            target_table = normalize_blob_path(raw_blob_path=blob_location)

    if is_use_fqn and target_table is not None:

        is_sql_pool = output_dataset.linked_service_name is None\
        and isinstance(output_dataset_info,SingleTableDataset) and\
        synapse_workspace_name is not None

        target_host_prefix = None

        if is_sql_pool:

            sql_pool_name = output_dataset_info.reference_name

            target_host_prefix = get_sql_pool_host_prefix(synapse_workspace_name=synapse_workspace_name,\
                                     sql_pool_name=sql_pool_name)
        
        else:

            if output_dataset.linked_service_name is not None:

                linked_service = find_linked_service(linked_services=linked_services,\
                                                    search_linked_service_name=output_dataset.linked_service_name)
                           
                if linked_service is not None:
                    target_host_prefix = get_linked_service_host_prefix(linked_service=linked_service)

        if target_host_prefix is not None:

            target_table = add_source_host_prefix(table_value=target_table,\
                                                  host_prefix=target_host_prefix)

                            
    return target_table

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


def get_pipeline_table_lineage(static_pipeline:StaticPipeline,\
                                runtime_context:PipelineRuntimeContext,\
                                linked_services:List[LinkedService],\
                                is_use_fqn:bool,\
                                plugins:List[LineagePluginWrapper],\
                                synapse_workspace_name:Optional[str],\
                                logger:Optional[Logger])->Tuple[List[ActivityLineageContext],Set[LineageActivityInfo]]:
    """
    Return lineage of each activity in the pipeline , and activity it have skip
    """
    result:List[ActivityLineageContext] = list()

    # state of whether we can get lineage from activity

    lineage_activities:Set[LineageActivityInfo] = set()

    for edge in static_pipeline.virtual_graph:

        generic_activity = static_pipeline.activities[edge.node_name]

        lineage:List[Edge] = list()

        is_skippped = False

        is_sql_pool = False

        if generic_activity.activity_type==ActivityType.Execute:
            continue

        elif generic_activity.activity_type in [ActivityType.Procedure,ActivityType.Script]:

            plugin_context = None

            if generic_activity.activity_type==ActivityType.Procedure:
                plugin_context,is_sql_pool = get_procedure_context(procedure_activity=generic_activity.raw_activity,\
                                                runtime_context=runtime_context,\
                                                logger=logger)
                            
                
            elif generic_activity.activity_type==ActivityType.Script:
                plugin_context = get_script_context(script_activity=generic_activity.raw_activity,\
                                                    runtime_context=runtime_context,\
                                                    logger=logger)

            # if we cannot generate the context for the plugin , it is a skip activities
            if plugin_context is None:                
                
                is_skippped = True
            
            if plugin_context is not None and \
                len(plugins)>0:
                
                linked_service_name = plugin_context.linked_service_name

                linked_service = None

                database_conection = None

                if linked_service_name is not None:
                    linked_service = find_linked_service(linked_services=linked_services,
                                        search_linked_service_name=linked_service_name)

                if linked_service is not None and\
                    not is_sql_pool:   

                    pipeline_parameters:Dict[str,str] = dict()

                    linked_service_parameters:Dict[str,str] = dict()

                    if has_field(plugin_context,"pipeline_parameters") and\
                        isinstance(plugin_context.pipeline_parameters,dict):

                        pipeline_parameters = plugin_context.pipeline_parameters

                    if has_field(plugin_context,"linked_service_parameters") and\
                        isinstance(plugin_context.linked_service_parameters,dict):

                        linked_service_parameters = plugin_context.linked_service_parameters     

                    database_conection = get_database_connection(linked_service=linked_service,\
                                                                pipeline_parameters=pipeline_parameters,\
                                                                linked_service_parameters=linked_service_parameters)
                                       
                if is_sql_pool: 
                    database_conection = get_sql_pool_database_connection(linked_service_name=linked_service_name,\
                                                                          synapse_workspace_name=synapse_workspace_name)

                plugin_lineages = resolve_activity_plugins(plugins=plugins,\
                                context=plugin_context,\
                                connection=database_conection)
        
                # if there is an exception in plugin function or the handler cannot provide lineage , it is a skip activity
                
                if plugin_lineages is None or \
                    len(plugin_lineages)==0:
                    
                    is_skippped = True

                if not is_skippped:

                    for plugin_lineage in plugin_lineages:

                        source_tables = plugin_lineage[0]

                        target_table = plugin_lineage[1]

                        lineage = add_lineage(initial_lineage=lineage,\
                                          sources=source_tables,\
                                          target=target_table)
                        
                    result.append(ActivityLineageContext(
                        pipeline_name=runtime_context.pipeline_name,\
                        pipeline_run_id=runtime_context.run_id,\
                        pipeline_run_status=runtime_context.pipeline_run_status,\
                        pipeline_run_start=runtime_context.run_start,\
                        pipeline_run_end=runtime_context.run_end,\
                        activity_name=generic_activity.name,\
                        activity_type=generic_activity.activity_type,\
                        lineage=lineage
                    ))
                        
            lineage_activities.add(LineageActivityInfo(
                pipeline_name=static_pipeline.pipeline_name,\
                activity_name=generic_activity.name,\
                activity_type=generic_activity.activity_type,\
                is_skipped=is_skippped
                )
            )

        
        elif generic_activity.activity_type==ActivityType.Copy:

            # will only support lineage if we support both input and output

            if not (generic_activity.is_input_supported and generic_activity.is_output_supported):

                if logger is not None:
                    logger.warning("unsupported activity.skipping",extra={
                        "event":"unsupported_activity",
                        "pipeline":runtime_context.pipeline_name,
                        "activity":generic_activity.name
                    })
                
                continue

            # by common sense , we should only support single table dataset and location dataset as sink

            if not isinstance(generic_activity.output_dataset.info,(SingleTableDataset,LocationDataset)):
                continue
            
            source_tables = resolve_source_table(activity=generic_activity,\
                                                runtime=runtime_context,\
                                                linked_services=linked_services,\
                                                is_use_fqn=is_use_fqn,\
                                                synapse_workspace_name=synapse_workspace_name,\
                                                logger=logger)
            
            target_table = resolve_target_table(activity=generic_activity,\
                                                runtime=runtime_context,\
                                                linked_services=linked_services,\
                                                is_use_fqn=is_use_fqn,\
                                                synapse_workspace_name=synapse_workspace_name,\
                                                logger=logger)
        
            # we cannot add lineage when we cannot parse the sink 

            if target_table is None:

                is_skippped = True
                                        
            
            if not is_skippped:

                lineage = add_lineage(initial_lineage=lineage,\
                                      sources=source_tables,\
                                        target=target_table)
                            
                result.append(ActivityLineageContext(
                    pipeline_name=runtime_context.pipeline_name,\
                    pipeline_run_id=runtime_context.run_id,\
                    pipeline_run_status=runtime_context.pipeline_run_status,\
                    pipeline_run_start=runtime_context.run_start,\
                    pipeline_run_end=runtime_context.run_end,\
                    activity_name=generic_activity.name,\
                    activity_type=generic_activity.activity_type,\
                    lineage=lineage
                ))

            lineage_activities.add(LineageActivityInfo(
                    pipeline_name=static_pipeline.pipeline_name,\
                    activity_name=generic_activity.name,\
                    activity_type=generic_activity.activity_type,\
                    is_skipped=is_skippped
                )
            )
            
    return result,lineage_activities