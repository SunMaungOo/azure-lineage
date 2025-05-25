from typing import Dict,Any,Optional
from model import (
    DatasetType,
    ProcessDataset,
    SingleTableDataset,
    QueryDataset,
    LocationDataset,
    LinkedServiceType,
    APILinkedServiceResource,
    ProcessLinkedService,
    DatabaseLinkedService,
    BlobLinkedService,
    Parameter,
    ParameterType,
    LinkedService
)
from util import create_parameter,get_connection_properties
from azure.mgmt.datafactory.models import DatasetResource

def get_dataset_type(azure_dataset_type:str)->DatasetType:

    mapping:Dict[str,DatasetType] = {
        "OracleTable":DatasetType.Oracle,
        "AzureSqlTable":DatasetType.AzureSQL,
        "AzureSqlDWTable":DatasetType.Synapse,
        "SqlServerTable":DatasetType.OnPrimeMSSQL,
        "OracleSource":DatasetType.Oracle,
        "Parquet":DatasetType.Blob
    }

    if azure_dataset_type in mapping:
        return mapping[azure_dataset_type]
    
    return DatasetType.Unsupported

def get_linked_service_type(azure_linked_service_type:str)->LinkedServiceType:

    mapping:Dict[str,DatasetType] = {
        "Oracle":LinkedServiceType.Oracle,
        "AzureSqlDatabase":LinkedServiceType.AzureSQL,
        "AzureSqlDW":LinkedServiceType.Synapse,
        "SqlServer":LinkedServiceType.OnPrimeMSSQL,
        "AzureBlobStorage":LinkedServiceType.Blob,
        "AzureBlobFS":LinkedServiceType.Blob
    }

    if azure_linked_service_type in mapping:
        return mapping[azure_linked_service_type]
    
    return LinkedServiceType.Unsupported


def get_sql_script(input_source_obj:Dict[str,Any],dataset_type:DatasetType)->Optional[str]:
    """
    Get the sql script for the input dataset
    """
    if dataset_type==DatasetType.Oracle:
        return input_source_obj["oracleReaderQuery"]
    elif dataset_type==DatasetType.AzureSQL or\
        dataset_type==DatasetType.Synapse or\
        dataset_type==DatasetType.OnPrimeMSSQL:
        return input_source_obj["sqlReaderQuery"]
    
    return None

def get_dataset_info(dataset_resource:DatasetResource,\
                     dataset_name:str,\
                     dataset_type:DatasetType)->Optional[ProcessDataset]:
    
    info = None

    if dataset_type==DatasetType.Oracle \
        or dataset_type==DatasetType.AzureSQL\
        or dataset_type==DatasetType.Synapse\
        or dataset_type==DatasetType.OnPrimeMSSQL:

        schema = None

        table = None

        if dataset_resource.properties.schema_type_properties_schema is not None:
            schema = create_parameter(parameter_value=dataset_resource.properties.schema_type_properties_schema)

        if dataset_resource.properties.table is not None:
            table = create_parameter(parameter_value=dataset_resource.properties.table)

        if schema is None and table is None:
            info = QueryDataset(name=dataset_name,\
                                type=dataset_type)
        else:
           info = SingleTableDataset(name=dataset_name,\
                                     type=dataset_type,\
                                     schema=schema,\
                                     table=table)
           
    elif dataset_type==DatasetType.Blob:
        #use the dataset name as blob location

        info = LocationDataset(
            name=dataset_name,\
            type=dataset_type,\
            location=dataset_name
        )


    return info

def get_linked_service_info(linked_service_resource:APILinkedServiceResource)->Optional[ProcessLinkedService]:

    linked_service_parameter_value = "@{linkedService()"

    info = None

    azure_data_type = linked_service_resource.azure_data_type

    linked_service_type = get_linked_service_type(azure_linked_service_type=azure_data_type)


    if linked_service_type in [LinkedServiceType.AzureSQL,
        LinkedServiceType.Synapse,
        LinkedServiceType.OnPrimeMSSQL]:

        connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.connection_string)

        host = connection_properties["DataSource"]

        host_parameter_type = ParameterType.Static

        # to handle tcp:host,port format

        if "tcp:" in host:
            host = host.replace("tcp:","").split(",")[0]


        if linked_service_parameter_value in host:
            host_parameter_type = ParameterType.Expression

        host = Parameter(
            value=host,\
            parameter_type=host_parameter_type
        )

        database = connection_properties["InitialCatalog"]

        database_parameter_type = ParameterType.Static

        if linked_service_parameter_value in database:
            database_parameter_type = ParameterType.Expression

        database = Parameter(
            value=database,\
            parameter_type=database_parameter_type
        )

        info = DatabaseLinkedService(
            host=host,\
            database=database
        )


    elif linked_service_type == LinkedServiceType.Oracle:

        connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.connection_string)

        host = connection_properties["host"]

        host_parameter_type = ParameterType.Static

        if linked_service_parameter_value in host:
            host_parameter_type = ParameterType.Expression

        host = Parameter(
            value=host,\
            parameter_type=host_parameter_type
        )

        if "sid" in connection_properties:
            database = connection_properties["sid"]

        elif "serviceName" in connection_properties:
            database = connection_properties["serviceName"]

        database_parameter_type = ParameterType.Static

        if linked_service_parameter_value in database:
            database_parameter_type = ParameterType.Expression

        database = Parameter(
            value=database,\
            parameter_type=database_parameter_type
        )

        info = DatabaseLinkedService(
            host=host,\
            database=database
        )


    elif linked_service_type == LinkedServiceType.Blob:

        if azure_data_type == "AzureBlobStorage":
            
            connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.connection_string)

            info = BlobLinkedService(
                url=Parameter(value=connection_properties["AccountName"],\
                              parameter_type=ParameterType.Static)
            )

        elif azure_data_type == "AzureBlobFS":
             
            url_connection_str:str = linked_service_resource.properties.url

            host = url_connection_str.split("//")[1].replace("/","")

            info = BlobLinkedService(
                url=Parameter(value=host,\
                              parameter_type=ParameterType.Static)
            )

    return info
