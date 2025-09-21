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
import re

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

        info = get_mssql_processed_linked_service(mssql_linked_service_resource=linked_service_resource,\
                                                  linked_service_parameter_value=linked_service_parameter_value)

        
    elif linked_service_type == LinkedServiceType.Oracle:

        info = get_oracle_processed_linked_service(oracle_linked_service_resource=linked_service_resource,\
                                         linked_service_parameter_value=linked_service_parameter_value)
        

    elif linked_service_type == LinkedServiceType.Blob:

        if azure_data_type == "AzureBlobStorage":

            connection_properties = None

            if "connection_string" in linked_service_resource.properties:
                connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.connection_string)
            elif "connectionString" in linked_service_resource.properties.typeProperties:
                connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.typeProperties.connectionString)

            

            info = BlobLinkedService(
                url=Parameter(value=connection_properties["accountname"],\
                              parameter_type=ParameterType.Static)
            )

        elif azure_data_type == "AzureBlobFS":

            url_connection_str = None

            if "url" in linked_service_resource.properties:
                url_connection_str = linked_service_resource.properties.url

            elif "url" in linked_service_resource.properties.typeProperties:
                url_connection_str = linked_service_resource.properties.typeProperties.url

            host = url_connection_str.split("//")[1].replace("/","")

            info = BlobLinkedService(
                url=Parameter(value=host,\
                              parameter_type=ParameterType.Static)
            )

    return info

def get_mssql_processed_linked_service(mssql_linked_service_resource:APILinkedServiceResource,\
                                       linked_service_parameter_value:str)->Optional[ProcessLinkedService]:
    connection_string = None

    # for old version of sql server linked service

    if "connection_string" in mssql_linked_service_resource.properties:
        connection_string = mssql_linked_service_resource.properties.connection_string
    else:
        # for new version of sql server linked service
        connection_string = mssql_linked_service_resource.properties.typeProperties.connectionString

    #if it is a dict type , it mean the linked service connection string is in azure key-vault (we are going to ignore it)
        
    if not isinstance(connection_string,str):
        return None
    
    connection_properties = get_connection_properties(connection_str=connection_string)

    host = connection_properties["datasource"]

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

    database = connection_properties["initialcatalog"]

    database_parameter_type = ParameterType.Static

    if linked_service_parameter_value in database:
        database_parameter_type = ParameterType.Expression

    database = Parameter(
        value=database,\
        parameter_type=database_parameter_type
    )

    return DatabaseLinkedService(
        host=host,\
        database=database
    )



def get_oracle_processed_linked_service(oracle_linked_service_resource:APILinkedServiceResource,\
                              linked_service_parameter_value:str)->Optional[ProcessLinkedService]:
    
    connection_string = None

    connection_properties = None

    # for old version of oracle linked service

    if "connection_string" in oracle_linked_service_resource.properties:
        connection_string = oracle_linked_service_resource.properties.connection_string
    elif "connectionString" in oracle_linked_service_resource.properties.typeProperties:
        # for new version of oracle server linked service
        connection_string = oracle_linked_service_resource.properties.typeProperties.connectionString
    elif "server" in oracle_linked_service_resource.properties.typeProperties:

        server_info:str = oracle_linked_service_resource.properties.typeProperties.server

        # oracle TNS (Transparent Network Substrate) descriptor string 

        if "(DESCRIPTION=" in server_info:
            
            connection_properties = dict()

            host_match = re.search(r'\(HOST=([^)]+)\)', server_info, re.IGNORECASE)

            if host_match:
                connection_properties["host"]=host_match.group(1)

            sid_match = re.search(r'\(SID=([^)]+)\)', server_info, re.IGNORECASE)

            if sid_match:
                connection_properties["sid"]=sid_match.group(1)
            
            service_name_match = re.search(r'\(SERVICE_NAME=([^)]+)\)', server_info, re.IGNORECASE)

            if service_name_match:
                connection_properties["servicename"]=service_name_match.group(1)

        else:
            # easy connect format. Example host:port/serviceName 
            connection_properties = dict()
            connection_properties["host"] = server_info.split(":")[0]
            connection_properties["servicename"] = server_info.split("/")[1]

     #if it is a dict type , it mean the linked service connection string is in azure key-vault (we are going to ignore it)
        
    if connection_properties is None and \
        not isinstance(connection_string,str):
        return None
    
    # if it not have been handled

    if connection_properties is None:
        connection_properties = get_connection_properties(connection_str=connection_string)

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

    elif "servicename" in connection_properties:
        database = connection_properties["servicename"]

    database_parameter_type = ParameterType.Static
    
    if linked_service_parameter_value in database:
        database_parameter_type = ParameterType.Expression

    database = Parameter(
        value=database,\
        parameter_type=database_parameter_type
    )

    return DatabaseLinkedService(
            host=host,\
            database=database
        )
    