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
from util import create_parameter,get_connection_properties,has_field
from azure.mgmt.datafactory.models import DatasetResource
import re

def get_mongodb_host(mongodb_connection_string:str)->Optional[str]:
    """
    Return the first host of mongodb connecting string
    """

    # remove mongodb , mongodb+srv:// url part

    stripped = re.sub(r"^mongodb(\+srv)?://", "", mongodb_connection_string)

    # if it have user name and password

    if "@" in stripped:

        # remove user name and password

        stripped = stripped.split("@",1)[1]

    hosts = stripped.split(",")

    if len(hosts)==0:
        return None
    
    first_host = hosts[0]

    # remove the port if it exist

    return first_host.split(":")[0]

def get_dataset_type(azure_dataset_type:str)->DatasetType:

    mapping:Dict[str,DatasetType] = {
        "OracleTable":DatasetType.Oracle,
        "AzureSqlTable":DatasetType.AzureSQL,
        "AzureSqlDWTable":DatasetType.Synapse,
        "SqlServerTable":DatasetType.OnPrimeMSSQL,
        "OracleSource":DatasetType.Oracle,
        "Parquet":DatasetType.Blob,
        "DelimitedText":DatasetType.Blob,
        "Json":DatasetType.Blob,
        "Parquet":DatasetType.Blob,
        "Excel":DatasetType.Blob,
        "SqlPoolReference":DatasetType.Synapse,
        "MongoDbV2Collection":DatasetType.MongoDB,
        "MongoDbAtlasCollection":DatasetType.MongoDB
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
        "AzureBlobFS":LinkedServiceType.Blob,
        "MongoDbV2":LinkedServiceType.MongoDB,
        "MongoDbAtlas":LinkedServiceType.MongoDB
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

def is_synapse_sql_pool(dataset_resource:DatasetResource,\
                dataset_type:DatasetType)->bool:
    
    if dataset_type!=DatasetType.Synapse:
        return False
    
    if not has_field(dataset_resource.properties,"additional_properties"):
        return False
    
    if not has_field(dataset_resource.properties.additional_properties,"sqlPool"):
        return False
    
    if not has_field(dataset_resource.properties.additional_properties,"typeProperties"):
        return False

    return True

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

        if is_synapse_sql_pool(dataset_resource=dataset_resource,\
                               dataset_type=dataset_type):
                
            if "schema" in dataset_resource.properties.additional_properties["typeProperties"]:      
                schema = create_parameter(parameter_value=dataset_resource.properties.additional_properties["typeProperties"]["schema"])

            if "table" in dataset_resource.properties.additional_properties["typeProperties"]:
                table = create_parameter(parameter_value=dataset_resource.properties.additional_properties["typeProperties"]["table"])

        else:

            if dataset_resource.properties.schema_type_properties_schema is not None:
                schema = create_parameter(parameter_value=dataset_resource.properties.schema_type_properties_schema)

            if dataset_resource.properties.table is not None:
                table = create_parameter(parameter_value=dataset_resource.properties.table)

        reference_name = None

        if is_synapse_sql_pool(dataset_resource=dataset_resource,\
                               dataset_type=dataset_type):
            reference_name = dataset_resource.properties.additional_properties["sqlPool"]["referenceName"]

        # the user may set the empty schema and empty table field but will set the query field in the activity 
        # in those case , we must set the dataset to be QueryDataset instead of SingleTableDataset to get lineage
         
        is_query_dataset =  (schema is None and table is None) or\
        (len(schema.value.strip())==0 and len(table.value.strip())==0)

        if is_query_dataset:

            info = QueryDataset(name=dataset_name,\
                                type=dataset_type,\
                                reference_name=reference_name)
        else:
           
           info = SingleTableDataset(name=dataset_name,\
                                     type=dataset_type,\
                                     schema=schema,\
                                     table=table,\
                                     reference_name=reference_name)
           
    elif dataset_type==DatasetType.Blob:

        location = dataset_resource.properties.location

        container = None

        folder_path = None

        file_nane = None

        if has_field(location,"container") and\
            location.container is not None:
            container = create_parameter(parameter_value=location.container)

        if has_field(location,"folder_path") and\
            location.folder_path is not None:
            folder_path = create_parameter(parameter_value=location.folder_path)

        if has_field(location,"file_name") and\
            location.file_name is not None:
            file_nane = create_parameter(parameter_value=location.file_name)

        info = LocationDataset(
            name=dataset_name,\
            type=dataset_type,\
            container=container,\
            folder_path=folder_path,\
            file_name=file_nane
        )

    elif dataset_type==DatasetType.MongoDB:

        collection = None

        if has_field(dataset_resource.properties,"collection") and\
        dataset_resource.properties.collection is not None:
            collection = create_parameter(dataset_resource.properties.collection)

        info = SingleTableDataset(
            name=dataset_name,
            type=DatasetType.MongoDB,\
            schema=None,\
            table=collection,\
            reference_name=None
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

            if has_field(linked_service_resource.properties,"connection_string"):

                connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.connection_string)
            elif has_field(linked_service_resource.properties.typeProperties,"connectionString"):
                
                connection_properties = get_connection_properties(connection_str=linked_service_resource.properties.typeProperties.connectionString)

            

            info = BlobLinkedService(
                url=Parameter(value=connection_properties["accountname"],\
                              parameter_type=ParameterType.Static)
            )

        elif azure_data_type == "AzureBlobFS":

            url_connection_str = None

            if has_field(linked_service_resource.properties,"url"):
                url_connection_str = linked_service_resource.properties.url

            elif has_field(linked_service_resource.properties.typeProperties,"url"):
                url_connection_str = linked_service_resource.properties.typeProperties.url

            host = url_connection_str.split("//")[1].replace("/","")

            info = BlobLinkedService(
                url=Parameter(value=host,\
                              parameter_type=ParameterType.Static)
            )

    elif linked_service_type==LinkedServiceType.MongoDB:

        info = get_mongodb_processed_linked_service(mongodb_linked_service_resource=linked_service_resource,\
                                                    linked_service_parameter_value=linked_service_parameter_value)

    return info

def get_mssql_processed_linked_service(mssql_linked_service_resource:APILinkedServiceResource,\
                                       linked_service_parameter_value:str)->Optional[ProcessLinkedService]:
    connection_string:str = None

    # for old version of sql server linked service

    if has_field(mssql_linked_service_resource.properties,"connection_string"):
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
        
    connection_string:str = None

    connection_properties:Dict[str,str] = None

    is_processed = False

    if not is_processed and\
        has_field(oracle_linked_service_resource.properties,"server"):

        server_info:str = oracle_linked_service_resource.properties.server

        if server_info is not None:
        
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

            is_processed = True


    if not is_processed and \
        has_field(oracle_linked_service_resource.properties,"typeProperties") and \
        has_field(oracle_linked_service_resource.properties.typeProperties,"connectionString"):
        # for new version of oracle server linked service
        connection_string = oracle_linked_service_resource.properties.typeProperties.connectionString
        
        is_processed = connection_string is not None
        
    # for old version of oracle linked service
    if not is_processed and \
        has_field(oracle_linked_service_resource.properties,"connection_string"):
        connection_string = oracle_linked_service_resource.properties.connection_string

        is_processed = connection_string is not None

     #if connection_string it is a dict type , it mean the linked service connection string is in azure key-vault (we are going to ignore it)

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

def get_mongodb_processed_linked_service(mongodb_linked_service_resource:APILinkedServiceResource,\
                                       linked_service_parameter_value:str)->Optional[ProcessLinkedService]:
    
    host = None

    database = None

    connection_properties = mongodb_linked_service_resource.properties

    connection_string = None

    host_str = None

    if has_field(connection_properties,"connection_string") and\
    connection_properties.connection_string is not None:
        
        connection_string = connection_properties.connection_string

    # add support for MongoDbV2

    elif has_field(connection_properties,"typeProperties"):

        type_properties = connection_properties.typeProperties

        if has_field(type_properties,"connectionString") and\
        type_properties.connectionString is not None:
            
            connection_string =  type_properties.connectionString
        
    #if it is a dict type , it mean the linked service connection string is in azure key-vault (we are going to ignore it)

    if connection_string is None or\
        isinstance(connection_string,dict):
        return None
    
    host_str = get_mongodb_host(connection_string)

    if host_str is None:
        return None

    parameter_type = ParameterType.Static

    if linked_service_parameter_value in host_str:
        parameter_type = ParameterType.Expression

    host = Parameter(
        value=host,\
        parameter_type=parameter_type
    )
        
    if has_field(connection_properties,"database") and\
        connection_properties.database is not None:
        
        database = create_parameter(
            parameter_value= connection_properties.database
        )
        
    return DatabaseLinkedService(
        host=host,
        database=database
    )