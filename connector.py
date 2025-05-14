from typing import Dict,Any,Optional
from model import DatasetType,ProcessDataset,SingleTableDataset,QueryDataset,LocationDataset
from util import create_parameter
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