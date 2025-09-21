from client import AzureClient
from decouple import config

AZURE_CLIENT_ID = config("AZURE_CLIENT_ID",cast=str)

AZURE_TENANT_ID = config("AZURE_TENANT_ID",cast=str)

AZURE_CLIENT_SECRET = config("AZURE_CLIENT_SECRET",cast=str)

SUBSCRIPTION_ID = config("SUBSCRIPTION_ID",cast=str)

RESOURCE_GROUP_NAME = config("RESOURCE_GROUP_NAME",cast=str)

DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME = config("DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME",cast=str)

IS_AZURE_DATA_FACTORY = config("IS_AZURE_DATA_FACTORY",default=True,cast=bool)

DAYS_SEARCH = config("DAYS_SEARCH",default=1,cast=int)

OPENLINEAGE_NAMESPACE = config("OPENLINEAGE_NAMESPACE",default="my-namespace",cast=str)

OPENLINEAGE_OUTPUT_FILE_PATH = config("OPENLINEAGE_OUTPUT_FILE_PATH",default="openlineage.json",cast=str)

OPENLINEAGE_PRODUCER = config("OPENLINEAGE_PRODUCER",default="azure-lineage",cast=str)

IS_USE_FQN  = config("IS_USE_FQN",default=True,cast=bool)

LINEAGE_OUTPUT_FILE_PATH = config("LINEAGE_OUTPUT_FILE_PATH",default="lineage.json",cast=str)

def get_api_client()->AzureClient:
    return AzureClient(azure_client_id=AZURE_CLIENT_ID,\
        azure_tenant_id=AZURE_TENANT_ID,\
        azure_client_secret=AZURE_CLIENT_SECRET,\
        subscription_id=SUBSCRIPTION_ID,\
        resource_group_name=RESOURCE_GROUP_NAME,\
        data_factory_or_workspace=DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME,\
        is_data_factory=IS_AZURE_DATA_FACTORY)