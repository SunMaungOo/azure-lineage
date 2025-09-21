# Overview

I could not find any tool that creates data lineage from **Azure Data Factory** and **Azure Synapse**, so I decided to build one.  

This tool reads **pipeline execution logs** and **pipeline definition files**, then generates a lineage file that describes the flow of data across your datasets and activities.



## Supported Datasets

The following sources and sinks are currently supported:

| Dataset                  | Source | Sink |
|---------------------------|:------:|:----:|
| Azure SQL Database        |  Yes   | Yes  |
| Oracle Database           |  Yes   | Yes  |
| Azure Synapse             |  Yes   | Yes  |
| Microsoft SQL Database    |  Yes   | Yes  |
| Blob Storage              |  Yes   | Yes  |



## Supported Activities

- **CopyActivity**



## Environment Variables

The following environment variables are required or optional when running the extractor:

| Variable Name                          | Description                                                                 | Default |
|----------------------------------------|-----------------------------------------------------------------------------|---------|
| `AZURE_CLIENT_ID`                      | Client/Application ID created in Azure for authentication.                  | —       |
| `AZURE_TENANT_ID`                      | Azure Active Directory Tenant ID where the Data Factory/Synapse resides.    | —       |
| `AZURE_CLIENT_SECRET`                  | Secret value associated with the Azure application.                         | —       |
| `SUBSCRIPTION_ID`                      | Azure Subscription ID that contains the Data Factory/Synapse.               | —       |
| `RESOURCE_GROUP_NAME`                  | Name of the Resource Group that the Data Factory/Synapse is deployed in.    | —       |
| `DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME` | Name of Data Factory/Synapse to generate lineage from.                     | —       |
| `IS_AZURE_DATA_FACTORY`                | Whether the target is a Data Factory. Use `false` for Synapse.              | `true`  |
| `DAYS_SEARCH`                          | Number of days of logs to read when generating lineage.                      | `1`     |
| `OPENLINEAGE_NAMESPACE`                | Custom namespace for OpenLineage.                                           | `my-namespace` |
| `OPENLINEAGE_OUTPUT_FILE_PATH`                     | Custom output path for OpenLineage.                                         | `openlineage.json` |
| `OPENLINEAGE_PRODUCER`                 | Custom producer name for OpenLineage.                                       | `azure-lineage` |
| `IS_USE_FQN`                           | Whether to use fully qualified database names for lineage.                  | `true`  |
| `LINEAGE_OUTPUT_FILE_PATH`             | Custom output path for lineage.                                       | `lineage.json` |




## How to Use

You can run the lineage extraction using Docker:

```bash
docker run \
  -e "AZURE_CLIENT_ID=YOUR_AZURE_CLIENT_ID" \
  -e "AZURE_TENANT_ID=YOUR_TENANT_ID" \
  -e "AZURE_CLIENT_SECRET=YOUR_CLIENT_SECRET" \
  -e "SUBSCRIPTION_ID=YOUR_SUBSCRIPTION" \
  -e "RESOURCE_GROUP_NAME=YOUR_RESOURCE_GROUP_NAME" \
  -e "DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME=YOUR_WORKSPACE_NAME" \
  -e "IS_AZURE_DATA_FACTORY=true" \
  -e "DAYS_SEARCH=1" \
  -e "OPENLINEAGE_NAMESPACE=my-namespace" \
  -e "OUTPUT_FILE_NAME=openlineage.json" \
  -e "OPENLINEAGE_PRODUCER=azure-lineage" \
  -e "IS_USE_FQN=true" \
  sunmaungoo/azure-lineage
```