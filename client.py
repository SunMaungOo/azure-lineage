import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from typing import List,Optional
from model import APIDatasetResource,APITriggerResource,APIPipelineResource,APIPipelineRun,APIActivityRun
from datetime import datetime,timedelta
from azure.mgmt.datafactory.models import RunFilterParameters,RunQueryFilter
from datetime import datetime
from azure.synapse.artifacts import ArtifactsClient

class AzureClient:
    """
    Azure client for both azure data factory and azure synapse
    """
    def __init__(self,\
                 azure_client_id:str,\
                 azure_tenant_id:str,\
                 azure_client_secret:str,\
                 subscription_id:str,
                 resource_group_name:str,
                 data_factory_or_workspace:str,\
                 is_data_factory:bool=True):
                
        os.environ["AZURE_CLIENT_ID"] = azure_client_id
        os.environ["AZURE_TENANT_ID"] = azure_tenant_id
        os.environ["AZURE_CLIENT_SECRET"] = azure_client_secret

        self.resource_group_name = resource_group_name
        self.data_factory_or_workspace = data_factory_or_workspace

        if is_data_factory:
            self.client = DataFactoryClient(credential=DefaultAzureCredential(),\
                                        subscription_id=subscription_id,\
                                        resource_group_name=resource_group_name,\
                                        data_factory_name=data_factory_or_workspace)
        else:
            self.client = SynapseClient(credential=DefaultAzureCredential(),\
                                        workspace_name=data_factory_or_workspace)
            
    def get_datasets(self)->Optional[List[APIDatasetResource]]:
        return self.client.get_datasets() 
    
    def get_triggers(self)->Optional[List[APITriggerResource]]:
        return self.client.get_triggers()

    def get_pipelines(self)->Optional[List[APIPipelineResource]]:
        return self.client.get_pipelines()

    def get_pipeline_runs(self,pipeline_name:str,days:int=1)->Optional[List[APIPipelineRun]]:
        return self.client.get_pipeline_runs(pipeline_name=pipeline_name,\
                                             days=days)

    def get_activities_run(self,pipeline_run:APIPipelineRun)->Optional[List[APIActivityRun]]:
        return self.client.get_activities_run(pipeline_run=pipeline_run)
   

class DataFactoryClient:

    def __init__(self,\
                 credential:DefaultAzureCredential,\
                 subscription_id:str,\
                 resource_group_name:str,\
                 data_factory_name:str):
        
        self.client = DataFactoryManagementClient(
                credential=credential,
                subscription_id=subscription_id
        )

        self.resource_group_name = resource_group_name

        self.data_factory_name = data_factory_name

    def get_datasets(self)->Optional[List[APIDatasetResource]]:
        
        try:
            return [
                APIDatasetResource(
                    dataset_name=dataset_resource.name,\
                    linked_service_name=dataset_resource.properties.linked_service_name.reference_name,\
                    azure_data_type=dataset_resource.properties.type,\
                    properties=dataset_resource.properties
                )
                for dataset_resource in self.client.datasets.list_by_factory(resource_group_name=self.resource_group_name,\
                                                                            factory_name=self.data_factory_name)
            ]
        except Exception:
            return None

    def get_triggers(self)->Optional[List[APITriggerResource]]:
        
        triggers:List[APITriggerResource] = list()

        try:
            for trigger_resource in self.client.triggers.list_by_factory(resource_group_name=self.resource_group_name,\
                                                                        factory_name=self.data_factory_name):
                
                trigger_name = trigger_resource.name

                trigger_type = trigger_resource.properties.type

                runtime_state = trigger_resource.properties.runtime_state

                pipeline_names:List[str] = list()

                if hasattr(trigger_resource.properties,"pipelines"):

                    pipeline_names=[
                        pipeline.pipeline_reference.reference_name \
                        for pipeline in trigger_resource.properties.pipelines
                    ]
    
                triggers.append(APITriggerResource(
                        trigger_name=trigger_name,\
                        trigger_type=trigger_type,\
                        runtime_state=runtime_state,\
                        pipeline_names=pipeline_names
                    ))

                    
            return triggers
        
        except Exception:
            return None
        
    def get_pipelines(self)->Optional[List[APIPipelineResource]]:

        pipelines:List[APIPipelineResource] = list()

        try:
            for pipeline_resource in self.client.pipelines.list_by_factory(
                resource_group_name=self.resource_group_name,\
                factory_name=self.data_factory_name
            ):
                pipeline_name = pipeline_resource.name

                activities = list()

                if hasattr(pipeline_resource,"activities"):
                    activities = pipeline_resource.activities

                pipelines.append(
                    APIPipelineResource(
                        name=pipeline_name,\
                        activities=activities
                    )
                )

            return pipelines
        except Exception:
            return None

    def get_pipeline_runs(self,pipeline_name:str,days:int=1)->Optional[List[APIPipelineRun]]:

        if days<1:
            return None

        time_now = datetime.now()

        time_from = time_now - timedelta(days=days)

        filter_params = RunFilterParameters(
            last_updated_after=time_from,
            last_updated_before=time_now,
            filters=[
                RunQueryFilter(
                    operand="PipelineName",
                    operator="Equals",
                    values=[pipeline_name]
                )
            ]
        )


        try:
            pipeline_runs = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group_name,\
                factory_name=self.data_factory_name,\
                filter_parameters=filter_params
            )

            return [APIPipelineRun(
                pipeline_name=x.pipeline_name,\
                run_id=x.run_id,\
                run_start=x.run_start,\
                run_end=x.run_end,\
                is_latest=x.is_latest,
                parameters=x.parameters
            ) for x in pipeline_runs.value]
        
        except Exception:
            return None

    def get_activities_run(self,pipeline_run:APIPipelineRun)->Optional[List[APIActivityRun]]:

        filter_params = RunFilterParameters(
            last_updated_after=pipeline_run.run_start,
            last_updated_before=pipeline_run.run_end
        )

        activities_run:List[APIActivityRun] = list()
        
        try:

            respond = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group_name,\
                factory_name=self.data_factory_name,\
                run_id=pipeline_run.run_id,
                filter_parameters=filter_params
            )

            for activity in respond.value:

                input = None

                if hasattr(activity,"input"):
                    input = activity.input

                activities_run.append(
                    APIActivityRun(
                        activity_name=activity.activity_name,
                        activity_type=activity.activity_type,
                        input=input
                    )
                )

            return activities_run

        except Exception:
            return None


class SynapseClient:

    def __init__(self,\
                 credential:DefaultAzureCredential,\
                 workspace_name:str):
        
        self.client = ArtifactsClient(credential=credential,\
                                      endpoint=f"https://{workspace_name}.dev.azuresynapse.net")
        
    
    def get_datasets(self)->List[APIDatasetResource]:

        dataset:List[APIDatasetResource] = list()

        try:

            for dataset_resource in self.client.dataset.get_datasets_by_workspace():

                # SqlPoolTable does not have the linked servie reference name
                
                linked_service_name = ""

                if dataset_resource.properties.linked_service_name is not None:
                    linked_service_name = dataset_resource.properties.linked_service_name.reference_name


                dataset.append(
                    APIDatasetResource(
                        dataset_name=dataset_resource.name,\
                        linked_service_name=linked_service_name,\
                        azure_data_type=dataset_resource.properties.type,\
                        properties=dataset_resource.properties
                    )
                )
            
            return dataset

        except Exception as e:
            return None
            

    def get_triggers(self)->Optional[List[APITriggerResource]]:

        triggers:List[APITriggerResource] = list()


        try:
            for trigger_resource in self.client.trigger.get_triggers_by_workspace():
                
                trigger_name = trigger_resource.name

                trigger_type = trigger_resource.properties.type

                runtime_state = trigger_resource.properties.runtime_state

                pipeline_names:List[str] = list()

                if hasattr(trigger_resource.properties,"pipelines"):

                    pipeline_names=[
                        pipeline.pipeline_reference.reference_name \
                        for pipeline in trigger_resource.properties.pipelines
                    ]
    
                triggers.append(APITriggerResource(
                        trigger_name=trigger_name,\
                        trigger_type=trigger_type,\
                        runtime_state=runtime_state,\
                        pipeline_names=pipeline_names
                    ))

                    
            return triggers
        
        except Exception:
            return None

    def get_pipelines(self)->Optional[List[APIPipelineResource]]:

        pipelines:List[APIPipelineResource] = list()


        try:
            for pipeline_resource in self.client.pipeline.get_pipelines_by_workspace():
                pipeline_name = pipeline_resource.name

                activities = list()

                if hasattr(pipeline_resource,"activities"):
                    activities = pipeline_resource.activities

                pipelines.append(
                    APIPipelineResource(
                        name=pipeline_name,\
                        activities=activities
                    )
                )

            return pipelines
        except Exception:
            return None

    def get_pipeline_runs(self,pipeline_name:str,days:int=1)->Optional[List[APIPipelineRun]]:

        if days<1:
            return None

        time_now = datetime.now()

        time_from = time_now - timedelta(days=days)

        filter_params = RunFilterParameters(
            last_updated_after=time_from,
            last_updated_before=time_now,
            filters=[
                RunQueryFilter(
                    operand="PipelineName",
                    operator="Equals",
                    values=[pipeline_name]
                )
            ]
        )

        try:
            pipeline_runs = self.client.pipeline_run.query_pipeline_runs_by_workspace(
                filter_parameters=filter_params
            )

            return [APIPipelineRun(
                pipeline_name=x.pipeline_name,\
                run_id=x.run_id,\
                run_start=x.run_start,\
                run_end=x.run_end,\
                is_latest=x.is_latest,
                parameters=x.parameters
            ) for x in pipeline_runs.value]
        
        except Exception:
            return None
 

    def get_activities_run(self,pipeline_run:APIPipelineRun)->Optional[List[APIActivityRun]]:

        filter_params = RunFilterParameters(
            last_updated_after=pipeline_run.run_start,
            last_updated_before=pipeline_run.run_end
        )

        activities_run:List[APIActivityRun] = list()
        
        try:

            respond = self.client.pipeline_run.query_activity_runs(
                pipeline_name=pipeline_run.pipeline_name,\
                run_id=pipeline_run.run_id,\
                filter_parameters=filter_params
            )

            for activity in respond.value:

                input = None

                if hasattr(activity,"input"):
                    input = activity.input

                activities_run.append(
                    APIActivityRun(
                        activity_name=activity.activity_name,
                        activity_type=activity.activity_type,
                        input=input
                    )
                )

            return activities_run

        except Exception:
            return None