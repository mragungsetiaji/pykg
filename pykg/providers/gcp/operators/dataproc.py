from google.cloud import dataproc_v1 as dataproc
from loguru import logger

class DataprocOperator(object):

    def __init__(self, 
        project_id, region, cluster_name, service_account, config_bucket,
        temp_bucket=None
    ):
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.service_account = service_account
        self.config_bucket = config_bucket
        self.temp_bucket = None

        # pytest variable
        self.is_cluster_created = False
        self.is_job_sumitted = False
        self.is_cluster_deleted = False

    def create_cluster(self):
        cluster_client = dataproc.ClusterControllerClient.from_service_account_file(
            filename=self.service_account,
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )

        # Create the cluster config.
        cluster = {
            "project_id": self.project_id,
            "cluster_name": self.cluster_name,
            "config": {
                "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-1"},
                "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-1"},
                "config_bucket": self.config_bucket,
                "temp_bucket": self.temp_bucket
            },
        }

        logger.info("Creating Dataproc Cluster.")
        operation = cluster_client.create_cluster(
            request={
                "project_id": self.project_id, 
                "region": self.region, 
                "cluster": cluster}
        )
        response = operation.result()
        logger.info("Dataproc Cluster created: {}".format(response.cluster_name))
        self.is_cluster_created = True
        return response

    def submit_job(self, job_file_path):
        job_client = dataproc.JobControllerClient.from_service_account_file(
            filename=self.service_account,
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )

        # https://cloud.google.com/dataproc/docs/reference/rest/v1/PySparkJob
        job = {
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {"main_python_file_uri": job_file_path},
        }

        logger.info("Submit Job: {}".format(job_file_path))
        operation = job_client.submit_job_as_operation(
            request={"project_id": self.project_id, "region": self.region, "job": job}
        )
        response = operation.result()
        logger.info("Job finished successfully.")
        self.is_job_sumitted = True
        return response

    def delete_cluster(self):
        logger.info("Starting to delete cluster: {}.".format(self.cluster_name))
        cluster_client = dataproc.ClusterControllerClient.from_service_account_file(
            filename=self.service_account,
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        
        operation = cluster_client.delete_cluster(
            request={
                "project_id": self.project_id,
                "region": self.region,
                "cluster_name": self.cluster_name,}
        )
        response = operation.result()
        logger.info("Cluster {} successfully deleted.".format(self.cluster_name))
        self.is_cluster_deleted = True
        return response