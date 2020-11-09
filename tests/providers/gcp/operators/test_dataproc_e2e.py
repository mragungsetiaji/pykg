import os
import uuid
import pytest

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
from pykg.providers.gcp.operators.dataproc import DataprocOperator

GOOGLE_APPLICATION_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
PROJECT_ID = os.environ["PROJECT_ID"]
REGION = "us-central1"
CLUSTER_NAME = "py-qs-test-{}".format(str(uuid.uuid4()))
STAGING_BUCKET = "py-dataproc-qs-bucket-{}".format(str(uuid.uuid4()))
JOB_FILE_NAME = "sum.py"
JOB_FILE_PATH = "gs://{}/{}".format(STAGING_BUCKET, JOB_FILE_NAME)
SORT_CODE = (
    "import pyspark\n"
    "sc = pyspark.SparkContext()\n"
    "rdd = sc.parallelize((1,2,3,4,5))\n"
    "sum = rdd.reduce(lambda x,y : x + y)\n"
)

@pytest.fixture(autouse=True)
def setup_teardown():
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(STAGING_BUCKET)
    blob = bucket.blob(JOB_FILE_NAME)
    blob.upload_from_string(SORT_CODE)

    yield

    #blob.delete()
    #bucket.delete()

def test_dataproc_e2e(capsys):
    out, _ = capsys.readouterr()

    client = DataprocOperator(
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        service_account=GOOGLE_APPLICATION_CREDENTIALS,
        config_bucket=STAGING_BUCKET
    )

    client.create_cluster()
    client.submit_job(JOB_FILE_PATH)
    client.delete_cluster()

    assert client.is_cluster_created == True
    assert client.is_job_sumitted == True
    assert client.is_cluster_deleted == True

    

