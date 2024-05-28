from __future__ import annotations
import os
import socket
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator,
)
from airflow.utils.trigger_rule import TriggerRule
# Variables definition
DAG_ID = "dataproc_dag"
PROJECT_ID = "hsbc-10534429-fdreu-dev"
REGION_ID = "europe-west2"
ZONE_ID = f"{REGION_ID}-b"
ENV_ID = PROJECT_ID.split("-")[3]
PROJECT_SHORT = PROJECT_ID.split("-")[2]
SUBNETWORK_ID = f"projects/{PROJECT_ID}/regions/{REGION_ID}/subnetworks/dataproc-{REGION_ID}"
SA_ID = f"terraform-jenkins-usr@{PROJECT_ID}.iam.gserviceaccount.com"
KMS_KEY_ID = f"projects/hsbc-6320774-kms-{ENV_ID}/locations/{REGION_ID}/keyRings/computeEngine/cryptoKeys/computeEngine"
CLUSTER_NAME = f"dpc-{ENV_ID}-{DAG_ID}".replace("_", "-")
STAGING_BUCKET = f"dataproc-staging-{PROJECT_SHORT}-{ENV_ID}"
TEMP_BUCKET = f"dataproc-temp-{PROJECT_SHORT}-{ENV_ID}"
INIT_FILE_URL = f"gs://{STAGING_BUCKET}/init-scripts/dq-cluster-init.sh"
JOB_FILE_URL = f"gs://{TEMP_BUCKET}/pyspark_sort.py"
PROXY_IP = "192.168.127.197"
CLUSTER_CONFIG = ClusterGenerator(
    project_id = PROJECT_ID,
    storage_bucket = STAGING_BUCKET,
    zone = ZONE_ID,
    subnetwork_uri = SUBNETWORK_ID,
    internal_ip_only = True,
    tags = [
        "dataproc",
    ],
    idle_delete_ttl = 900,
    customer_managed_key = KMS_KEY_ID,
    init_actions_uris = [
        INIT_FILE_URL,
    ],
    init_action_timeout = "5m",
    metadata = {
        "PROXY_IP": PROXY_IP,
        "SHARED_FOLDER": "",
    },
    properties = {
        "core:fs.defaultFS": f"gs://{STAGING_BUCKET}"
    },
    enable_component_gateway = True,
    optional_components = [
        "JUPYTER",
    ],
service_account = SA_ID,
    service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.full_control"
    ],
    image_version = "2.1-rocky8",
    num_masters = 1,
    master_machine_type = "n2-standard-2",
    master_disk_type = "pd-balanced",
    master_disk_size = 32,
    num_workers = 2,
    worker_machine_type = "n2-standard-2",
    worker_disk_type = "pd-balanced",
    worker_disk_size = 32,
).make()
CLUSTER_CONFIG.update({"temp_bucket": TEMP_BUCKET})
# Job definition
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URL},
}
 
with models.DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2023, 9, 1),
    catchup=False,
    tags=["dataproc", "demo"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME,
        region=REGION_ID,
        cluster_config=CLUSTER_CONFIG,
    )


pyspark_task = DataprocSubmitJobOperator(

        task_id="pyspark_task",

        job=PYSPARK_JOB,

        region=REGION_ID,

        project_id=PROJECT_ID,

    )

    delete_cluster = DataprocDeleteClusterOperator(

        task_id="delete_cluster",

        project_id=PROJECT_ID,

        cluster_name=CLUSTER_NAME,

        region=REGION_ID,

        trigger_rule=TriggerRule.ALL_DONE,

    )

    (

        create_cluster

        >> pyspark_task

        >> delete_cluster

    )
