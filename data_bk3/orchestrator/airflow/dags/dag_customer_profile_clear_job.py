from airflow import DAG
# from airflow.providers.google.cloud.operators.dataflow import DataflowJobCancelOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from googleapiclient.discovery import build

# with DAG(
#     dag_id="cancel_dataflow_job",
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     cancel_job = DataflowJobCancelOperator(
#         task_id="cancel_dataflow",
#         # PROJECT_ID = Variable.get("project_id")

#         project_id=Variable.get("project_id"),
#         location="asia-southeast1",
#         job_id=Variable.get("kill_dataflow_job_id"),
#     )

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

with DAG(
    dag_id="cancel_dataflow_cli",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    params = {
        "job_id": Param(
            default = "",
            type = "string",
            title = "job_id",
            ),
        }
) as dag:

    cancel_job = BashOperator(
        task_id="cancel_dataflow",
        bash_command="""
        gcloud dataflow jobs cancel {{ params.job_id }} \
          --project={{ var.value.project_id }} \
          --region=asia-southeast1
        """
    )
