from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json

airbyte_connection_id = Variable.get("AIRBYTE_CONNECTION_ID")
dbt_acc_id = Variable.get("DBT_ACCOUNT_ID")
dbt_job_id = Variable.get("DBT_JOB_ID")
dbt_api_token = Variable.get("DBT_API_TOKEN")

dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token {}'.format(dbt_api_token)
}
def getDbtMessage(message):
  return {'cause': message}
def getDbtApiLink(jobId, accountId):
  return 'api/v2/accounts/{0}/jobs/{1}/run/'.format(accountId, jobId)

def getDbtApiOperator(task_id, jobId, message='Triggered by Airflow', accountId=dbt_acc_id):
  return SimpleHttpOperator(
    task_id=task_id,
    method='POST',
    data=json.dumps(getDbtMessage(message)),
    http_conn_id='dbt_api',
    endpoint=getDbtApiLink(jobId, accountId),
    headers=dbt_header
  )

with DAG(dag_id='trigger_airbyte_dbt_job',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
         ) as dag:

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte_example',
        connection_id=airbyte_connection_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_run = getDbtApiOperator('load_cases', dbt_job_id)

    airbyte_sync >> dbt_run
