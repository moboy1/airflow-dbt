from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.hooks.mysql_hook import MySqlHook
import pendulum
import os
import pandas as pd
from pathlib import Path
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup

AIRBYTE_CONNECTION_ID_JOURNAL_ENTRIES = '4bacce8c-de5e-4509-9149-c58dadb5f74e'
AIRBYTE_CONNECTION_ID_DEPOSIT_TRANSACTIONS = '2fe027a7-c83b-4b09-ad8d-b2d109b78808'

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROFILE_YAML_PATH = Path(__file__).parent / "dbt/rtgs/profiles.yml"

AIRFLOW_MYSQL_CONNECTION_ID = 'airflow-mysql'
MYSQL_DATABASE = 'staging'
MYSQL_QUERY = 'SELECT * FROM stg_rtgs_journal_entries_clean'


# Define a Python function to save the data as a local file
def save_data_to_local_file(*args, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id=AIRFLOW_MYSQL_CONNECTION_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(MYSQL_QUERY)
    data = cursor.fetchall()
    conn.close()

    df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    df.to_csv('/opt/airflow/output/stg_rtgs_journals_entries.txt', sep='|', index=False)


with DAG(dag_id='airbyte_rtgs_data_processing_airflow_dag',
         default_args={'owner': 'airflow'},
         schedule='@daily',
         start_date=pendulum.today('UTC').add(days=-1)
         ) as dag:

    trigger_airbyte_sync_journal_entries = AirbyteTriggerSyncOperator(
        task_id='airbyte_trigger_sync_rtgs_journal_entries',
        airbyte_conn_id='airflow-call-to-airbyte',
        connection_id=AIRBYTE_CONNECTION_ID_JOURNAL_ENTRIES,
        asynchronous=True
    )

    trigger_airbyte_sync_deposit_transactions = AirbyteTriggerSyncOperator(
        task_id='airbyte_trigger_sync_rtgs_deposit_transactions',
        airbyte_conn_id='airflow-call-to-airbyte',
        connection_id=AIRBYTE_CONNECTION_ID_DEPOSIT_TRANSACTIONS,
        asynchronous=True
    )

    wait_for_sync_completion_journal_entries = AirbyteJobSensor(
        task_id='airbyte_check_sync_rtgs_journal_entries',
        airbyte_conn_id='airflow-call-to-airbyte',
        airbyte_job_id=trigger_airbyte_sync_journal_entries.output
    )

    wait_for_sync_completion_deposit_transactions = AirbyteJobSensor(
        task_id='airbyte_check_rtgs_deposit_transactions',
        airbyte_conn_id='airflow-call-to-airbyte',
        airbyte_job_id=trigger_airbyte_sync_deposit_transactions.output
    )

    rtgs_data_transformation = DbtTaskGroup(
        group_id="rtgs_data_transformation",
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "rtgs",
        ),
        profile_config=ProfileConfig(
            profile_name="staging",
            target_name="dev",
            profiles_yml_filepath=PROFILE_YAML_PATH,
        ),
    )

    extract_data_from_mysql = MySqlOperator(
        task_id='extract_rtgs_data_from_mysql',
        mysql_conn_id=AIRFLOW_MYSQL_CONNECTION_ID,
        sql=MYSQL_QUERY,
        database=MYSQL_DATABASE,
    )

    save_data_to_local_file = PythonOperator(
        task_id='save_rtgs_data_to_local_file',
        python_callable=save_data_to_local_file,
        provide_context=True,
    )

trigger_airbyte_sync_journal_entries >> wait_for_sync_completion_journal_entries
trigger_airbyte_sync_deposit_transactions >> wait_for_sync_completion_deposit_transactions
wait_for_sync_completion_journal_entries >> rtgs_data_transformation
wait_for_sync_completion_deposit_transactions >> rtgs_data_transformation
rtgs_data_transformation >> extract_data_from_mysql
extract_data_from_mysql >> save_data_to_local_file