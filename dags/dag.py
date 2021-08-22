from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

import sys
sys.path.insert(0,"/root/airflow/dags/subfolder")
from subfolder.max_block_query import get_max
from subfolder.extract import *
from subfolder.load import load_temp_to_perm

token = 'XXXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXX'
table_id = "alien-sol-322920.Solanalysis.sol_blocks"
dataset_id = 'temp'
source_filename = 'temp.csv'
client = bigquery.Client(location="US", project="alien-sol-322920")


def get_max_block(ti):
    start_number = get_max()
    ti.xcom_push(key='start_number', value=start_number)
    
def staging_table(ti):
    start_number=ti.xcom_pull(key='start_number', task_ids='start_number{0}'.format(state))
    temp = StagingTable(token=token, start_number = start_number)
    temp.extract_data()  
    return temp

    
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 1),
    'email': ['rlu413@protonmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '*/2 * * * *',
    'catchup': False
}


with DAG("sol_etl", default_args=default_args) as dag:

    # Big Query get max block number of saved data
    max_block_query = PythonOperator(
        task_id = 'solana_api',
        python_callable = staging_table
    )

    # Solana Beach API to BigQuery staging table
    solana_beach_api = PythonOperator(
        task_id = 'solana_api',
        python_callable = staging_table
    )

    # Staging Table to Main Table
    to_bigquery = PythonOperator(
        task_id = 'to_bigquery',
        python_callable = load_temp_to_perm
        op_jwargs = {'table_id'= table_id, 'dataset_id'=dataset_id, 'source_filename'=source_filename, 'client'=client}
    )
    
    max_block_query >> solana_beach_api >> to_bigquery