from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'wahyudi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aerohealth_daily_ingestion',
    default_args=default_args,
    description='Pipeline harian untuk mengekstrak data IQAir dan Google Trends',
    schedule_interval='@daily',
    start_date=datetime(2026, 8, 25),
    catchup=False,
    tags=['aerohealth', 'ingestion'],
) as dag:

    task_fetch_iqair = BashOperator(
        task_id='fetch_iqair_pekanbaru',
        bash_command='python /opt/airflow/scripts/ingest_iqair.py',
    )

    task_fetch_trends = BashOperator(
        task_id='fetch_google_trends_riau',
        bash_command='python /opt/airflow/scripts/ingest_trends.py',
    )

    task_load_postgres = BashOperator(
        task_id='load_csv_to_postgres',
        bash_command='python /opt/airflow/scripts/load_to_postgres.py',
    )

    DBT_BASE = (
        'cd /opt/airflow/dbt_aerohealth && dbt run '
        '--project-dir /opt/airflow/dbt_aerohealth '
        '--profiles-dir /opt/airflow/dbt_aerohealth --select {layer}'
    )

    task_dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=DBT_BASE.format(layer='staging'),
    )
    
    task_dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=DBT_BASE.format(layer='intermediate'),
    )

    task_dbt_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=DBT_BASE.format(layer='mart'),
    )


    [task_fetch_iqair, task_fetch_trends] >> task_load_postgres >> task_dbt_staging >> task_dbt_intermediate >> task_dbt_mart