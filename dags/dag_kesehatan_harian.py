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

    # ── Extract ──────────────────────────────────────
    task_fetch_iqair = BashOperator(
        task_id='fetch_iqair_pekanbaru',
        bash_command='python /opt/airflow/scripts/ingest_iqair.py',
    )

    task_fetch_trends = BashOperator(
        task_id='fetch_google_trends_riau',
        bash_command='python /opt/airflow/scripts/ingest_trends.py',
    )

    # ── Load ─────────────────────────────────────────
    task_load_postgres = BashOperator(
        task_id='load_csv_to_postgres',
        bash_command='python /opt/airflow/scripts/load_to_postgres.py',
    )

    DBT_BASE = (
        'cd /opt/airflow/dbt_aerohealth && dbt run '
        '--project-dir /opt/airflow/dbt_aerohealth '
        '--profiles-dir /opt/airflow/dbt_aerohealth --select {model}'
    )

    # ── Transform: Staging (Silver) — per data source ─
    task_stg_iqair = BashOperator(
        task_id='dbt_run_stg_iqair',
        bash_command=DBT_BASE.format(model='stg_iqair'),
    )

    task_stg_trends = BashOperator(
        task_id='dbt_run_stg_google_trends',
        bash_command=DBT_BASE.format(model='stg_google_trends'),
    )

    # ── Transform: Intermediate (Silver) ──────────────
    task_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=DBT_BASE.format(model='int_health_correlation'),
    )

    # ── Transform: Dimensional (Silver) — per table ───
    task_dim_tanggal = BashOperator(
        task_id='dbt_run_dim_tanggal',
        bash_command=DBT_BASE.format(model='dim_tanggal'),
    )

    task_dim_lokasi = BashOperator(
        task_id='dbt_run_dim_lokasi',
        bash_command=DBT_BASE.format(model='dim_lokasi'),
    )

    task_fact = BashOperator(
        task_id='dbt_run_fact',
        bash_command=DBT_BASE.format(model='fact_health_correlation'),
    )

    # ── Transform: Mart (Gold) — siap dashboard ───────
    task_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=DBT_BASE.format(model='gold_health_correlation'),
    )

    # ── Dependency chain ───────────────────────────────
    [task_fetch_iqair, task_fetch_trends] >> task_load_postgres
    task_load_postgres >> [task_stg_iqair, task_stg_trends]
    [task_stg_iqair, task_stg_trends] >> task_intermediate
    task_intermediate >> [task_dim_tanggal, task_dim_lokasi]
    [task_dim_tanggal, task_dim_lokasi] >> task_fact
    task_fact >> task_gold