from airflow import DAG
from airflow.providers.standard.operators.bash.BashOperator import BashOperator 
from datetime import datetime, timedelta

default_args = {
    'owner': 'engenheiro_dados',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_fraud_pipeline',
    default_args=default_args,
    description='Pipeline de ETL para Detecção de Fraudes (Bronze, Silver, Gold)',
    schedule='@daily',
    catchup=False,
    tags=['bdd2', 'etl', 'fraude'],
    
) as dag:
    
    task_bronze = BashOperator(
        task_id='load_bronze_layer',
        bash_command='python /home/aluno/airflow/dags/etl_bronze.py',
    )

    task_silver = BashOperator(
        task_id='transform_silver_layer',
        bash_command='python /home/aluno/airflow/dags/etl_silver.py',
    )

    task_gold = BashOperator(
        task_id='curate_gold_layer',
        bash_command='python /home/aluno/airflow/dags/etl_gold.py',
    )

    task_bronze >> task_silver >> task_gold
