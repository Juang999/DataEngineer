from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spreadsheet_dimension",
    start_date=datetime(2025, 10, 20),
    schedule_interval="@daily",
    catchup=False
) as dag:
    etl_dim_product_spreadsheet = BashOperator(
        task_id="etl_dim_product_spreadsheet",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_product_spreadsheet.py"
    )

    etl_dim_product_spreadsheet