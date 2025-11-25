from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="inventory_receipt_report",
    start_date=datetime(2025, 11, 25),
    schedule_interval="0 6,18 * * *",
    catchup=False
) as dag:
    etl_dim_entities = BashOperator(
        task_id="etl_dim_entities",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_entities.py"
    )

    etl_dim_products = BashOperator(
        task_id="etl_dim_products",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session//dim/dim_products.py"
    )

    etl_dim_locations = BashOperator(
        task_id="etl_dim_locations",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_locations.py"
    )

    etl_fact_inventory_receipt_report = BashOperator(
        task_id="etl_fact_inventory_historical",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/facts/fact_inventory_receipt_report.py"
    )

    etl_dim_entities >> [etl_dim_products, etl_dim_locations] >> etl_fact_inventory_receipt_report