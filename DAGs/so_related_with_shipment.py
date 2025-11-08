from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="so_related_with_shipment",
    start_date=datetime(2025, 10, 17),
    schedule="*/10 * * * *",
    catchup=False
) as dag:
    etl_dim_sites = BashOperator(
        task_id="etl_dim_sites",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_sites.py"
    )

    etl_dim_entities = BashOperator(
        task_id="etl_dim_entities",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_entities.py"
    )

    etl_dim_groups = BashOperator(
        task_id="etl_dim_groups",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_groups.py"
    )

    etl_dim_currency = BashOperator(
        task_id="etl_dim_currency",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_currency.py"
    )

    etl_dim_pricelist = BashOperator(
        task_id=r"etl_dim_pricelist",
        bash_command=r"/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_pricelist_name.py"
    )

    etl_dim_customers = BashOperator(
        task_id="etl_dim_customers",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_customers.py"
    )

    etl_dim_products = BashOperator(
        task_id="etl_dim_products",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_products.py"
    )

    etl_dim_locations = BashOperator(
        task_id="etl_dim_locations",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/dim/dim_locations.py"
    )

    etl_fact_sales_shipment = BashOperator(
        task_id="etl_fact_sales_shipment",
        bash_command="/home/arthemist666/miniconda3/envs/sales-pipeline/bin/python /mnt/c/Users/User/Project/DataEngineer/increment-session/facts/fact_sales_shipment.py"
    )

    etl_dim_sites >> etl_dim_entities >> [etl_dim_groups, etl_dim_currency, etl_dim_pricelist, etl_dim_products] >> etl_dim_customers >> etl_dim_locations >> etl_fact_sales_shipment