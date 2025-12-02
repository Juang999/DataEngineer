import os
from pathlib import Path
from dotenv import load_dotenv

class Environment():
    path_environment = Path(__file__).parent.joinpath(".env")
    load_dotenv(dotenv_path=path_environment)

    def __init__(self):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        path_credentials = str(Path(__file__).parent.joinpath("credentials.json"))

        self.source_exapro = {
            "dialect": os.getenv("DB_SOURCE_EXAPRO_DIALECT"),
            "username": os.getenv("DB_SOURCE_EXAPRO_USERNAME"),
            "password": os.getenv("DB_SOURCE_EXAPRO_PASSWORD"),
            "host": os.getenv("DB_SOURCE_EXAPRO_HOST"),
            "port": os.getenv("DB_SOURCE_EXAPRO_PORT"),
            "database": os.getenv("DB_SOURCE_EXAPRO_DATABASE"),
        }

        self.env_dev_destination_warehouse = {
            "dialect": os.getenv("DB_DEV_WAREHOUSE_DIALECT"),
            "username": os.getenv("DB_DEV_WAREHOUSE_USERNAME"),
            "password": os.getenv("DB_DEV_WAREHOUSE_PASSWORD"),
            "host": os.getenv("DB_DEV_WAREHOUSE_HOST"),
            "port": os.getenv("DB_DEV_WAREHOUSE_PORT"),
            "database": os.getenv("DB_DEV_WAREHOUSE_DATABASE"),
        }

        self.env_prod_destination_warehouse = {
            "dialect": os.getenv("DB_PROD_WAREHOUSE_DIALECT"),
            "username": os.getenv("DB_PROD_WAREHOUSE_USERNAME"),
            "password": os.getenv("DB_PROD_WAREHOUSE_PASSWORD"),
            "host": os.getenv("DB_PROD_WAREHOUSE_HOST"),
            "port": os.getenv("DB_PROD_WAREHOUSE_PORT"),
            "database": os.getenv("DB_PROD_WAREHOUSE_DATABASE"),
        }

        self.env_spreadsheet = {
            "scope": scopes,
            "credentials": path_credentials,
            "worksheets": {
                # worksheets Dim Database Dashboard
                "dashboard_dim_customer": os.getenv("DASHBOARD_WS_DIM_CUSTOMER"),
                "dashboard_dim_warehouse": os.getenv("DASHBOARD_WS_DIM_WAREHOUSE"),
                "dashboard_dim_product": os.getenv("DASHBOARD_WS_DIM_PRODUCT"),
                # worksheets Data Dimension
                "dimension_dim_partner": os.getenv("DIMENSION_WS_DIM_PARTNER"),
                "dimension_dim_warehouse": os.getenv("DIMENSION_WS_DIM_WAREHOUSE"),
                "dimension_dim_product": os.getenv("DIMENSION_WS_DIM_PRODUCT")
            },
            "spreadsheets_id": {
                "data_dimension": os.getenv("DIMENSION_SHEET_ID"),
                "dim_database_dashboard": os.getenv("DASHBOARD_SHEET_ID")
            }
        }